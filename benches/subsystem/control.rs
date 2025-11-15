//! Subsystem benchmarks for control domain operations
//!
//! These benchmarks test full control domain operations end-to-end,
//! including handler processing, system management, and domain logic.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::{Domain, DomainContext, DomainResponse};
use fitz::core::control::{ControlDomain, ControlService};
use fitz::protocol::frame::{build_tlv, PooledFrame};
use fitz::protocol::tags::*;
use fitz::routing::RouteFamilyId;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// ---------------------------------------------------------
// Shared runtime and services
// ---------------------------------------------------------
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

static CONTROL_DOMAIN: OnceLock<Arc<ControlDomain>> = OnceLock::new();
fn control_domain() -> Arc<ControlDomain> {
    CONTROL_DOMAIN.get_or_init(|| {
        rt().block_on(async {
            Arc::new(ControlDomain::new().await)
        })
    })
}

// ---------------------------------------------------------
// Helper functions
// ---------------------------------------------------------

fn create_control_frame(operation: &str, data: Option<&[u8]>) -> PooledFrame {
    let route = format!("control://system/{}", operation);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    if let Some(d) = data {
        build_tlv(TAG_BODY, d, &mut payload);
    }
    PooledFrame::from_vec(payload)
}

fn create_control_config_frame(operation: &str, config_key: &str, config_value: Option<&[u8]>) -> PooledFrame {
    let route = format!("control://config/{}", operation);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_CONFIG_KEY, config_key.as_bytes(), &mut payload);
    if let Some(val) = config_value {
        build_tlv(TAG_CONFIG_VALUE, val, &mut payload);
    }
    PooledFrame::from_vec(payload)
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_control_heartbeat(c: &mut Criterion) {
    let domain = control_domain();
    let frame = create_control_frame("heartbeat", None);

    c.bench_function("control_heartbeat", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "control://system/heartbeat".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_control_metrics(c: &mut Criterion) {
    let domain = control_domain();
    let frame = create_control_frame("metrics", None);

    c.bench_function("control_metrics", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "control://system/metrics".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_control_health_check(c: &mut Criterion) {
    let domain = control_domain();
    let frame = create_control_frame("health", None);

    c.bench_function("control_health_check", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "control://system/health".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_control_config_get(c: &mut Criterion) {
    let domain = control_domain();
    let frame = create_control_config_frame("get", "server.port", None);

    c.bench_function("control_config_get", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "control://config/get".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_control_config_set(c: &mut Criterion) {
    let domain = control_domain();

    c.bench_function("control_config_set", |b| {
        b.iter_batched(
            || format!("dynamic_config_{}", fastrand::u64(0..1000)),
            |config_key| {
                let config_value = b"8080";
                let frame = create_control_config_frame("set", &config_key, Some(config_value));
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "control://config/set".to_string(),
                    payload: frame.payload(),
                };

                rt().block_on(async {
                    let result = domain.handle(ctx).await;
                    criterion::black_box(result);
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_control_config_list(c: &mut Criterion) {
    let domain = control_domain();
    let frame = create_control_frame("config_list", None);

    c.bench_function("control_config_list", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "control://system/config_list".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_control_shutdown(c: &mut Criterion) {
    let domain = control_domain();

    c.bench_function("control_shutdown", |b| {
        b.iter_batched(
            || {
                // Create a fresh domain for each shutdown test
                rt().block_on(async {
                    Arc::new(ControlDomain::new().await)
                })
            },
            |test_domain| {
                let frame = create_control_frame("shutdown", Some(b"graceful"));
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "control://system/shutdown".to_string(),
                    payload: frame.payload(),
                };

                rt().block_on(async {
                    let result = test_domain.handle(ctx).await;
                    criterion::black_box(result);
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_control_restart(c: &mut Criterion) {
    let domain = control_domain();

    c.bench_function("control_restart", |b| {
        b.iter_batched(
            || {
                // Create a fresh domain for each restart test
                rt().block_on(async {
                    Arc::new(ControlDomain::new().await)
                })
            },
            |test_domain| {
                let frame = create_control_frame("restart", Some(b"immediate"));
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "control://system/restart".to_string(),
                    payload: frame.payload(),
                };

                rt().block_on(async {
                    let result = test_domain.handle(ctx).await;
                    criterion::black_box(result);
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_control_log_level_set(c: &mut Criterion) {
    let domain = control_domain();

    c.bench_function("control_log_level_set", |b| {
        b.iter(|| {
            let log_config = r#"{"level": "DEBUG", "target": "fitz::core"}"#;
            let frame = create_control_frame("set_log_level", Some(log_config.as_bytes()));
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "control://system/set_log_level".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_control_feature_flags(c: &mut Criterion) {
    let domain = control_domain();

    c.bench_function("control_feature_flags", |b| {
        b.iter(|| {
            let route = "control://system/feature_flags";
            let mut payload = Vec::new();
            build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
            build_tlv(TAG_FEATURE_NAME, b"experimental_feature", &mut payload);
            let frame = PooledFrame::from_vec(payload);

            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: route.to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_control_performance_stats(c: &mut Criterion) {
    let domain = control_domain();

    c.bench_function("control_performance_stats", |b| {
        b.iter(|| {
            let route = "control://system/performance_stats";
            let mut payload = Vec::new();
            build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
            build_tlv(TAG_TIME_RANGE, b"last_5_minutes", &mut payload);
            let frame = PooledFrame::from_vec(payload);

            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: route.to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_control_concurrent_operations(c: &mut Criterion) {
    let domain = control_domain();

    c.bench_function("control_concurrent_operations", |b| {
        b.iter(|| {
            rt().block_on(async {
                let mut handles = Vec::new();

                // Mix of different control operations
                let operations = vec![
                    ("heartbeat", None),
                    ("metrics", None),
                    ("health", None),
                    ("config_list", None),
                ];

                for (op, data) in operations {
                    let frame = create_control_frame(op, data);
                    let ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: format!("control://system/{}", op),
                        payload: frame.payload(),
                    };

                    let domain_clone = Arc::clone(&domain);
                    handles.push(tokio::spawn(async move {
                        domain_clone.handle(ctx).await
                    }));
                }

                for handle in handles {
                    let result = handle.await.unwrap();
                    criterion::black_box(result);
                }
            });
        })
    });
}

fn bench_control_bulk_config_update(c: &mut Criterion) {
    let domain = control_domain();

    c.bench_function("control_bulk_config_update", |b| {
        b.iter_batched(
            || {
                // Create bulk config update payload
                let config_updates = r#"{
                    "server.port": "9090",
                    "server.host": "0.0.0.0",
                    "logging.level": "INFO",
                    "cache.enabled": "true"
                }"#;
                create_control_frame("bulk_config_update", Some(config_updates.as_bytes()))
            },
            |frame| {
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "control://system/bulk_config_update".to_string(),
                    payload: frame.payload(),
                };

                rt().block_on(async {
                    let result = domain.handle(ctx).await;
                    criterion::black_box(result);
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    name = control_subsystem;
    config = config::criterion_config();
    targets =
        bench_control_heartbeat,
        bench_control_metrics,
        bench_control_health_check,
        bench_control_config_get,
        bench_control_config_set,
        bench_control_config_list,
        bench_control_shutdown,
        bench_control_restart,
        bench_control_log_level_set,
        bench_control_feature_flags,
        bench_control_performance_stats,
        bench_control_concurrent_operations,
        bench_control_bulk_config_update
);

criterion_main!(control_subsystem);