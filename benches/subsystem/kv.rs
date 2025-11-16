//! Subsystem benchmarks for KV domain operations
//!
//! These benchmarks test full KV domain operations end-to-end,
//! including handler processing, storage interactions, and domain logic.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::DomainContext;
use fitz::core::kv::KvDomain;
use fitz::protocol::frame::{build_tlv, PooledFrame};
use fitz::protocol::tags::*;
use fitz::routing::RouteFamilyId;
use fitz::protocol::route::parse_route;
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

static KV_DOMAIN: OnceLock<Arc<KvDomain>> = OnceLock::new();
fn kv_domain() -> Arc<KvDomain> {
    KV_DOMAIN
        .get_or_init(|| {
            rt().block_on(async {
                let store = fitz::storage::midge_adapter::create_memory_store()
                    .expect("create memory store");
                Arc::new(KvDomain::new(store))
            })
        })
        .clone()
}

// ---------------------------------------------------------
// Helper functions
// ---------------------------------------------------------

fn create_kv_frame(
    _operation: &str,
    _realm: &str,
    _area: &str,
    _resource: &str,
    key: &str,
    value: Option<&[u8]>,
) -> PooledFrame {
    let mut payload = Vec::new();
    build_tlv(TAG_ID, key.as_bytes(), &mut payload);
    if let Some(val) = value {
        build_tlv(TAG_BODY, val, &mut payload);
    }
    PooledFrame::from_vec(payload)
}

fn create_kv_batch_frame(_realm: &str, _area: &str, operations: &[(&str, &str, Option<&[u8]>)]) -> PooledFrame {
    let mut payload = Vec::new();

    // Body format expected by KvService::handle_batch: newline-separated
    // "PUT key value" and "DELETE key" operations
    let mut batch_body = Vec::new();
    for (op, key, value) in operations {
        match *op {
            "put" | "PUT" => {
                batch_body.extend_from_slice(b"PUT ");
                batch_body.extend_from_slice(key.as_bytes());
                batch_body.push(b' ');
                if let Some(val) = value {
                    batch_body.extend_from_slice(val);
                }
            }
            "delete" | "DELETE" => {
                batch_body.extend_from_slice(b"DELETE ");
                batch_body.extend_from_slice(key.as_bytes());
            }
            _ => continue,
        }
        batch_body.push(b'\n');
    }

    build_tlv(TAG_BODY, &batch_body, &mut payload);
    PooledFrame::from_vec(payload)
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_kv_put_small(c: &mut Criterion) {
    let domain = kv_domain();
    let frame = create_kv_frame("put", "test", "config", "app", "setting1", Some(b"small value"));

    c.bench_function("kv_put_small", |b| {
        b.iter(|| {
            let route_str = "kv://test/config/app/put".to_string();
            let route = parse_route(&route_str).expect("parse route");
            let ctx = DomainContext {
                route,
                route_str,
                payload: frame.payload(),
                channel_id: 1,
                route_family: RouteFamilyId::new(),
                sender: None,
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_kv_put_large(c: &mut Criterion) {
    let large_value = vec![b'x'; 64 * 1024]; // 64KB
    let domain = kv_domain();
    let frame = create_kv_frame("put", "test", "config", "app", "large_setting", Some(&large_value));

    c.bench_function("kv_put_large", |b| {
        b.iter(|| {
            let route_str = "kv://test/config/app/put".to_string();
            let route = parse_route(&route_str).expect("parse route");
            let ctx = DomainContext {
                route,
                route_str,
                payload: frame.payload(),
                channel_id: 1,
                route_family: RouteFamilyId::new(),
                sender: None,
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_kv_get_hit(c: &mut Criterion) {
    let domain = kv_domain();

    // Setup: put a value first
    let put_frame = create_kv_frame("put", "test", "config", "app", "get_test", Some(b"test value"));
    let put_ctx = DomainContext {
        route_family: RouteFamilyId::new(),
        route_str: "kv://test/config/app/put".to_string(),
        payload: put_frame.payload(),
    };
    rt().block_on(async {
        let _ = domain.handle(put_ctx).await;
    });

    let get_frame = create_kv_frame("get", "test", "config", "app", "get_test", None);

    c.bench_function("kv_get_hit", |b| {
        b.iter(|| {
            let route_str = "kv://test/config/app/get".to_string();
            let route = parse_route(&route_str).expect("parse route");
            let ctx = DomainContext {
                route,
                route_str,
                payload: get_frame.payload(),
                channel_id: 1,
                route_family: RouteFamilyId::new(),
                sender: None,
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_kv_get_miss(c: &mut Criterion) {
    let domain = kv_domain();
    let frame = create_kv_frame("get", "test", "config", "app", "missing_key", None);

    c.bench_function("kv_get_miss", |b| {
        b.iter(|| {
            let route_str = "kv://test/config/app/get".to_string();
            let route = parse_route(&route_str).expect("parse route");
            let ctx = DomainContext {
                route,
                route_str,
                payload: frame.payload(),
                channel_id: 1,
                route_family: RouteFamilyId::new(),
                sender: None,
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_kv_delete(c: &mut Criterion) {
    let domain = kv_domain();

    c.bench_function("kv_delete", |b| {
        b.iter_batched(
            || {
                // Setup: put a value
                let put_frame = create_kv_frame("put", "test", "config", "app", "delete_test", Some(b"value"));
                let route_str = "kv://test/config/app/put".to_string();
                let route = parse_route(&route_str).expect("parse route");
                let put_ctx = DomainContext {
                    route,
                    route_str,
                    payload: put_frame.payload(),
                    channel_id: 1,
                    route_family: RouteFamilyId::new(),
                    sender: None,
                };
                rt().block_on(async {
                    let _ = domain.handle(put_ctx).await;
                });
            },
            |_| {
                let delete_frame =
                    create_kv_frame("delete", "test", "config", "app", "delete_test", None);
                let route_str = "kv://test/config/app/delete".to_string();
                let route = parse_route(&route_str).expect("parse route");
                let ctx = DomainContext {
                    route,
                    route_str,
                    payload: delete_frame.payload(),
                    channel_id: 1,
                    route_family: RouteFamilyId::new(),
                    sender: None,
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

fn bench_kv_scan_small(c: &mut Criterion) {
    let domain = kv_domain();

    // Setup: put some values with similar keys
    for i in 0..10 {
        let put_frame = create_kv_frame("put", "test", "config", "app", &format!("scan_key_{:02}", i), Some(b"value"));
        let route_str = "kv://test/config/app/put".to_string();
        let route = parse_route(&route_str).expect("parse route");
        let put_ctx = DomainContext {
            route,
            route_str,
            payload: put_frame.payload(),
            channel_id: 1,
            route_family: RouteFamilyId::new(),
            sender: None,
        };
        rt().block_on(async {
            let _ = domain.handle(put_ctx).await;
        });
    }

    c.bench_function("kv_scan_small", |b| {
        b.iter(|| {
            let route = "kv://test/config/*/scan";
            let mut payload = Vec::new();
            build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
            build_tlv(TAG_BODY, b"scan_key_00\nscan_key_99", &mut payload);
            let frame = PooledFrame::from_vec(payload);

            let route_str = route.to_string();
            let parsed = parse_route(&route_str).expect("parse route");
            let ctx = DomainContext {
                route: parsed,
                route_str,
                payload: frame.payload(),
                channel_id: 1,
                route_family: RouteFamilyId::new(),
                sender: None,
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_kv_batch_operations(c: &mut Criterion) {
    let domain = kv_domain();

    c.bench_function("kv_batch_operations", |b| {
        b.iter_batched(
            || {
                vec![
                    ("put", "batch_key_1", Some(b"value1")),
                    ("put", "batch_key_2", Some(b"value2")),
                    ("get", "batch_key_1", None),
                    ("delete", "batch_key_2", None),
                ]
            },
            |operations| {
                let frame = create_kv_batch_frame("test", "config", &operations);
                let route_str = "kv://test/config/*/batch".to_string();
                let route = parse_route(&route_str).expect("parse route");
                let ctx = DomainContext {
                    route,
                    route_str,
                    payload: frame.payload(),
                    channel_id: 1,
                    route_family: RouteFamilyId::new(),
                    sender: None,
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

fn bench_kv_multi_tenant_isolation(c: &mut Criterion) {
    let domain = kv_domain();

    c.bench_function("kv_multi_tenant_isolation", |b| {
        b.iter(|| {
            rt().block_on(async {
                // Operations on different tenants
                let frame1 = create_kv_frame("put", "tenant1", "config", "app", "key1", Some(b"tenant1_value"));
                let ctx1 = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "kv://tenant1/config/app/put".to_string(),
                    payload: frame1.payload(),
                };

                let frame2 = create_kv_frame("put", "tenant2", "config", "app", "key1", Some(b"tenant2_value"));
                let ctx2 = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "kv://tenant2/config/app/put".to_string(),
                    payload: frame2.payload(),
                };

                let (result1, result2) = tokio::join!(
                    domain.handle(ctx1),
                    domain.handle(ctx2)
                );

                criterion::black_box((result1, result2));
            });
        })
    });
}

criterion_group!(
    name = kv_subsystem;
    config = config::criterion_config();
    targets =
        bench_kv_put_small,
        bench_kv_put_large,
        bench_kv_get_hit,
        bench_kv_get_miss,
        bench_kv_delete,
        bench_kv_scan_small,
        bench_kv_batch_operations,
        bench_kv_multi_tenant_isolation
);

criterion_main!(kv_subsystem);