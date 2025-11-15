//! Subsystem benchmarks for lease domain operations
//!
//! These benchmarks test full lease domain operations end-to-end,
//! including handler processing, coordination logic, and domain operations.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::{Domain, DomainContext, DomainResponse};
use fitz::core::lease::{LeaseDomain, LeaseService};
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

static LEASE_DOMAIN: OnceLock<Arc<LeaseDomain>> = OnceLock::new();
fn lease_domain() -> Arc<LeaseDomain> {
    LEASE_DOMAIN.get_or_init(|| {
        rt().block_on(async {
            Arc::new(LeaseDomain::new().await)
        })
    })
}

// ---------------------------------------------------------
// Helper functions
// ---------------------------------------------------------

fn create_lease_frame(operation: &str, resource: &str, holder: &str, ttl_seconds: Option<u64>) -> PooledFrame {
    let route = format!("lease://{}/{}", resource, operation);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_LEASE_HOLDER, holder.as_bytes(), &mut payload);
    if let Some(ttl) = ttl_seconds {
        build_tlv(TAG_LEASE_TTL, &ttl.to_le_bytes(), &mut payload);
    }
    PooledFrame::from_vec(payload)
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_lease_acquire(c: &mut Criterion) {
    let domain = lease_domain();

    c.bench_function("lease_acquire", |b| {
        b.iter_batched(
            || format!("resource_{}", fastrand::u64(0..1000000)),
            |resource| {
                let frame = create_lease_frame("acquire", &resource, "client_1", Some(30));
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: format!("lease://{}/acquire", resource),
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

fn bench_lease_renew(c: &mut Criterion) {
    let domain = lease_domain();

    c.bench_function("lease_renew", |b| {
        b.iter_batched(
            || {
                // Setup: acquire a lease first
                let resource = format!("renew_resource_{}", fastrand::u64(0..1000000));
                let acquire_frame = create_lease_frame("acquire", &resource, "client_renew", Some(30));
                let acquire_ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: format!("lease://{}/acquire", resource),
                    payload: acquire_frame.payload(),
                };
                rt().block_on(async {
                    let _ = domain.handle(acquire_ctx).await;
                });
                resource
            },
            |resource| {
                let renew_frame = create_lease_frame("renew", &resource, "client_renew", Some(30));
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: format!("lease://{}/renew", resource),
                    payload: renew_frame.payload(),
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

fn bench_lease_release(c: &mut Criterion) {
    let domain = lease_domain();

    c.bench_function("lease_release", |b| {
        b.iter_batched(
            || {
                // Setup: acquire a lease first
                let resource = format!("release_resource_{}", fastrand::u64(0..1000000));
                let acquire_frame = create_lease_frame("acquire", &resource, "client_release", Some(30));
                let acquire_ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: format!("lease://{}/acquire", resource),
                    payload: acquire_frame.payload(),
                };
                rt().block_on(async {
                    let _ = domain.handle(acquire_ctx).await;
                });
                resource
            },
            |resource| {
                let release_frame = create_lease_frame("release", &resource, "client_release", None);
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: format!("lease://{}/release", resource),
                    payload: release_frame.payload(),
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

fn bench_lease_acquire_contended(c: &mut Criterion) {
    let domain = lease_domain();

    c.bench_function("lease_acquire_contended", |b| {
        b.iter(|| {
            rt().block_on(async {
                let resource = "contended_resource";
                let mut handles = Vec::new();

                // Multiple clients trying to acquire the same lease
                for i in 0..5 {
                    let holder = format!("client_{}", i);
                    let frame = create_lease_frame("acquire", resource, &holder, Some(30));
                    let ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: format!("lease://{}/acquire", resource),
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

fn bench_lease_keep_alive(c: &mut Criterion) {
    let domain = lease_domain();

    c.bench_function("lease_keep_alive", |b| {
        b.iter_batched(
            || {
                // Setup: acquire a lease first
                let resource = format!("keepalive_resource_{}", fastrand::u64(0..1000000));
                let acquire_frame = create_lease_frame("acquire", &resource, "client_keepalive", Some(30));
                let acquire_ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: format!("lease://{}/acquire", resource),
                    payload: acquire_frame.payload(),
                };
                rt().block_on(async {
                    let _ = domain.handle(acquire_ctx).await;
                });
                resource
            },
            |resource| {
                rt().block_on(async {
                    // Send multiple keep-alive messages
                    for _ in 0..10 {
                        let keepalive_frame = create_lease_frame("keep_alive", &resource, "client_keepalive", Some(30));
                        let ctx = DomainContext {
                            route_family: RouteFamilyId::new(),
                            route_str: format!("lease://{}/keep_alive", resource),
                            payload: keepalive_frame.payload(),
                        };
                        let result = domain.handle(ctx).await;
                        criterion::black_box(result);
                    }
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_lease_watch(c: &mut Criterion) {
    let domain = lease_domain();

    c.bench_function("lease_watch", |b| {
        b.iter_batched(
            || format!("watch_resource_{}", fastrand::u64(0..1000000)),
            |resource| {
                let watch_frame = create_lease_frame("watch", &resource, "watcher_client", None);
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: format!("lease://{}/watch", resource),
                    payload: watch_frame.payload(),
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

fn bench_lease_list(c: &mut Criterion) {
    let domain = lease_domain();

    // Setup: create some leases
    for i in 0..10 {
        let resource = format!("list_resource_{}", i);
        let frame = create_lease_frame("acquire", &resource, "list_client", Some(300));
        let ctx = DomainContext {
            route_family: RouteFamilyId::new(),
            route_str: format!("lease://{}/acquire", resource),
            payload: frame.payload(),
        };
        rt().block_on(async {
            let _ = domain.handle(ctx).await;
        });
    }

    c.bench_function("lease_list", |b| {
        b.iter(|| {
            let route = "lease://*/list";
            let mut payload = Vec::new();
            build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
            build_tlv(TAG_LEASE_HOLDER, b"list_client", &mut payload);
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

fn bench_lease_revoke(c: &mut Criterion) {
    let domain = lease_domain();

    c.bench_function("lease_revoke", |b| {
        b.iter_batched(
            || {
                // Setup: acquire a lease first
                let resource = format!("revoke_resource_{}", fastrand::u64(0..1000000));
                let acquire_frame = create_lease_frame("acquire", &resource, "client_revoke", Some(30));
                let acquire_ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: format!("lease://{}/acquire", resource),
                    payload: acquire_frame.payload(),
                };
                rt().block_on(async {
                    let _ = domain.handle(acquire_ctx).await;
                });
                resource
            },
            |resource| {
                let revoke_frame = create_lease_frame("revoke", &resource, "admin", None);
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: format!("lease://{}/revoke", resource),
                    payload: revoke_frame.payload(),
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

fn bench_lease_multi_resource_acquire(c: &mut Criterion) {
    let domain = lease_domain();

    c.bench_function("lease_multi_resource_acquire", |b| {
        b.iter(|| {
            rt().block_on(async {
                let mut handles = Vec::new();

                // Acquire leases on multiple different resources
                for i in 0..10 {
                    let resource = format!("multi_resource_{}", i);
                    let holder = format!("client_{}", i);
                    let frame = create_lease_frame("acquire", &resource, &holder, Some(60));
                    let ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: format!("lease://{}/acquire", resource),
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

fn bench_lease_expiration_check(c: &mut Criterion) {
    let domain = lease_domain();

    // Setup: create some expired leases
    for i in 0..5 {
        let resource = format!("expired_resource_{}", i);
        let frame = create_lease_frame("acquire", &resource, "expired_client", Some(1)); // 1 second TTL
        let ctx = DomainContext {
            route_family: RouteFamilyId::new(),
            route_str: format!("lease://{}/acquire", resource),
            payload: frame.payload(),
        };
        rt().block_on(async {
            let _ = domain.handle(ctx).await;
        });
    }

    // Wait a bit for expiration
    std::thread::sleep(std::time::Duration::from_secs(2));

    c.bench_function("lease_expiration_check", |b| {
        b.iter(|| {
            let route = "lease://*/check_expired";
            let mut payload = Vec::new();
            build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
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

fn bench_lease_stats(c: &mut Criterion) {
    let domain = lease_domain();

    c.bench_function("lease_stats", |b| {
        b.iter(|| {
            let route = "lease://*/stats";
            let mut payload = Vec::new();
            build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
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

criterion_group!(
    name = lease_subsystem;
    config = config::criterion_config();
    targets =
        bench_lease_acquire,
        bench_lease_renew,
        bench_lease_release,
        bench_lease_acquire_contended,
        bench_lease_keep_alive,
        bench_lease_watch,
        bench_lease_list,
        bench_lease_revoke,
        bench_lease_multi_resource_acquire,
        bench_lease_expiration_check,
        bench_lease_stats
);

criterion_main!(lease_subsystem);