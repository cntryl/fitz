//! Subsystem benchmarks for queue domain operations
//!
//! These benchmarks test full queue domain operations end-to-end,
//! including handler processing, storage interactions, and domain logic.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::{Domain, DomainContext, DomainResponse};
use fitz::core::queue::{QueueDomain, QueueService};
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

static QUEUE_DOMAIN: OnceLock<Arc<QueueDomain>> = OnceLock::new();
fn queue_domain() -> Arc<QueueDomain> {
    QUEUE_DOMAIN.get_or_init(|| {
        rt().block_on(async {
            Arc::new(QueueDomain::new().await)
        })
    })
}

// ---------------------------------------------------------
// Helper functions
// ---------------------------------------------------------

fn create_enqueue_frame(realm: &str, area: &str, resource: &str, body: &[u8]) -> PooledFrame {
    let route = format!("queue://{}/{}/{}/enqueue", realm, area, resource);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_BODY, body, &mut payload);
    PooledFrame::from_vec(payload)
}

fn create_reserve_frame(realm: &str, area: &str, resource: &str) -> PooledFrame {
    let route = format!("queue://{}/{}/{}/reserve", realm, area, resource);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_LEASE_SECS, 30u32, &mut payload);
    PooledFrame::from_vec(payload)
}

fn create_complete_frame(realm: &str, area: &str, resource: &str, token: &str) -> PooledFrame {
    let route = format!("queue://{}/{}/{}/complete", realm, area, resource);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_DELIVERY_TOKEN, token.as_bytes(), &mut payload);
    PooledFrame::from_vec(payload)
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_queue_enqueue_small(c: &mut Criterion) {
    let domain = queue_domain();
    let frame = create_enqueue_frame("test", "orders", "pending", b"small message");

    c.bench_function("queue_enqueue_small", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "queue://test/orders/pending/enqueue".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_queue_enqueue_large(c: &mut Criterion) {
    let large_body = vec![b'x'; 64 * 1024]; // 64KB
    let domain = queue_domain();
    let frame = create_enqueue_frame("test", "orders", "pending", &large_body);

    c.bench_function("queue_enqueue_large", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "queue://test/orders/pending/enqueue".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_queue_reserve_empty(c: &mut Criterion) {
    let domain = queue_domain();
    let frame = create_reserve_frame("test", "orders", "empty");

    c.bench_function("queue_reserve_empty", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "queue://test/orders/empty/reserve".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_queue_round_trip(c: &mut Criterion) {
    let domain = queue_domain();

    c.bench_function("queue_round_trip", |b| {
        b.iter_batched(
            || {
                // Setup: enqueue a message
                let enqueue_frame = create_enqueue_frame("test", "roundtrip", "queue", b"test message");
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "queue://test/roundtrip/queue/enqueue".to_string(),
                    payload: enqueue_frame.payload(),
                };
                rt().block_on(async {
                    let _ = domain.handle(ctx).await;
                });
            },
            |_| {
                rt().block_on(async {
                    // Reserve the message
                    let reserve_frame = create_reserve_frame("test", "roundtrip", "queue");
                    let reserve_ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "queue://test/roundtrip/queue/reserve".to_string(),
                        payload: reserve_frame.payload(),
                    };

                    if let DomainResponse::Frame(frame) = domain.handle(reserve_ctx).await {
                        // Extract delivery token and complete
                        if let Some(token_bytes) = fitz::protocol::frame::parse_bytes(frame.payload(), TAG_DELIVERY_TOKEN) {
                            if let Ok(token) = std::str::from_utf8(&token_bytes) {
                                let complete_frame = create_complete_frame("test", "roundtrip", "queue", token);
                                let complete_ctx = DomainContext {
                                    route_family: RouteFamilyId::new(),
                                    route_str: "queue://test/roundtrip/queue/complete".to_string(),
                                    payload: complete_frame.payload(),
                                };
                                let _ = domain.handle(complete_ctx).await;
                            }
                        }
                    }
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_queue_list_messages(c: &mut Criterion) {
    let domain = queue_domain();

    // Setup: enqueue some messages
    for i in 0..10 {
        let frame = create_enqueue_frame("test", "list", "queue", format!("message {}", i).as_bytes());
        let ctx = DomainContext {
            route_family: RouteFamilyId::new(),
            route_str: "queue://test/list/queue/enqueue".to_string(),
            payload: frame.payload(),
        };
        rt().block_on(async {
            let _ = domain.handle(ctx).await;
        });
    }

    c.bench_function("queue_list_messages", |b| {
        b.iter(|| {
            let route = "queue://test/list/queue/list";
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

fn bench_queue_multi_tenant_isolation(c: &mut Criterion) {
    let domain = queue_domain();

    c.bench_function("queue_multi_tenant_isolation", |b| {
        b.iter(|| {
            rt().block_on(async {
                // Enqueue to different tenants simultaneously
                let frame1 = create_enqueue_frame("tenant1", "orders", "pending", b"tenant1 message");
                let ctx1 = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "queue://tenant1/orders/pending/enqueue".to_string(),
                    payload: frame1.payload(),
                };

                let frame2 = create_enqueue_frame("tenant2", "orders", "pending", b"tenant2 message");
                let ctx2 = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "queue://tenant2/orders/pending/enqueue".to_string(),
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
    name = queue_subsystem;
    config = config::criterion_config();
    targets =
        bench_queue_enqueue_small,
        bench_queue_enqueue_large,
        bench_queue_reserve_empty,
        bench_queue_round_trip,
        bench_queue_list_messages,
        bench_queue_multi_tenant_isolation
);

criterion_main!(queue_subsystem);