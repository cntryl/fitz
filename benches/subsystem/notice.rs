//! Subsystem benchmarks for notice domain operations
//!
//! These benchmarks test full notice domain operations end-to-end,
//! including handler processing, pub/sub routing, and domain logic.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::{Domain, DomainContext, DomainResponse};
use fitz::core::notice::{NoticeDomain, NoticeService};
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

static NOTICE_DOMAIN: OnceLock<Arc<NoticeDomain>> = OnceLock::new();
fn notice_domain() -> Arc<NoticeDomain> {
    NOTICE_DOMAIN.get_or_init(|| {
        rt().block_on(async {
            Arc::new(NoticeDomain::new().await)
        })
    })
}

// ---------------------------------------------------------
// Helper functions
// ---------------------------------------------------------

fn create_notice_publish_frame(realm: &str, topic: &str, routing_key: &str, data: &[u8]) -> PooledFrame {
    let route = format!("notice://{}/{}/publish", realm, topic);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_ROUTING_KEY, routing_key.as_bytes(), &mut payload);
    build_tlv(TAG_BODY, data, &mut payload);
    PooledFrame::from_vec(payload)
}

fn create_notice_subscribe_frame(realm: &str, topic: &str, routing_pattern: &str, subscriber_id: &str) -> PooledFrame {
    let route = format!("notice://{}/{}/subscribe", realm, topic);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_ROUTING_KEY, routing_pattern.as_bytes(), &mut payload);
    build_tlv(TAG_SUBSCRIBER_ID, subscriber_id.as_bytes(), &mut payload);
    PooledFrame::from_vec(payload)
}

fn create_notice_unsubscribe_frame(realm: &str, topic: &str, subscriber_id: &str) -> PooledFrame {
    let route = format!("notice://{}/{}/unsubscribe", realm, topic);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_SUBSCRIBER_ID, subscriber_id.as_bytes(), &mut payload);
    PooledFrame::from_vec(payload)
}

fn create_notice_poll_frame(realm: &str, topic: &str, subscriber_id: &str, max_messages: u32) -> PooledFrame {
    let route = format!("notice://{}/{}/poll", realm, topic);
    let mut payload = Vec::new();
    build_tlv(TAG_ROUTE, route.as_bytes(), &mut payload);
    build_tlv(TAG_SUBSCRIBER_ID, subscriber_id.as_bytes(), &mut payload);
    build_tlv(TAG_LIMIT, &max_messages.to_le_bytes(), &mut payload);
    PooledFrame::from_vec(payload)
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_notice_publish_small(c: &mut Criterion) {
    let domain = notice_domain();
    let data = b"small notice payload";
    let frame = create_notice_publish_frame("test", "events", "user.created", data);

    c.bench_function("notice_publish_small", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "notice://test/events/publish".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_notice_publish_large(c: &mut Criterion) {
    let large_data = vec![b'x'; 64 * 1024]; // 64KB payload
    let domain = notice_domain();
    let frame = create_notice_publish_frame("test", "events", "user.created", &large_data);

    c.bench_function("notice_publish_large", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "notice://test/events/publish".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_notice_subscribe(c: &mut Criterion) {
    let domain = notice_domain();

    c.bench_function("notice_subscribe", |b| {
        b.iter_batched(
            || format!("subscriber_{}", fastrand::u64(0..1000)),
            |subscriber_id| {
                let frame = create_notice_subscribe_frame("test", "events", "user.*", &subscriber_id);
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "notice://test/events/subscribe".to_string(),
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

fn bench_notice_unsubscribe(c: &mut Criterion) {
    let domain = notice_domain();

    c.bench_function("notice_unsubscribe", |b| {
        b.iter_batched(
            || {
                // Setup: subscribe first
                let subscriber_id = format!("unsub_{}", fastrand::u64(0..1000));
                let sub_frame = create_notice_subscribe_frame("test", "events", "user.*", &subscriber_id);
                let sub_ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "notice://test/events/subscribe".to_string(),
                    payload: sub_frame.payload(),
                };
                rt().block_on(async {
                    let _ = domain.handle(sub_ctx).await;
                });
                subscriber_id
            },
            |subscriber_id| {
                let frame = create_notice_unsubscribe_frame("test", "events", &subscriber_id);
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "notice://test/events/unsubscribe".to_string(),
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

fn bench_notice_publish_with_subscribers(c: &mut Criterion) {
    let domain = notice_domain();

    // Setup: create multiple subscribers
    for i in 0..10 {
        let subscriber_id = format!("bench_sub_{}", i);
        let frame = create_notice_subscribe_frame("test", "events", "user.created", &subscriber_id);
        let ctx = DomainContext {
            route_family: RouteFamilyId::new(),
            route_str: "notice://test/events/subscribe".to_string(),
            payload: frame.payload(),
        };
        rt().block_on(async {
            let _ = domain.handle(ctx).await;
        });
    }

    let data = b"user created event";
    let frame = create_notice_publish_frame("test", "events", "user.created", data);

    c.bench_function("notice_publish_with_subscribers", |b| {
        b.iter(|| {
            let ctx = DomainContext {
                route_family: RouteFamilyId::new(),
                route_str: "notice://test/events/publish".to_string(),
                payload: frame.payload(),
            };

            rt().block_on(async {
                let result = domain.handle(ctx).await;
                criterion::black_box(result);
            });
        })
    });
}

fn bench_notice_poll_empty(c: &mut Criterion) {
    let domain = notice_domain();

    c.bench_function("notice_poll_empty", |b| {
        b.iter_batched(
            || format!("poll_sub_{}", fastrand::u64(0..1000)),
            |subscriber_id| {
                let frame = create_notice_poll_frame("test", "events", &subscriber_id, 10);
                let ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "notice://test/events/poll".to_string(),
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

fn bench_notice_publish_and_poll(c: &mut Criterion) {
    let domain = notice_domain();

    c.bench_function("notice_publish_and_poll", |b| {
        b.iter_batched(
            || {
                // Setup: subscribe
                let subscriber_id = format!("pub_poll_sub_{}", fastrand::u64(0..1000));
                let sub_frame = create_notice_subscribe_frame("test", "events", "order.*", &subscriber_id);
                let sub_ctx = DomainContext {
                    route_family: RouteFamilyId::new(),
                    route_str: "notice://test/events/subscribe".to_string(),
                    payload: sub_frame.payload(),
                };
                rt().block_on(async {
                    let _ = domain.handle(sub_ctx).await;
                });
                subscriber_id
            },
            |subscriber_id| {
                rt().block_on(async {
                    // Publish
                    let pub_data = b"order placed event";
                    let pub_frame = create_notice_publish_frame("test", "events", "order.placed", pub_data);
                    let pub_ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "notice://test/events/publish".to_string(),
                        payload: pub_frame.payload(),
                    };
                    let _ = domain.handle(pub_ctx).await;

                    // Poll
                    let poll_frame = create_notice_poll_frame("test", "events", &subscriber_id, 5);
                    let poll_ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "notice://test/events/poll".to_string(),
                        payload: poll_frame.payload(),
                    };
                    let result = domain.handle(poll_ctx).await;

                    criterion::black_box(result);
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_notice_wildcard_matching(c: &mut Criterion) {
    let domain = notice_domain();

    // Setup: subscribe with wildcards
    let patterns = vec![
        "user.*",
        "*.created",
        "order.#",
        "system.*.error",
    ];

    for (i, pattern) in patterns.iter().enumerate() {
        let subscriber_id = format!("wildcard_sub_{}", i);
        let frame = create_notice_subscribe_frame("test", "events", pattern, &subscriber_id);
        let ctx = DomainContext {
            route_family: RouteFamilyId::new(),
            route_str: "notice://test/events/subscribe".to_string(),
            payload: frame.payload(),
        };
        rt().block_on(async {
            let _ = domain.handle(ctx).await;
        });
    }

    c.bench_function("notice_wildcard_matching", |b| {
        b.iter(|| {
            let test_keys = vec![
                "user.created",
                "user.updated",
                "order.placed",
                "order.shipped",
                "system.auth.error",
                "system.db.error",
            ];

            rt().block_on(async {
                for routing_key in &test_keys {
                    let data = format!("event for {}", routing_key).into_bytes();
                    let frame = create_notice_publish_frame("test", "events", routing_key, &data);
                    let ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "notice://test/events/publish".to_string(),
                        payload: frame.payload(),
                    };
                    let result = domain.handle(ctx).await;
                    criterion::black_box(result);
                }
            });
        })
    });
}

fn bench_notice_multi_topic_publish(c: &mut Criterion) {
    let domain = notice_domain();

    c.bench_function("notice_multi_topic_publish", |b| {
        b.iter(|| {
            rt().block_on(async {
                let topics = vec!["events", "metrics", "logs", "traces"];

                for topic in &topics {
                    let data = format!("data for {}", topic).into_bytes();
                    let frame = create_notice_publish_frame("test", topic, "test.key", &data);
                    let ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: format!("notice://test/{}/publish", topic),
                        payload: frame.payload(),
                    };
                    let result = domain.handle(ctx).await;
                    criterion::black_box(result);
                }
            });
        })
    });
}

fn bench_notice_high_volume_publish(c: &mut Criterion) {
    let domain = notice_domain();

    c.bench_function("notice_high_volume_publish", |b| {
        b.iter(|| {
            rt().block_on(async {
                let mut handles = Vec::new();

                // Publish 50 messages concurrently
                for i in 0..50 {
                    let data = format!("high volume message {}", i).into_bytes();
                    let frame = create_notice_publish_frame("test", "events", &format!("batch.{}", i), &data);
                    let ctx = DomainContext {
                        route_family: RouteFamilyId::new(),
                        route_str: "notice://test/events/publish".to_string(),
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

criterion_group!(
    name = notice_subsystem;
    config = config::criterion_config();
    targets =
        bench_notice_publish_small,
        bench_notice_publish_large,
        bench_notice_subscribe,
        bench_notice_unsubscribe,
        bench_notice_publish_with_subscribers,
        bench_notice_poll_empty,
        bench_notice_publish_and_poll,
        bench_notice_wildcard_matching,
        bench_notice_multi_topic_publish,
        bench_notice_high_volume_publish
);

criterion_main!(notice_subsystem);