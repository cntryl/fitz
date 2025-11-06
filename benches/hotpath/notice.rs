use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::domain::{Domain, DomainResponse};
use fitz::core::notice::{NoticeDomain, NoticeService, RouteTable, RtSubscription};
use fitz::protocol::frame::build_tlv;
use fitz::protocol::tags::*;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

#[path = "../config.rs"]
mod config;

// Shared Tokio runtime
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

// Helper to build a DomainRequest for a given raw route and payload
fn make_request(raw: &str, payload: Vec<u8>) -> fitz::core::domain::DomainRequest {
    fitz::core::domain::DomainRequest {
        route: fitz::protocol::route::Route {
            scheme: fitz::protocol::route::Scheme::Notice,
            realm: Some("bench".to_string()),
            area: Some("hotpath".to_string()),
            resource: Some("test".to_string()),
            operation: Some("event".to_string()),
            raw: raw.to_string(),
        },
        route_str: raw.to_string(),
        payload,
        channel_id: 1,
    }
}

// Shared NoticeService instance
fn shared_service() -> Arc<std::sync::Mutex<NoticeService>> {
    Arc::new(std::sync::Mutex::new(NoticeService::new()))
}

// Shared NoticeDomain instance
fn shared_domain() -> Arc<NoticeDomain> {
    Arc::new(NoticeDomain::new())
}

// ============================================================================
// ROUTE TABLE BENCHMARKS
// ============================================================================

/// Benchmark: Insert subscription into route table
fn bench_route_table_insert(c: &mut Criterion) {
    c.bench_function("route_table_insert", |b| {
        b.iter(|| {
            let mut rt = RouteTable::new();
            let (tx, _rx) = mpsc::channel(100);
            for i in 0..10 {
                let sub = RtSubscription {
                    id: i,
                    route_pattern: format!("notice://realm/area{}/resource/op", i),
                    channel_id: 1,
                    sender: tx.clone(),
                };
                rt.insert(sub);
            }
            rt
        });
    });
}

/// Benchmark: Remove subscription from route table
fn bench_route_table_remove(c: &mut Criterion) {
    c.bench_function("route_table_remove", |b| {
        b.iter(|| {
            let mut rt = RouteTable::new();
            let (tx, _rx) = mpsc::channel(100);
            for i in 0..10 {
                let sub = RtSubscription {
                    id: i,
                    route_pattern: format!("notice://realm/area{}/resource/op", i),
                    channel_id: 1,
                    sender: tx.clone(),
                };
                rt.insert(sub);
            }
            // Remove half of them
            for i in 0..5 {
                rt.remove(i);
            }
            rt
        });
    });
}

/// Benchmark: Find matching subscribers - exact match
fn bench_route_table_match_exact(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel(100);
    for i in 0..100 {
        let sub = RtSubscription {
            id: i,
            route_pattern: format!("notice://realm/area{}/resource/op", i),
            channel_id: 1,
            sender: tx.clone(),
        };
        rt.insert(sub);
    }
    
    c.bench_function("route_table_match_exact", |b| {
        b.iter(|| {
            rt.matching_subscribers("notice://realm/area42/resource/op")
        });
    });
}

/// Benchmark: Find matching subscribers - global wildcard
fn bench_route_table_match_global_wildcard(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel(100);
    
    // Add one global wildcard subscription
    let sub = RtSubscription {
        id: 1,
        route_pattern: "*".to_string(),
        channel_id: 1,
        sender: tx.clone(),
    };
    rt.insert(sub);
    
    // Add many specific subscriptions
    for i in 0..100 {
        let sub = RtSubscription {
            id: i + 2,
            route_pattern: format!("notice://realm/area{}/resource/op", i),
            channel_id: 1,
            sender: tx.clone(),
        };
        rt.insert(sub);
    }
    
    c.bench_function("route_table_match_global_wildcard", |b| {
        b.iter(|| {
            rt.matching_subscribers("notice://realm/area42/resource/op")
        });
    });
}

/// Benchmark: Find matching subscribers - trailing wildcard
fn bench_route_table_match_trailing_wildcard(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel(100);
    
    // Add trailing wildcard subscriptions at different levels
    for i in 0..20 {
        let sub = RtSubscription {
            id: i,
            route_pattern: format!("notice://realm/area{}/*", i),
            channel_id: 1,
            sender: tx.clone(),
        };
        rt.insert(sub);
    }
    
    c.bench_function("route_table_match_trailing_wildcard", |b| {
        b.iter(|| {
            rt.matching_subscribers("notice://realm/area10/resource/op")
        });
    });
}

/// Benchmark: Find matching subscribers - mid-path wildcard
fn bench_route_table_match_mid_path_wildcard(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel(100);
    
    // Add mid-path wildcard subscriptions
    for i in 0..20 {
        let sub = RtSubscription {
            id: i,
            route_pattern: format!("notice://realm/*/resource{}/op", i),
            channel_id: 1,
            sender: tx.clone(),
        };
        rt.insert(sub);
    }
    
    c.bench_function("route_table_match_mid_path_wildcard", |b| {
        b.iter(|| {
            rt.matching_subscribers("notice://realm/anyarea/resource10/op")
        });
    });
}

/// Benchmark: Find matching subscribers - no matches
fn bench_route_table_match_none(c: &mut Criterion) {
    let mut rt = RouteTable::new();
    let (tx, _rx) = mpsc::channel(100);
    
    for i in 0..100 {
        let sub = RtSubscription {
            id: i,
            route_pattern: format!("notice://other/area{}/resource/op", i),
            channel_id: 1,
            sender: tx.clone(),
        };
        rt.insert(sub);
    }
    
    c.bench_function("route_table_match_none", |b| {
        b.iter(|| {
            rt.matching_subscribers("notice://nomatch/area/resource/op")
        });
    });
}

/// Benchmark: Cleanup channel subscriptions
fn bench_route_table_cleanup_channel(c: &mut Criterion) {
    c.bench_function("route_table_cleanup_channel", |b| {
        b.iter(|| {
            let mut rt = RouteTable::new();
            let (tx, _rx) = mpsc::channel(100);
            
            // Add subscriptions for multiple channels
            for channel_id in 1..=5 {
                for i in 0..20 {
                    let sub = RtSubscription {
                        id: (channel_id * 100 + i) as u64,
                        route_pattern: format!("notice://realm/area{}/resource/op", i),
                        channel_id,
                        sender: tx.clone(),
                    };
                    rt.insert(sub);
                }
            }
            
            // Cleanup channel 3
            rt.cleanup_channel(3);
            rt
        });
    });
}

// ============================================================================
// SERVICE BENCHMARKS
// ============================================================================

/// Benchmark: Subscribe to route pattern
fn bench_service_subscribe(c: &mut Criterion) {
    let svc = shared_service();
    
    c.bench_function("service_subscribe", |b| {
        b.iter(|| {
            let mut svc = svc.lock().unwrap();
            let (tx, _rx) = mpsc::channel(100);
            svc.subscribe("notice://bench/hotpath/test/*".to_string(), 1, tx)
        });
    });
}

/// Benchmark: Unsubscribe by ID
fn bench_service_unsubscribe(c: &mut Criterion) {
    c.bench_function("service_unsubscribe", |b| {
        b.iter(|| {
            let svc = shared_service();
            let mut svc = svc.lock().unwrap();
            let (tx, _rx) = mpsc::channel(100);
            let sub_id = svc.subscribe("notice://bench/hotpath/test/*".to_string(), 1, tx);
            svc.unsubscribe(sub_id)
        });
    });
}

/// Benchmark: Publish with no subscribers
fn bench_service_publish_no_subscribers(c: &mut Criterion) {
    let svc = shared_service();
    
    c.bench_function("service_publish_no_subscribers", |b| {
        b.iter(|| {
            let mut svc = svc.lock().unwrap();
            svc.publish(
                "notice://bench/hotpath/test/event",
                Some("msg-123"),
                b"benchmark payload",
            )
        });
    });
}

/// Benchmark: Publish with 1 subscriber
fn bench_service_publish_one_subscriber(c: &mut Criterion) {
    let svc = shared_service();
    let (tx, _rx) = mpsc::channel(1000);
    {
        let mut svc = svc.lock().unwrap();
        svc.subscribe("notice://bench/hotpath/test/*".to_string(), 1, tx);
    }
    
    c.bench_function("service_publish_one_subscriber", |b| {
        b.iter(|| {
            let mut svc = svc.lock().unwrap();
            svc.publish(
                "notice://bench/hotpath/test/event",
                Some("msg-123"),
                b"benchmark payload",
            )
        });
    });
}

/// Benchmark: Publish with 10 subscribers
fn bench_service_publish_ten_subscribers(c: &mut Criterion) {
    let svc = shared_service();
    
    // Add 10 subscribers with different patterns
    for i in 0..10 {
        let (tx, _rx) = mpsc::channel(1000);
        let mut svc = svc.lock().unwrap();
        let pattern = if i < 5 {
            "notice://bench/hotpath/test/*".to_string()
        } else {
            format!("notice://bench/hotpath/test/event{}", i)
        };
        svc.subscribe(pattern, i as u32, tx);
    }
    
    c.bench_function("service_publish_ten_subscribers", |b| {
        b.iter(|| {
            let mut svc = svc.lock().unwrap();
            svc.publish(
                "notice://bench/hotpath/test/event",
                Some("msg-123"),
                b"benchmark payload",
            )
        });
    });
}

/// Benchmark: Publish with wildcard matching (5 matching out of 100 total)
fn bench_service_publish_wildcard_matching(c: &mut Criterion) {
    let svc = shared_service();
    
    // Add 100 subscribers, only 5 will match
    for i in 0..100 {
        let (tx, _rx) = mpsc::channel(1000);
        let mut svc = svc.lock().unwrap();
        let pattern = if i < 5 {
            "notice://bench/hotpath/*".to_string()
        } else {
            format!("notice://other/area{}/resource/op", i)
        };
        svc.subscribe(pattern, i as u32, tx);
    }
    
    c.bench_function("service_publish_wildcard_matching", |b| {
        b.iter(|| {
            let mut svc = svc.lock().unwrap();
            svc.publish(
                "notice://bench/hotpath/test/event",
                Some("msg-123"),
                b"benchmark payload",
            )
        });
    });
}

/// Benchmark: Subscribe, publish, and unsubscribe cycle
fn bench_service_subscribe_publish_unsubscribe_cycle(c: &mut Criterion) {
    let svc = shared_service();
    
    c.bench_function("service_subscribe_publish_unsubscribe_cycle", |b| {
        b.iter(|| {
            let (tx, _rx) = mpsc::channel(1000);
            let mut svc = svc.lock().unwrap();
            let sub_id = svc.subscribe("notice://bench/hotpath/test/*".to_string(), 1, tx);
            let _ = svc.publish(
                "notice://bench/hotpath/test/event",
                Some("msg-123"),
                b"benchmark payload",
            );
            svc.unsubscribe(sub_id)
        });
    });
}

// ============================================================================
// HANDLER BENCHMARKS
// ============================================================================

/// Benchmark: Subscribe via handler
fn bench_handler_subscribe(c: &mut Criterion) {
    let domain = shared_domain();
    
    c.bench_function("handler_subscribe", |b| {
        b.iter(|| {
            let domain = domain.clone();
            rt().block_on(async move {
                let mut payload = Vec::new();
                build_tlv(TAG_SUBSCRIBE, b"notice://bench/hotpath/test/*", &mut payload);
                
                let req = make_request("notice://bench/hotpath/test/*", payload);
                (&*domain).handle(req).await
            })
        });
    });
}

/// Benchmark: Publish via handler (no subscribers)
fn bench_handler_publish_no_subscribers(c: &mut Criterion) {
    let domain = shared_domain();
    
    c.bench_function("handler_publish_no_subscribers", |b| {
        b.iter(|| {
            let domain = domain.clone();
            rt().block_on(async move {
                let mut payload = Vec::new();
                build_tlv(TAG_BODY, b"benchmark payload", &mut payload);
                build_tlv(TAG_ID, b"msg-123", &mut payload);
                
                let req = make_request("notice://bench/hotpath/test/event", payload);
                (&*domain).handle(req).await
            })
        });
    });
}

/// Benchmark: Publish via handler (with 1 subscriber)
fn bench_handler_publish_one_subscriber(c: &mut Criterion) {
    let domain = shared_domain();
    
    // Subscribe first
    rt().block_on(async {
        let mut payload = Vec::new();
        build_tlv(TAG_SUBSCRIBE, b"notice://bench/hotpath/test/*", &mut payload);
        let req = make_request("notice://bench/hotpath/test/*", payload);
        let _ = (&*domain).handle(req).await;
    });
    
    c.bench_function("handler_publish_one_subscriber", |b| {
        b.iter(|| {
            let domain = domain.clone();
            rt().block_on(async move {
                let mut payload = Vec::new();
                build_tlv(TAG_BODY, b"benchmark payload", &mut payload);
                build_tlv(TAG_ID, b"msg-123", &mut payload);
                
                let req = make_request("notice://bench/hotpath/test/event", payload);
                (&*domain).handle(req).await
            })
        });
    });
}

/// Benchmark: Subscribe, publish, unsubscribe via handler
fn bench_handler_subscribe_publish_unsubscribe(c: &mut Criterion) {
    let domain = shared_domain();
    
    c.bench_function("handler_subscribe_publish_unsubscribe", |b| {
        b.iter(|| {
            let domain = domain.clone();
            rt().block_on(async move {
                // Subscribe
                let mut sub_payload = Vec::new();
                build_tlv(TAG_SUBSCRIBE, b"notice://bench/hotpath/test/*", &mut sub_payload);
                let sub_req = make_request("notice://bench/hotpath/test/*", sub_payload);
                let sub_resp = (&*domain).handle(sub_req).await;
                
                // Extract subscription ID
                let sub_id = if let DomainResponse::Frame(frame) = sub_resp {
                    fitz::protocol::frame::find_tlv(frame.as_ref(), TAG_ID)
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .map(|s| s.to_string())
                } else {
                    None
                };
                
                // Publish
                let mut pub_payload = Vec::new();
                build_tlv(TAG_BODY, b"benchmark payload", &mut pub_payload);
                let pub_req = make_request("notice://bench/hotpath/test/event", pub_payload);
                let _ = (&*domain).handle(pub_req).await;
                
                // Unsubscribe
                if let Some(sub_id) = sub_id {
                    let mut unsub_payload = Vec::new();
                    build_tlv(TAG_UNSUBSCRIBE, sub_id.as_bytes(), &mut unsub_payload);
                    let unsub_req = make_request("notice://bench/hotpath/test/*", unsub_payload);
                    let _ = (&*domain).handle(unsub_req).await;
                }
            })
        });
    });
}

// ============================================================================
// SCALING BENCHMARKS (Confirm scaling behavior)
// ============================================================================

/// Benchmark: Match exact route at various scales (1K, 10K, 100K subscriptions)
fn bench_route_table_match_exact_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_table_match_exact_scaling");
    
    for &n in &[1_000, 10_000, 100_000] {
        let mut rt = RouteTable::new();
        let (tx, _rx) = mpsc::channel(100);
        
        // Insert N unique subscriptions
        for i in 0..n {
            let sub = RtSubscription {
                id: i,
                route_pattern: format!("notice://realm/area{}/resource/op", i),
                channel_id: 1,
                sender: tx.clone(),
            };
            rt.insert(sub);
        }
        
        group.bench_with_input(format!("{}", n), &rt, |b, rt| {
            b.iter(|| {
                rt.matching_subscribers(&format!("notice://realm/area{}/resource/op", n / 2))
            });
        });
    }
    
    group.finish();
}

/// Benchmark: Match with wildcards at various scales (1K, 10K, 100K subscriptions)
fn bench_route_table_match_wildcard_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("route_table_match_wildcard_scaling");
    
    for &n in &[1_000, 10_000, 100_000] {
        let mut rt = RouteTable::new();
        let (tx, _rx) = mpsc::channel(100);
        
        // Insert N subscriptions (10% with trailing wildcards)
        for i in 0..n {
            let pattern = if i % 10 == 0 {
                format!("notice://realm/area{}/*", i)
            } else {
                format!("notice://realm/area{}/resource/op", i)
            };
            let sub = RtSubscription {
                id: i,
                route_pattern: pattern,
                channel_id: 1,
                sender: tx.clone(),
            };
            rt.insert(sub);
        }
        
        group.bench_with_input(format!("{}", n), &rt, |b, rt| {
            b.iter(|| {
                rt.matching_subscribers(&format!("notice://realm/area{}/resource/op", n / 2))
            });
        });
    }
    
    group.finish();
}

criterion_group! {
    name = hotpath_notice;
    config = config::criterion_config();
    targets =
        // Route table benchmarks (baseline: 100 subs)
        bench_route_table_insert,
        bench_route_table_remove,
        bench_route_table_match_exact,
        bench_route_table_match_global_wildcard,
        bench_route_table_match_trailing_wildcard,
        bench_route_table_match_mid_path_wildcard,
        bench_route_table_match_none,
        bench_route_table_cleanup_channel,
        // Service benchmarks
        bench_service_subscribe,
        bench_service_unsubscribe,
        bench_service_publish_no_subscribers,
        bench_service_publish_one_subscriber,
        bench_service_publish_ten_subscribers,
        bench_service_publish_wildcard_matching,
        bench_service_subscribe_publish_unsubscribe_cycle,
        // Handler benchmarks
        bench_handler_subscribe,
        bench_handler_publish_no_subscribers,
        bench_handler_publish_one_subscriber,
        bench_handler_subscribe_publish_unsubscribe,
        // Scaling benchmarks
        bench_route_table_match_exact_scaling,
        bench_route_table_match_wildcard_scaling,
}

criterion_main!(hotpath_notice);
