//! Comprehensive hotpath benchmarks for Notice domain - handler -> service layering
//!
//! Tests:
//! - High volume: sequential subscribe/publish operations
//! - High concurrency: parallel operations from multiple clients
//! - Breakdown points: subscriber counts, message sizes, wildcard matching
//!
//! Goal: Understand pub/sub performance and identify scalability limits

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fitz::core::domain::{Domain, DomainContext};
use fitz::core::notice::NoticeDomain;
use fitz::protocol::route::Route;
use fitz::protocol::tags::{TAG_BODY, TAG_ID, TAG_SUBSCRIBE};
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

/// Build TLV payload for subscribe
#[allow(dead_code)]
fn build_subscribe_payload() -> Vec<u8> {
    vec![TAG_SUBSCRIBE, 0]
}

/// Build TLV payload for publish
fn build_publish_payload(msg_id: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    
    if let Some(id) = msg_id {
        payload.push(TAG_ID);
        payload.push(id.len() as u8);
        payload.extend_from_slice(id.as_bytes());
    }
    
    payload.push(TAG_BODY);
    if body.len() <= 254 {
        payload.push(body.len() as u8);
        payload.extend_from_slice(body);
    } else {
        payload.push(255);
        payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
        payload.extend_from_slice(body);
    }
    
    payload
}

/// Build route for notice operation
fn build_route(realm: &str, area: &str, resource: &str, operation: &str) -> Route {
    let route_str = format!("notice://{}/{}/{}/{}", realm, area, resource, operation);
    Route {
        scheme: fitz::protocol::route::Scheme::Notice,
        realm: Some(realm.to_string()),
        area: Some(area.to_string()),
        resource: Some(resource.to_string()),
        operation: Some(operation.to_string()),
        raw: route_str.clone(),
    }
}

/// Sequential publish operations (no subscribers)
fn bench_sequential_publish_no_subscribers(c: &mut Criterion) {
    let domain = NoticeDomain::new();
    
    let mut group = c.benchmark_group("notice_sequential_publish_no_subs");
    
    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                for i in 0..count {
                    let body = format!("message_{}", i).into_bytes();
                    let payload = build_publish_payload(Some(&format!("msg_{}", i)), &body);
                    let route = build_route("realm1", "area1", "alerts", "critical");
                    
                    let ctx = DomainContext {
                        route,
                        route_str: "notice://realm1/area1/alerts/critical".to_string(),
                        payload,
                        channel_id: 1,
                        route_family: 0,
                        sender: None,
                    };
                    
                    black_box(domain.handle(ctx));
                }
            });
        });
    }
    
    group.finish();
}

/// Test message size impact
fn bench_message_sizes(c: &mut Criterion) {
    let domain = NoticeDomain::new();
    
    let mut group = c.benchmark_group("notice_message_sizes");
    
    for size in [64, 256, 1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let body = vec![0u8; size as usize];
                let payload = build_publish_payload(Some("msg_id"), &body);
                let route = build_route("realm1", "area1", "data", "stream");
                
                let ctx = DomainContext {
                    route,
                    route_str: "notice://realm1/area1/data/stream".to_string(),
                    payload,
                    channel_id: 1,
                    route_family: 0,
                    sender: None,
                };
                
                black_box(domain.handle(ctx));
            });
        });
    }
    
    group.finish();
}

/// Concurrent publish from multiple route families (multi-tenant)
fn bench_concurrent_multitenant_publish(c: &mut Criterion) {
    let domain = Arc::new(NoticeDomain::new());
    
    c.bench_function("notice_concurrent_multitenant", |b| {
        b.iter(|| {
            // Simulate 10 tenants publishing 10 messages each
            for rf in 0..10u32 {
                for i in 0..10 {
                    let domain: Arc<NoticeDomain> = Arc::clone(&domain);
                    let body = format!("tenant_{}_message_{}", rf, i).into_bytes();
                    let payload = build_publish_payload(Some(&format!("msg_{}_{}", rf, i)), &body);
                    let route = build_route(
                        &format!("realm{}", rf),
                        &format!("area{}", rf),
                        "events",
                        "update",
                    );
                    
                    let ctx = DomainContext {
                        route,
                        route_str: format!("notice://realm{}/area{}/events/update", rf, rf),
                        payload,
                        channel_id: i as u32,
                        route_family: rf,
                        sender: None,
                    };
                    
                    black_box(domain.handle(ctx));
                }
            }
        });
    });
}

/// Test route matching complexity with wildcards
fn bench_wildcard_matching(c: &mut Criterion) {
    let domain = NoticeDomain::new();
    
    c.bench_function("notice_wildcard_matching", |b| {
        b.iter(|| {
            // Test various wildcard patterns
            let patterns = vec![
                ("realm1", "area1", "resource1", "op1"),
                ("realm1", "area1", "*", "op1"),
                ("realm1", "*", "*", "op1"),
                ("*", "*", "*", "op1"),
            ];
            
            for (realm, area, resource, operation) in patterns {
                let body = b"test_message".to_vec();
                let payload = build_publish_payload(Some("msg_id"), &body);
                let route = build_route(realm, area, resource, operation);
                let route_str = format!("notice://{}/{}/{}/{}", realm, area, resource, operation);
                
                let ctx = DomainContext {
                    route,
                    route_str,
                    payload,
                    channel_id: 1,
                    route_family: 0,
                    sender: None,
                };
                
                black_box(domain.handle(ctx));
            }
        });
    });
}

/// High frequency publish (stress test)
fn bench_high_frequency_publish(c: &mut Criterion) {
    let domain = Arc::new(NoticeDomain::new());
    
    c.bench_function("notice_high_frequency", |b| {
        b.iter(|| {
            // 1000 rapid-fire publishes
            for i in 0..1000 {
                let domain: Arc<NoticeDomain> = Arc::clone(&domain);
                let body = vec![0u8; 128];
                let payload = build_publish_payload(None, &body);
                let route = build_route("realm1", "area1", "stream", "data");
                
                let ctx = DomainContext {
                    route,
                    route_str: "notice://realm1/area1/stream/data".to_string(),
                    payload,
                    channel_id: (i % 10) as u32,
                    route_family: 0,
                    sender: None,
                };
                
                black_box(domain.handle(ctx));
            }
        });
    });
}

/// EXTREME: Broadcast fanout - 1 publish delivered to many subscribers
fn bench_extreme_broadcast_fanout(c: &mut Criterion) {
    use crossbeam_channel;
    
    let mut group = c.benchmark_group("notice_extreme_broadcast_fanout");
    
    for subscriber_count in [10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_subscribers", subscriber_count)),
            &subscriber_count,
            |b, &subscriber_count| {
                b.iter(|| {
                    let domain = NoticeDomain::new();
                    
                    // Subscribe many channels to the same route
                    let sub_payload = build_subscribe_payload();
                    for channel_id in 0..subscriber_count {
                        let (tx, _rx) = crossbeam_channel::bounded(100);
                        let route = build_route("realm1", "area1", "broadcast", "alert");
                        let ctx = DomainContext {
                            route,
                            route_str: "notice://realm1/area1/broadcast/alert".to_string(),
                            payload: sub_payload.clone(),
                            channel_id: channel_id as u32,
                            route_family: 0,
                            sender: Some(tx),
                        };
                        let _ = domain.handle(ctx);
                    }
                    
                    // Now publish once - should fan out to all subscribers
                    let body = vec![0u8; 256];
                    let payload = build_publish_payload(Some("broadcast_msg"), &body);
                    let route = build_route("realm1", "area1", "broadcast", "alert");
                    let ctx = DomainContext {
                        route,
                        route_str: "notice://realm1/area1/broadcast/alert".to_string(),
                        payload,
                        channel_id: 9999,
                        route_family: 0,
                        sender: None,
                    };
                    
                    black_box(domain.handle(ctx));
                });
            },
        );
    }
    
    group.finish();
}

/// EXTREME: Subscription churn - rapid subscribe/unsubscribe operations
fn bench_extreme_subscription_churn(c: &mut Criterion) {
    use crossbeam_channel;
    
    let mut group = c.benchmark_group("notice_extreme_subscription_churn");
    
    for churn_count in [100, 1000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_cycles", churn_count)),
            &churn_count,
            |b, &churn_count| {
                b.iter(|| {
                    let domain = NoticeDomain::new();
                    let sub_payload = build_subscribe_payload();
                    
                    // Rapidly add and implicitly remove (via domain recreation) subscriptions
                    for i in 0..churn_count {
                        let (tx, _rx) = crossbeam_channel::bounded(100);
                        let route = build_route(
                            "realm1",
                            "area1",
                            &format!("resource_{}", i % 10),
                            "update",
                        );
                        let route_str = format!(
                            "notice://realm1/area1/resource_{}/update",
                            i % 10
                        );
                        let ctx = DomainContext {
                            route,
                            route_str,
                            payload: sub_payload.clone(),
                            channel_id: i as u32,
                            route_family: 0,
                            sender: Some(tx),
                        };
                        black_box(domain.handle(ctx));
                    }
                });
            },
        );
    }
    
    group.finish();
}

/// EXTREME: Wildcard explosion - many overlapping wildcard patterns
fn bench_extreme_wildcard_explosion(c: &mut Criterion) {
    use crossbeam_channel;
    
    let mut group = c.benchmark_group("notice_extreme_wildcard_explosion");
    
    for wildcard_count in [10, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_wildcards", wildcard_count)),
            &wildcard_count,
            |b, &wildcard_count| {
                b.iter(|| {
                    let domain = NoticeDomain::new();
                    let sub_payload = build_subscribe_payload();
                    
                    // Create many overlapping wildcard subscriptions
                    for i in 0..wildcard_count {
                        let (tx, _rx) = crossbeam_channel::bounded(100);
                        
                        // Mix of wildcard patterns that could all match the same publish
                        let (area, resource) = match i % 4 {
                            0 => ("*", "*"),
                            1 => ("area1", "*"),
                            2 => ("*", "events"),
                            _ => ("area1", "events"),
                        };
                        
                        let route = build_route("realm1", area, resource, "update");
                        let route_str = format!("notice://realm1/{}/{}/update", area, resource);
                        let ctx = DomainContext {
                            route,
                            route_str,
                            payload: sub_payload.clone(),
                            channel_id: i as u32,
                            route_family: 0,
                            sender: Some(tx),
                        };
                        let _ = domain.handle(ctx);
                    }
                    
                    // Publish to a route that matches ALL wildcards
                    let body = vec![0u8; 128];
                    let payload = build_publish_payload(Some("wildcard_test"), &body);
                    let route = build_route("realm1", "area1", "events", "update");
                    let ctx = DomainContext {
                        route,
                        route_str: "notice://realm1/area1/events/update".to_string(),
                        payload,
                        channel_id: 9999,
                        route_family: 0,
                        sender: None,
                    };
                    
                    black_box(domain.handle(ctx));
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    name = hotpath_notice_core;
    config = config::criterion_config();
    targets = 
        bench_sequential_publish_no_subscribers,
        bench_message_sizes,
        bench_concurrent_multitenant_publish,
        bench_wildcard_matching,
        bench_high_frequency_publish,
        bench_extreme_broadcast_fanout,
        bench_extreme_subscription_churn,
        bench_extreme_wildcard_explosion
);
criterion_main!(hotpath_notice_core);
