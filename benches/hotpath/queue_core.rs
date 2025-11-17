//! Hotpath benchmarks for Queue domain handler->service layer
//!
//! Tests queue enqueue/dequeue operations, message ordering, TTL/lease management,
//! and concurrent producer/consumer patterns.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fitz::core::domain::{Domain, DomainContext};
use fitz::core::queue::QueueDomain;
use fitz::protocol::route::Route;
use fitz::protocol::tags::{TAG_BODY, TAG_ID, TAG_LEASE, TAG_DELIVERY_TOKEN};
use fitz::storage::midge_adapter;
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

/// Build TLV payload for queue enqueue
fn build_enqueue_payload(body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    
    // TAG_BODY
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

/// Build TLV payload for queue reserve (dequeue)
fn build_reserve_payload(lease_secs: u64) -> Vec<u8> {
    let mut payload = Vec::new();
    
    // TAG_LEASE
    payload.push(TAG_LEASE);
    payload.push(8);
    payload.extend_from_slice(&lease_secs.to_be_bytes());
    
    payload
}

/// Build TLV payload for queue consume
#[allow(dead_code)]
fn build_consume_payload(message_id: &str, delivery_token: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    
    // TAG_ID
    payload.push(TAG_ID);
    payload.push(message_id.len() as u8);
    payload.extend_from_slice(message_id.as_bytes());
    
    // TAG_DELIVERY_TOKEN
    payload.push(TAG_DELIVERY_TOKEN);
    payload.push(delivery_token.len() as u8);
    payload.extend_from_slice(delivery_token.as_bytes());
    
    payload
}

/// Build route for queue operation
fn build_route(operation: &str) -> Route {
    let route_str = format!("queue://realm1/area1/{}", operation);
    Route {
        scheme: fitz::protocol::route::Scheme::Queue,
        realm: Some("realm1".to_string()),
        area: Some("area1".to_string()),
        resource: Some(operation.to_string()),
        operation: None,
        raw: route_str.clone(),
    }
}

/// Sequential enqueue operations
fn bench_sequential_enqueue(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_sequential_enqueue");
    group.sample_size(10); // Limit iterations to prevent unbounded memory growth
    
    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_batched(
                || {
                    let store = midge_adapter::create_memory_store().expect("create store");
                    QueueDomain::new(store)
                },
                |domain| {
                    for i in 0..count {
                        let body = format!("message-{}", i).into_bytes();
                        let payload = build_enqueue_payload(&body);
                        let route = build_route("enqueue");
                        
                        let ctx = DomainContext {
                            route,
                            route_str: "queue://realm1/area1/enqueue".to_string(),
                            payload,
                            channel_id: 1,
                            route_family: 0,

                        };
                        
                        let _response = domain.handle(ctx);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    
    group.finish();
}

/// Sequential reserve (dequeue) operations after warm-up
fn bench_sequential_reserve(c: &mut Criterion) {
    let store = midge_adapter::create_memory_store().expect("create store");
    let domain = QueueDomain::new(store);
    
    // Warm-up: enqueue 10k messages
    for i in 0..10000 {
        let body = format!("message-{}", i).into_bytes();
        let payload = build_enqueue_payload(&body);
        let route = build_route("enqueue");
        let ctx = DomainContext {
            route,
            route_str: "queue://realm1/area1/enqueue".to_string(),
            payload,
            channel_id: 1,
            route_family: 0,

        };
        domain.handle(ctx);
    }
    
    let mut group = c.benchmark_group("queue_sequential_reserve");
    
    for count in [100, 1000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                // Arrange & Act
                for _ in 0..count {
                    let payload = build_reserve_payload(30); // 30 second lease
                    let route = build_route("reserve");
                    
                    let ctx = DomainContext {
                        route,
                        route_str: "queue://realm1/area1/reserve".to_string(),
                        payload,
                        channel_id: 1,
                        route_family: 0,

                    };
                    
                    let _response = domain.handle(ctx);
                }
                
                // Assert - implicit success
            });
        });
    }
    
    group.finish();
}

/// Concurrent producer/consumer pattern
fn bench_concurrent_producer_consumer(c: &mut Criterion) {
    c.bench_function("queue_concurrent_producer_consumer", |b| {
        b.iter(|| {
            // Arrange
            let store = midge_adapter::create_memory_store().expect("create store");
            let domain: Arc<QueueDomain> = Arc::clone(&Arc::new(QueueDomain::new(store)));

            // Act - 10 producer threads + 10 consumer threads
            let mut handles = vec![];

            // Producers
            for i in 0..10 {
                let domain = Arc::clone(&domain);
                handles.push(std::thread::spawn(move || {
                    for j in 0..10 {
                        let body = format!("message-{}-{}", i, j).into_bytes();
                        let payload = build_enqueue_payload(&body);
                        let route = build_route("enqueue");
                        let ctx = DomainContext {
                            route,
                            route_str: "queue://realm1/area1/enqueue".to_string(),
                            payload,
                            channel_id: i as u32,
                            route_family: 0,

                        };
                        domain.handle(ctx);
                    }
                }));
            }

            // Consumers
            for i in 0..10 {
                let domain = Arc::clone(&domain);
                handles.push(std::thread::spawn(move || {
                    for _ in 0..10 {
                        let payload = build_reserve_payload(30);
                        let route = build_route("reserve");
                        let ctx = DomainContext {
                            route,
                            route_str: "queue://realm1/area1/reserve".to_string(),
                            payload,
                            channel_id: (100 + i) as u32,
                            route_family: 0,

                        };
                        domain.handle(ctx);
                    }
                }));
            }

            for handle in handles {
                let _ = handle.join();
            }

            // Assert - implicit success
        });
    });
}

/// Message sizes benchmark
fn bench_message_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_message_sizes");
    group.sample_size(10); // Limit iterations to prevent unbounded memory growth
    
    for &size in &[64, 256, 1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let store = midge_adapter::create_memory_store().expect("create store");
                    (QueueDomain::new(store), vec![0u8; size])
                },
                |(domain, body)| {
                    let payload = build_enqueue_payload(&body);
                    let route = build_route("enqueue");
                    
                    let ctx = DomainContext {
                        route,
                        route_str: "queue://realm1/area1/enqueue".to_string(),
                        payload,
                        channel_id: 1,
                        route_family: 0,

                    };
                    
                    let _response = domain.handle(ctx);
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    
    group.finish();
}

criterion_group!(
    name = hotpath_queue_core;
    config = config::criterion_config();
    targets =
        bench_sequential_enqueue,
        bench_sequential_reserve,
        bench_concurrent_producer_consumer,
        bench_message_sizes
);
criterion_main!(hotpath_queue_core);
