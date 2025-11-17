//! Hotpath benchmarks for Stream domain handler->service layer
//!
//! Tests stream append/read operations, watermark tracking, event batching,
//! and concurrent writer patterns.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fitz::core::domain::{Domain, DomainContext};
use fitz::core::stream::StreamDomain;
use fitz::protocol::route::Route;
use fitz::protocol::tags::{TAG_BODY, TAG_METADATA, TAG_SEQ};
use fitz::storage::midge_adapter;
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

/// Build TLV payload for stream append
fn build_append_payload(body: &[u8]) -> Vec<u8> {
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

/// Build TLV payload for stream append with metadata
#[allow(dead_code)]
fn build_append_with_metadata_payload(body: &[u8], metadata: &[u8]) -> Vec<u8> {
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
    
    // TAG_METADATA
    payload.push(TAG_METADATA);
    if metadata.len() <= 254 {
        payload.push(metadata.len() as u8);
        payload.extend_from_slice(metadata);
    } else {
        payload.push(255);
        payload.extend_from_slice(&(metadata.len() as u32).to_be_bytes());
        payload.extend_from_slice(metadata);
    }
    
    payload
}

/// Build TLV payload for stream read
#[allow(dead_code)]
fn build_read_payload(from_seq: u64) -> Vec<u8> {
    let mut payload = Vec::new();
    
    // TAG_SEQ
    payload.push(TAG_SEQ);
    payload.push(8);
    payload.extend_from_slice(&from_seq.to_be_bytes());
    
    payload
}

/// Build route for stream operation
fn build_route(operation: &str) -> Route {
    let route_str = format!("stream://realm1/area1/{}", operation);
    Route {
        scheme: fitz::protocol::route::Scheme::Stream,
        realm: Some("realm1".to_string()),
        area: Some("area1".to_string()),
        resource: Some(operation.to_string()),
        operation: None,
        raw: route_str.clone(),
    }
}

/// Build route for stream operation with explicit operation segment
fn build_route_with_operation(resource: &str, operation: &str) -> Route {
    let route_str = format!("stream://realm1/area1/{}/{}", resource, operation);
    Route {
        scheme: fitz::protocol::route::Scheme::Stream,
        realm: Some("realm1".to_string()),
        area: Some("area1".to_string()),
        resource: Some(resource.to_string()),
        operation: Some(operation.to_string()),
        raw: route_str,
    }
}

/// Sequential append operations
fn bench_sequential_append(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_sequential_append");
    group.sample_size(10); // Limit iterations to prevent unbounded memory growth
    
    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_batched(
                || {
                    let store = midge_adapter::create_memory_store().expect("create store");
                    StreamDomain::new(store)
                },
                |domain| {
                    for i in 0..count {
                        let body = format!("event-{}", i).into_bytes();
                        let payload = build_append_payload(&body);
                        let route = build_route("append");
                        
                        let ctx = DomainContext {
                            route,
                            route_str: "stream://realm1/area1/append".to_string(),
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

/// Concurrent writers
fn bench_concurrent_writers(c: &mut Criterion) {
    c.bench_function("stream_concurrent_writers", |b| {
        b.iter(|| {
            // Arrange
            let store = midge_adapter::create_memory_store().expect("create store");
            let domain: Arc<StreamDomain> = Arc::clone(&Arc::new(StreamDomain::new(store)));

            // Act - 10 concurrent writers, each writing 10 events
            let handles: Vec<_> = (0..10)
                .map(|i| {
                    let domain = Arc::clone(&domain);
                    std::thread::spawn(move || {
                        for j in 0..10 {
                            let body = format!("event-{}-{}", i, j).into_bytes();
                            let payload = build_append_payload(&body);
                            let route = build_route("append");
                            let ctx = DomainContext {
                                route,
                                route_str: "stream://realm1/area1/append".to_string(),
                                payload,
                                channel_id: i as u32,
                                route_family: 0,

                            };
                            domain.handle(ctx);
                        }
                    })
                })
                .collect();

            for handle in handles {
                let _ = handle.join();
            }

            // Assert - implicit success
        });
    });
}

/// Event sizes benchmark
fn bench_event_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("stream_event_sizes");
    group.sample_size(10); // Limit iterations to prevent unbounded memory growth
    
    for &size in &[64, 256, 1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    // Create fresh store and domain per iteration to prevent memory accumulation
                    let store = midge_adapter::create_memory_store().expect("create store");
                    let domain = StreamDomain::new(store);
                    (domain, vec![0u8; size])
                },
                |(domain, body)| {
                    // Act
                    let payload = build_append_payload(&body);
                    let route = build_route("append");
                    let ctx = DomainContext {
                        route,
                        route_str: "stream://realm1/area1/append".to_string(),
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

/// Multi-tenant concurrent append
fn bench_multitenant_append(c: &mut Criterion) {
    c.bench_function("stream_multitenant_append", |b| {
        b.iter(|| {
            // Arrange
            let store = midge_adapter::create_memory_store().expect("create store");
            let domain: Arc<StreamDomain> = Arc::clone(&Arc::new(StreamDomain::new(store)));

            // Act - 10 tenants (route families), each appending 10 events
            let handles: Vec<_> = (0..10)
                .map(|tenant_id| {
                    let domain = Arc::clone(&domain);
                    std::thread::spawn(move || {
                        for i in 0..10 {
                            let body = format!("tenant{}-event{}", tenant_id, i).into_bytes();
                            let payload = build_append_payload(&body);
                            let route = build_route("append");
                            let ctx = DomainContext {
                                route,
                                route_str: "stream://realm1/area1/append".to_string(),
                                payload,
                                channel_id: tenant_id as u32,
                                route_family: tenant_id as u32,

                            };
                            domain.handle(ctx);
                        }
                    })
                })
                .collect();

            for handle in handles {
                let _ = handle.join();
            }

            // Assert - implicit success
        });
    });
}

/// Sequential read operations after warm-up
fn bench_sequential_read(c: &mut Criterion) {
    // Warm-up dataset: 10k appends
    let store = midge_adapter::create_memory_store().expect("create store");
    let domain = StreamDomain::new(store);
    for i in 0..10_000 {
        let body = format!("event-{}", i).into_bytes();
        let payload = build_append_payload(&body);
        let route = build_route("append");
        let ctx = DomainContext {
            route,
            route_str: "stream://realm1/area1/append".to_string(),
            payload,
            channel_id: 1,
            route_family: 0,

        };
        let _ = domain.handle(ctx);
    }

    let mut group = c.benchmark_group("stream_sequential_read");
    for &count in &[100usize, 1000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &_count| {
            b.iter(|| {
                // Read starting at 0 with default limit
                let payload = build_read_payload(0);
                let route = build_route_with_operation("resource1", "read");
                let ctx = DomainContext {
                    route,
                    route_str: "stream://realm1/area1/resource1/read".to_string(),
                    payload,
                    channel_id: 2,
                    route_family: 0,

                };
                let _ = domain.handle(ctx);
            });
        });
    }
    group.finish();
}

/// Range read by moving start sequence
fn bench_range_read(c: &mut Criterion) {
    let store = midge_adapter::create_memory_store().expect("create store");
    let domain = StreamDomain::new(store);
    // Warm up smaller dataset
    for i in 0..2_000 {
        let body = format!("event-{}", i).into_bytes();
        let payload = build_append_payload(&body);
        let route = build_route("append");
        let ctx = DomainContext {
            route,
            route_str: "stream://realm1/area1/append".to_string(),
            payload,
            channel_id: 1,
            route_family: 0,

        };
        let _ = domain.handle(ctx);
    }

    let mut group = c.benchmark_group("stream_range_read");
    for &start in &[0u64, 500, 1_000, 1_500] {
        group.bench_with_input(BenchmarkId::from_parameter(start), &start, |b, &start| {
            b.iter(|| {
                let payload = build_read_payload(start);
                let route = build_route_with_operation("resource1", "read");
                let ctx = DomainContext {
                    route,
                    route_str: "stream://realm1/area1/resource1/read".to_string(),
                    payload,
                    channel_id: 3,
                    route_family: 0,

                };
                let _ = domain.handle(ctx);
            });
        });
    }
    group.finish();
}

criterion_group!(
    name = hotpath_stream_core;
    config = config::criterion_config();
    targets =
        bench_sequential_append,
        bench_concurrent_writers,
        bench_event_sizes,
    bench_multitenant_append,
    bench_sequential_read,
    bench_range_read
);
criterion_main!(hotpath_stream_core);
