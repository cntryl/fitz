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

/// Sequential append operations
fn bench_sequential_append(c: &mut Criterion) {
    let store = midge_adapter::create_memory_store().expect("create store");
    let domain = StreamDomain::new(store);
    
    let mut group = c.benchmark_group("stream_sequential_append");
    
    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                // Arrange & Act
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
                        sender: None,
                    };
                    
                    let _response = domain.handle(ctx);
                }
                
                // Assert - implicit success
            });
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
                                sender: None,
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
    
    for &size in &[64, 256, 1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            let store = midge_adapter::create_memory_store().expect("create store");
            let domain = StreamDomain::new(store);
            
            b.iter(|| {
                // Arrange
                let body = vec![0u8; size];
                let payload = build_append_payload(&body);
                let route = build_route("append");
                
                // Act
                let ctx = DomainContext {
                    route,
                    route_str: "stream://realm1/area1/append".to_string(),
                    payload,
                    channel_id: 1,
                    route_family: 0,
                    sender: None,
                };
                
                let _response = domain.handle(ctx);
                
                // Assert - implicit success
            });
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
                                sender: None,
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

criterion_group!(
    name = hotpath_stream_core;
    config = config::criterion_config();
    targets =
        bench_sequential_append,
        bench_concurrent_writers,
        bench_event_sizes,
        bench_multitenant_append
);
criterion_main!(hotpath_stream_core);
