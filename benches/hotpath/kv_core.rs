//! Hotpath benchmarks for KV domain - handler -> service layering
//!
//! Tests:
//! - High volume: sequential operations (10k ops)
//! - High concurrency: parallel operations from multiple "threads"
//! - Breakdown points: increasing payload sizes, key counts
//!
//! Goal: Understand where KV performance degrades and optimize accordingly

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fitz::core::domain::{Domain, DomainContext};
use fitz::core::kv::KvDomain;
use fitz::protocol::route::Route;
use fitz::protocol::tags::{TAG_BODY, TAG_ID};
use fitz::storage::midge_adapter;
use std::sync::Arc;

#[path = "../config.rs"]
mod config;

/// Build TLV payload for KV operations
fn build_kv_payload(key: &str, value: Option<&[u8]>) -> Vec<u8> {
    let mut payload = Vec::new();
    
    // TAG_ID (key)
    payload.push(TAG_ID);
    payload.push(key.len() as u8);
    payload.extend_from_slice(key.as_bytes());
    
    // TAG_BODY (value) - optional for GET
    if let Some(v) = value {
        payload.push(TAG_BODY);
        if v.len() <= 254 {
            payload.push(v.len() as u8);
            payload.extend_from_slice(v);
        } else {
            // Extended length encoding
            payload.push(255);
            payload.extend_from_slice(&(v.len() as u32).to_be_bytes());
            payload.extend_from_slice(v);
        }
    }
    
    payload
}

/// Build route for KV operation
fn build_route(operation: &str) -> Route {
    let route_str = format!("kv://realm1/area1/{}", operation);
    Route {
        scheme: fitz::protocol::route::Scheme::Kv,
        realm: Some("realm1".to_string()),
        area: Some("area1".to_string()),
        resource: Some(operation.to_string()),
        operation: None,
        raw: route_str.clone(),
    }
}

/// Sequential PUT operations - tests handler overhead
fn bench_sequential_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_sequential_put");
    group.sample_size(10); // Limit iterations to prevent unbounded memory growth
    
    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_batched(
                || {
                    let store = midge_adapter::create_memory_store().expect("create store");
                    KvDomain::new(store)
                },
                |domain| {
                    for i in 0..count {
                        let key = format!("key{:08}", i);
                        let value = vec![0u8; 64];
                        let payload = build_kv_payload(&key, Some(&value));
                        let route = build_route("put");
                        
                        let ctx = DomainContext {
                            route,
                            route_str: format!("kv://realm1/area1/put"),
                            payload,
                            channel_id: 1,
                            route_family: 0,

                        };
                        
                        black_box(domain.handle(ctx));
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    
    group.finish();
}

/// Sequential GET operations after warm-up
fn bench_sequential_get(c: &mut Criterion) {
    let store = midge_adapter::create_memory_store().expect("create store");
    let domain = KvDomain::new(store);
    
    // Warm-up: insert 10k keys
    for i in 0..10000 {
        let key = format!("key{:08}", i);
        let value = vec![0u8; 64];
        let payload = build_kv_payload(&key, Some(&value));
        let route = build_route("put");
        let ctx = DomainContext {
            route,
            route_str: format!("kv://realm1/area1/put"),
            payload,
            channel_id: 1,
            route_family: 0,

        };
        domain.handle(ctx);
    }
    
    let mut group = c.benchmark_group("kv_sequential_get");
    
    for count in [100, 1000, 10000] {
        group.throughput(Throughput::Elements(count));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                for i in 0..count {
                    let key = format!("key{:08}", i);
                    let payload = build_kv_payload(&key, None);
                    let route = build_route("get");
                    
                    let ctx = DomainContext {
                        route,
                        route_str: format!("kv://realm1/area1/get"),
                        payload,
                        channel_id: 1,
                        route_family: 0,

                    };
                    
                    black_box(domain.handle(ctx));
                }
            });
        });
    }
    
    group.finish();
}

/// Simulated concurrent operations (Arc-shared domain)
fn bench_concurrent_mixed(c: &mut Criterion) {
    let store = midge_adapter::create_memory_store().expect("create store");
    let domain = Arc::new(KvDomain::new(store));
    
    // Warm-up
    for i in 0..1000 {
        let key = format!("key{:08}", i);
        let value = vec![0u8; 64];
        let payload = build_kv_payload(&key, Some(&value));
        let route = build_route("put");
        let ctx = DomainContext {
            route,
            route_str: format!("kv://realm1/area1/put"),
            payload,
            channel_id: 1,
            route_family: 0,

        };
        domain.handle(ctx);
    }
    
    c.bench_function("kv_concurrent_mixed", |b| {
        b.iter(|| {
            // Simulate 100 concurrent operations (50 reads, 50 writes)
            for i in 0..100 {
                let domain: Arc<KvDomain> = Arc::clone(&domain);
                let key = format!("key{:08}", i % 1000);
                
                if i % 2 == 0 {
                    // GET
                    let payload = build_kv_payload(&key, None);
                    let route = build_route("get");
                    let ctx = DomainContext {
                        route,
                        route_str: format!("kv://realm1/area1/get"),
                        payload,
                        channel_id: (i % 10) as u32,
                        route_family: 0,

                    };
                    black_box(domain.handle(ctx));
                } else {
                    // PUT
                    let value = vec![0u8; 64];
                    let payload = build_kv_payload(&key, Some(&value));
                    let route = build_route("put");
                    let ctx = DomainContext {
                        route,
                        route_str: format!("kv://realm1/area1/put"),
                        payload,
                        channel_id: (i % 10) as u32,
                        route_family: 0,

                    };
                    black_box(domain.handle(ctx));
                }
            }
        });
    });
}

/// Test performance breakdown with increasing payload sizes
fn bench_payload_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv_payload_sizes");
    group.sample_size(10); // Limit iterations to prevent unbounded memory growth
    
    for size in [64, 256, 1024, 4096, 16384] {
        group.throughput(Throughput::Bytes(size));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let store = midge_adapter::create_memory_store().expect("create store");
                    (KvDomain::new(store), vec![0u8; size as usize])
                },
                |(domain, value)| {
                    let key = "test_key".to_string();
                    let payload = build_kv_payload(&key, Some(&value));
                    let route = build_route("put");
                    
                    let ctx = DomainContext {
                        route,
                        route_str: format!("kv://realm1/area1/put"),
                        payload,
                        channel_id: 1,
                        route_family: 0,

                    };
                    
                    black_box(domain.handle(ctx));
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    
    group.finish();
}

criterion_group!(
    name = hotpath_kv_core;
    config = config::criterion_config();
    targets = 
        bench_sequential_put,
        bench_sequential_get,
        bench_concurrent_mixed,
        bench_payload_sizes
);
criterion_main!(hotpath_kv_core);
