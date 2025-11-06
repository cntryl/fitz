//! Lease domain hotpath benchmarks
//!
//! Measures performance of critical lease operations:
//! - acquire: First lease acquisition (cold path)
//! - acquire_contended: Multiple waiters competing for same lease
//! - extend: Active lease extension
//! - release: Lease release with waiter handoff
//! - peek: Read-only lease inspection

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fitz::core::lease::service::LeaseService;
use std::sync::Arc;
use tokio::runtime::Runtime;

// Include the shared config module from parent directory
#[path = "../config.rs"]
mod config;

/// Benchmark: Acquire a lease (no contention)
fn bench_lease_acquire_uncontended(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("lease_acquire_uncontended", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Arrange
                let service = LeaseService::new();

                // Act: Acquire lease with no contention
                let grant = service
                    .acquire("bench/resource".to_string(), 60)
                    .await
                    .unwrap();

                // Prevent optimization
                black_box(grant)
            })
        });
    });
}

/// Benchmark: Acquire lease when one already exists (becomes waiter)
fn bench_lease_acquire_contended(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("lease_acquire_contended", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Arrange: Create service with existing lease
                let service = Arc::new(LeaseService::new());
                let _existing = service
                    .acquire("bench/resource".to_string(), 60)
                    .await
                    .unwrap();

                // Act: Second acquire becomes a waiter (non-blocking in bench)
                let service_clone = service.clone();
                let handle = tokio::spawn(async move {
                    service_clone
                        .acquire("bench/resource".to_string(), 30)
                        .await
                });

                // Measure enqueue latency, not waiting time
                black_box(handle.abort());
            })
        });
    });
}

/// Benchmark: Extend an active lease
fn bench_lease_extend(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("lease_extend", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Arrange: Acquire a lease first
                let service = LeaseService::new();
                let grant = service
                    .acquire("bench/resource".to_string(), 60)
                    .await
                    .unwrap();

                // Act: Extend the lease
                let result = service
                    .extend("bench/resource".to_string(), &grant.id, &grant.token, 30)
                    .await;

                black_box(result)
            })
        });
    });
}

/// Benchmark: Release a lease (no waiters)
fn bench_lease_release_no_waiters(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("lease_release_no_waiters", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Arrange: Acquire a lease
                let service = LeaseService::new();
                let grant = service
                    .acquire("bench/resource".to_string(), 60)
                    .await
                    .unwrap();

                // Act: Release the lease
                let result = service
                    .release("bench/resource".to_string(), &grant.id, &grant.token)
                    .await;

                black_box(result)
            })
        });
    });
}

/// Benchmark: Release lease with waiter handoff
fn bench_lease_release_with_waiter(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("lease_release_with_waiter", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Arrange: Acquire lease and enqueue a waiter
                let service = Arc::new(LeaseService::new());
                let grant = service
                    .acquire("bench/resource".to_string(), 60)
                    .await
                    .unwrap();

                let service_clone = service.clone();
                let waiter_handle = tokio::spawn(async move {
                    service_clone
                        .acquire("bench/resource".to_string(), 30)
                        .await
                });

                // Small delay to ensure waiter is enqueued
                tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;

                // Act: Release lease (grants to waiter)
                let result = service
                    .release("bench/resource".to_string(), &grant.id, &grant.token)
                    .await;

                // Cleanup
                let _ = waiter_handle.await;
                black_box(result)
            })
        });
    });
}

/// Benchmark: Peek at active lease
fn bench_lease_peek(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("lease_peek", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Arrange: Acquire a lease
                let service = LeaseService::new();
                let _grant = service
                    .acquire("bench/resource".to_string(), 60)
                    .await
                    .unwrap();

                // Act: Peek at the lease
                let result = service.peek("bench/resource").await;

                black_box(result)
            })
        });
    });
}

/// Benchmark: Peek when no lease exists
fn bench_lease_peek_empty(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("lease_peek_empty", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Arrange: Empty service
                let service = LeaseService::new();

                // Act: Peek at non-existent lease
                let result = service.peek("bench/resource").await;

                black_box(result)
            })
        });
    });
}

/// Benchmark: Token computation (HMAC generation)
fn bench_lease_token_computation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("lease_token_computation", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Arrange
                let service = LeaseService::new();

                // Act: Acquire triggers token computation
                let grant = service
                    .acquire("bench/resource".to_string(), 60)
                    .await
                    .unwrap();

                black_box(grant.token)
            })
        });
    });
}

/// Benchmark: Lease acquire/release cycle
fn bench_lease_acquire_release_cycle(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("lease_acquire_release_cycle", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Arrange
                let service = LeaseService::new();

                // Act: Full cycle
                let grant = service
                    .acquire("bench/resource".to_string(), 60)
                    .await
                    .unwrap();

                let result = service
                    .release("bench/resource".to_string(), &grant.id, &grant.token)
                    .await;

                black_box(result)
            })
        });
    });
}

/// Benchmark: Multiple concurrent leases (different keys)
fn bench_lease_concurrent_different_keys(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("lease_concurrent_different_keys");

    for num_keys in [10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            &num_keys,
            |b, &num_keys| {
                b.iter(|| {
                    rt.block_on(async {
                        // Arrange
                        let service = Arc::new(LeaseService::new());

                        // Act: Acquire multiple leases concurrently
                        let handles: Vec<_> = (0..num_keys)
                            .map(|i| {
                                let service_clone = service.clone();
                                tokio::spawn(async move {
                                    service_clone
                                        .acquire(format!("bench/resource{}", i), 60)
                                        .await
                                })
                            })
                            .collect();

                        let results = futures::future::join_all(handles).await;
                        black_box(results)
                    })
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Queue depth (number of waiters per lease)
fn bench_lease_waiter_queue_depth(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("lease_waiter_queue_depth");

    for num_waiters in [5, 10, 20] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_waiters),
            &num_waiters,
            |b, &num_waiters| {
                b.iter(|| {
                    rt.block_on(async {
                        // Arrange: One active lease
                        let service = Arc::new(LeaseService::new());
                        let grant = service
                            .acquire("bench/resource".to_string(), 60)
                            .await
                            .unwrap();

                        // Act: Enqueue multiple waiters
                        let handles: Vec<_> = (0..num_waiters)
                            .map(|_| {
                                let service_clone = service.clone();
                                tokio::spawn(async move {
                                    service_clone
                                        .acquire("bench/resource".to_string(), 30)
                                        .await
                                })
                            })
                            .collect();

                        // Small delay to ensure all waiters enqueue
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                        // Release and measure handoff to first waiter
                        let result = service
                            .release("bench/resource".to_string(), &grant.id, &grant.token)
                            .await;

                        // Cleanup: abort remaining waiters
                        for handle in handles {
                            handle.abort();
                        }

                        black_box(result)
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = hotpath_lease;
    config = config::criterion_config();
    targets =
        bench_lease_acquire_uncontended,
        bench_lease_acquire_contended,
        bench_lease_extend,
        bench_lease_release_no_waiters,
        bench_lease_release_with_waiter,
        bench_lease_peek,
        bench_lease_peek_empty,
        bench_lease_token_computation,
        bench_lease_acquire_release_cycle,
        bench_lease_concurrent_different_keys,
        bench_lease_waiter_queue_depth,
}

criterion_main!(hotpath_lease);
