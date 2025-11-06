//! Lease domain hotpath benchmarks
//!
//! Optimized for speed: single global runtime, shared LeaseService, no sleeps.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use fitz::core::lease::service::LeaseService;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// Shared Tokio runtime (initialized once)
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

// Shared lease service for most benches
fn shared_service() -> Arc<LeaseService> {
    rt().block_on(async { LeaseService::new() })
}

/// Benchmark: Acquire a lease (no contention)
fn bench_lease_acquire_uncontended(c: &mut Criterion) {
    let service = shared_service();

    c.bench_function("lease_acquire_uncontended", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/resource_{}", counter % 1000);
            let svc = service.clone();
            rt().block_on(async move { black_box(svc.acquire(key, 60).await.unwrap()) })
        });
    });
}

/// Benchmark: Acquire lease with contention (enqueue waiter only)
fn bench_lease_acquire_contended(c: &mut Criterion) {
    let service = shared_service();

    c.bench_function("lease_acquire_contended", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/resource_{}", counter % 1000);
            let svc = service.clone();

            rt().block_on(async move {
                let _ = svc.acquire(key.clone(), 60).await.unwrap();
                let waiter = svc.clone();
                let handle = tokio::spawn(async move { waiter.acquire(key, 30).await });
                handle.abort(); // don't wait
            })
        });
    });
}

/// Benchmark: Extend active lease
fn bench_lease_extend(c: &mut Criterion) {
    let service = shared_service();

    c.bench_function("lease_extend", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/resource_{}", counter % 1000);
            let svc = service.clone();
            rt().block_on(async move {
                let grant = svc.acquire(key.clone(), 60).await.unwrap();
                black_box(svc.extend(key, &grant.id, &grant.token, 30).await)
            })
        });
    });
}

/// Benchmark: Release lease (no waiters)
fn bench_lease_release_no_waiters(c: &mut Criterion) {
    let service = shared_service();

    c.bench_function("lease_release_no_waiters", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/resource_{}", counter % 1000);
            let svc = service.clone();
            rt().block_on(async move {
                let grant = svc.acquire(key.clone(), 60).await.unwrap();
                black_box(svc.release(key, &grant.id, &grant.token).await)
            })
        });
    });
}

/// Benchmark: Peek active lease
fn bench_lease_peek(c: &mut Criterion) {
    let service = shared_service();

    c.bench_function("lease_peek", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/resource_{}", counter % 1000);
            let svc = service.clone();
            rt().block_on(async move {
                let _ = svc.acquire(key.clone(), 60).await.unwrap();
                black_box(svc.peek(&key).await)
            })
        });
    });
}

/// Benchmark: Peek at missing lease
fn bench_lease_peek_empty(c: &mut Criterion) {
    let service = shared_service();

    c.bench_function("lease_peek_empty", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/empty_{}", counter % 1000);
            let svc = service.clone();
            rt().block_on(async move { black_box(svc.peek(&key).await) })
        });
    });
}

/// Benchmark: Full acquire → release cycle
fn bench_lease_acquire_release_cycle(c: &mut Criterion) {
    let service = shared_service();

    c.bench_function("lease_acquire_release_cycle", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/resource_{}", counter % 1000);
            let svc = service.clone();
            rt().block_on(async move {
                let grant = svc.acquire(key.clone(), 60).await.unwrap();
                black_box(svc.release(key, &grant.id, &grant.token).await)
            })
        });
    });
}

/// Benchmark: Multiple concurrent keys
fn bench_lease_concurrent_different_keys(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_concurrent_different_keys");

    for &num_keys in &[10, 50, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(num_keys), &num_keys, |b, &num_keys| {
            b.iter(|| {
                rt().block_on(async {
                    let svc = LeaseService::new();
                    let tasks: Vec<_> = (0..num_keys)
                        .map(|i| {
                            let s = svc.clone();
                            tokio::spawn(async move {
                                s.acquire(format!("bench/resource{}", i), 60).await
                            })
                        })
                        .collect();
                    black_box(futures::future::join_all(tasks).await)
                })
            });
        });
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
        bench_lease_peek,
        bench_lease_peek_empty,
        bench_lease_acquire_release_cycle,
        bench_lease_concurrent_different_keys,
}

criterion_main!(hotpath_lease);
