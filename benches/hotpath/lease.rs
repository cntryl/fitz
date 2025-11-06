//! Lease domain hotpath benchmarks
//!
//! Extremely fast version using batched async loops and minimal Criterion overhead.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::lease::service::LeaseService;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// Shared Tokio runtime
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

// Shared LeaseService instance
fn shared_service() -> Arc<LeaseService> {
    rt().block_on(async { LeaseService::new() })
}

/// Benchmark: Acquire a lease (no contention)
fn bench_acquire_uncontended(c: &mut Criterion) {
    let svc = shared_service();
    c.bench_function("lease_acquire_uncontended", |b| {
        b.iter_custom(|iters| {
            let svc = svc.clone();
            let rt = rt();
            rt.block_on(async move {
                let start = std::time::Instant::now();
                for i in 0..iters {
                    let key = format!("bench/acquire_uncontended_{}", i % 512);
                    let _ = svc.acquire(key, 60).await.unwrap();
                }
                start.elapsed()
            })
        });
    });
}

/// Benchmark: Extend active lease
fn bench_extend(c: &mut Criterion) {
    let svc = shared_service();
    c.bench_function("lease_extend", |b| {
        b.iter_custom(|iters| {
            let svc = svc.clone();
            let rt = rt();
            rt.block_on(async move {
                let start = std::time::Instant::now();
                for i in 0..iters {
                    let key = format!("bench/extend_{}", i % 512);
                    let grant = svc.acquire(key.clone(), 60).await.unwrap();
                    let _ = svc.extend(key, &grant.id, &grant.token, 30).await;
                }
                start.elapsed()
            })
        });
    });
}

/// Benchmark: Release lease (no waiters)
fn bench_release(c: &mut Criterion) {
    let svc = shared_service();
    c.bench_function("lease_release_no_waiters", |b| {
        b.iter_custom(|iters| {
            let svc = svc.clone();
            let rt = rt();
            rt.block_on(async move {
                let start = std::time::Instant::now();
                for i in 0..iters {
                    let key = format!("bench/release_{}", i % 512);
                    let grant = svc.acquire(key.clone(), 60).await.unwrap();
                    let _ = svc.release(key, &grant.id, &grant.token).await;
                }
                start.elapsed()
            })
        });
    });
}

/// Benchmark: Peek existing lease
fn bench_peek(c: &mut Criterion) {
    let svc = shared_service();
    c.bench_function("lease_peek", |b| {
        b.iter_custom(|iters| {
            let svc = svc.clone();
            let rt = rt();
            rt.block_on(async move {
                let start = std::time::Instant::now();
                for i in 0..iters {
                    let key = format!("bench/peek_{}", i % 512);
                    let _ = svc.acquire(key.clone(), 60).await.unwrap();
                    let _ = svc.peek(&key).await;
                }
                start.elapsed()
            })
        });
    });
}

/// Benchmark: Acquire → Release cycle
fn bench_cycle(c: &mut Criterion) {
    let svc = shared_service();
    c.bench_function("lease_acquire_release_cycle", |b| {
        b.iter_custom(|iters| {
            let svc = svc.clone();
            let rt = rt();
            rt.block_on(async move {
                let start = std::time::Instant::now();
                for i in 0..iters {
                    let key = format!("bench/cycle_{}", i % 512);
                    let grant = svc.acquire(key.clone(), 60).await.unwrap();
                    let _ = svc.release(key, &grant.id, &grant.token).await;
                }
                start.elapsed()
            })
        });
    });
}

criterion_group! {
    name = hotpath_lease;
    config = config::criterion_config();
    targets =
        bench_acquire_uncontended,
        bench_extend,
        bench_release,
        bench_peek,
        bench_cycle,
}

criterion_main!(hotpath_lease);
