use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::lease::service::{LeaseConfig, LeaseService};
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// Shared Tokio runtime
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

// Shared LeaseService instance with bench-optimized config
fn shared_service() -> Arc<LeaseService> {
    rt().block_on(async {
        LeaseService::new_with_config(LeaseConfig {
            disable_timers: true,
        })
    })
}

/// Benchmark: Acquire a lease (no contention)
fn bench_acquire_uncontended(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;
    c.bench_function("lease_acquire_uncontended", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/{}", counter % 64);
            let svc = svc.clone();
            rt().block_on(async move {
                let grant = svc.acquire(key.clone(), 3).await.unwrap();
                // release immediately to avoid leaving the lease active and
                // blocking future iterations that reuse the same key
                let _ = svc.release(key, &grant.id, &grant.token).await;
                grant
            })
        });
    });
}

/// Benchmark: Extend active lease
fn bench_extend(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;
    c.bench_function("lease_extend", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/{}", counter % 64);
            let svc = svc.clone();
            rt().block_on(async move {
                let grant = svc.acquire(key.clone(), 3).await.unwrap();
                let res = svc.extend(key.clone(), &grant.id, &grant.token, 2).await;
                // clean up so the next iteration is uncontended
                let _ = svc.release(key, &grant.id, &grant.token).await;
                res
            })
        });
    });
}

/// Benchmark: Release lease (no waiters)
fn bench_release(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;
    c.bench_function("lease_release_no_waiters", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/{}", counter % 64);
            let svc = svc.clone();
            rt().block_on(async move {
                let grant = svc.acquire(key.clone(), 3).await.unwrap();
                svc.release(key, &grant.id, &grant.token).await
            })
        });
    });
}

/// Benchmark: Peek existing lease
fn bench_peek(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;
    c.bench_function("lease_peek", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/{}", counter % 64);
            let svc = svc.clone();
            rt().block_on(async move {
                let grant = svc.acquire(key.clone(), 3).await.unwrap();
                let res = svc.peek(&key).await;
                let _ = svc.release(key, &grant.id, &grant.token).await;
                res
            })
        });
    });
}

/// Benchmark: Acquire → Release cycle
fn bench_cycle(c: &mut Criterion) {
    let svc = shared_service();
    let mut counter = 0u64;
    c.bench_function("lease_acquire_release_cycle", |b| {
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let key = format!("bench/{}", counter % 64);
            let svc = svc.clone();
            rt().block_on(async move {
                let grant = svc.acquire(key.clone(), 3).await.unwrap();
                svc.release(key, &grant.id, &grant.token).await
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
