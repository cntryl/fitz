//! Zero-warmup hotpath benchmarks for lease coordination primitives.
//!
//! These tests pre-initialize all async infrastructure and use `iter_custom`
//! for true microsecond-precision measurement with nearly zero warmup time.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::lease::LeaseService;
use fitz::routing::DEFAULT_RF;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

// ---------------------------------------------------------
// Fast Initialization (Happens BEFORE any benchmark runs)
// ---------------------------------------------------------

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    })
}

static SERVICE: OnceLock<Arc<LeaseService>> = OnceLock::new();
fn service() -> Arc<LeaseService> {
    SERVICE
        .get_or_init(|| {
            env::set_var("FITZ_LEASE_SPAWN_EXPIRER", "0");
            rt().block_on(async { LeaseService::new_no_expirer() })
        })
        .clone()
}

static KEYS: OnceLock<Vec<String>> = OnceLock::new();
fn keys() -> &'static [String] {
    KEYS.get_or_init(|| {
        (0..1000)
            .map(|i| format!("lease://bench/area/key_{:04}", i))
            .collect()
    })
}

static UNIQUE: OnceLock<AtomicU64> = OnceLock::new();
fn next_id() -> u64 {
    UNIQUE
        .get_or_init(|| AtomicU64::new(1))
        .fetch_add(1, Ordering::Relaxed)
}

// ---------------------------------------------------------
// ZERO-WARMUP BENCHMARKS
// ---------------------------------------------------------

fn bench_acquire(c: &mut Criterion) {
    let svc = service();
    let list = keys();
    let rf = DEFAULT_RF;

    c.bench_function("lease_acquire", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt().block_on(async {
                for i in 0..iters {
                    let key = &list[(i as usize) % list.len()];
                    let _ = svc.acquire(rf, key, 30).await;
                }
            });
            start.elapsed()
        });
    });
}

fn bench_renew(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;

    c.bench_function("lease_renew", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt().block_on(async {
                for _ in 0..iters {
                    let key = format!("lease://bench/unique/{:016x}", next_id());
                    let grant = svc.acquire(rf, &key, 30).await.unwrap();
                    let _ = svc.renew(rf, &key, &grant.id, &grant.token, 30).await;
                }
            });
            start.elapsed()
        });
    });
}

fn bench_surrender(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;

    c.bench_function("lease_surrender", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt().block_on(async {
                for _ in 0..iters {
                    let key = format!("lease://bench/unique/{:016x}", next_id());
                    let grant = svc.acquire(rf, &key, 30).await.unwrap();
                    let _ = svc.surrender(rf, &key, &grant.id, &grant.token).await;
                }
            });
            start.elapsed()
        });
    });
}

fn bench_acquire_contended(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;
    let key = "lease://bench/contended/resource";

    // Pre-hold lease to force contention
    rt().block_on(async {
        let _ = svc.acquire(rf, key, 300).await.unwrap();
    });

    c.bench_function("lease_acquire_contended", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt().block_on(async {
                for _ in 0..iters {
                    let _ = svc.acquire(rf, key, 30).await;
                }
            });
            start.elapsed()
        });
    });
}

fn bench_realm_isolation(c: &mut Criterion) {
    let svc = service();

    c.bench_function("lease_realm_isolation", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt().block_on(async {
                for _ in 0..iters {
                    let _ = svc.acquire(1, "lease://realm1/area/conflict", 30).await;
                    let _ = svc.acquire(2, "lease://realm2/area/conflict", 30).await;
                }
            });
            start.elapsed()
        });
    });
}

fn bench_concurrent_ops(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;

    c.bench_function("lease_concurrent_operations", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt().block_on(async {
                for i in 0..iters {
                    let key = format!("lease://bench/concurrent/{:06}", i);
                    let _ = svc.acquire(rf, &key, 30).await;
                }
            });
            start.elapsed()
        });
    });
}

fn bench_renew_keepalive(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;

    c.bench_function("lease_renew_keep_alive", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt().block_on(async {
                for _ in 0..iters {
                    let key = format!("lease://bench/unique/{:016x}", next_id());
                    let grant = svc.acquire(rf, &key, 30).await.unwrap();
                    for _ in 0..5 {
                        let _ = svc.renew(rf, &key, &grant.id, &grant.token, 30).await;
                    }
                }
            });
            start.elapsed()
        });
    });
}

fn bench_expiry_handling(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;

    c.bench_function("lease_expiry_handling", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            rt().block_on(async {
                for _ in 0..iters {
                    let key = format!("lease://bench/unique/{:016x}", next_id());
                    let grant = svc.acquire(rf, &key, 30).await.unwrap();
                    let _ = svc.surrender(rf, &key, &grant.id, &grant.token).await;
                    let _ = svc.renew(rf, &key, &grant.id, &grant.token, 30).await;
                }
            });
            start.elapsed()
        });
    });
}

// ---------------------------------------------------------
// Criterion Group
// ---------------------------------------------------------

criterion_group!(
    name = hotpath_lease;
    config = config::criterion_config();
    targets =
        bench_acquire,
        bench_renew,
        bench_surrender,
        bench_acquire_contended,
        bench_realm_isolation,
        bench_concurrent_ops,
        bench_renew_keepalive,
        bench_expiry_handling,
);

criterion_main!(hotpath_lease);
