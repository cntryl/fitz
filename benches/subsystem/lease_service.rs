//! Subsystem Bench: LeaseService
//!
//! This file mirrors existing `benches/hotpath/lease.rs` but is conceptually a
//! subsystem benchmark exercising the full `LeaseService` behavior (no E2E
//! routing, but real DashMap, RwLock, HMAC etc.).

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::lease::LeaseService;
use fitz::routing::DEFAULT_RF;
use std::env;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

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

const MAX_ITERS: u64 = 5_000;

fn bench_acquire(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;

    c.bench_function("lease_acquire_subsystem", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();
            rt().block_on(async {
                for i in 0..MAX_ITERS {
                    let key = format!("lease://bench/area/key_{:04}", i % 1024);
                    let _ = svc.acquire(rf, &key, 30).await;
                }
            });
            start.elapsed()
        })
    });
}

fn bench_renew(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;

    c.bench_function("lease_renew_subsystem", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();
            rt().block_on(async {
                // Pre-acquire leases for renewal
                let mut grants = Vec::new();
                for i in 0..(MAX_ITERS / 10) { // Fewer leases since renew is more targeted
                    let key = format!("lease://bench/renew/key_{:04}", i % 256);
                    if let Ok(grant) = svc.acquire(rf, &key, 30).await {
                        grants.push((key, grant));
                    }
                }

                // Now benchmark renews
                for (key, grant) in &grants {
                    let _ = svc.renew(rf, key, &grant.id, &grant.token, 30).await;
                }

                // Cleanup
                for (key, grant) in grants {
                    let _ = svc.surrender(rf, &key, &grant.id, &grant.token).await;
                }
            });
            start.elapsed()
        })
    });
}

fn bench_release(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;

    c.bench_function("lease_surrender_subsystem", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();
            rt().block_on(async {
                // Pre-acquire leases for release
                let mut grants = Vec::new();
                for i in 0..(MAX_ITERS / 10) {
                    let key = format!("lease://bench/surrender/key_{:04}", i % 256);
                    if let Ok(grant) = svc.acquire(rf, &key, 30).await {
                        grants.push((key, grant));
                    }
                }

                // Benchmark surrenders
                for (key, grant) in grants {
                    let _ = svc.surrender(rf, &key, &grant.id, &grant.token).await;
                }
            });
            start.elapsed()
        })
    });
}

fn bench_contention(c: &mut Criterion) {
    let svc = service();
    let rf = DEFAULT_RF;

    c.bench_function("lease_contention_subsystem", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();
            rt().block_on(async {
                let contended_key = "lease://bench/contention/hot_resource";

                // Acquire initial lease (will cause contention)
                let holder = svc.acquire(rf, contended_key, 30).await.unwrap();

                // Spawn multiple waiters
                let mut waiters = Vec::new();
                for _ in 0..10 {
                    let svc_clone = svc.clone();
                    let waiter = tokio::spawn(async move {
                        let _ = svc_clone.acquire(rf, contended_key, 5).await;
                    });
                    waiters.push(waiter);
                }

                // Give waiters time to queue up
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                // Surrender to trigger grant to waiters
                let _ = svc.surrender(rf, contended_key, &holder.id, &holder.token).await;

                // Wait for all waiters to complete
                for waiter in waiters {
                    let _ = waiter.await;
                }
            });
            start.elapsed()
        })
    });
}

fn bench_concurrent_operations(c: &mut Criterion) {
    let svc = Arc::new(service());
    let rf = DEFAULT_RF;

    c.bench_function("lease_concurrent_operations_subsystem", |b| {
        b.iter_custom(|_| {
            let start = Instant::now();
            rt().block_on(async {
                let mut tasks = Vec::new();

                // Spawn concurrent operations across different keys
                for i in 0..50 {
                    let svc_clone = svc.clone();
                    let task = tokio::spawn(async move {
                        let key = format!("lease://bench/concurrent/key_{:04}", i);

                        // Acquire
                        let grant = svc_clone.acquire(rf, &key, 10).await.unwrap();

                        // Renew
                        let _ = svc_clone.renew(rf, &key, &grant.id, &grant.token, 5).await;

                        // Surrender
                        let _ = svc_clone.surrender(rf, &key, &grant.id, &grant.token).await;
                    });
                    tasks.push(task);
                }

                // Wait for all concurrent operations to complete
                for task in tasks {
                    let result = task.await;
                    assert!(result.is_ok());
                }
            });
            start.elapsed()
        })
    });
}

fn bench_domain_sync_operations(c: &mut Criterion) {
    let svc = service();

    c.bench_function("lease_domain_sync_token_generation", |b| {
        b.iter(|| {
            let key = "lease://bench/sync/key";
            let id = "test-id";
            let expiry = Instant::now() + Duration::from_secs(30);
            let _token = svc.bench_token_generation(key, id, expiry);
        })
    });
}

fn bench_domain_sync_state_transitions(c: &mut Criterion) {
    let svc = service();

    c.bench_function("lease_domain_sync_state_transitions", |b| {
        b.iter(|| {
            let _transitions = svc.bench_lease_state_transitions();
        })
    });
}

fn bench_domain_sync_uuid_generation(c: &mut Criterion) {
    let svc = service();

    c.bench_function("lease_domain_sync_uuid_generation", |b| {
        b.iter(|| {
            let _uuid = svc.bench_uuid_generation();
        })
    });
}

criterion_group!(
    name = subsystem_lease_service;
    config = config::criterion_config();
    targets =
        bench_acquire,
        bench_renew,
        bench_release,
        bench_contention,
        bench_concurrent_operations,
        bench_domain_sync_operations,
        bench_domain_sync_state_transitions,
        bench_domain_sync_uuid_generation,
);
criterion_main!(subsystem_lease_service);
