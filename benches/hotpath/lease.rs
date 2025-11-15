//! Hotpath benchmarks for lease coordination primitives
//!
//! These benchmarks test the core lease operations that are performance-critical:
//! acquire, renew, release, and contention scenarios.

use criterion::{criterion_group, criterion_main, Criterion};
use fitz::core::lease::service::LeaseService;
use fitz::routing::DEFAULT_RF;
use std::sync::{Arc, OnceLock};
use tokio::runtime::Runtime;

#[path = "../config.rs"]
mod config;

static TEST_KEYS: OnceLock<Vec<String>> = OnceLock::new();
fn test_keys() -> &'static [String] {
    TEST_KEYS.get_or_init(|| {
        (0..1000)
            .map(|i| format!("lease://test/area/resource_{:04}", i))
            .collect()
    })
}
static RT: OnceLock<Runtime> = OnceLock::new();
fn rt() -> &'static Runtime {
    RT.get_or_init(|| Runtime::new().expect("runtime"))
}

static LEASE_SERVICE: OnceLock<Arc<LeaseService>> = OnceLock::new();
fn lease_service() -> Arc<LeaseService> {
    LEASE_SERVICE.get_or_init(|| {
        std::env::set_var("FITZ_LEASE_SPAWN_EXPIRER", "0");
        rt().block_on(async {
            LeaseService::new_no_expirer()
        })
    }).clone()
}

// ---------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------

fn bench_lease_acquire(c: &mut Criterion) {
    let service = lease_service();
    let rf = DEFAULT_RF;
    let keys = test_keys();
    let mut counter = 0;

    c.bench_function("lease_acquire", |b| {
        b.iter(|| {
            let key = &keys[counter % keys.len()];
            counter += 1;
            rt().block_on(async {
                let result = service.acquire(rf, key, 30).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_lease_renew(c: &mut Criterion) {
    let service = lease_service();
    let rf = DEFAULT_RF;
    let keys = test_keys();
    let mut counter = 0;

    c.bench_function("lease_renew", |b| {
        b.iter_batched(
            || {
                // Setup: acquire lease
                let key = keys[counter % keys.len()].clone();
                counter += 1;
                let grant = rt().block_on(async {
                    service.acquire(rf, &key, 30).await.unwrap()
                });
                (key, grant)
            },
            |(resource, grant)| {
                rt().block_on(async {
                    let result = service.renew(rf, &resource, &grant.id, &grant.token, 30).await;
                    criterion::black_box(result.ok());
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_lease_surrender(c: &mut Criterion) {
    let service = lease_service();
    let rf = DEFAULT_RF;
    let keys = test_keys();
    let mut counter = 0;

    c.bench_function("lease_surrender", |b| {
        b.iter_batched(
            || {
                // Setup: acquire lease
                let key = keys[counter % keys.len()].clone();
                counter += 1;
                let grant = rt().block_on(async {
                    service.acquire(rf, &key, 30).await.unwrap()
                });
                (key, grant)
            },
            |(resource, grant)| {
                rt().block_on(async {
                    let result = service.surrender(rf, &resource, &grant.id, &grant.token).await;
                    criterion::black_box(result.ok());
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_lease_acquire_contended(c: &mut Criterion) {
    let service = lease_service();
    let rf = DEFAULT_RF;

    // Setup: one lease is already held
    let contended_key = "lease://test/area/contended_resource";
    rt().block_on(async {
        let _ = service.acquire(rf, contended_key, 300).await.unwrap();
    });

    c.bench_function("lease_acquire_contended", |b| {
        b.iter(|| {
            rt().block_on(async {
                let result = service.acquire(rf, contended_key, 30).await;
                criterion::black_box(result.ok());
            });
        })
    });
}

fn bench_lease_multi_tenant_isolation(c: &mut Criterion) {
    let service = lease_service();

    c.bench_function("lease_multi_tenant_isolation", |b| {
        b.iter(|| {
            rt().block_on(async {
                let rf1 = 1u32;
                let rf2 = 2u32;

                // Same key in different tenants should not conflict
                let result1 = service.acquire(rf1, "lease://tenant1/area/shared_key", 30).await;
                let result2 = service.acquire(rf2, "lease://tenant2/area/shared_key", 30).await;

                criterion::black_box((result1.ok(), result2.ok()));
            });
        })
    });
}

fn bench_lease_concurrent_operations(c: &mut Criterion) {
    let service = lease_service();

    c.bench_function("lease_concurrent_operations", |b| {
        b.iter(|| {
            rt().block_on(async {
                let rf = DEFAULT_RF;
                let mut handles = Vec::new();

                // Launch multiple concurrent operations
                for i in 0..10 {
                    let service_clone = Arc::clone(&service);
                    let key = format!("lease://test/area/concurrent_{}", i);
                    handles.push(tokio::spawn(async move {
                        service_clone.acquire(rf, &key, 30).await
                    }));
                }

                for handle in handles {
                    let result = handle.await.unwrap();
                    criterion::black_box(result.ok());
                }
            });
        })
    });
}

fn bench_lease_renew_keep_alive(c: &mut Criterion) {
    let service = lease_service();
    let rf = DEFAULT_RF;
    let keys = test_keys();
    let mut counter = 0;

    c.bench_function("lease_renew_keep_alive", |b| {
        b.iter_batched(
            || {
                // Setup: acquire a lease
                let key = keys[counter % keys.len()].clone();
                counter += 1;
                let grant = rt().block_on(async {
                    service.acquire(rf, &key, 30).await.unwrap()
                });
                (key, grant)
            },
            |(resource, grant)| {
                rt().block_on(async {
                    // Simulate keep-alive by renewing multiple times
                    for _ in 0..5 {
                        let result = service.renew(rf, &resource, &grant.id, &grant.token, 30).await;
                        criterion::black_box(result.ok());
                    }
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

fn bench_lease_expiry_handling(c: &mut Criterion) {
    let service = lease_service();
    let rf = DEFAULT_RF;
    let keys = test_keys();
    let mut counter = 0;

    c.bench_function("lease_expiry_handling", |b| {
        b.iter_batched(
            || {
                // Setup: acquire a lease and immediately surrender it so that
                // subsequent renew behaves like an "expired"/invalid lease
                // without incurring long wall-clock sleeps in the hotpath bench.
                let key = keys[counter % keys.len()].clone();
                counter += 1;
                let grant = rt().block_on(async {
                    let grant = service.acquire(rf, &key, 30).await.unwrap();
                    service
                        .surrender(rf, &key, &grant.id, &grant.token)
                        .await
                        .unwrap();
                    grant
                });
                (key, grant)
            },
            |(resource, grant)| {
                rt().block_on(async {
                    // Renew against an already-surrendered lease to exercise
                    // the failure path without time-based expiry.
                    let result = service.renew(rf, &resource, &grant.id, &grant.token, 30).await;
                    criterion::black_box(result.ok());
                });
            },
            criterion::BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    name = hotpath_lease;
    config = config::criterion_config();
    targets =
        bench_lease_acquire,
        bench_lease_renew,
        bench_lease_surrender,
        bench_lease_acquire_contended,
        bench_lease_multi_tenant_isolation,
        bench_lease_concurrent_operations,
        bench_lease_renew_keep_alive,
        bench_lease_expiry_handling
);

criterion_main!(hotpath_lease);


