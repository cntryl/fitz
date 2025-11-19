//! Hotpath benchmarks for Lease domain.
//!
//! Measures ONLY the internal logic of the Lease service:
//!   - Acquire: request lease on resource
//!   - Renew: extend existing lease
//!   - Surrender: voluntarily release lease
//!
//! Zero frame parsing, zero engine, zero outbound delivery.
//! This is the true "business logic" bench.

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use fitz::core::lease::LeaseService;

#[path = "../config.rs"]
mod config;

// -----------------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------------

fn bench_hot_acquire(c: &mut Criterion) {
    let svc = LeaseService::new();

    let mut group = c.benchmark_group("lease_hot_acquire");
    group.bench_function("acquire", |b| {
        b.iter_batched(
            || LeaseService::new(),
            |svc| {
                svc.acquire(0, "lease://realm/area/resource", 300).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_renew(c: &mut Criterion) {
    let svc = LeaseService::new();
    // Pre-acquire a lease
    let grant = svc.acquire(0, "lease://realm/area/resource", 300).unwrap();

    let mut group = c.benchmark_group("lease_hot_renew");
    group.bench_function("renew", |b| {
        b.iter_batched(
            || {
                let svc = LeaseService::new();
                let grant = svc.acquire(0, "lease://realm/area/resource", 300).unwrap();
                (svc, grant)
            },
            |(svc, grant)| {
                svc.renew(0, "lease://realm/area/resource", &grant.id, &grant.token, 300).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn bench_hot_surrender(c: &mut Criterion) {
    let svc = LeaseService::new();

    let mut group = c.benchmark_group("lease_hot_surrender");
    group.bench_function("surrender", |b| {
        b.iter_batched(
            || {
                let svc = LeaseService::new();
                let grant = svc.acquire(0, "lease://realm/area/resource", 300).unwrap();
                (svc, grant)
            },
            |(svc, grant)| {
                svc.surrender(0, "lease://realm/area/resource", grant.id, grant.token).unwrap();
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

criterion_group!(
    name = hotpath_lease;
    config = config::criterion_config();
    targets =
        bench_hot_acquire,
        bench_hot_renew,
        bench_hot_surrender
);
criterion_main!(hotpath_lease);