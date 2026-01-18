//! Lease domain tier 1 hotpath benchmarks
//!
//! Pure acquire/renew/release operation latency
//! Target: <1 µs per operation

use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::testkit::lease::create_test_lease_context;
use fitz::runtime::actor::Actor;

#[path = "config.rs"]
mod config;

fn bench_lease_acquire(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/locks/db-migration/acquire");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    let mut group = c.benchmark_group("lease_acquire");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("acquire_first_lease", |b| {
        b.iter(|| {
            let msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-1".to_string()),
                ttl_secs: black_box(30),
            };
            actor.receive(msg, &mut ctx);
        })
    });
    group.finish();
}

fn bench_lease_renew(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/locks/db-migration/renew");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    // Setup: acquire a lease first
    let acquire_msg = LeaseMessage::Acquire {
        family_id: family,
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire_msg, &mut ctx);

    let mut group = c.benchmark_group("lease_renew");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("renew_existing_lease", |b| {
        b.iter(|| {
            let msg = LeaseMessage::Renew {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-1".to_string()),
                fencing_token: black_box(1),
                ttl_secs: black_box(30),
            };
            actor.receive(msg, &mut ctx);
        })
    });
    group.finish();
}

fn bench_lease_release(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/locks/db-migration/release");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    // Setup: acquire a lease first
    let acquire_msg = LeaseMessage::Acquire {
        family_id: family,
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire_msg, &mut ctx);

    let mut group = c.benchmark_group("lease_release");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("release_held_lease", |b| {
        b.iter(|| {
            let msg = LeaseMessage::Release {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-1".to_string()),
                fencing_token: black_box(1),
            };
            actor.receive(msg, &mut ctx);
        })
    });
    group.finish();
}

fn bench_lease_query(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/locks/db-migration/query");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    // Setup: acquire a lease
    let acquire_msg = LeaseMessage::Acquire {
        family_id: family,
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire_msg, &mut ctx);

    let mut group = c.benchmark_group("lease_query");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("query_lease_status", |b| {
        b.iter(|| {
            let msg = LeaseMessage::Query {
                family_id: black_box(family),
                route: black_box(route.clone()),
            };
            actor.receive(msg, &mut ctx);
        })
    });
    group.finish();
}

fn bench_lease_idempotent_acquire(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/locks/db-migration/acquire");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    // Setup: acquire once to establish baseline
    let acquire_msg = LeaseMessage::Acquire {
        family_id: family,
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire_msg, &mut ctx);

    let mut group = c.benchmark_group("lease_idempotent_acquire");
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("acquire_when_already_held", |b| {
        b.iter(|| {
            let msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-1".to_string()),
                ttl_secs: black_box(30),
            };
            actor.receive(msg, &mut ctx);
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_lease_acquire, bench_lease_renew, bench_lease_release, bench_lease_query, bench_lease_idempotent_acquire
}
criterion_main!(benches);
