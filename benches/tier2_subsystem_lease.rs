//! Lease domain tier 2 subsystem benchmarks
//!
//! Measure acquire/renew/release transaction lifecycle patterns
//! Include handler coordination overhead

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::actor::Actor;
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::testkit::lease::create_test_lease_context;
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn bench_acquire_renew_cycle(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/locks/db-migration/cycle");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    // Pre-acquire so we have a token for renew
    let acquire_msg = LeaseMessage::Acquire {
        family_id: family,
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire_msg, &mut ctx);

    let mut group = c.benchmark_group("acquire_renew_cycle");
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(1_u64));

    group.bench_function("renew_cycle", |b| {
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

fn bench_full_lifecycle(c: &mut Criterion) {
    let family = RouteFamily::new(1);

    let mut group = c.benchmark_group("full_lifecycle");
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(1_u64));

    group.bench_function("acquire_renew_release", |b| {
        b.iter(|| {
            let mut actor = LeaseActor::new(family);
            let mut ctx = create_test_lease_context(None);
            let route = Route::new("lease://realm/locks/lifecycle-test/acquire");

            // Acquire
            let acquire_msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-1".to_string()),
                ttl_secs: black_box(30),
            };
            actor.receive(acquire_msg, &mut ctx);

            // Renew
            let renew_msg = LeaseMessage::Renew {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-1".to_string()),
                fencing_token: black_box(1),
                ttl_secs: black_box(30),
            };
            actor.receive(renew_msg, &mut ctx);

            // Release
            let release_msg = LeaseMessage::Release {
                family_id: black_box(family),
                route: black_box(route),
                owner_id: black_box("client-1".to_string()),
                fencing_token: black_box(1),
            };
            actor.receive(release_msg, &mut ctx);
        })
    });
    group.finish();
}

fn bench_contended_renewal(c: &mut Criterion) {
    let family = RouteFamily::new(1);

    let mut group = c.benchmark_group("contended_renewal");
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(10_u64));

    group.bench_function("ten_concurrent_renewals", |b| {
        b.iter(|| {
            let mut actor = LeaseActor::new(family);
            let mut ctx = create_test_lease_context(None);

            // Setup: create 10 leases
            for i in 0..10 {
                let route = Route::new(format!("lease://realm/locks/lock-{}/acquire", i));
                let acquire_msg = LeaseMessage::Acquire {
                    family_id: family,
                    route,
                    owner_id: format!("client-{}", i),
                    ttl_secs: 30,
                };
                actor.receive(acquire_msg, &mut ctx);
            }

            // Renew all of them
            for i in 0..10 {
                let route = Route::new(format!("lease://realm/locks/lock-{}/renew", i));
                let renew_msg = LeaseMessage::Renew {
                    family_id: black_box(family),
                    route: black_box(route),
                    owner_id: black_box(format!("client-{}", i)),
                    fencing_token: black_box((i + 1) as u64),
                    ttl_secs: black_box(30),
                };
                actor.receive(renew_msg, &mut ctx);
            }
        })
    });
    group.finish();
}

fn bench_token_validation(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/locks/db-migration/token-validation");
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

    let mut group = c.benchmark_group("token_validation");
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(1_u64));

    // Measure cost of fencing (wrong token)
    group.bench_function("renew_with_wrong_token", |b| {
        b.iter(|| {
            let msg = LeaseMessage::Renew {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-1".to_string()),
                fencing_token: black_box(999), // Wrong token
                ttl_secs: black_box(30),
            };
            actor.receive(msg, &mut ctx);
        })
    });
    group.finish();
}

fn bench_multi_owner_contention(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/locks/db-migration/contention");

    let mut group = c.benchmark_group("multi_owner_contention");
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(1_u64));

    group.bench_function("acquire_held_by_other", |b| {
        let mut actor = LeaseActor::new(family);
        let mut ctx = create_test_lease_context(None);

        // Setup: acquire with client-1
        let acquire_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "client-1".to_string(),
            ttl_secs: 30,
        };
        actor.receive(acquire_msg, &mut ctx);

        b.iter(|| {
            let msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-2".to_string()), // Different owner
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
    targets = bench_acquire_renew_cycle,
              bench_full_lifecycle,
              bench_contended_renewal,
              bench_token_validation,
              bench_multi_owner_contention
}
criterion_main!(benches);
