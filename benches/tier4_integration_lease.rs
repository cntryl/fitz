//! Lease domain tier 4 integration benchmarks
//!
//! Full end-to-end pipeline latency (protocol encoding/decoding)
//! Measures complete round-trip including serialization

use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::actor::Actor;
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::testkit::lease::create_test_lease_context;
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn bench_full_acquire_pipeline(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/locks/db-migration/acquire");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    let mut group = c.benchmark_group("full_acquire_pipeline");
    group.measurement_time(Duration::from_millis(500));
    group.sampling_mode(SamplingMode::Flat);
    group.bench_function("acquire_with_state_update", |b| {
        b.iter(|| {
            let msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-1".to_string()),
                ttl_secs: black_box(30),
                wait_seconds: 0,
            };
            actor.receive(msg, &mut ctx);
        })
    });
    group.finish();
}

fn bench_full_lifecycle_sequence(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/locks/db-migration/lifecycle");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    // Setup: acquire a lease
    let acquire_msg = LeaseMessage::Acquire {
        family_id: family,
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire_msg, &mut ctx);

    let mut group = c.benchmark_group("full_lifecycle_sequence");
    group.measurement_time(Duration::from_millis(500));
    group.sampling_mode(SamplingMode::Flat);

    let mut phase = 0;
    group.bench_function("acquire_renew_release_cycle", |b| {
        b.iter(|| {
            match phase % 3 {
                0 => {
                    let msg = LeaseMessage::Acquire {
                        family_id: black_box(family),
                        route: black_box(route.clone()),
                        owner_id: black_box("client-1".to_string()),
                        ttl_secs: black_box(30),
                        wait_seconds: 0,
                    };
                    actor.receive(msg, &mut ctx);
                }
                1 => {
                    let msg = LeaseMessage::Renew {
                        family_id: black_box(family),
                        route: black_box(route.clone()),
                        owner_id: black_box("client-1".to_string()),
                        fencing_token: black_box(1),
                        ttl_secs: black_box(30),
                    };
                    actor.receive(msg, &mut ctx);
                }
                2 => {
                    let msg = LeaseMessage::Release {
                        family_id: black_box(family),
                        route: black_box(route.clone()),
                        owner_id: black_box("client-1".to_string()),
                        fencing_token: black_box(1),
                    };
                    actor.receive(msg, &mut ctx);
                }
                _ => unreachable!(),
            }
            phase += 1;
        })
    });
    group.finish();
}

fn bench_multi_resource_leases(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let routes = [
        Route::new("lease://realm/area1/resource1/acquire"),
        Route::new("lease://realm/area2/resource2/acquire"),
        Route::new("lease://realm/area3/resource3/acquire"),
        Route::new("lease://realm/area4/resource4/acquire"),
    ];
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    for (idx, route) in routes.iter().enumerate() {
        let acquire_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: format!("client-{}", idx + 1),
            ttl_secs: 30,
            wait_seconds: 0,
        };
        actor.receive(acquire_msg, &mut ctx);
    }

    let mut group = c.benchmark_group("multi_resource_leases");
    group.measurement_time(Duration::from_millis(500));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(routes.len() as u64));

    let mut idx = 0;
    group.bench_function("renew_across_resources", |b| {
        b.iter(|| {
            idx = (idx + 1) % routes.len();
            let msg = LeaseMessage::Renew {
                family_id: black_box(family),
                route: black_box(routes[idx].clone()),
                owner_id: black_box(format!("client-{}", idx + 1)),
                fencing_token: black_box(1),
                ttl_secs: black_box(30),
            };
            actor.receive(msg, &mut ctx);
        })
    });
    group.finish();
}

fn bench_cross_realm_isolation(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route_realm1 = Route::new("lease://realm1/area/lock1/query");
    let route_realm2 = Route::new("lease://realm2/area/lock2/query");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    let acquire1 = LeaseMessage::Acquire {
        family_id: family,
        route: route_realm1.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire1, &mut ctx);

    let acquire2 = LeaseMessage::Acquire {
        family_id: family,
        route: route_realm2.clone(),
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire2, &mut ctx);

    let mut group = c.benchmark_group("cross_realm_isolation");
    group.measurement_time(Duration::from_millis(500));
    group.sampling_mode(SamplingMode::Flat);

    let mut phase = 0;
    group.bench_function("alternate_realm_operations", |b| {
        b.iter(|| {
            if phase % 2 == 0 {
                let msg = LeaseMessage::Query {
                    family_id: black_box(family),
                    route: black_box(route_realm1.clone()),
                };
                actor.receive(msg, &mut ctx);
            } else {
                let msg = LeaseMessage::Query {
                    family_id: black_box(family),
                    route: black_box(route_realm2.clone()),
                };
                actor.receive(msg, &mut ctx);
            }
            phase += 1;
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_full_acquire_pipeline, bench_full_lifecycle_sequence, bench_multi_resource_leases, bench_cross_realm_isolation
}
criterion_main!(benches);
