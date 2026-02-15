//! Lease domain tier 3 system benchmarks
//!
//! Concurrent lease contention and route isolation measurement
//! Compare baseline vs multi-route contention impact

use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::actor::Actor;
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::testkit::lease::create_test_lease_context;
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn bench_single_route_intensive(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let base_route = "lease://realm/area";
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    let mut group = c.benchmark_group("single_route_intensive");
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(100_u64));

    let mut idx = 0;
    group.bench_function("acquire_release_sequence", |b| {
        b.iter(|| {
            let route_str = format!("{}/lock{}/acquire", base_route, idx);
            let route = Route::new(&route_str);

            let acquire_msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-1".to_string()),
                ttl_secs: black_box(30),
                wait_seconds: 0,
            };
            actor.receive(acquire_msg, &mut ctx);

            let release_msg = LeaseMessage::Release {
                family_id: black_box(family),
                route: black_box(route),
                owner_id: black_box("client-1".to_string()),
                fencing_token: black_box(1),
            };
            actor.receive(release_msg, &mut ctx);

            idx = (idx + 1) % 100;
        })
    });
    group.finish();
}

fn bench_dual_route_concurrent(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route1 = Route::new("lease://realm/area1/lock1/renew");
    let route2 = Route::new("lease://realm/area2/lock2/renew");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    // Setup: acquire leases on both routes
    let acquire1 = LeaseMessage::Acquire {
        family_id: family,
        route: route1.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire1, &mut ctx);

    let acquire2 = LeaseMessage::Acquire {
        family_id: family,
        route: route2.clone(),
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire2, &mut ctx);

    let mut group = c.benchmark_group("dual_route_concurrent");
    group.measurement_time(Duration::from_millis(500));
    group.sampling_mode(SamplingMode::Flat);

    let mut phase = 0;
    group.bench_function("alternate_renew_operations", |b| {
        b.iter(|| {
            if phase % 2 == 0 {
                let msg = LeaseMessage::Renew {
                    family_id: black_box(family),
                    route: black_box(route1.clone()),
                    owner_id: black_box("client-1".to_string()),
                    fencing_token: black_box(1),
                    ttl_secs: black_box(30),
                };
                actor.receive(msg, &mut ctx);
            } else {
                let msg = LeaseMessage::Renew {
                    family_id: black_box(family),
                    route: black_box(route2.clone()),
                    owner_id: black_box("client-2".to_string()),
                    fencing_token: black_box(1),
                    ttl_secs: black_box(30),
                };
                actor.receive(msg, &mut ctx);
            }
            phase += 1;
        })
    });
    group.finish();
}

fn bench_triple_route_contention(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route1 = Route::new("lease://realm/area1/lock1/query");
    let route2 = Route::new("lease://realm/area2/lock2/query");
    let route3 = Route::new("lease://realm/area3/lock3/query");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    // Setup: acquire leases on all three routes
    for (route, client) in &[
        (route1.clone(), "client-1"),
        (route2.clone(), "client-2"),
        (route3.clone(), "client-3"),
    ] {
        let acquire_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: client.to_string(),
            ttl_secs: 30,
            wait_seconds: 0,
        };
        actor.receive(acquire_msg, &mut ctx);
    }

    let mut group = c.benchmark_group("triple_route_contention");
    group.measurement_time(Duration::from_millis(500));
    group.sampling_mode(SamplingMode::Flat);

    let mut phase = 0;
    group.bench_function("round_robin_query_operations", |b| {
        b.iter(|| {
            match phase % 3 {
                0 => {
                    let msg = LeaseMessage::Query {
                        family_id: black_box(family),
                        route: black_box(route1.clone()),
                    };
                    actor.receive(msg, &mut ctx);
                }
                1 => {
                    let msg = LeaseMessage::Query {
                        family_id: black_box(family),
                        route: black_box(route2.clone()),
                    };
                    actor.receive(msg, &mut ctx);
                }
                2 => {
                    let msg = LeaseMessage::Query {
                        family_id: black_box(family),
                        route: black_box(route3.clone()),
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

fn bench_mixed_operations_high_load(c: &mut Criterion) {
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/area/lock1/mixed");
    let mut actor = LeaseActor::new(family);
    let mut ctx = create_test_lease_context(None);

    // Setup: pre-acquire lease
    let acquire_msg = LeaseMessage::Acquire {
        family_id: family,
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire_msg, &mut ctx);

    let mut group = c.benchmark_group("mixed_operations_high_load");
    group.measurement_time(Duration::from_millis(500));
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3_u64));

    let mut phase = 0;
    group.bench_function("cycling_query_renew_operations", |b| {
        b.iter(|| {
            match phase % 3 {
                0 => {
                    let msg = LeaseMessage::Query {
                        family_id: black_box(family),
                        route: black_box(route.clone()),
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
                    let msg = LeaseMessage::Query {
                        family_id: black_box(family),
                        route: black_box(route.clone()),
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

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_single_route_intensive, bench_dual_route_concurrent, bench_triple_route_contention, bench_mixed_operations_high_load
}
criterion_main!(benches);
