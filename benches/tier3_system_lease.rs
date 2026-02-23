//! Lease domain tier 3 system benchmarks using stress
//!
//! Concurrent lease contention and route isolation measurement
//! Tests impact of route isolation and client contention on performance
//!
//! Each test measures a single operation with all setup/teardown outside the measurement loop.
//! Target: ops/sec via set_elements(count)

use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::actor::Actor;
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::testkit::lease::create_test_lease_context;

#[stress_test]
fn should_complete_acquire_release_sequence(ctx: &mut StressContext) {
    ctx.set_elements(100);
    ctx.tag("scenario", "single_route_intensive");

    // Setup: Create actor and context
    let family = RouteFamily::new(1);
    let base_route = "lease://realm/area";
    let mut actor = LeaseActor::new(family);
    let mut bench_ctx = create_test_lease_context(None);

    let mut idx = 0;
    ctx.measure(|| {
        let route_str = format!("{}/lock{}/acquire", base_route, idx);
        let route = Route::new(&route_str);

        // Acquire
        let acquire_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "client-1".to_string(),
            ttl_secs: 30,
            wait_seconds: 0,
        };
        actor.receive(acquire_msg, &mut bench_ctx);

        // Release
        let release_msg = LeaseMessage::Release {
            family_id: family,
            route,
            owner_id: "client-1".to_string(),
            fencing_token: 1,
        };
        actor.receive(release_msg, &mut bench_ctx);

        idx = (idx + 1) % 100;
    });
}

#[stress_test]
fn should_complete_alternate_renew_operations(ctx: &mut StressContext) {
    ctx.set_elements(100);
    ctx.tag("scenario", "dual_route_concurrent");

    // Setup: Create actor and pre-acquire leases on two routes
    let family = RouteFamily::new(1);
    let route1 = Route::new("lease://realm/area1/lock1/renew");
    let route2 = Route::new("lease://realm/area2/lock2/renew");
    let mut actor = LeaseActor::new(family);
    let mut bench_ctx = create_test_lease_context(None);

    // Setup: acquire leases on both routes
    let acquire1 = LeaseMessage::Acquire {
        family_id: family,
        route: route1.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire1, &mut bench_ctx);

    let acquire2 = LeaseMessage::Acquire {
        family_id: family,
        route: route2.clone(),
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire2, &mut bench_ctx);

    let mut phase = 0;
    ctx.measure(|| {
        if phase % 2 == 0 {
            let msg = LeaseMessage::Extend {
                family_id: family,
                route: route1.clone(),
                owner_id: "client-1".to_string(),
                fencing_token: 1,
                ttl_secs: 30,
            };
            actor.receive(msg, &mut bench_ctx);
        } else {
            let msg = LeaseMessage::Extend {
                family_id: family,
                route: route2.clone(),
                owner_id: "client-2".to_string(),
                fencing_token: 1,
                ttl_secs: 30,
            };
            actor.receive(msg, &mut bench_ctx);
        }
        phase += 1;
    });
}

#[stress_test]
fn should_complete_round_robin_query_operations(ctx: &mut StressContext) {
    ctx.set_elements(100);
    ctx.tag("scenario", "triple_route_contention");

    // Setup: Create actor and pre-acquire leases on three routes
    let family = RouteFamily::new(1);
    let route1 = Route::new("lease://realm/area1/lock1/query");
    let route2 = Route::new("lease://realm/area2/lock2/query");
    let route3 = Route::new("lease://realm/area3/lock3/query");
    let mut actor = LeaseActor::new(family);
    let mut bench_ctx = create_test_lease_context(None);

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
        actor.receive(acquire_msg, &mut bench_ctx);
    }

    let mut phase = 0;
    ctx.measure(|| {
        match phase % 3 {
            0 => {
                let msg = LeaseMessage::Query {
                    family_id: family,
                    route: route1.clone(),
                };
                actor.receive(msg, &mut bench_ctx);
            }
            1 => {
                let msg = LeaseMessage::Query {
                    family_id: family,
                    route: route2.clone(),
                };
                actor.receive(msg, &mut bench_ctx);
            }
            2 => {
                let msg = LeaseMessage::Query {
                    family_id: family,
                    route: route3.clone(),
                };
                actor.receive(msg, &mut bench_ctx);
            }
            _ => unreachable!(),
        }
        phase += 1;
    });
}

#[stress_test]
fn should_complete_cycling_query_renew_operations(ctx: &mut StressContext) {
    ctx.set_elements(3); // 3 different operations cycling
    ctx.tag("scenario", "mixed_operations_high_load");

    // Setup: Create actor and pre-acquire lease
    let family = RouteFamily::new(1);
    let route = Route::new("lease://realm/area/lock1/mixed");
    let mut actor = LeaseActor::new(family);
    let mut bench_ctx = create_test_lease_context(None);

    // Setup: pre-acquire lease
    let acquire_msg = LeaseMessage::Acquire {
        family_id: family,
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire_msg, &mut bench_ctx);

    let mut phase = 0;
    ctx.measure(|| {
        match phase % 3 {
            0 => {
                let msg = LeaseMessage::Query {
                    family_id: family,
                    route: route.clone(),
                };
                actor.receive(msg, &mut bench_ctx);
            }
            1 => {
                let msg = LeaseMessage::Extend {
                    family_id: family,
                    route: route.clone(),
                    owner_id: "client-1".to_string(),
                    fencing_token: 1,
                    ttl_secs: 30,
                };
                actor.receive(msg, &mut bench_ctx);
            }
            2 => {
                let msg = LeaseMessage::Query {
                    family_id: family,
                    route: route.clone(),
                };
                actor.receive(msg, &mut bench_ctx);
            }
            _ => unreachable!(),
        }
        phase += 1;
    });
}

stress_main!();
