// Schedule domain tier 3 system benchmarks using stress
//
// Schedule creation, cancellation, listing, and fire-scanning.
// Tests sustained schedule operations with mixed workloads.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via set_elements(count)

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

fn create_test_actor() -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    )
}

fn precompute_data(count: usize) -> (Vec<String>, Vec<String>, Vec<Bytes>) {
    let routes = (0..count)
        .map(|i| format!("schedule://acme/jobs/task{:06}", i))
        .collect();

    let crons = (0..count)
        .map(|i| {
            let patterns = ["* * * * *", "0 * * * *", "0 0 * * *", "0 2 1 * *"];
            patterns[i % patterns.len()].to_string()
        })
        .collect();

    let payloads = (0..count)
        .map(|i| Bytes::from(format!("payload-{:06}", i)))
        .collect();

    (routes, crons, payloads)
}

#[stress_test]
fn should_complete_system_create(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "create_operation");

    // Setup: Actor + precomputed data
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(1000);

    let mut idx = 0;
    ctx.measure(|| {
        let _response = actor.handle(ScheduleMessage::Create {
            route: routes[idx % routes.len()].clone(),
            cron: crons[idx % crons.len()].clone(),
            payload: payloads[idx % payloads.len()].clone(),
        });
        idx += 1;
    });
}

#[stress_test]
fn should_complete_system_cancel(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "cancel_operation");

    // Setup: Create actor with pre-populated schedules
    let (routes, crons, payloads) = precompute_data(1000);
    let mut actor = create_test_actor();

    for i in 0..routes.len() {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }

    let mut idx = 0;
    ctx.measure(|| {
        let _response = actor.handle(ScheduleMessage::Cancel {
            route: routes[idx % routes.len()].clone(),
        });
        idx += 1;
    });
}

#[stress_test]
fn should_complete_system_list_10_schedules(ctx: &mut StressContext) {
    ctx.set_elements(10);
    ctx.tag("scenario", "list_10");

    // Setup: Create actor with 10 schedules
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(10);

    for i in 0..10 {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }

    ctx.measure(|| {
        let _response = actor.handle(ScheduleMessage::List);
    });
}

#[stress_test]
fn should_complete_system_list_100_schedules(ctx: &mut StressContext) {
    ctx.set_elements(100);
    ctx.tag("scenario", "list_100");

    // Setup: Create actor with 100 schedules
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(100);

    for i in 0..100 {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }

    ctx.measure(|| {
        let _response = actor.handle(ScheduleMessage::List);
    });
}

#[stress_test]
fn should_complete_system_list_1000_schedules(ctx: &mut StressContext) {
    ctx.set_elements(1000);
    ctx.tag("scenario", "list_1000");

    // Setup: Create actor with 1000 schedules
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(1000);

    for i in 0..1000 {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }

    ctx.measure(|| {
        let _response = actor.handle(ScheduleMessage::List);
    });
}

#[stress_test]
fn should_complete_system_mixed_workload(ctx: &mut StressContext) {
    ctx.set_elements(4); // CREATE + LIST + CREATE + CANCEL
    ctx.tag("scenario", "mixed_workload");

    // Setup: Create actor with 100 pre-populated schedules
    let (routes, crons, payloads) = precompute_data(1000);
    let mut actor = create_test_actor();

    for i in 0..100 {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }

    let mut idx = 100;
    ctx.measure(|| {
        // CREATE
        let _r1 = actor.handle(ScheduleMessage::Create {
            route: routes[idx % routes.len()].clone(),
            cron: crons[idx % crons.len()].clone(),
            payload: payloads[idx % payloads.len()].clone(),
        });

        // LIST
        let _r2 = actor.handle(ScheduleMessage::List);

        // CREATE (another)
        let _r3 = actor.handle(ScheduleMessage::Create {
            route: routes[(idx + 1) % routes.len()].clone(),
            cron: crons[(idx + 1) % crons.len()].clone(),
            payload: payloads[(idx + 1) % payloads.len()].clone(),
        });

        // CANCEL
        let _r4 = actor.handle(ScheduleMessage::Cancel {
            route: routes[(idx + 50) % routes.len()].clone(),
        });

        idx += 2;
    });
}

#[stress_test]
fn should_complete_system_scan_and_fire_100_schedules(ctx: &mut StressContext) {
    ctx.set_elements(100);
    ctx.tag("scenario", "scan_fire_100");

    // Setup: Create actor with 100 schedules
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(100);

    for i in 0..100 {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }

    ctx.measure(|| {
        let _fired = actor.scan_and_fire();
    });
}

#[stress_test]
fn should_complete_system_scan_and_fire_1000_schedules(ctx: &mut StressContext) {
    ctx.set_elements(1000);
    ctx.tag("scenario", "scan_fire_1000");

    // Setup: Create actor with 1000 schedules
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(1000);

    for i in 0..1000 {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }

    ctx.measure(|| {
        let _fired = actor.scan_and_fire();
    });
}

#[stress_test]
fn should_complete_system_scan_and_fire_10000_schedules(ctx: &mut StressContext) {
    ctx.set_elements(10000);
    ctx.tag("scenario", "scan_fire_10000");

    // Setup: Create actor with 10000 schedules
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(10000);

    for i in 0..10000 {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }

    ctx.measure(|| {
        let _fired = actor.scan_and_fire();
    });
}

stress_main!();
