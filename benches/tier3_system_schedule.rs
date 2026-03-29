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

fn populate_actor(
    actor: &mut ScheduleActor,
    routes: &[String],
    crons: &[String],
    payloads: &[Bytes],
    count: usize,
) {
    for i in 0..count {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }
}

fn create_scan_actor(count: usize) -> ScheduleActor {
    let (routes, crons, payloads) = precompute_data(count);
    let mut actor = create_test_actor();
    populate_actor(&mut actor, &routes, &crons, &payloads, count);
    actor
}

#[stress_test]
fn should_complete_system_create(ctx: &mut StressContext) {
    ctx.tag("scenario", "create_operation");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "single_create");

    // Setup: Actor + precomputed data
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(1000);

    let mut idx = 0;
    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _response = actor.handle(ScheduleMessage::Create {
            route: routes[idx % routes.len()].clone(),
            cron: crons[idx % crons.len()].clone(),
            payload: payloads[idx % payloads.len()].clone(),
        });
        idx += 1;
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_system_cancel(ctx: &mut StressContext) {
    ctx.tag("scenario", "cancel_operation");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "single_cancel");

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
    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _response = actor.handle(ScheduleMessage::Cancel {
            route: routes[idx % routes.len()].clone(),
        });
        idx += 1;
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_system_list_10_schedules(ctx: &mut StressContext) {
    ctx.tag("scenario", "list_10");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "10_scanned");

    // Setup: Create actor with 10 schedules
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(10);

    populate_actor(&mut actor, &routes, &crons, &payloads, 10);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _response = actor.handle(ScheduleMessage::List {
            offset: 0,
            limit: 0,
        });
    });
    ctx.set_elements(10 * iterations as u64);
}

#[stress_test]
fn should_complete_system_list_100_schedules(ctx: &mut StressContext) {
    ctx.tag("scenario", "list_100");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "100_scanned");

    // Setup: Create actor with 100 schedules
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(100);

    populate_actor(&mut actor, &routes, &crons, &payloads, 100);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _response = actor.handle(ScheduleMessage::List {
            offset: 0,
            limit: 0,
        });
    });
    ctx.set_elements(100 * iterations as u64);
}

#[stress_test]
fn should_complete_system_list_1000_schedules(ctx: &mut StressContext) {
    ctx.tag("scenario", "list_1000");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "1000_scanned");

    // Setup: Create actor with 1000 schedules
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(1000);

    populate_actor(&mut actor, &routes, &crons, &payloads, 1000);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _response = actor.handle(ScheduleMessage::List {
            offset: 0,
            limit: 0,
        });
    });
    ctx.set_elements(1000 * iterations as u64);
}

#[stress_test]
fn should_complete_system_mixed_workload(ctx: &mut StressContext) {
    ctx.tag("scenario", "mixed_workload");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "2_create_1_list_1_cancel");

    // Setup: Create actor with 100 pre-populated schedules
    let (routes, crons, payloads) = precompute_data(1000);
    let mut actor = create_test_actor();

    populate_actor(&mut actor, &routes, &crons, &payloads, 100);

    let mut idx = 100;
    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _r1 = actor.handle(ScheduleMessage::Create {
            route: routes[idx % routes.len()].clone(),
            cron: crons[idx % crons.len()].clone(),
            payload: payloads[idx % payloads.len()].clone(),
        });

        let _r2 = actor.handle(ScheduleMessage::List {
            offset: 0,
            limit: 0,
        });

        let _r3 = actor.handle(ScheduleMessage::Create {
            route: routes[(idx + 1) % routes.len()].clone(),
            cron: crons[(idx + 1) % crons.len()].clone(),
            payload: payloads[(idx + 1) % payloads.len()].clone(),
        });

        let _r4 = actor.handle(ScheduleMessage::Cancel {
            route: routes[(idx + 50) % routes.len()].clone(),
        });

        idx += 2;
    });
    ctx.set_elements(4 * iterations as u64); // CREATE + LIST + CREATE + CANCEL
}

#[stress_test]
fn should_complete_system_scan_and_fire_not_ready_1000_schedules(ctx: &mut StressContext) {
    ctx.tag("scenario", "scan_fire_not_ready_1000");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "1000_scanned");
    ctx.tag("ready_state", "none_ready");

    let mut actor = create_scan_actor(1000);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        actor.bench_prepare_scan(0);
        let _fired = actor.scan_and_fire();
    });
    ctx.set_elements(1000 * iterations as u64);
}

#[stress_test]
fn should_complete_system_scan_and_fire_partially_ready_1000_schedules(ctx: &mut StressContext) {
    ctx.tag("scenario", "scan_fire_partial_ready_1000");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "1000_scanned");
    ctx.tag("ready_state", "partial_ready");

    let mut actor = create_scan_actor(1000);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        actor.bench_prepare_scan(100);
        let _fired = actor.scan_and_fire();
    });
    ctx.set_elements(1000 * iterations as u64);
}

#[stress_test]
fn should_complete_system_scan_and_fire_all_ready_1000_schedules(ctx: &mut StressContext) {
    ctx.tag("scenario", "scan_fire_all_ready_1000");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "1000_scanned");
    ctx.tag("ready_state", "all_ready");

    let mut actor = create_scan_actor(1000);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        actor.bench_prepare_scan(1000);
        let _fired = actor.scan_and_fire();
    });
    ctx.set_elements(1000 * iterations as u64);
}

stress_main!();
