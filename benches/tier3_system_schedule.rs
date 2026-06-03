// Schedule domain tier 3 system benchmarks using stress
//
// Schedule creation, cancellation, listing, and fire-scanning.
// Tests sustained schedule operations with mixed workloads.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via set_elements(count)

#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{StressContext, stress_main, stress_test};
use fitz::domains::schedule::protocol::validate_concrete_schedule_route;
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use std::collections::VecDeque;

const SUSTAINED_ACTIVE_SCHEDULES: usize = 1024;
const MIXED_INITIAL_SCHEDULES: usize = 256;
const MIXED_LIST_LIMIT: u64 = 128;

fn create_test_actor() -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    )
}

fn build_route(index: usize) -> String {
    let route = format!("schedule://acme/jobs/task{:06}/run", index);
    validate_concrete_schedule_route(&route).expect("valid schedule benchmark route");
    route
}

fn cron_for(index: usize) -> &'static str {
    let patterns = ["* * * * *", "0 * * * *", "0 0 * * *", "0 2 1 * *"];
    patterns[index % patterns.len()]
}

fn create_unique_schedule(actor: &mut ScheduleActor, index: usize) -> String {
    let route = build_route(index);
    let changed = actor
        .create_schedule(
            route.clone(),
            cron_for(index).to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("schedule create should succeed");
    assert!(changed, "schedule bench create must insert a unique route");
    route
}

fn populate_live_routes(
    actor: &mut ScheduleActor,
    start_index: usize,
    count: usize,
) -> VecDeque<String> {
    (start_index..start_index + count)
        .map(|index| create_unique_schedule(actor, index))
        .collect()
}

fn assert_uncached_list_count(actor: &mut ScheduleActor, limit: u64, expected_count: usize) {
    let (entries, total_count) = actor.list_entries(0, limit);
    assert_eq!(
        entries.len(),
        expected_count,
        "unexpected schedule list page size"
    );
    assert!(
        total_count >= expected_count as u64,
        "total schedule count should cover the uncached page"
    );
}

fn precompute_data(count: usize) -> (Vec<String>, Vec<String>, Vec<Bytes>) {
    let routes = (0..count).map(build_route).collect();

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
        let response = actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
        assert!(
            matches!(response, ScheduleResponse::Ok),
            "schedule bench setup create should succeed for {}",
            routes[i]
        );
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
    ctx.tag("batch_size", "single_unique_create");

    let mut actor = create_test_actor();

    let mut next_index = 0usize;
    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _route = create_unique_schedule(&mut actor, next_index);
            next_index += 1;
        },
    );
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_system_cancel_create_churn(ctx: &mut StressContext) {
    ctx.tag("scenario", "cancel_create_churn");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "1_cancel_1_create");

    let mut actor = create_test_actor();
    let mut live_routes = populate_live_routes(&mut actor, 0, SUSTAINED_ACTIVE_SCHEDULES);
    let mut next_index = SUSTAINED_ACTIVE_SCHEDULES;

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let route = live_routes
                .pop_front()
                .expect("schedule churn must keep at least one live route");
            let deleted = actor
                .delete_schedule(route)
                .expect("schedule cancel should succeed");
            assert!(
                deleted,
                "schedule churn cancel must remove an existing route"
            );

            let route = create_unique_schedule(&mut actor, next_index);
            next_index += 1;
            live_routes.push_back(route);
        },
    );
    ctx.set_elements(2 * iterations as u64);
}

#[stress_test]
fn should_complete_system_list_uncached_9_of_10_schedules(ctx: &mut StressContext) {
    ctx.tag("scenario", "list_uncached_9_of_10");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "9_scanned_uncached");

    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(10);
    populate_actor(&mut actor, &routes, &crons, &payloads, 10);
    assert_uncached_list_count(&mut actor, 9, 9);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let (entries, total_count) = actor.list_entries(0, 9);
            assert_eq!(
                entries.len(),
                9,
                "uncached list should return nine schedules"
            );
            assert_eq!(total_count, 10, "total schedule count should remain stable");
        },
    );
    ctx.set_elements(9 * iterations as u64);
}

#[stress_test]
fn should_complete_system_list_uncached_99_of_100_schedules(ctx: &mut StressContext) {
    ctx.tag("scenario", "list_uncached_99_of_100");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "99_scanned_uncached");

    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(100);
    populate_actor(&mut actor, &routes, &crons, &payloads, 100);
    assert_uncached_list_count(&mut actor, 99, 99);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let (entries, total_count) = actor.list_entries(0, 99);
            assert_eq!(
                entries.len(),
                99,
                "uncached list should return ninety-nine schedules"
            );
            assert_eq!(
                total_count, 100,
                "total schedule count should remain stable"
            );
        },
    );
    ctx.set_elements(99 * iterations as u64);
}

#[stress_test]
fn should_complete_system_list_uncached_999_of_1000_schedules(ctx: &mut StressContext) {
    ctx.tag("scenario", "list_uncached_999_of_1000");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "999_scanned_uncached");

    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(1000);
    populate_actor(&mut actor, &routes, &crons, &payloads, 1000);
    assert_uncached_list_count(&mut actor, 999, 999);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let (entries, total_count) = actor.list_entries(0, 999);
            assert_eq!(
                entries.len(),
                999,
                "uncached list should return nine hundred ninety-nine schedules"
            );
            assert_eq!(
                total_count, 1000,
                "total schedule count should remain stable"
            );
        },
    );
    ctx.set_elements(999 * iterations as u64);
}

#[stress_test]
fn should_complete_system_mixed_workload(ctx: &mut StressContext) {
    ctx.tag("scenario", "mixed_workload");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "2_create_1_uncached_list_1_cancel");

    let mut actor = create_test_actor();
    let mut live_routes = populate_live_routes(&mut actor, 0, MIXED_INITIAL_SCHEDULES);
    let mut next_index = MIXED_INITIAL_SCHEDULES;

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let first_route = create_unique_schedule(&mut actor, next_index);
            next_index += 1;
            live_routes.push_back(first_route);

            let (entries, total_count) = actor.list_entries(0, MIXED_LIST_LIMIT);
            assert_eq!(
                entries.len(),
                MIXED_LIST_LIMIT as usize,
                "mixed workload list must avoid the shared full-list cache"
            );
            assert!(
                total_count >= MIXED_LIST_LIMIT,
                "mixed workload list must retain enough schedules for uncached paging"
            );

            let second_route = create_unique_schedule(&mut actor, next_index);
            next_index += 1;
            live_routes.push_back(second_route);

            let route = live_routes
                .pop_front()
                .expect("mixed workload must keep at least one live schedule");
            let deleted = actor
                .delete_schedule(route)
                .expect("mixed workload cancel should succeed");
            assert!(
                deleted,
                "mixed workload cancel must remove an existing route"
            );
        },
    );
    ctx.set_elements(4 * iterations as u64); // CREATE + LIST + CREATE + CANCEL
}

#[stress_test]
fn should_complete_system_collect_due_occurrences_not_ready_1000_schedules(
    ctx: &mut StressContext,
) {
    ctx.tag("scenario", "collect_due_occurrences_not_ready_1000");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "1000_scanned");
    ctx.tag("ready_state", "none_ready");

    let mut actor = create_scan_actor(1000);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            actor.bench_prepare_scan(0);
            let _fired = actor.collect_due_occurrences_for_publish();
        },
    );
    ctx.set_elements(1000 * iterations as u64);
}

#[stress_test]
fn should_complete_system_collect_due_occurrences_partially_ready_1000_schedules(
    ctx: &mut StressContext,
) {
    ctx.tag("scenario", "collect_due_occurrences_partial_ready_1000");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "1000_scanned");
    ctx.tag("ready_state", "partial_ready");

    let mut actor = create_scan_actor(1000);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            actor.bench_prepare_scan(100);
            let _fired = actor.collect_due_occurrences_for_publish();
        },
    );
    ctx.set_elements(1000 * iterations as u64);
}

#[stress_test]
fn should_complete_system_collect_due_occurrences_all_ready_1000_schedules(
    ctx: &mut StressContext,
) {
    ctx.tag("scenario", "collect_due_occurrences_all_ready_1000");
    ctx.tag("measurement_scope", "direct_actor");
    ctx.tag("batch_size", "1000_scanned");
    ctx.tag("ready_state", "all_ready");

    let mut actor = create_scan_actor(1000);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            actor.bench_prepare_scan(1000);
            let _fired = actor.collect_due_occurrences_for_publish();
        },
    );
    ctx.set_elements(1000 * iterations as u64);
}

stress_main!();
