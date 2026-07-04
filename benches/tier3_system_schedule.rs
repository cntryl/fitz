// Schedule domain tier 3 system benchmarks using stress
//
// Schedule creation, cancellation, listing, and fire-scanning.
// Tests sustained schedule operations with mixed workloads.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via record_completed(count)

#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::domains::schedule::protocol::validate_concrete_schedule_route;
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use std::collections::VecDeque;

const SUSTAINED_ACTIVE_SCHEDULES: usize = 1024;
const MIXED_INITIAL_SCHEDULES: usize = 256;
const MIXED_LIST_LIMIT: u64 = 128;

#[inline]
fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn create_test_actor() -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    )
}

fn build_route(index: usize) -> String {
    let route = format!("schedule://acme/jobs/task{index:06}/run");
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

fn returned_schedule_elements(iterations: u64, returned_per_iteration: u64) -> u64 {
    iterations.saturating_mul(returned_per_iteration)
}

fn measure_prepared_due_collection(
    ctx: &mut StressContext,
    actor: &mut ScheduleActor,
    ready_count: usize,
    expected_fired_count: usize,
) -> u64 {
    let iterations = ctx.measure_workload(|| {
        actor.bench_prepare_scan(ready_count);
        let claims = actor.bench_claim_due_fires();
        let mut delivered = Vec::with_capacity(claims.len());
        let fired: Vec<_> = claims
            .into_iter()
            .map(|claim| {
                delivered.push((claim.fire_ms, claim.route.clone()));
                (claim.route, claim.payload)
            })
            .collect();
        assert_eq!(
            fired.len(),
            expected_fired_count,
            "schedule due benchmark must publish the expected ready occurrence count"
        );
        if !delivered.is_empty() {
            let (acked, _) = actor
                .bench_ack_pending_fire_claims(&delivered)
                .expect("schedule due benchmark ack should succeed");
            assert_eq!(
                acked,
                delivered.len(),
                "schedule due benchmark should ack every claimed occurrence after timing"
            );
            actor
                .bench_drain_storage()
                .expect("schedule due benchmark cleanup writes should drain");
        }
    });

    iterations
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
        .map(|i| Bytes::from(format!("payload-{i:06}")))
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

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_system_create(ctx: &mut StressContext) {
    ctx.parameter("scenario", "create_operation");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "single_unique_create");

    let mut actor = create_test_actor();

    let mut next_index = 0usize;
    let iterations = ctx.measure_workload(|| {
        let _route = create_unique_schedule(&mut actor, next_index);
        next_index += 1;
    });
    stress_config::record_completed(ctx, iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_system_cancel_create_churn(ctx: &mut StressContext) {
    ctx.parameter("scenario", "cancel_create_churn");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "1_cancel_1_create");

    let mut actor = create_test_actor();
    let mut live_routes = populate_live_routes(&mut actor, 0, SUSTAINED_ACTIVE_SCHEDULES);
    let mut next_index = SUSTAINED_ACTIVE_SCHEDULES;

    let iterations = ctx.measure_workload(|| {
        let route = live_routes
            .pop_front()
            .expect("schedule churn must keep at least one live route");
        let deleted = actor
            .delete_schedule(&route)
            .expect("schedule cancel should succeed");
        assert!(
            deleted,
            "schedule churn cancel must remove an existing route"
        );

        let route = create_unique_schedule(&mut actor, next_index);
        next_index += 1;
        live_routes.push_back(route);
    });
    stress_config::record_completed(ctx, 2 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_system_list_uncached_9_of_10_schedules(ctx: &mut StressContext) {
    ctx.parameter("scenario", "list_uncached_9_of_10");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "single_list_call");
    ctx.parameter("reported_element", "returned_schedule");
    ctx.parameter("page_size", "9");
    ctx.parameter("total_schedules", "10");

    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(10);
    populate_actor(&mut actor, &routes, &crons, &payloads, 10);
    assert_uncached_list_count(&mut actor, 9, 9);

    let iterations = ctx.measure_workload(|| {
        let (entries, total_count) = actor.list_entries(0, 9);
        assert_eq!(
            entries.len(),
            9,
            "uncached list should return nine schedules"
        );
        assert_eq!(total_count, 10, "total schedule count should remain stable");
    });
    stress_config::record_completed(ctx, returned_schedule_elements(iterations, 9));
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_system_list_uncached_99_of_100_schedules(ctx: &mut StressContext) {
    ctx.parameter("scenario", "list_uncached_99_of_100");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "single_list_call");
    ctx.parameter("reported_element", "returned_schedule");
    ctx.parameter("page_size", "99");
    ctx.parameter("total_schedules", "100");

    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(100);
    populate_actor(&mut actor, &routes, &crons, &payloads, 100);
    assert_uncached_list_count(&mut actor, 99, 99);

    let iterations = ctx.measure_workload(|| {
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
    });
    stress_config::record_completed(ctx, returned_schedule_elements(iterations, 99));
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_system_list_uncached_999_of_1000_schedules(ctx: &mut StressContext) {
    ctx.parameter("scenario", "list_uncached_999_of_1000");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "single_list_call");
    ctx.parameter("reported_element", "returned_schedule");
    ctx.parameter("page_size", "999");
    ctx.parameter("total_schedules", "1000");

    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(1000);
    populate_actor(&mut actor, &routes, &crons, &payloads, 1000);
    assert_uncached_list_count(&mut actor, 999, 999);

    let iterations = ctx.measure_workload(|| {
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
    });
    stress_config::record_completed(ctx, returned_schedule_elements(iterations, 999));
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_system_mixed_workload(ctx: &mut StressContext) {
    ctx.parameter("scenario", "mixed_workload");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "2_create_1_uncached_list_1_cancel");

    let mut actor = create_test_actor();
    let mut live_routes = populate_live_routes(&mut actor, 0, MIXED_INITIAL_SCHEDULES);
    let mut next_index = MIXED_INITIAL_SCHEDULES;

    let iterations = ctx.measure_workload(|| {
        let first_route = create_unique_schedule(&mut actor, next_index);
        next_index += 1;
        live_routes.push_back(first_route);

        let (entries, total_count) = actor.list_entries(0, MIXED_LIST_LIMIT);
        assert_eq!(
            entries.len(),
            u64_to_usize_saturating(MIXED_LIST_LIMIT),
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
            .delete_schedule(&route)
            .expect("mixed workload cancel should succeed");
        assert!(
            deleted,
            "mixed workload cancel must remove an existing route"
        );
    });
    stress_config::record_completed(ctx, 4 * iterations); // CREATE + LIST + CREATE + CANCEL
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_system_collect_due_occurrences_not_ready_1000_schedules(
    ctx: &mut StressContext,
) {
    ctx.parameter("scenario", "collect_due_occurrences_not_ready_1000");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "1000_scanned");
    ctx.parameter("ready_state", "none_ready");

    let mut actor = create_scan_actor(1000);

    let iterations = ctx.measure_workload(|| {
        actor.bench_prepare_scan(0);
        let fired = actor.collect_due_occurrences_for_publish();
        assert!(
            fired.is_empty(),
            "not-ready schedule due benchmark must not publish occurrences"
        );
    });
    stress_config::record_completed(ctx, 1000 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_system_collect_due_occurrences_partially_ready_1000_schedules(
    ctx: &mut StressContext,
) {
    ctx.parameter("scenario", "collect_due_occurrences_partial_ready_1000");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "1000_scanned");
    ctx.parameter("ready_state", "partial_ready");
    ctx.parameter("setup_scope", "prepared_scan_reset_outside_timer");

    let mut actor = create_scan_actor(1000);

    let iterations = measure_prepared_due_collection(ctx, &mut actor, 100, 100);
    stress_config::record_completed(ctx, 1000 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_system_collect_due_occurrences_all_ready_1000_schedules(
    ctx: &mut StressContext,
) {
    ctx.parameter("scenario", "collect_due_occurrences_all_ready_1000");
    ctx.parameter("measurement_scope", "direct_actor");
    ctx.parameter("batch_size", "1000_scanned");
    ctx.parameter("ready_state", "all_ready");
    ctx.parameter("setup_scope", "prepared_scan_reset_outside_timer");

    let mut actor = create_scan_actor(1000);

    let iterations = measure_prepared_due_collection(ctx, &mut actor, 1000, 1000);
    stress_config::record_completed(ctx, 1000 * iterations);
}

stress_main!();
