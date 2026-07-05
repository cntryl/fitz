#![allow(deprecated)]
//! Stress benchmarks for schedule churn and shared full-list cache invalidation.

use bytes::Bytes;
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress, stress_main, StressContext};
use fitz::domains::schedule::protocol::validate_concrete_schedule_route;
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use std::hint::black_box;

const CHURN_CASE_COUNT: usize = 4;

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
    for index in 0..count {
        let response = actor.handle(ScheduleMessage::Create {
            route: routes[index].clone(),
            cron: crons[index].clone(),
            payload: payloads[index].clone(),
        });
        assert!(
            matches!(response, ScheduleResponse::Ok),
            "schedule bench setup create should succeed for {}",
            routes[index]
        );
    }
}

fn cancel_existing(ctx: &mut StressContext, name: &str, count: usize) {
    let (routes, crons, payloads) = precompute_data(count);
    let victim_index = count / 2;
    let mut actor = create_test_actor();
    populate_actor(&mut actor, &routes, &crons, &payloads, count);
    let route = routes[victim_index].clone();

    tier2_stress::measure_once(ctx, name, count as u64, || {
        let response = actor.handle(ScheduleMessage::Cancel { route });
        assert!(matches!(response, ScheduleResponse::Ok));
        black_box(actor.schedule_count());
    });
}

fn delete_then_full_list_shared_cache(ctx: &mut StressContext, name: &str, count: usize) {
    let (routes, crons, payloads) = precompute_data(count);
    let victim_index = count / 2;
    let route = routes[victim_index].clone();
    let mut cases = (0..CHURN_CASE_COUNT)
        .map(|_| {
            let mut actor = create_test_actor();
            populate_actor(&mut actor, &routes, &crons, &payloads, count);
            let (cached, _) = actor.list_entries(0, 0);
            (actor, cached.len())
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, name, CHURN_CASE_COUNT as u64, || {
        for (actor, cached_len) in &mut cases {
            black_box(*cached_len);
            let response = actor.handle(ScheduleMessage::Cancel {
                route: route.clone(),
            });
            assert!(matches!(response, ScheduleResponse::Ok));
            let (entries, total_count) = actor.list_entries(0, 0);
            black_box((entries.len(), total_count));
        }
    });
}

fn upsert_then_full_list_shared_cache(ctx: &mut StressContext, name: &str, count: usize) {
    let (routes, crons, payloads) = precompute_data(count);
    let victim_index = count / 2;
    let route = routes[victim_index].clone();
    let mut cases = (0..CHURN_CASE_COUNT)
        .map(|_| {
            let mut actor = create_test_actor();
            populate_actor(&mut actor, &routes, &crons, &payloads, count);
            let (cached, _) = actor.list_entries(0, 0);
            (actor, cached.len())
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(ctx, name, CHURN_CASE_COUNT as u64, || {
        for (actor, cached_len) in &mut cases {
            black_box(*cached_len);
            let response = actor.handle(ScheduleMessage::Create {
                route: route.clone(),
                cron: "0 5 * * *".to_string(),
                payload: Bytes::from_static(b"replacement"),
            });
            assert!(matches!(response, ScheduleResponse::Ok));
            let (entries, total_count) = actor.list_entries(0, 0);
            black_box((entries.len(), total_count));
        }
    });
}

#[stress(tier = 2, name = "cancel_existing_100_mixed_crons")]
fn should_cancel_existing_100_mixed_crons(ctx: &mut StressContext) {
    cancel_existing(ctx, "cancel_existing_100_mixed_crons", 100);
}

#[stress(tier = 2, name = "cancel_existing_1000_mixed_crons")]
fn should_cancel_existing_1000_mixed_crons(ctx: &mut StressContext) {
    cancel_existing(ctx, "cancel_existing_1000_mixed_crons", 1000);
}

#[stress(tier = 2, name = "delete_then_full_list_shared_cache_100_mixed_crons")]
fn should_delete_then_full_list_shared_cache_100_mixed_crons(ctx: &mut StressContext) {
    delete_then_full_list_shared_cache(
        ctx,
        "delete_then_full_list_shared_cache_100_mixed_crons",
        100,
    );
}

#[stress(tier = 2, name = "delete_then_full_list_shared_cache_1000_mixed_crons")]
fn should_delete_then_full_list_shared_cache_1000_mixed_crons(ctx: &mut StressContext) {
    delete_then_full_list_shared_cache(
        ctx,
        "delete_then_full_list_shared_cache_1000_mixed_crons",
        1000,
    );
}

#[stress(tier = 2, name = "upsert_then_full_list_shared_cache_100_mixed_crons")]
fn should_upsert_then_full_list_shared_cache_100_mixed_crons(ctx: &mut StressContext) {
    upsert_then_full_list_shared_cache(
        ctx,
        "upsert_then_full_list_shared_cache_100_mixed_crons",
        100,
    );
}

#[stress(tier = 2, name = "upsert_then_full_list_shared_cache_1000_mixed_crons")]
fn should_upsert_then_full_list_shared_cache_1000_mixed_crons(ctx: &mut StressContext) {
    upsert_then_full_list_shared_cache(
        ctx,
        "upsert_then_full_list_shared_cache_1000_mixed_crons",
        1000,
    );
}

stress_main!();
