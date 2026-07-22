#![allow(deprecated)]
//! Stress benchmarks for schedule churn and shared full-list cache invalidation.

use bytes::Bytes;
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{black_box, stress, stress_main, StressContext};
use fitz::domains::schedule::protocol::validate_concrete_schedule_route;
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use std::time::{Duration, Instant};

const DELETE_CHURN_OPERATION_COUNT: u64 = 1024;
const UPSERT_CHURN_OPERATION_COUNT: u64 = 1024;

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
            delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
            payload: payloads[index].clone(),
        });
        assert!(
            matches!(response, ScheduleResponse::Ok),
            "schedule bench setup create should succeed for {}",
            routes[index]
        );
    }
}

fn delete_then_full_list_shared_cache(ctx: &mut StressContext, name: &str, count: usize) {
    let (routes, crons, payloads) = precompute_data(count);
    let victim_index = count / 2;
    let route = routes[victim_index].clone();
    let mut actor = create_test_actor();
    populate_actor(&mut actor, &routes, &crons, &payloads, count);
    let count_u64 = count as u64;
    let mut total = Duration::ZERO;

    for _ in 0..DELETE_CHURN_OPERATION_COUNT {
        let (cached, cached_total) = actor.list_entries(0, 0);
        assert_eq!(
            cached_total, count_u64,
            "delete churn setup should restore route"
        );
        black_box(cached.len());

        let started = Instant::now();
        let response = actor.handle(ScheduleMessage::Cancel {
            route: route.clone(),
        });
        assert!(matches!(response, ScheduleResponse::Ok));
        let (entries, total_count) = actor.list_entries(0, 0);
        total += started.elapsed();
        assert_eq!(
            total_count,
            count_u64 - 1,
            "delete churn should remove exactly one route"
        );
        black_box((entries.len(), total_count));

        let response = actor.handle(ScheduleMessage::Create {
            route: route.clone(),
            cron: crons[victim_index].clone(),
            delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
            payload: payloads[victim_index].clone(),
        });
        assert!(
            matches!(response, ScheduleResponse::Ok),
            "delete churn restore should succeed"
        );
    }

    tier2_stress::record_duration(ctx, name, total, DELETE_CHURN_OPERATION_COUNT);
}

fn upsert_then_full_list_shared_cache(ctx: &mut StressContext, name: &str, count: usize) {
    let (routes, crons, payloads) = precompute_data(count);
    let victim_index = count / 2;
    let route = routes[victim_index].clone();
    let replacement_crons = [
        "0 5 * * *".to_string(),
        "0 6 * * *".to_string(),
        "0 7 * * *".to_string(),
        "0 8 * * *".to_string(),
    ];
    let replacement_payloads = [
        Bytes::from_static(b"replacement-a"),
        Bytes::from_static(b"replacement-b"),
        Bytes::from_static(b"replacement-c"),
        Bytes::from_static(b"replacement-d"),
    ];
    let mut actor = create_test_actor();
    populate_actor(&mut actor, &routes, &crons, &payloads, count);
    let (cached, _) = actor.list_entries(0, 0);
    black_box(cached.len());

    tier2_stress::measure_once(ctx, name, UPSERT_CHURN_OPERATION_COUNT, || {
        let mut replacement_index = 0usize;
        for _ in 0..UPSERT_CHURN_OPERATION_COUNT {
            let response = actor.handle(ScheduleMessage::Create {
                route: route.clone(),
                cron: replacement_crons[replacement_index].clone(),
                delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
                payload: replacement_payloads[replacement_index].clone(),
            });
            replacement_index = (replacement_index + 1) % replacement_crons.len();
            assert!(matches!(response, ScheduleResponse::Ok));
            let (entries, total_count) = actor.list_entries(0, 0);
            black_box((entries.len(), total_count));
        }
    });
}

#[stress(tier = 2, name = "delete_then_full_list_shared_cache_1000_mixed_crons")]
fn should_delete_then_full_list_shared_cache_1000_mixed_crons(ctx: &mut StressContext) {
    delete_then_full_list_shared_cache(
        ctx,
        "delete_then_full_list_shared_cache_1000_mixed_crons",
        1000,
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
