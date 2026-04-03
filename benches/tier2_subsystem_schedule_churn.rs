//! Criterion benchmarks for schedule churn and shared full-list cache invalidation.

use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::domains::schedule::protocol::validate_concrete_schedule_route;
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

#[path = "criterion_config.rs"]
mod criterion_config;

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

fn bench_cancel_churn(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_schedule_churn");
    group.sampling_mode(SamplingMode::Flat);

    for count in [100usize, 1000usize] {
        let (routes, crons, payloads) = precompute_data(count);
        let victim_index = count / 2;
        group.throughput(Throughput::Elements(count as u64));

        group.bench_function(format!("cancel_existing_{}_mixed_crons", count), |b| {
            b.iter_batched(
                || {
                    let mut actor = create_test_actor();
                    populate_actor(&mut actor, &routes, &crons, &payloads, count);
                    (actor, routes[victim_index].clone())
                },
                |(mut actor, route)| {
                    let response = actor.handle(ScheduleMessage::Cancel { route });
                    assert!(matches!(response, ScheduleResponse::Ok));
                    black_box(actor.schedule_count());
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

fn bench_shared_full_list_cache(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_schedule_list_cache");
    group.sampling_mode(SamplingMode::Flat);

    for count in [100usize, 1000usize] {
        let (routes, crons, payloads) = precompute_data(count);
        let victim_index = count / 2;
        group.throughput(Throughput::Elements(count as u64));

        group.bench_function(
            format!("delete_then_full_list_shared_cache_{}_mixed_crons", count),
            |b| {
                b.iter_batched(
                    || {
                        let mut actor = create_test_actor();
                        populate_actor(&mut actor, &routes, &crons, &payloads, count);
                        let (cached, _) = actor.list_entries(0, 0);
                        (actor, cached, routes[victim_index].clone())
                    },
                    |(mut actor, cached, route)| {
                        black_box(cached.len());
                        let response = actor.handle(ScheduleMessage::Cancel { route });
                        assert!(matches!(response, ScheduleResponse::Ok));
                        let (entries, total_count) = actor.list_entries(0, 0);
                        black_box((entries.len(), total_count));
                    },
                    BatchSize::SmallInput,
                )
            },
        );

        group.bench_function(
            format!("upsert_then_full_list_shared_cache_{}_mixed_crons", count),
            |b| {
                b.iter_batched(
                    || {
                        let mut actor = create_test_actor();
                        populate_actor(&mut actor, &routes, &crons, &payloads, count);
                        let (cached, _) = actor.list_entries(0, 0);
                        (actor, cached, routes[victim_index].clone())
                    },
                    |(mut actor, cached, route)| {
                        black_box(cached.len());
                        let response = actor.handle(ScheduleMessage::Create {
                            route,
                            cron: "0 5 * * *".to_string(),
                            payload: Bytes::from_static(b"replacement"),
                        });
                        assert!(matches!(response, ScheduleResponse::Ok));
                        let (entries, total_count) = actor.list_entries(0, 0);
                        black_box((entries.len(), total_count));
                    },
                    BatchSize::SmallInput,
                )
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_cancel_churn, bench_shared_full_list_cache
}
criterion_main!(benches);