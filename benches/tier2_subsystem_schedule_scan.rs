//! Criterion benchmark for schedule due-occurrence collection for live publish.
//! Provides a single Criterion source of truth so report naming is consistent and stale
//! "schedule_system_collect_due_occurrences" entries can be replaced.

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
) {
    for i in 0..routes.len() {
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

fn bench_scan_shapes(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_schedule_scan");
    group.sampling_mode(SamplingMode::Flat);

    for count in [100usize, 1000usize] {
        let (routes, crons, payloads) = precompute_data(count);
        group.throughput(Throughput::Elements(count as u64));
        let partial_ready = (count / 10).max(1);

        for (label, ready_count) in [
            ("none_ready", 0usize),
            ("partial_ready", partial_ready),
            ("all_ready", count),
        ] {
            group.bench_function(format!("scan_{}_{}_mixed_crons", label, count), |b| {
                b.iter_batched(
                    || {
                        let mut actor = create_test_actor();
                        populate_actor(&mut actor, &routes, &crons, &payloads);
                        actor.bench_prepare_scan(ready_count);
                        actor
                    },
                    |mut actor| {
                        black_box(actor.collect_due_occurrences_for_publish());
                    },
                    BatchSize::SmallInput,
                )
            });

            group.bench_function(
                format!("scan_cpu_only_{}_{}_mixed_crons", label, count),
                |b| {
                    b.iter_batched(
                        || {
                            let mut actor = create_test_actor();
                            populate_actor(&mut actor, &routes, &crons, &payloads);
                            actor.bench_prepare_scan(ready_count);
                            actor
                        },
                        |mut actor| {
                            black_box(actor.collect_due_occurrences_for_publish_cpu_only());
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_scan_shapes
}
criterion_main!(benches);
