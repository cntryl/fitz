//! Criterion benchmark for schedule domain scan_and_fire (same code path as tier3 stress).
//! Provides a single Criterion source of truth so report naming is consistent and stale
//! "schedule_system_scan_and_fire" entries can be replaced.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput};
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

#[path = "config.rs"]
mod config;

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

fn bench_scan_and_fire_100(c: &mut Criterion) {
    let (routes, crons, payloads) = precompute_data(100);

    let mut group = c.benchmark_group("schedule_scan_and_fire");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    group.bench_function("scan_and_fire_100", |b| {
        b.iter_batched(
            || {
                let mut actor = create_test_actor();
                for i in 0..100 {
                    actor.handle(ScheduleMessage::Create {
                        route: routes[i].clone(),
                        cron: crons[i].clone(),
                        payload: payloads[i].clone(),
                    });
                }
                actor
            },
            |mut actor| {
                black_box(actor.scan_and_fire());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_scan_and_fire_100
}
criterion_main!(benches);
