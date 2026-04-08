//! Criterion benchmark for schedule due-occurrence collection on the production scan path.
//! This intentionally measures only collect_due_occurrences_for_publish and avoids
//! benchmark-only shortcut paths.

use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput,
};
use fitz::domains::schedule::protocol::{validate_concrete_schedule_route, Clock};
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[path = "criterion_config.rs"]
mod criterion_config;

const FIXED_BENCH_EPOCH_MS: u64 = 1_775_200_000_000;

struct FixedClock {
    now_instant: Instant,
    now_epoch_ms: u64,
}

impl FixedClock {
    fn new(now_epoch_ms: u64) -> Self {
        Self {
            now_instant: Instant::now(),
            now_epoch_ms,
        }
    }
}

impl Clock for FixedClock {
    fn now_instant(&self) -> Instant {
        self.now_instant
    }

    fn now_epoch_ms(&self) -> u64 {
        self.now_epoch_ms
    }
}

fn create_test_actor(clock: Arc<dyn Clock>) -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    ScheduleActor::new_with_clock(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
        clock,
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

fn create_populated_actor(count: usize, clock: Arc<dyn Clock>) -> ScheduleActor {
    let (routes, crons, payloads) = precompute_data(count);
    let mut actor = create_test_actor(clock);
    populate_actor(&mut actor, &routes, &crons, &payloads);
    actor
}

fn bench_scan_shapes(c: &mut Criterion) {
    let bench_clock: Arc<dyn Clock> = Arc::new(FixedClock::new(FIXED_BENCH_EPOCH_MS));
    let mut group = c.benchmark_group("subsystem_schedule_scan");
    group.sampling_mode(SamplingMode::Flat);

    for count in [100usize, 1000usize] {
        group.throughput(Throughput::Elements(count as u64));
        let partial_ready = (count / 10).max(1);

        for (label, ready_count) in [("partial_ready", partial_ready), ("all_ready", count)] {
            group.bench_function(format!("scan_{}_{}_mixed_crons", label, count), |b| {
                let bench_clock = bench_clock.clone();
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;

                    for _ in 0..iters {
                        let mut actor = create_populated_actor(count, bench_clock.clone());
                        actor.bench_prepare_scan(ready_count);
                        let start = Instant::now();
                        black_box(actor.collect_due_occurrences_for_publish());
                        total += start.elapsed();
                    }

                    total
                })
            });
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
