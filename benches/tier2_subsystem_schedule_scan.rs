//! Criterion benchmark for schedule due-occurrence collection on the production scan path.
//! This intentionally measures only collect_due_occurrences_for_publish and avoids
//! benchmark-only shortcut paths.

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::schedule::protocol::{validate_concrete_schedule_route, Clock};
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[path = "criterion_config.rs"]
mod criterion_config;

const FIXED_BENCH_EPOCH_MS: u64 = 1_775_200_000_000;
const TIMED_BATCH_SIZE: u64 = 8;

struct ScheduleFixtures {
    routes: Vec<String>,
    crons: Vec<String>,
    payloads: Vec<Bytes>,
}

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

fn precompute_data(count: usize) -> ScheduleFixtures {
    ScheduleFixtures {
        routes: (0..count).map(build_route).collect(),
        crons: (0..count)
            .map(|i| {
                let patterns = ["* * * * *", "0 * * * *", "0 0 * * *", "0 2 1 * *"];
                patterns[i % patterns.len()].to_string()
            })
            .collect(),
        payloads: (0..count)
            .map(|i| Bytes::from(format!("payload-{:06}", i)))
            .collect(),
    }
}

fn populate_actor(actor: &mut ScheduleActor, fixtures: &ScheduleFixtures) {
    for i in 0..fixtures.routes.len() {
        let response = actor.handle(ScheduleMessage::Create {
            route: fixtures.routes[i].clone(),
            cron: fixtures.crons[i].clone(),
            payload: fixtures.payloads[i].clone(),
        });
        assert!(
            matches!(response, ScheduleResponse::Ok),
            "schedule bench setup create should succeed for {}",
            fixtures.routes[i]
        );
    }
}

fn create_populated_actor(fixtures: &ScheduleFixtures, clock: Arc<dyn Clock>) -> ScheduleActor {
    let mut actor = create_test_actor(clock);
    populate_actor(&mut actor, fixtures);
    actor
}

fn time_with_fresh_actors<F>(
    iters: u64,
    mut create_actor: F,
    mut measure: impl FnMut(&mut ScheduleActor),
) -> Duration
where
    F: FnMut() -> ScheduleActor,
{
    let mut remaining = iters;
    let mut total = Duration::ZERO;

    while remaining > 0 {
        let chunk_len = remaining.min(TIMED_BATCH_SIZE) as usize;
        let mut actors: Vec<ScheduleActor> = (0..chunk_len).map(|_| create_actor()).collect();
        let start = Instant::now();
        for actor in &mut actors {
            measure(actor);
        }
        total += start.elapsed();
        remaining -= chunk_len as u64;
    }

    total
}

fn bench_scan_shapes(c: &mut Criterion) {
    let bench_clock: Arc<dyn Clock> = Arc::new(FixedClock::new(FIXED_BENCH_EPOCH_MS));
    let mut group = c.benchmark_group("subsystem_schedule_scan");
    group.sampling_mode(SamplingMode::Flat);

    for count in [100usize, 1000usize] {
        let fixtures = precompute_data(count);
        group.throughput(Throughput::Elements(count as u64));
        let partial_ready = (count / 10).max(1);

        for (label, ready_count) in [("partial_ready", partial_ready), ("all_ready", count)] {
            group.bench_function(format!("scan_{}_{}_mixed_crons", label, count), |b| {
                let bench_clock = bench_clock.clone();
                let fixtures = &fixtures;
                b.iter_custom(|iters| {
                    time_with_fresh_actors(
                        iters,
                        || {
                            let mut actor = create_populated_actor(fixtures, bench_clock.clone());
                            actor.bench_prepare_scan(ready_count);
                            actor
                        },
                        |actor| {
                            black_box(actor.collect_due_occurrences_for_publish());
                        },
                    )
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
