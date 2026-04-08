use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput,
};
use fitz::benchkit::{create_bench_schedule_sink, register_session_counting_sink, route_frame};
use fitz::boot::domains::ScheduleDomainSink;
use fitz::domains::schedule::protocol::{validate_concrete_schedule_route, Clock};
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::runtime::{DomainPublishEvent, Router};
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
            .map(|index| {
                let patterns = ["* * * * *", "0 * * * *", "0 0 * * *", "0 2 1 * *"];
                patterns[index % patterns.len()].to_string()
            })
            .collect(),
        payloads: (0..count)
            .map(|index| Bytes::from(format!("payload-{:06}", index)))
            .collect(),
    }
}

fn populate_actor(
    actor: &mut ScheduleActor,
    fixtures: &ScheduleFixtures,
) {
    for index in 0..fixtures.routes.len() {
        let response = actor.handle(ScheduleMessage::Create {
            route: fixtures.routes[index].clone(),
            cron: fixtures.crons[index].clone(),
            payload: fixtures.payloads[index].clone(),
        });
        assert!(
            matches!(response, ScheduleResponse::Ok),
            "schedule bench setup create should succeed for {}",
            fixtures.routes[index]
        );
    }
}

fn create_populated_actor(fixtures: &ScheduleFixtures, clock: Arc<dyn Clock>) -> ScheduleActor {
    let mut actor = create_test_actor(clock);
    populate_actor(&mut actor, fixtures);
    actor
}

fn time_with_fresh_inputs<T, FCreate, FMeasure>(
    iters: u64,
    mut create_input: FCreate,
    mut measure: FMeasure,
) -> Duration
where
    FCreate: FnMut() -> T,
    FMeasure: FnMut(&mut T),
{
    let mut remaining = iters;
    let mut total = Duration::ZERO;

    while remaining > 0 {
        let chunk_len = remaining.min(TIMED_BATCH_SIZE) as usize;
        let mut inputs: Vec<T> = (0..chunk_len).map(|_| create_input()).collect();
        let start = Instant::now();
        for input in &mut inputs {
            measure(input);
        }
        total += start.elapsed();
        remaining -= chunk_len as u64;
    }

    total
}

fn claim_due_deliveries(actor: &mut ScheduleActor, ready_count: usize) -> Vec<(u64, String)> {
    actor.bench_prepare_scan(ready_count);
    actor
        .bench_claim_due_fires()
        .into_iter()
        .map(|claim| (claim.fire_ms, claim.route))
        .collect()
}

fn encode_schedule_subscribe(pattern: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(pattern);
    Bytes::from(encoder.finish())
}

fn create_publish_case(subscriber_count: usize) -> (Arc<ScheduleDomainSink>, DomainPublishEvent) {
    let family = RouteFamily::new(1);
    let route = build_route(0);
    let router = Arc::new(Router::new());
    let sink = create_bench_schedule_sink(router.clone());
    router.register_domain_pattern("schedule", sink.clone());

    for index in 0..subscriber_count {
        let session_id = (index + 1) as u64;
        let (subscriber_address, subscriber_sink) =
            register_session_counting_sink(&router, family, session_id);
        route_frame(
            &router,
            &subscriber_address,
            &route,
            session_id,
            ChannelId::Sub,
            703,
            encode_schedule_subscribe(&route),
            family,
        )
        .expect("subscribe schedule benchmark route");
        subscriber_sink.reset();
    }

    (
        sink,
        DomainPublishEvent::new(family, Route::new(route), Bytes::from_static(b"payload")),
    )
}

fn bench_claim_due_persistence(c: &mut Criterion) {
    let bench_clock: Arc<dyn Clock> = Arc::new(FixedClock::new(FIXED_BENCH_EPOCH_MS));
    let mut group = c.benchmark_group("subsystem_schedule_fire_claim");
    group.sampling_mode(SamplingMode::Flat);

    for count in [100usize, 1000usize] {
        let fixtures = precompute_data(count);
        let partial_ready = (count / 10).max(1);

        for (label, ready_count) in [("partial_ready", partial_ready), ("all_ready", count)] {
            group.throughput(Throughput::Elements(ready_count as u64));
            group.bench_function(format!("claim_due_{}_{}_mixed_crons", label, count), |b| {
                let bench_clock = bench_clock.clone();
                let fixtures = &fixtures;
                b.iter_custom(|iters| {
                    time_with_fresh_inputs(
                        iters,
                        || {
                            let mut actor = create_populated_actor(fixtures, bench_clock.clone());
                            actor.bench_prepare_scan(ready_count);
                            actor
                        },
                        |actor| {
                            black_box(actor.bench_claim_due_fires());
                        },
                    )
                })
            });
        }
    }

    group.finish();
}

fn bench_ack_persistence(c: &mut Criterion) {
    let bench_clock: Arc<dyn Clock> = Arc::new(FixedClock::new(FIXED_BENCH_EPOCH_MS));
    let mut group = c.benchmark_group("subsystem_schedule_fire_ack");
    group.sampling_mode(SamplingMode::Flat);

    for count in [100usize, 1000usize] {
        let fixtures = precompute_data(count);
        let partial_ready = (count / 10).max(1);

        for (label, ready_count) in [("partial_ready", partial_ready), ("all_ready", count)] {
            group.throughput(Throughput::Elements(ready_count as u64));
            group.bench_function(format!("ack_claims_{}_{}_mixed_crons", label, count), |b| {
                let bench_clock = bench_clock.clone();
                let fixtures = &fixtures;
                b.iter_custom(|iters| {
                    time_with_fresh_inputs(
                        iters,
                        || {
                            let mut actor = create_populated_actor(fixtures, bench_clock.clone());
                            let deliveries = claim_due_deliveries(&mut actor, ready_count);
                            (actor, deliveries)
                        },
                        |(actor, deliveries)| {
                            black_box(
                                actor
                                    .bench_ack_pending_fire_claims(deliveries)
                                    .expect("ack pending fire claims"),
                            );
                        },
                    )
                })
            });
        }
    }

    group.finish();
}

fn bench_publish_fanout(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_schedule_fire_publish");
    group.sampling_mode(SamplingMode::Flat);

    for subscriber_count in [1usize, 10usize, 100usize] {
        group.throughput(Throughput::Elements(subscriber_count as u64));
        group.bench_function(
            format!("publish_exact_route_{}_subscribers", subscriber_count),
            |b| {
                let (sink, event) = create_publish_case(subscriber_count);
                b.iter(|| {
                    sink.bench_publish_event(black_box(&event))
                        .expect("publish event");
                    black_box(());
                })
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_claim_due_persistence, bench_ack_persistence, bench_publish_fanout
}
criterion_main!(benches);
