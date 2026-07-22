#![allow(deprecated)]
use bytes::Bytes;
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{black_box, stress, stress_main, StressContext};
use fitz::benchkit::{
    create_bench_schedule_sink, create_bench_store_with_cfs, register_session_counting_sink,
    route_frame, wait_for_counting_sinks_each_count, CountingSink,
};
use fitz::domains::schedule::protocol::{validate_concrete_schedule_route, Clock};
use fitz::domains::schedule::sink::ScheduleDomainSink;
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::runtime::{DomainPublishEvent, Router};
use std::sync::Arc;
use std::time::{Duration, Instant};

const FIXED_BENCH_EPOCH_MS: u64 = 1_775_200_000_000;
const TIMED_BATCH_SIZE: u64 = 32;
const TIMED_BATCH_REPEAT: u64 = 32;
const CLAIM_DUE_ALL_READY_1000_REPEAT: u64 = 256;
const PUBLISH_REPEAT_COUNT: u64 = 2_048;
const PUBLISH_CHUNK_SIZE: u64 = 256;

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
    let store = create_bench_store_with_cfs([1, 2, 3, 4, 5]);
    ScheduleActor::new_with_clock(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
        clock,
    )
}

fn build_route(index: usize) -> String {
    let route = format!("schedule://acme/jobs/task{index:06}/run");
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
            .map(|index| Bytes::from(format!("payload-{index:06}")))
            .collect(),
    }
}

fn populate_actor(actor: &mut ScheduleActor, fixtures: &ScheduleFixtures) {
    for index in 0..fixtures.routes.len() {
        let response = actor.handle(ScheduleMessage::Create {
            route: fixtures.routes[index].clone(),
            cron: fixtures.crons[index].clone(),
            delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
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
    repeat_count: u64,
    mut create_input: FCreate,
    mut measure: FMeasure,
) -> Duration
where
    FCreate: FnMut() -> T,
    FMeasure: FnMut(&mut T),
{
    let mut remaining = repeat_count;
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

fn claim_due_repeat_count(count: usize, ready_count: usize) -> u64 {
    if count == 1000 && ready_count == 1000 {
        CLAIM_DUE_ALL_READY_1000_REPEAT
    } else {
        TIMED_BATCH_REPEAT
    }
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

fn create_publish_case(
    subscriber_count: usize,
) -> (
    Arc<ScheduleDomainSink>,
    DomainPublishEvent,
    Vec<Arc<CountingSink>>,
) {
    let family = RouteFamily::new(1);
    let route = build_route(0);
    let router = Arc::new(Router::new());
    let sink = create_bench_schedule_sink(router.clone());
    router.register_domain_pattern("schedule", sink.clone());

    let mut subscriber_sinks = Vec::with_capacity(subscriber_count);

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
        let subscribe_ack_count = subscriber_sink.wait_for_count(1, Duration::from_secs(1));
        assert_eq!(
            subscribe_ack_count, 1,
            "schedule subscribe should ack before publish benchmark"
        );
        subscriber_sink.reset();
        subscriber_sinks.push(subscriber_sink);
    }

    (
        sink,
        DomainPublishEvent::new(family, Route::new(route), Bytes::from_static(b"payload")),
        subscriber_sinks,
    )
}

fn claim_due(ctx: &mut StressContext, name: &str, count: usize, ready_count: usize) {
    let bench_clock: Arc<dyn Clock> = Arc::new(FixedClock::new(FIXED_BENCH_EPOCH_MS));
    let fixtures = precompute_data(count);
    let repeat_count = claim_due_repeat_count(count, ready_count);
    let duration = time_with_fresh_inputs(
        repeat_count,
        || {
            let mut actor = create_populated_actor(&fixtures, bench_clock.clone());
            actor.bench_prepare_scan(ready_count);
            actor
        },
        |actor| {
            black_box(actor.bench_claim_due_fires());
        },
    );
    tier2_stress::record_duration(
        ctx,
        name,
        duration,
        (ready_count as u64).saturating_mul(repeat_count),
    );
}

fn ack_claims(ctx: &mut StressContext, name: &str, count: usize, ready_count: usize) {
    let bench_clock: Arc<dyn Clock> = Arc::new(FixedClock::new(FIXED_BENCH_EPOCH_MS));
    let fixtures = precompute_data(count);
    let duration = time_with_fresh_inputs(
        TIMED_BATCH_REPEAT,
        || {
            let mut actor = create_populated_actor(&fixtures, bench_clock.clone());
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
    );
    tier2_stress::record_duration(
        ctx,
        name,
        duration,
        (ready_count as u64).saturating_mul(TIMED_BATCH_REPEAT),
    );
}

fn publish_exact_route(ctx: &mut StressContext, name: &str, subscriber_count: usize) {
    let (sink, event, subscriber_sinks) = create_publish_case(subscriber_count);
    let mut remaining = PUBLISH_REPEAT_COUNT;
    let mut total = Duration::ZERO;
    while remaining > 0 {
        let chunk = remaining.min(PUBLISH_CHUNK_SIZE);
        for subscriber_sink in &subscriber_sinks {
            subscriber_sink.reset();
        }
        let start = Instant::now();
        for _ in 0..chunk {
            sink.bench_publish_event(black_box(&event));
        }
        total += start.elapsed();
        let expected_per_subscriber =
            usize::try_from(chunk).expect("schedule publish count fits usize");
        let delivery_count = wait_for_counting_sinks_each_count(
            &subscriber_sinks,
            expected_per_subscriber,
            Duration::from_secs(1),
        );
        assert_eq!(
            delivery_count,
            subscriber_count * expected_per_subscriber,
            "schedule publish should reach every subscriber exactly once"
        );
        assert!(
            subscriber_sinks
                .iter()
                .all(|sink| sink.count() == expected_per_subscriber),
            "schedule publish should not skip or duplicate subscriber deliveries"
        );
        for subscriber_sink in &subscriber_sinks {
            subscriber_sink.reset();
        }
        remaining -= chunk;
    }
    tier2_stress::record_duration(
        ctx,
        name,
        total,
        PUBLISH_REPEAT_COUNT.saturating_mul(subscriber_count as u64),
    );
}

#[stress(tier = 2, name = "claim_due_all_ready_100_mixed_crons")]
fn should_claim_due_all_ready_100_mixed_crons(ctx: &mut StressContext) {
    claim_due(ctx, "claim_due_all_ready_100_mixed_crons", 100, 100);
}

#[stress(tier = 2, name = "claim_due_partial_ready_1000_mixed_crons")]
fn should_claim_due_partial_ready_1000_mixed_crons(ctx: &mut StressContext) {
    claim_due(ctx, "claim_due_partial_ready_1000_mixed_crons", 1000, 100);
}

#[stress(tier = 2, name = "claim_due_all_ready_1000_mixed_crons")]
fn should_claim_due_all_ready_1000_mixed_crons(ctx: &mut StressContext) {
    claim_due(ctx, "claim_due_all_ready_1000_mixed_crons", 1000, 1000);
}

#[stress(tier = 2, name = "ack_claims_all_ready_100_mixed_crons")]
fn should_ack_claims_all_ready_100_mixed_crons(ctx: &mut StressContext) {
    ack_claims(ctx, "ack_claims_all_ready_100_mixed_crons", 100, 100);
}

#[stress(tier = 2, name = "ack_claims_partial_ready_1000_mixed_crons")]
fn should_ack_claims_partial_ready_1000_mixed_crons(ctx: &mut StressContext) {
    ack_claims(ctx, "ack_claims_partial_ready_1000_mixed_crons", 1000, 100);
}

#[stress(tier = 2, name = "ack_claims_all_ready_1000_mixed_crons")]
fn should_ack_claims_all_ready_1000_mixed_crons(ctx: &mut StressContext) {
    ack_claims(ctx, "ack_claims_all_ready_1000_mixed_crons", 1000, 1000);
}

#[stress(tier = 2, name = "publish_exact_route_10_subscribers")]
fn should_publish_exact_route_10_subscribers(ctx: &mut StressContext) {
    publish_exact_route(ctx, "publish_exact_route_10_subscribers", 10);
}

#[stress(tier = 2, name = "publish_exact_route_100_subscribers")]
fn should_publish_exact_route_100_subscribers(ctx: &mut StressContext) {
    publish_exact_route(ctx, "publish_exact_route_100_subscribers", 100);
}

stress_main!();
