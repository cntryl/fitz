use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::benchkit::{create_bench_schedule_sink, register_session_counting_sink, route_frame};
use fitz::boot::domains::ScheduleDomainSink;
use fitz::domains::schedule::protocol::validate_concrete_schedule_route;
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::payload_codec::PayloadEncoder;
use fitz::runtime::routing::{Route, RouteFamily};
use fitz::runtime::{DomainPublishEvent, Router};
use fitz::testkit::create_test_engine_with_cfs;
use std::sync::Arc;

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
        .map(|index| {
            let patterns = ["* * * * *", "0 * * * *", "0 0 * * *", "0 2 1 * *"];
            patterns[index % patterns.len()].to_string()
        })
        .collect();
    let payloads = (0..count)
        .map(|index| Bytes::from(format!("payload-{:06}", index)))
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

fn create_claim_case(count: usize, ready_count: usize) -> ScheduleActor {
    let (routes, crons, payloads) = precompute_data(count);
    let mut actor = create_test_actor();
    populate_actor(&mut actor, &routes, &crons, &payloads, count);
    actor.bench_prepare_scan(ready_count);
    actor
}

fn create_ack_case(count: usize, ready_count: usize) -> (ScheduleActor, Vec<(u64, String)>) {
    let mut actor = create_claim_case(count, ready_count);
    let deliveries = actor
        .bench_claim_due_fires()
        .into_iter()
        .map(|claim| (claim.fire_ms, claim.route))
        .collect();
    (actor, deliveries)
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
    let mut group = c.benchmark_group("subsystem_schedule_fire_claim");
    group.sampling_mode(SamplingMode::Flat);

    for count in [100usize, 1000usize] {
        let partial_ready = (count / 10).max(1);

        for (label, ready_count) in [("partial_ready", partial_ready), ("all_ready", count)] {
            group.throughput(Throughput::Elements(ready_count as u64));
            group.bench_function(format!("claim_due_{}_{}_mixed_crons", label, count), |b| {
                b.iter_batched(
                    || create_claim_case(count, ready_count),
                    |mut actor| {
                        black_box(actor.bench_claim_due_fires());
                    },
                    BatchSize::SmallInput,
                )
            });
        }
    }

    group.finish();
}

fn bench_ack_persistence(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_schedule_fire_ack");
    group.sampling_mode(SamplingMode::Flat);

    for count in [100usize, 1000usize] {
        let partial_ready = (count / 10).max(1);

        for (label, ready_count) in [("partial_ready", partial_ready), ("all_ready", count)] {
            group.throughput(Throughput::Elements(ready_count as u64));
            group.bench_function(format!("ack_claims_{}_{}_mixed_crons", label, count), |b| {
                b.iter_batched(
                    || create_ack_case(count, ready_count),
                    |(mut actor, deliveries)| {
                        black_box(
                            actor
                                .bench_ack_pending_fire_claims(&deliveries)
                                .expect("ack pending fire claims"),
                        );
                    },
                    BatchSize::SmallInput,
                )
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
                b.iter_batched(
                    || create_publish_case(subscriber_count),
                    |(sink, event)| {
                        sink.bench_publish_event(&event).expect("publish event");
                        black_box(());
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
    targets = bench_claim_due_persistence, bench_ack_persistence, bench_publish_fanout
}
criterion_main!(benches);
