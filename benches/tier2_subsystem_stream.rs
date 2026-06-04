#![allow(deprecated)]
use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::benchkit::{
    build_stream_subscribe, create_bench_stream_sink, extract_single_tlv_field,
    register_session_counting_sink, register_session_queue_sink, route_frame, CountingSink,
    FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::domain_event::DomainPublishEvent;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

#[path = "criterion_config.rs"]
mod criterion_config;

const CLIENT_SESSION_ID: u64 = 1;
const SUBSCRIBE_DESTINATION: &str = "stream://realm/area/control/append";
const COMMIT_NOTIFY_ROUTE: &str = "stream://realm/area/orders";

struct StreamPatternCase {
    label: &'static str,
    pattern: &'static str,
}

struct PreparedStreamNotifyCase {
    sink: Arc<fitz::boot::domains::StreamDomainSink>,
    destination: RouteAddress,
    event: DomainPublishEvent,
    subscriber_sinks: Vec<Arc<CountingSink>>,
}

impl PreparedStreamNotifyCase {
    fn publish_once(&self) {
        self.sink
            .deliver(Envelope::new(self.destination.clone(), self.event.clone()))
            .expect("stream commit notify publish");
    }

    fn validate_and_reset(&self) {
        self.publish_once();

        for subscriber_sink in &self.subscriber_sinks {
            assert_eq!(
                subscriber_sink.count(),
                1,
                "expected one stream notify per subscriber"
            );
            subscriber_sink.reset();
        }
    }
}

fn encode_commit_notify_payload() -> Bytes {
    Bytes::from(
        json!({
            "event": "committed",
            "first_resource_offset": 0,
            "last_resource_offset": 0,
            "first_area_offset": 0,
            "last_area_offset": 0,
            "first_realm_offset": 0,
            "last_realm_offset": 0,
            "batch_size": 1,
        })
        .to_string(),
    )
}

fn setup_stream_request_sink() -> (Arc<Router>, RouteFamily, RouteAddress, Arc<FrameQueueSink>) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_stream_sink(router.clone());
    router.register_domain_pattern("stream", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    (router, family, source, inbox)
}

fn build_stream_subscribe_request(pattern: &str) -> (u16, Bytes) {
    let subscribe_frame = build_stream_subscribe(pattern);
    extract_single_tlv_field(&subscribe_frame)
}

fn register_stream_subscription(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    session_id: u64,
    msg_type: u16,
    payload: Bytes,
) {
    route_frame(
        router.as_ref(),
        source,
        SUBSCRIBE_DESTINATION,
        session_id,
        ChannelId::Pub,
        msg_type,
        payload,
        family,
    )
    .expect("stream subscribe");
}

fn assert_stream_subscribe_success(response: &[u8]) {
    assert_eq!(
        response.first().copied(),
        Some(0),
        "expected stream subscribe success"
    );
}

fn prepare_notify_case(subscriber_count: usize, pattern: &str) -> PreparedStreamNotifyCase {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_stream_sink(router.clone());
    router.register_domain_pattern("stream", sink.clone() as Arc<dyn MailboxSink>);
    let (subscribe_msg_type, subscribe_payload) = build_stream_subscribe_request(pattern);

    let mut subscriber_sinks = Vec::with_capacity(subscriber_count);
    for index in 0..subscriber_count {
        let session_id = 10_000 + index as u64;
        let (subscriber_source, subscriber_sink) =
            register_session_counting_sink(&router, family, session_id);
        register_stream_subscription(
            &router,
            family,
            &subscriber_source,
            session_id,
            subscribe_msg_type,
            subscribe_payload.clone(),
        );
        subscriber_sink.reset();
        subscriber_sinks.push(subscriber_sink);
    }

    let case = PreparedStreamNotifyCase {
        sink,
        destination: RouteAddress::new(family, Route::new(COMMIT_NOTIFY_ROUTE)),
        event: DomainPublishEvent::new(
            family,
            Route::new(COMMIT_NOTIFY_ROUTE),
            encode_commit_notify_payload(),
        ),
        subscriber_sinks,
    };
    case.validate_and_reset();
    case
}

fn bench_stream_subscribe_register_primary(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsystem_stream");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    group.measurement_time(Duration::from_millis(300));
    let (subscribe_msg_type, subscribe_payload) =
        build_stream_subscribe_request(COMMIT_NOTIFY_ROUTE);

    group.bench_function("subscribe_register_primary", |b| {
        b.iter_batched(
            setup_stream_request_sink,
            |(router, family, source, inbox)| {
                register_stream_subscription(
                    &router,
                    family,
                    &source,
                    CLIENT_SESSION_ID,
                    subscribe_msg_type,
                    subscribe_payload.clone(),
                );
                let response = inbox
                    .drain()
                    .last()
                    .map(|frame| frame.payload.clone())
                    .expect("stream subscribe response");
                assert_stream_subscribe_success(response.as_ref());
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_stream_commit_notify_primary(c: &mut Criterion) {
    let pattern_cases = [
        StreamPatternCase {
            label: "exact_route",
            pattern: COMMIT_NOTIFY_ROUTE,
        },
        StreamPatternCase {
            label: "single_star",
            pattern: "stream://realm/area/*",
        },
        StreamPatternCase {
            label: "double_star",
            pattern: "stream://realm/**",
        },
    ];

    let mut group = c.benchmark_group("subsystem_stream");
    group.sampling_mode(SamplingMode::Flat);

    for pattern_case in pattern_cases {
        for subscriber_count in [1usize, 16usize, 64usize, 256usize] {
            let case = prepare_notify_case(subscriber_count, pattern_case.pattern);
            group.throughput(Throughput::Elements(subscriber_count as u64));
            group.bench_function(
                format!(
                    "commit_notify_{}_{}_subscribers_primary",
                    pattern_case.label, subscriber_count
                ),
                |b| {
                    b.iter(|| {
                        case.publish_once();
                        black_box(());
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_stream_subscribe_register_primary, bench_stream_commit_notify_primary
}
criterion_main!(benches);
