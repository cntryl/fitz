#![allow(deprecated)]
use bytes::Bytes;
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_stream_subscribe, create_bench_stream_sink, drain_frame_queue_sinks_after_each_count,
    extract_single_tlv_field, register_session_counting_sink, register_session_queue_sink,
    route_frame, wait_for_counting_sinks_each_count, CountingSink, FrameQueueSink,
};
use fitz::domains::stream::sink::StreamDomainSink;
use fitz::protocol::frame::ChannelId;
use fitz::runtime::domain_event::DomainPublishEvent;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use serde_json::json;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CLIENT_SESSION_ID: u64 = 1;
const SUBSCRIBE_REGISTER_BATCH_SIZE: usize = 2048;
const SUBSCRIBE_REGISTER_CASE_COUNT: usize = 4;
const COMMIT_NOTIFY_REPEAT_COUNT: u64 = 2_048;
const COMMIT_NOTIFY_CHUNK_SIZE: u64 = 256;
const SUBSCRIBE_DESTINATION: &str = "stream://realm/area/control/append";
const COMMIT_NOTIFY_ROUTE: &str = "stream://realm/area/orders";

struct PreparedStreamNotifyCase {
    sink: Arc<StreamDomainSink>,
    destination: RouteAddress,
    event: DomainPublishEvent,
    subscriber_sinks: Vec<Arc<CountingSink>>,
}

struct PreparedStreamSubscribeCase {
    router: Arc<Router>,
    family: RouteFamily,
    subscribers: Vec<(u64, RouteAddress, Arc<FrameQueueSink>)>,
    msg_type: u16,
    payload: Bytes,
}

impl PreparedStreamNotifyCase {
    fn publish_once(&self) {
        self.sink
            .deliver(Envelope::new(self.destination.clone(), self.event.clone()))
            .expect("stream commit notify publish");
    }

    fn validate_and_reset(&self) {
        self.publish_once();
        self.wait_for_deliveries_per_subscriber(1);
        self.reset_subscriber_counts();
    }

    fn wait_for_deliveries_per_subscriber(&self, expected_per_subscriber: usize) {
        let delivery_count = wait_for_counting_sinks_each_count(
            &self.subscriber_sinks,
            expected_per_subscriber,
            Duration::from_secs(1),
        );
        assert_eq!(
            delivery_count,
            self.subscriber_sinks.len() * expected_per_subscriber,
            "expected stream notify delivery count per subscriber"
        );
        assert!(
            self.subscriber_sinks
                .iter()
                .all(|sink| sink.count() == expected_per_subscriber),
            "stream notify should not skip or duplicate subscriber deliveries"
        );
    }

    fn reset_subscriber_counts(&self) {
        for subscriber_sink in &self.subscriber_sinks {
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

fn build_stream_subscribe_request(pattern: &str) -> (u16, Bytes) {
    let subscribe_frame = build_stream_subscribe(pattern);
    extract_single_tlv_field(&subscribe_frame)
}

fn prepare_stream_subscribe_case() -> PreparedStreamSubscribeCase {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_stream_sink(router.clone());
    router.register_domain_pattern("stream", sink as Arc<dyn MailboxSink>);
    let (msg_type, payload) = build_stream_subscribe_request(COMMIT_NOTIFY_ROUTE);
    let subscribers = (0..SUBSCRIBE_REGISTER_BATCH_SIZE)
        .map(|index| {
            let session_id = CLIENT_SESSION_ID + index as u64;
            let (source, inbox) = register_session_queue_sink(&router, family, session_id);
            (session_id, source, inbox)
        })
        .collect();

    PreparedStreamSubscribeCase {
        router,
        family,
        subscribers,
        msg_type,
        payload,
    }
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
        assert_eq!(
            subscriber_sink.wait_for_count(1, Duration::from_secs(1)),
            1,
            "stream subscribe should ack before notify measurement"
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

#[stress(tier = 2, name = "subscribe_register_2048_sessions_x4_cases_primary")]
fn should_subscribe_register_2048_sessions_x4_cases_primary(ctx: &mut StressContext) {
    let cases = (0..SUBSCRIBE_REGISTER_CASE_COUNT)
        .map(|_| prepare_stream_subscribe_case())
        .collect::<Vec<_>>();
    let start = Instant::now();
    for case in &cases {
        for (session_id, source, _) in &case.subscribers {
            register_stream_subscription(
                &case.router,
                case.family,
                source,
                *session_id,
                case.msg_type,
                case.payload.clone(),
            );
        }
    }
    let duration = start.elapsed();

    for case in &cases {
        let inboxes = case
            .subscribers
            .iter()
            .map(|(_, _, inbox)| inbox.clone())
            .collect::<Vec<_>>();
        let responses =
            drain_frame_queue_sinks_after_each_count(&inboxes, 1, Duration::from_secs(1));
        assert_eq!(
            responses.len(),
            SUBSCRIBE_REGISTER_BATCH_SIZE,
            "stream subscribe should ack every registration"
        );
        for response in responses {
            assert_stream_subscribe_success(response.payload.as_ref());
        }
    }
    for case in cases {
        case.router.clear();
    }
    tier2_stress::record_duration(
        ctx,
        duration,
        (SUBSCRIBE_REGISTER_BATCH_SIZE * SUBSCRIBE_REGISTER_CASE_COUNT) as u64,
    );
}

fn commit_notify(ctx: &mut StressContext, subscriber_count: usize, pattern: &str) {
    let case = prepare_notify_case(subscriber_count, pattern);
    let mut remaining = COMMIT_NOTIFY_REPEAT_COUNT;
    let mut total = Duration::ZERO;
    while remaining > 0 {
        let chunk = remaining.min(COMMIT_NOTIFY_CHUNK_SIZE);
        case.reset_subscriber_counts();
        let start = Instant::now();
        for _ in 0..chunk {
            case.publish_once();
            black_box(());
        }
        total += start.elapsed();
        let expected_per_subscriber =
            usize::try_from(chunk).expect("stream publish count fits usize");
        case.wait_for_deliveries_per_subscriber(expected_per_subscriber);
        case.reset_subscriber_counts();
        remaining -= chunk;
    }
    tier2_stress::record_duration(
        ctx,
        total,
        COMMIT_NOTIFY_REPEAT_COUNT.saturating_mul(subscriber_count as u64),
    );
}

macro_rules! stream_commit_notify_bench {
    ($fn_name:ident, $stress_name:literal, $subscribers:expr, $pattern:expr) => {
        #[stress(tier = 2, name = $stress_name)]
        fn $fn_name(ctx: &mut StressContext) {
            commit_notify(ctx, $subscribers, $pattern);
        }
    };
}

stream_commit_notify_bench!(
    should_commit_notify_exact_route_16_subscribers_primary,
    "commit_notify_exact_route_16_subscribers_primary",
    16,
    COMMIT_NOTIFY_ROUTE
);
stream_commit_notify_bench!(
    should_commit_notify_exact_route_64_subscribers_primary,
    "commit_notify_exact_route_64_subscribers_primary",
    64,
    COMMIT_NOTIFY_ROUTE
);
stream_commit_notify_bench!(
    should_commit_notify_exact_route_256_subscribers_primary,
    "commit_notify_exact_route_256_subscribers_primary",
    256,
    COMMIT_NOTIFY_ROUTE
);
stream_commit_notify_bench!(
    should_commit_notify_single_star_16_subscribers_primary,
    "commit_notify_single_star_16_subscribers_primary",
    16,
    "stream://realm/area/*"
);
stream_commit_notify_bench!(
    should_commit_notify_single_star_64_subscribers_primary,
    "commit_notify_single_star_64_subscribers_primary",
    64,
    "stream://realm/area/*"
);
stream_commit_notify_bench!(
    should_commit_notify_single_star_256_subscribers_primary,
    "commit_notify_single_star_256_subscribers_primary",
    256,
    "stream://realm/area/*"
);
stream_commit_notify_bench!(
    should_commit_notify_double_star_16_subscribers_primary,
    "commit_notify_double_star_16_subscribers_primary",
    16,
    "stream://realm/**"
);
stream_commit_notify_bench!(
    should_commit_notify_double_star_64_subscribers_primary,
    "commit_notify_double_star_64_subscribers_primary",
    64,
    "stream://realm/**"
);
stream_commit_notify_bench!(
    should_commit_notify_double_star_256_subscribers_primary,
    "commit_notify_double_star_256_subscribers_primary",
    256,
    "stream://realm/**"
);

stress_main!();
