// Notice domain tier 3 system benchmarks using live domain sinks.
//
// Concurrent fanout, pattern matching, and subscriber lifecycle.
// Tests sustained notification delivery through the same sink/router path the
// server uses in-process.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via set_elements(count)

#[path = "stress_config.rs"]
mod stress_config;

use bytes::BufMut;
use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_notice_publish, build_notice_subscribe, create_bench_notice_sink,
    extract_single_tlv_field, parse_notice_subscription_id, register_session_counting_sink,
    register_session_queue_sink, route_frame, wait_for_counting_sinks_each_count, CountingSink,
    FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::Duration;

const PUBLISHER_SESSION_ID: u64 = 10_000;
const LIFECYCLE_SESSION_ID: u64 = 20_000;
const NOTICE_FANOUT_CONFIRM_BATCH_SIZE: usize = 64;

#[derive(Clone, Copy)]
struct NoticeFanoutCase {
    scenario: &'static str,
    subscriber_count: usize,
    pattern: &'static str,
    publish_route: &'static str,
    payload: &'static [u8],
    match_kind: &'static str,
}

struct NoticeRequestHarness {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
    session_id: u64,
}

fn setup_notice_sink(
    subscriber_count: usize,
    pattern: &str,
) -> (
    Arc<Router>,
    RouteFamily,
    RouteAddress,
    Vec<Arc<CountingSink>>,
) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_notice_sink(router.clone());
    router.register_domain_pattern("notice", sink as Arc<dyn MailboxSink>);

    let subscribe_frame = build_notice_subscribe(pattern);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);
    let mut subscriber_sinks = Vec::with_capacity(subscriber_count);

    for i in 0..subscriber_count {
        let session_id = i as u64 + 1;
        let (source, sink) = register_session_counting_sink(&router, family, session_id);
        route_frame(
            router.as_ref(),
            &source,
            pattern,
            session_id,
            ChannelId::Pub,
            subscribe_msg_type,
            subscribe_payload.clone(),
            family,
        )
        .expect("notice subscribe");
        let ack_count = sink.wait_for_count(1, Duration::from_secs(1));
        assert_eq!(
            ack_count, 1,
            "notice subscribe should ack before publish benchmark"
        );
        sink.reset();
        subscriber_sinks.push(sink);
    }

    let (publisher_source, _publisher_sink) =
        register_session_counting_sink(&router, family, PUBLISHER_SESSION_ID);

    (router, family, publisher_source, subscriber_sinks)
}

fn setup_notice_request_sink() -> NoticeRequestHarness {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_notice_sink(router.clone());
    router.register_domain_pattern("notice", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, LIFECYCLE_SESSION_ID);
    NoticeRequestHarness {
        router,
        family,
        source,
        inbox,
        session_id: LIFECYCLE_SESSION_ID,
    }
}

fn assert_notice_success(payload: &[u8]) {
    assert_eq!(payload.first().copied(), Some(0), "expected notice success");
}

fn parse_notice_subscribe_ok(payload: &[u8]) -> u64 {
    assert_notice_success(payload);
    parse_notice_subscription_id(&payload[1..])
        .expect("parse notice subscription id")
        .expect("subscription id")
}

fn encode_notice_unsubscribe_payload(subscription_id: u64) -> Bytes {
    let mut payload = Vec::with_capacity(8);
    payload.put_u64(subscription_id);
    Bytes::from(payload)
}

impl NoticeRequestHarness {
    fn request(&self, destination: &str, msg_type: u16, payload: Bytes) -> Bytes {
        route_frame(
            self.router.as_ref(),
            &self.source,
            destination,
            self.session_id,
            ChannelId::Sub,
            msg_type,
            payload,
            self.family,
        )
        .expect("notice request");

        self.inbox
            .drain()
            .last()
            .map(|frame| frame.payload.clone())
            .expect("notice response")
    }
}

fn measure_notice_fanout(ctx: &mut StressContext, case: NoticeFanoutCase) {
    ctx.tag("scenario", case.scenario);
    ctx.tag("measurement_scope", "routed_fanout");
    let batch_size_tag = format!("{NOTICE_FANOUT_CONFIRM_BATCH_SIZE}_publishes");
    ctx.tag("batch_size", batch_size_tag.as_str());
    let subscriber_count = case.subscriber_count.to_string();
    ctx.tag("subscriber_count", subscriber_count.as_str());
    ctx.tag("match_kind", case.match_kind);

    let (router, family, publisher_source, subscriber_sinks) =
        setup_notice_sink(case.subscriber_count, case.pattern);
    let publish_frame = build_notice_publish(case.publish_route, case.payload);
    let (msg_type, payload) = extract_single_tlv_field(&publish_frame);
    let mut expected_per_subscriber = 0usize;

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            for _ in 0..NOTICE_FANOUT_CONFIRM_BATCH_SIZE {
                route_frame(
                    router.as_ref(),
                    &publisher_source,
                    case.publish_route,
                    PUBLISHER_SESSION_ID,
                    ChannelId::Pub,
                    msg_type,
                    payload.clone(),
                    family,
                )
                .expect("notice publish");
            }
            expected_per_subscriber += NOTICE_FANOUT_CONFIRM_BATCH_SIZE;
            let delivered = wait_for_counting_sinks_each_count(
                &subscriber_sinks,
                expected_per_subscriber,
                Duration::from_secs(1),
            );
            assert_eq!(
                delivered,
                case.subscriber_count * expected_per_subscriber,
                "notice publish should deliver exactly once per matching subscriber"
            );
        },
    );
    let batch_size =
        u64::try_from(NOTICE_FANOUT_CONFIRM_BATCH_SIZE).expect("notice fanout batch size fits u64");
    ctx.set_elements(iterations as u64 * batch_size);
}

fn single_star_scaling_case(subscriber_count: usize) -> NoticeFanoutCase {
    NoticeFanoutCase {
        scenario: "fanout_subscriber_scaling",
        subscriber_count,
        pattern: "notice://realm/area/orders/*",
        publish_route: "notice://realm/area/orders/create",
        payload: b"subscriber scaling fanout",
        match_kind: "single_star",
    }
}

fn double_star_scaling_case(subscriber_count: usize) -> NoticeFanoutCase {
    NoticeFanoutCase {
        scenario: "fanout_subscriber_scaling",
        subscriber_count,
        pattern: "notice://realm/area/orders/**",
        publish_route: "notice://realm/area/orders/create",
        payload: b"subscriber scaling fanout",
        match_kind: "double_star",
    }
}

#[stress_test]
fn should_complete_fanout_sustained_load(ctx: &mut StressContext) {
    measure_notice_fanout(
        ctx,
        NoticeFanoutCase {
            scenario: "sustained_fanout",
            subscriber_count: 1,
            pattern: "notice://realm/area/orders/*",
            publish_route: "notice://realm/area/orders/create",
            payload: b"sustained fanout message",
            match_kind: "single_star",
        },
    );
}

#[stress_test]
fn should_complete_pattern_matching_scaling(ctx: &mut StressContext) {
    measure_notice_fanout(
        ctx,
        NoticeFanoutCase {
            scenario: "pattern_matching",
            subscriber_count: 1,
            pattern: "notice://realm/area/**",
            publish_route: "notice://realm/area/orders/created",
            payload: b"pattern match message",
            match_kind: "double_star",
        },
    );
}

#[stress_test]
fn should_complete_fanout_high_subscriber_count(ctx: &mut StressContext) {
    measure_notice_fanout(
        ctx,
        NoticeFanoutCase {
            scenario: "high_subscriber_count",
            subscriber_count: 100,
            pattern: "notice://realm/area/orders/*",
            publish_route: "notice://realm/area/orders/create",
            payload: b"high subscriber fanout",
            match_kind: "single_star",
        },
    );
}

#[stress_test]
fn should_complete_fanout_subscriber_scaling_1(ctx: &mut StressContext) {
    measure_notice_fanout(ctx, single_star_scaling_case(1));
}

#[stress_test]
fn should_complete_fanout_subscriber_scaling_16(ctx: &mut StressContext) {
    measure_notice_fanout(ctx, single_star_scaling_case(16));
}

#[stress_test]
fn should_complete_fanout_subscriber_scaling_64(ctx: &mut StressContext) {
    measure_notice_fanout(ctx, single_star_scaling_case(64));
}

#[stress_test]
fn should_complete_fanout_subscriber_scaling_256(ctx: &mut StressContext) {
    measure_notice_fanout(ctx, single_star_scaling_case(256));
}

#[stress_test]
fn should_complete_fanout_subscriber_scaling_1000(ctx: &mut StressContext) {
    measure_notice_fanout(ctx, single_star_scaling_case(1000));
}

#[stress_test]
fn should_complete_double_star_fanout_subscriber_scaling_1(ctx: &mut StressContext) {
    measure_notice_fanout(ctx, double_star_scaling_case(1));
}

#[stress_test]
fn should_complete_double_star_fanout_subscriber_scaling_16(ctx: &mut StressContext) {
    measure_notice_fanout(ctx, double_star_scaling_case(16));
}

#[stress_test]
fn should_complete_double_star_fanout_subscriber_scaling_64(ctx: &mut StressContext) {
    measure_notice_fanout(ctx, double_star_scaling_case(64));
}

#[stress_test]
fn should_complete_double_star_fanout_subscriber_scaling_256(ctx: &mut StressContext) {
    measure_notice_fanout(ctx, double_star_scaling_case(256));
}

#[stress_test]
fn should_complete_double_star_fanout_subscriber_scaling_1000(ctx: &mut StressContext) {
    measure_notice_fanout(ctx, double_star_scaling_case(1000));
}

#[stress_test]
fn should_complete_wildcard_subscribe_unsubscribe_cycle(ctx: &mut StressContext) {
    ctx.tag("scenario", "wildcard_subscribe_unsubscribe_cycle");
    ctx.tag("measurement_scope", "routed_lifecycle");
    ctx.tag("batch_size", "1_subscribe_1_unsubscribe");
    ctx.tag("subscriber_count", "1");

    let pattern = "notice://realm/area/orders/*";
    let subscribe_frame = build_notice_subscribe(pattern);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);
    let harness = setup_notice_request_sink();

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let subscribe_response =
                harness.request(pattern, subscribe_msg_type, subscribe_payload.clone());
            let subscription_id = parse_notice_subscribe_ok(&subscribe_response);

            let unsubscribe_response = harness.request(
                pattern,
                502,
                encode_notice_unsubscribe_payload(subscription_id),
            );
            assert_notice_success(&unsubscribe_response);
        },
    );
    ctx.set_elements(2 * iterations as u64);
}

stress_main!();
