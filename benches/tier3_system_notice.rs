// Notice domain tier 3 system benchmarks using live domain sinks.
//
// Concurrent fanout, pattern matching, and subscriber lifecycle.
// Tests sustained notification delivery through the same sink/router path the
// server uses in-process.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via set_elements(count)

use bytes::BufMut;
use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_notice_publish, build_notice_subscribe, create_bench_notice_sink,
    extract_single_tlv_field, parse_notice_subscription_id, register_session_counting_sink,
    register_session_queue_sink, route_frame, FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::Duration;

const PUBLISHER_SESSION_ID: u64 = 10_000;
const LIFECYCLE_SESSION_ID: u64 = 20_000;

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
) -> (Arc<Router>, RouteFamily, RouteAddress) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_notice_sink(router.clone());
    router.register_domain_pattern("notice", sink as Arc<dyn MailboxSink>);

    let subscribe_frame = build_notice_subscribe(pattern);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);

    for i in 0..subscriber_count {
        let session_id = i as u64 + 1;
        let (source, _sink) = register_session_counting_sink(&router, family, session_id);
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
    }

    let (publisher_source, _publisher_sink) =
        register_session_counting_sink(&router, family, PUBLISHER_SESSION_ID);

    (router, family, publisher_source)
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

#[stress_test]
fn should_complete_fanout_sustained_load(ctx: &mut StressContext) {
    ctx.tag("scenario", "sustained_fanout");
    ctx.tag("measurement_scope", "routed_fanout");
    ctx.tag("batch_size", "single_publish");
    ctx.tag("subscriber_count", "1");

    let (router, family, publisher_source) = setup_notice_sink(1, "notice://realm/area/orders/*");
    let publish_frame = build_notice_publish(
        "notice://realm/area/orders/create",
        Bytes::from_static(b"sustained fanout message").as_ref(),
    );
    let (msg_type, payload) = extract_single_tlv_field(&publish_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        route_frame(
            router.as_ref(),
            &publisher_source,
            "notice://realm/area/orders/create",
            PUBLISHER_SESSION_ID,
            ChannelId::Pub,
            msg_type,
            payload.clone(),
            family,
        )
        .expect("notice publish");
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_pattern_matching_scaling(ctx: &mut StressContext) {
    ctx.tag("scenario", "pattern_matching");
    ctx.tag("measurement_scope", "routed_fanout");
    ctx.tag("batch_size", "single_publish");
    ctx.tag("subscriber_count", "1");

    let (router, family, publisher_source) = setup_notice_sink(1, "notice://realm/area/**");
    let publish_frame = build_notice_publish(
        "notice://realm/area/orders/created",
        Bytes::from_static(b"pattern match message").as_ref(),
    );
    let (msg_type, payload) = extract_single_tlv_field(&publish_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        route_frame(
            router.as_ref(),
            &publisher_source,
            "notice://realm/area/orders/created",
            PUBLISHER_SESSION_ID,
            ChannelId::Pub,
            msg_type,
            payload.clone(),
            family,
        )
        .expect("notice publish");
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_fanout_high_subscriber_count(ctx: &mut StressContext) {
    ctx.tag("scenario", "high_subscriber_count");
    ctx.tag("measurement_scope", "routed_fanout");
    ctx.tag("batch_size", "single_publish");
    ctx.tag("subscriber_count", "100");

    let (router, family, publisher_source) = setup_notice_sink(100, "notice://realm/area/orders/*");
    let publish_frame = build_notice_publish(
        "notice://realm/area/orders/create",
        Bytes::from_static(b"high subscriber fanout").as_ref(),
    );
    let (msg_type, payload) = extract_single_tlv_field(&publish_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        route_frame(
            router.as_ref(),
            &publisher_source,
            "notice://realm/area/orders/create",
            PUBLISHER_SESSION_ID,
            ChannelId::Pub,
            msg_type,
            payload.clone(),
            family,
        )
        .expect("notice publish");
    });
    ctx.set_elements(iterations as u64);
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

    let iterations = ctx.measure_for(Duration::from_secs(5), || {
        let subscribe_response =
            harness.request(pattern, subscribe_msg_type, subscribe_payload.clone());
        let subscription_id = parse_notice_subscribe_ok(&subscribe_response);

        let unsubscribe_response = harness.request(
            pattern,
            502,
            encode_notice_unsubscribe_payload(subscription_id),
        );
        assert_notice_success(&unsubscribe_response);
    });
    ctx.set_elements(2 * iterations as u64);
}

stress_main!();
