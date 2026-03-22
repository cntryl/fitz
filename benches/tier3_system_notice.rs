// Notice domain tier 3 system benchmarks using live domain sinks.
//
// Concurrent fanout, pattern matching, and subscriber lifecycle.
// Tests sustained notification delivery through the same sink/router path the
// server uses in-process.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via set_elements(count)

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_notice_publish, build_notice_subscribe, create_bench_notice_sink,
    extract_single_tlv_field, register_session_counting_sink, route_frame,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;

const PUBLISHER_SESSION_ID: u64 = 10_000;

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

#[stress_test]
fn should_complete_fanout_sustained_load(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "sustained_fanout");

    let (router, family, publisher_source) = setup_notice_sink(1, "notice://realm/area/orders/*");
    let publish_frame = build_notice_publish(
        "notice://realm/area/orders/create",
        Bytes::from_static(b"sustained fanout message").as_ref(),
    );
    let (msg_type, payload) = extract_single_tlv_field(&publish_frame);

    ctx.measure(|| {
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
}

#[stress_test]
fn should_complete_pattern_matching_scaling(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "pattern_matching");

    let (router, family, publisher_source) = setup_notice_sink(1, "notice://realm/area/**");
    let publish_frame = build_notice_publish(
        "notice://realm/area/orders/created",
        Bytes::from_static(b"pattern match message").as_ref(),
    );
    let (msg_type, payload) = extract_single_tlv_field(&publish_frame);

    ctx.measure(|| {
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
}

#[stress_test]
fn should_complete_fanout_high_subscriber_count(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "high_subscriber_count");

    let (router, family, publisher_source) = setup_notice_sink(100, "notice://realm/area/orders/*");
    let publish_frame = build_notice_publish(
        "notice://realm/area/orders/create",
        Bytes::from_static(b"high subscriber fanout").as_ref(),
    );
    let (msg_type, payload) = extract_single_tlv_field(&publish_frame);

    ctx.measure(|| {
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
}

stress_main!();
