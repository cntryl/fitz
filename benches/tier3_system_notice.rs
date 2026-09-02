// Notice domain tier 3 system benchmarks using live domain sinks.
//
// Concurrent fanout, pattern matching, and subscriber lifecycle.
// Tests sustained notification delivery through the same sink/router path the
// server uses in-process.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via record_completed(count)

#[path = "stress_config.rs"]
mod stress_config;

use stress_config::StressContextExt;

use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_notice_publish, build_notice_subscribe, create_bench_notice_sink,
    extract_single_tlv_field, register_session_counting_sink, route_frame,
    wait_for_counting_sinks_each_count, CountingSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::Duration;

const PUBLISHER_SESSION_ID: u64 = 10_000;
const NOTICE_FANOUT_CONFIRM_BATCH_SIZE: usize = 1_024;
const NOTICE_HIGH_SUBSCRIBER_BATCH_SIZE: usize = 2_048;
// Keep each delivery burst within the production per-family lane capacity. The
// Notice path is intentionally fire-and-forget once that lane is saturated.
const NOTICE_DELIVERY_BATCH_SIZE: usize = 64;
const NOTICE_DELIVERY_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

fn configure_publish_measurement(ctx: &mut StressContext) {
    ctx.parameter("completed_unit", "published_messages");
    ctx.parameter("logical_unit", "publish_message");
}

#[derive(Clone, Copy)]
struct NoticeFanoutCase {
    scenario: &'static str,
    subscriber_count: usize,
    publishes_per_iteration: usize,
    pattern: &'static str,
    publish_route: &'static str,
    payload: &'static [u8],
    match_kind: &'static str,
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
fn measure_notice_fanout(ctx: &mut StressContext, name: &str, case: NoticeFanoutCase) {
    ctx.parameter("scenario", case.scenario);
    ctx.parameter("measurement_scope", "routed_fanout");
    let batch_size_tag = format!("{}_publishes", case.publishes_per_iteration);
    ctx.parameter("batch_size", batch_size_tag.as_str());
    let subscriber_count = case.subscriber_count.to_string();
    ctx.parameter("subscriber_count", subscriber_count.as_str());
    ctx.parameter("match_kind", case.match_kind);
    configure_publish_measurement(ctx);

    let (router, family, publisher_source, subscriber_sinks) =
        setup_notice_sink(case.subscriber_count, case.pattern);
    let publish_frame = build_notice_publish(case.publish_route, case.payload);
    let (msg_type, payload) = extract_single_tlv_field(&publish_frame);
    let mut expected_per_subscriber = 0usize;

    let iterations = ctx.measure_workload(name, || {
        let mut remaining = case.publishes_per_iteration;
        while remaining > 0 {
            let chunk = remaining.min(NOTICE_DELIVERY_BATCH_SIZE);
            for _ in 0..chunk {
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
            expected_per_subscriber += chunk;
            let delivered = wait_for_counting_sinks_each_count(
                &subscriber_sinks,
                expected_per_subscriber,
                NOTICE_DELIVERY_DRAIN_TIMEOUT,
            );
            assert_eq!(
                delivered,
                case.subscriber_count * expected_per_subscriber,
                "notice publish should deliver exactly once per matching subscriber"
            );
            remaining -= chunk;
        }
    });
    let batch_size =
        u64::try_from(case.publishes_per_iteration).expect("notice fanout batch size fits u64");
    stress_config::record_completed(ctx, iterations * batch_size);
}

#[stress(tier = 3)]
fn should_complete_fanout_sustained_load(ctx: &mut StressContext) {
    measure_notice_fanout(
        ctx,
        "complete_fanout_sustained_load",
        NoticeFanoutCase {
            scenario: "sustained_fanout",
            subscriber_count: 1,
            publishes_per_iteration: NOTICE_FANOUT_CONFIRM_BATCH_SIZE,
            pattern: "notice://realm/area/orders/*",
            publish_route: "notice://realm/area/orders/create",
            payload: b"sustained fanout message",
            match_kind: "single_star",
        },
    );
}

#[stress(tier = 3)]
fn should_complete_fanout_high_subscriber_count(ctx: &mut StressContext) {
    measure_notice_fanout(
        ctx,
        "complete_fanout_high_subscriber_count",
        NoticeFanoutCase {
            scenario: "high_subscriber_count",
            subscriber_count: 100,
            publishes_per_iteration: NOTICE_HIGH_SUBSCRIBER_BATCH_SIZE,
            pattern: "notice://realm/area/orders/*",
            publish_route: "notice://realm/area/orders/create",
            payload: b"high subscriber fanout",
            match_kind: "single_star",
        },
    );
}

stress_main!();
