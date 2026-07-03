//! Tier 4 live integration benchmarks for the bench-only Stream storage-model frontier.
//!
//! These benches swap the live test server's `stream` route registration with the
//! promotion-frontier prototype read sink after boot so direct, TCP, and WS reads
//! exercise the real session and transport paths without pretending the production
//! Stream domain has already been ported.

#[path = "support/stream_storage_model.rs"]
mod stream_storage_model;

#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_stream_read_with_limit, count_stream_read_records_from_payload, extract_single_tlv_field,
    register_session_queue_sink, route_raw_frame, shared_bench_runtime, FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::transport::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use std::time::Duration;
use stream_storage_model::{
    install_stream_read_prototype_sink, prepare_area_read_case, prepare_realm_read_case,
    prepare_resource_read_case, PrototypeReadCase, PROTOTYPE_ROUTE_FAMILY,
};

const CLIENT_SESSION_ID: u64 = 1;

struct DirectPrototypeContext {
    _server: TestServer,
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
}

fn start_server_with_promotion_frontier(case: &PrototypeReadCase) -> TestServer {
    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    install_stream_read_prototype_sink(&server.runtime.router(), case.replay_case.clone());
    server
}

fn setup_direct_context(case: &PrototypeReadCase) -> DirectPrototypeContext {
    let server = start_server_with_promotion_frontier(case);
    let router = server.runtime.router();
    let family = RouteFamily::new(PROTOTYPE_ROUTE_FAMILY);
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    DirectPrototypeContext {
        _server: server,
        router,
        family,
        source,
        inbox,
    }
}

fn direct_request(
    context: &DirectPrototypeContext,
    destination: &str,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_raw_frame(
        context.router.as_ref(),
        &context.source,
        destination,
        CLIENT_SESSION_ID,
        ChannelId::Pub,
        msg_type,
        payload,
        context.family,
    )
    .expect("prototype stream route");

    let responses = context.inbox.drain_after_count(1, Duration::from_secs(1));
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("prototype stream response")
}

fn prepare_validated_direct_read(
    context: &DirectPrototypeContext,
    route: &str,
    expected_count: usize,
) -> (u16, Bytes) {
    let read_frame = build_stream_read_with_limit(route, 0, expected_count as u64);
    let (msg_type, payload) = extract_single_tlv_field(&read_frame);
    let response = direct_request(context, route, msg_type, payload.clone());
    let count = count_stream_read_records_from_payload(response.as_ref())
        .expect("prototype direct read count");
    assert_eq!(
        count, expected_count,
        "unexpected direct read count for {route}"
    );
    (msg_type, payload)
}

#[stress_test]
fn should_complete_direct_resource_read_promotion_frontier_live_prototype(ctx: &mut StressContext) {
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "read_resource_exact");
    ctx.tag("measurement_scope", "direct_live_prototype");
    ctx.tag("candidate", "promotion_frontier");
    ctx.tag("batch_size", "100_events_scanned");

    let case = prepare_resource_read_case();
    let context = setup_direct_context(&case);
    let (msg_type, payload) =
        prepare_validated_direct_read(&context, case.route, case.expected_count);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = direct_request(&context, case.route, msg_type, payload.clone());
        },
    );
    ctx.set_elements(case.expected_count as u64 * iterations as u64);
}

#[stress_test]
fn should_complete_direct_area_wildcard_read_promotion_frontier_live_prototype(
    ctx: &mut StressContext,
) {
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "read_area_wildcard");
    ctx.tag("measurement_scope", "direct_live_prototype");
    ctx.tag("candidate", "promotion_frontier");
    ctx.tag("batch_size", "100_events_scanned");

    let case = prepare_area_read_case();
    let context = setup_direct_context(&case);
    let (msg_type, payload) =
        prepare_validated_direct_read(&context, case.route, case.expected_count);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = direct_request(&context, case.route, msg_type, payload.clone());
        },
    );
    ctx.set_elements(case.expected_count as u64 * iterations as u64);
}

#[stress_test]
fn should_complete_direct_realm_wildcard_read_promotion_frontier_live_prototype(
    ctx: &mut StressContext,
) {
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "read_realm_wildcard");
    ctx.tag("measurement_scope", "direct_live_prototype");
    ctx.tag("candidate", "promotion_frontier");
    ctx.tag("batch_size", "100_events_scanned");

    let case = prepare_realm_read_case();
    let context = setup_direct_context(&case);
    let (msg_type, payload) =
        prepare_validated_direct_read(&context, case.route, case.expected_count);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = direct_request(&context, case.route, msg_type, payload.clone());
        },
    );
    ctx.set_elements(case.expected_count as u64 * iterations as u64);
}

#[stress_test]
fn should_complete_tcp_resource_read_promotion_frontier_live_prototype(ctx: &mut StressContext) {
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "read_resource_exact");
    ctx.tag("measurement_scope", "tcp_live_prototype");
    ctx.tag("candidate", "promotion_frontier");
    ctx.tag("batch_size", "100_events_scanned");

    let case = prepare_resource_read_case();
    let runtime = shared_bench_runtime();
    let server = start_server_with_promotion_frontier(&case);
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");
    let read_frame = build_stream_read_with_limit(case.route, 0, case.expected_count as u64);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("resource read response");
    let count = fitz::benchkit::parse_stream_read_record_count(&validated_response)
        .expect("resource read count");
    assert_eq!(
        count, case.expected_count,
        "unexpected tcp resource read count"
    );

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = runtime
                .block_on(client.request(&read_frame, 2000))
                .expect("resource read response");
        },
    );
    ctx.set_elements(case.expected_count as u64 * iterations as u64);
}

#[stress_test]
fn should_complete_tcp_area_wildcard_read_promotion_frontier_live_prototype(
    ctx: &mut StressContext,
) {
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "read_area_wildcard");
    ctx.tag("measurement_scope", "tcp_live_prototype");
    ctx.tag("candidate", "promotion_frontier");
    ctx.tag("batch_size", "100_events_scanned");

    let case = prepare_area_read_case();
    let runtime = shared_bench_runtime();
    let server = start_server_with_promotion_frontier(&case);
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");
    let read_frame = build_stream_read_with_limit(case.route, 0, case.expected_count as u64);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("area read response");
    let count = fitz::benchkit::parse_stream_read_record_count(&validated_response)
        .expect("area read count");
    assert_eq!(count, case.expected_count, "unexpected tcp area read count");

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = runtime
                .block_on(client.request(&read_frame, 2000))
                .expect("area read response");
        },
    );
    ctx.set_elements(case.expected_count as u64 * iterations as u64);
}

#[stress_test]
fn should_complete_tcp_realm_wildcard_read_promotion_frontier_live_prototype(
    ctx: &mut StressContext,
) {
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "read_realm_wildcard");
    ctx.tag("measurement_scope", "tcp_live_prototype");
    ctx.tag("candidate", "promotion_frontier");
    ctx.tag("batch_size", "100_events_scanned");

    let case = prepare_realm_read_case();
    let runtime = shared_bench_runtime();
    let server = start_server_with_promotion_frontier(&case);
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");
    let read_frame = build_stream_read_with_limit(case.route, 0, case.expected_count as u64);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("realm read response");
    let count = fitz::benchkit::parse_stream_read_record_count(&validated_response)
        .expect("realm read count");
    assert_eq!(
        count, case.expected_count,
        "unexpected tcp realm read count"
    );

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = runtime
                .block_on(client.request(&read_frame, 2000))
                .expect("realm read response");
        },
    );
    ctx.set_elements(case.expected_count as u64 * iterations as u64);
}

#[stress_test]
fn should_complete_ws_resource_read_promotion_frontier_live_prototype(ctx: &mut StressContext) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "read_resource_exact");
    ctx.tag("measurement_scope", "ws_live_prototype");
    ctx.tag("candidate", "promotion_frontier");
    ctx.tag("batch_size", "100_events_scanned");

    let case = prepare_resource_read_case();
    let runtime = shared_bench_runtime();
    let server = start_server_with_promotion_frontier(&case);
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");
    let read_frame = build_stream_read_with_limit(case.route, 0, case.expected_count as u64);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("resource read response");
    let count = fitz::benchkit::parse_stream_read_record_count(&validated_response)
        .expect("resource read count");
    assert_eq!(
        count, case.expected_count,
        "unexpected ws resource read count"
    );

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = runtime
                .block_on(client.request(&read_frame, 2000))
                .expect("resource read response");
        },
    );
    ctx.set_elements(case.expected_count as u64 * iterations as u64);
}

#[stress_test]
fn should_complete_ws_area_wildcard_read_promotion_frontier_live_prototype(
    ctx: &mut StressContext,
) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "read_area_wildcard");
    ctx.tag("measurement_scope", "ws_live_prototype");
    ctx.tag("candidate", "promotion_frontier");
    ctx.tag("batch_size", "100_events_scanned");

    let case = prepare_area_read_case();
    let runtime = shared_bench_runtime();
    let server = start_server_with_promotion_frontier(&case);
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");
    let read_frame = build_stream_read_with_limit(case.route, 0, case.expected_count as u64);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("area read response");
    let count = fitz::benchkit::parse_stream_read_record_count(&validated_response)
        .expect("area read count");
    assert_eq!(count, case.expected_count, "unexpected ws area read count");

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = runtime
                .block_on(client.request(&read_frame, 2000))
                .expect("area read response");
        },
    );
    ctx.set_elements(case.expected_count as u64 * iterations as u64);
}

#[stress_test]
fn should_complete_ws_realm_wildcard_read_promotion_frontier_live_prototype(
    ctx: &mut StressContext,
) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "read_realm_wildcard");
    ctx.tag("measurement_scope", "ws_live_prototype");
    ctx.tag("candidate", "promotion_frontier");
    ctx.tag("batch_size", "100_events_scanned");

    let case = prepare_realm_read_case();
    let runtime = shared_bench_runtime();
    let server = start_server_with_promotion_frontier(&case);
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");
    let read_frame = build_stream_read_with_limit(case.route, 0, case.expected_count as u64);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("realm read response");
    let count = fitz::benchkit::parse_stream_read_record_count(&validated_response)
        .expect("realm read count");
    assert_eq!(count, case.expected_count, "unexpected ws realm read count");

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let _ = runtime
                .block_on(client.request(&read_frame, 2000))
                .expect("realm read response");
        },
    );
    ctx.set_elements(case.expected_count as u64 * iterations as u64);
}

stress_main!();
