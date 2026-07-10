// Stream domain tier 3 system benchmarks using live domain sinks.
//
// Append, read, batch write, multi-area concurrent, and offset tracking.
// These benches drive the same in-proc sink/runtime path as the live server.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via record_completed(count)

#[path = "stress_config.rs"]
mod stress_config;

use stress_config::StressContextExt;

use bytes::Bytes;
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_read,
    count_stream_read_records_from_payload, create_bench_stream_sink, extract_single_tlv_field,
    parse_stream_session_id, register_session_queue_sink, route_frame, FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;
use std::time::Duration;

const CLIENT_SESSION_ID: u64 = 1;
const STREAM_SYNC_COMMIT_MODE: u8 = 1;

fn configure_read_measurement(ctx: &mut StressContext) {
    ctx.parameter("completed_unit", "records_returned");
    ctx.parameter("logical_unit", "stream_record");
}

struct StreamBenchContext {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
}

fn setup_stream_sink() -> StreamBenchContext {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_stream_sink(router.clone());
    router.register_domain_pattern("stream", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    StreamBenchContext {
        router,
        family,
        source,
        inbox,
    }
}

fn request(
    context: &StreamBenchContext,
    destination: &str,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_frame(
        context.router.as_ref(),
        &context.source,
        destination,
        CLIENT_SESSION_ID,
        ChannelId::Pub,
        msg_type,
        payload,
        context.family,
    )
    .expect("stream route");
    let responses = context.inbox.drain_after_count(1, Duration::from_secs(1));
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("stream response")
}

fn begin_stream(context: &StreamBenchContext, route: &str) -> u64 {
    let begin_frame = build_stream_begin(route);
    let (msg_type, payload) = extract_single_tlv_field(&begin_frame);
    let response = request(context, route, msg_type, payload);
    parse_stream_session_id(response.as_ref()).unwrap_or_else(|error| {
        panic!("stream session id for {route}: {error}; response={response:?}");
    })
}

fn seed_committed_stream_route(
    context: &StreamBenchContext,
    route: &str,
    event_count: usize,
    body: &'static [u8],
) {
    let session_id = begin_stream(context, route);
    for expected_offset in 0..event_count as u64 {
        let append_frame = build_stream_append(session_id, expected_offset, body);
        let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
        let _ = request(context, route, append_msg_type, append_payload);
    }

    let commit_frame = build_stream_commit(session_id, STREAM_SYNC_COMMIT_MODE);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
    let _ = request(context, route, commit_msg_type, commit_payload);
}

fn prepare_validated_read(
    context: &StreamBenchContext,
    route: &str,
    expected_count: usize,
) -> (u16, Bytes) {
    let read_frame = build_stream_read(route, 0);
    let (read_msg_type, read_payload) = extract_single_tlv_field(&read_frame);
    let response = request(context, route, read_msg_type, read_payload.clone());
    let count = count_stream_read_records_from_payload(response.as_ref())
        .expect("stream read response count");
    assert_eq!(
        count, expected_count,
        "unexpected stream read count for {route}"
    );
    (read_msg_type, read_payload)
}

#[stress(tier = 3)]
fn should_complete_read_scan_throughput(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_scan");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("batch_size", "100_events_scanned");
    configure_read_measurement(ctx);

    let context = setup_stream_sink();
    let route = "stream://bench/system/read/read";
    let session_id = begin_stream(&context, route);
    let append_frame = build_stream_append(session_id, 0, b"read event");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    for _ in 0..100 {
        let _ = request(&context, route, append_msg_type, append_payload.clone());
    }
    let commit_frame = build_stream_commit(session_id, STREAM_SYNC_COMMIT_MODE);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
    let _ = request(&context, route, commit_msg_type, commit_payload);

    let read_frame = build_stream_read(route, 0);
    let (read_msg_type, read_payload) = extract_single_tlv_field(&read_frame);

    let iterations = ctx.measure_workload("complete_read_scan_throughput", || {
        let _ = request(&context, route, read_msg_type, read_payload.clone());
    });
    stress_config::record_completed(ctx, 100 * iterations);
}

#[stress(tier = 3)]
fn should_complete_area_wildcard_read_throughput(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_area_wildcard");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("read_scope", "area");
    ctx.parameter("batch_size", "100_events_scanned");
    configure_read_measurement(ctx);

    let context = setup_stream_sink();
    seed_committed_stream_route(
        &context,
        "stream://bench/area/orders",
        50,
        b"area read event",
    );
    seed_committed_stream_route(
        &context,
        "stream://bench/area/audits",
        50,
        b"area read event",
    );

    let read_route = "stream://bench/area/*";
    let (read_msg_type, read_payload) = prepare_validated_read(&context, read_route, 100);

    let iterations = ctx.measure_workload("complete_area_wildcard_read_throughput", || {
        let _ = request(&context, read_route, read_msg_type, read_payload.clone());
    });
    stress_config::record_completed(ctx, 100 * iterations);
}

stress_main!();
