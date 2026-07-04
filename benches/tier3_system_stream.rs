// Stream domain tier 3 system benchmarks using live domain sinks.
//
// Append, read, batch write, multi-area concurrent, and offset tracking.
// These benches drive the same in-proc sink/runtime path as the live server.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via record_completed(count)

#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_last,
    build_stream_read, build_stream_subscribe, count_stream_read_records_from_payload,
    create_bench_stream_sink, create_write_heavy_bench_stream_sink, extract_single_tlv_field,
    parse_stream_session_id, register_session_counting_sink, register_session_queue_sink,
    route_frame, FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

const CLIENT_SESSION_ID: u64 = 1;
const STREAM_SYNC_COMMIT_MODE: u8 = 1;

struct StreamBenchContext {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
}

fn setup_stream_sink() -> StreamBenchContext {
    setup_stream_sink_with(false)
}

fn setup_write_heavy_stream_sink() -> StreamBenchContext {
    setup_stream_sink_with(true)
}

fn setup_stream_sink_with(write_heavy: bool) -> StreamBenchContext {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = if write_heavy {
        create_write_heavy_bench_stream_sink(router.clone())
    } else {
        create_bench_stream_sink(router.clone())
    };
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

fn assert_stream_success(operation: &str, route: &str, response: &Bytes) {
    assert!(
        response.first().copied() == Some(0),
        "stream {operation} failed for {route}; response={response:?}"
    );
}

fn subscribe_stream(
    context: &StreamBenchContext,
    source: &RouteAddress,
    destination: &str,
    session_id: u64,
    pattern: &str,
) {
    let subscribe_frame = build_stream_subscribe(pattern);
    let (msg_type, payload) = extract_single_tlv_field(&subscribe_frame);
    route_frame(
        context.router.as_ref(),
        source,
        destination,
        session_id,
        ChannelId::Pub,
        msg_type,
        payload,
        context.family,
    )
    .expect("stream subscribe");
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

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_append_sustained_load(ctx: &mut StressContext) {
    ctx.parameter("scenario", "sustained_append");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("batch_size", "single_append");

    let context = setup_stream_sink();
    let route = "stream://bench/system/append/append";
    let session_id = begin_stream(&context, route);
    let append_frame = build_stream_append(session_id, 0, b"sustained append event");
    let (msg_type, payload) = extract_single_tlv_field(&append_frame);

    let iterations = ctx.measure_workload(|| {
        let _ = request(&context, route, msg_type, payload.clone());
    });
    stress_config::record_completed(ctx, iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_read_scan_throughput(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_scan");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("batch_size", "100_events_scanned");

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

    let iterations = ctx.measure_workload(|| {
        let _ = request(&context, route, read_msg_type, read_payload.clone());
    });
    stress_config::record_completed(ctx, 100 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_area_wildcard_read_throughput(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_area_wildcard");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("read_scope", "area");
    ctx.parameter("batch_size", "100_events_scanned");

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

    let iterations = ctx.measure_workload(|| {
        let _ = request(&context, read_route, read_msg_type, read_payload.clone());
    });
    stress_config::record_completed(ctx, 100 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_realm_wildcard_read_throughput(ctx: &mut StressContext) {
    ctx.parameter("scenario", "read_realm_wildcard");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("read_scope", "realm");
    ctx.parameter("batch_size", "100_events_scanned");

    let context = setup_stream_sink();
    seed_committed_stream_route(
        &context,
        "stream://bench/events/orders",
        50,
        b"realm read event",
    );
    seed_committed_stream_route(
        &context,
        "stream://bench/audit/ledger",
        50,
        b"realm read event",
    );

    let read_route = "stream://bench/*/*";
    let (read_msg_type, read_payload) = prepare_validated_read(&context, read_route, 100);

    let iterations = ctx.measure_workload(|| {
        let _ = request(&context, read_route, read_msg_type, read_payload.clone());
    });
    stress_config::record_completed(ctx, 100 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_batch_write_operations(ctx: &mut StressContext) {
    ctx.parameter("scenario", "batch_write");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("batch_size", "100_appends");

    let context = setup_stream_sink();
    let route = "stream://bench/system/batch/append";
    let session_id = begin_stream(&context, route);
    let append_frame = build_stream_append(session_id, 0, b"batch event");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);

    let iterations = ctx.measure_workload(|| {
        for _ in 0..100 {
            let _ = request(&context, route, append_msg_type, append_payload.clone());
        }
    });
    stress_config::record_completed(ctx, 100 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_multiarea_concurrent_writes(ctx: &mut StressContext) {
    ctx.parameter("scenario", "multiarea_writes");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("batch_size", "10_appends");
    ctx.parameter("area_count", "10");

    let context = setup_stream_sink();
    let routes: Vec<String> = (0..10)
        .map(|i| format!("stream://bench/system/area{i}/append"))
        .collect();
    let append_requests: Vec<(String, u16, Bytes)> = routes
        .iter()
        .map(|route| {
            let session_id = begin_stream(&context, route);
            let append_frame = build_stream_append(session_id, 0, b"concurrent write");
            let (msg_type, payload) = extract_single_tlv_field(&append_frame);
            (route.clone(), msg_type, payload)
        })
        .collect();

    let iterations = ctx.measure_workload(|| {
        for (route, msg_type, payload) in &append_requests {
            let _ = request(&context, route, *msg_type, payload.clone());
        }
    });
    stress_config::record_completed(ctx, 10 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_publish_fanout_with_subscribers(ctx: &mut StressContext) {
    ctx.parameter("scenario", "publish_fanout");
    ctx.parameter("measurement_scope", "routed_fanout");
    ctx.parameter("batch_size", "10_publishes");
    ctx.parameter("subscriber_count", "16");

    let context = setup_write_heavy_stream_sink();
    let subscribe_destination = "stream://bench/system/fanout-control/append";
    // Stream commit notifications are published on the committed resource route, not the append route.
    let notify_pattern = "stream://bench/system/*";
    let publish_routes: Vec<String> = (0..10)
        .map(|index| format!("stream://bench/system/fanout-{index}/append"))
        .collect();
    let expected_offsets: Vec<Cell<u64>> = publish_routes.iter().map(|_| Cell::new(0)).collect();

    let _subscriber_sinks: Vec<_> = (2..18)
        .map(|session_id| {
            let (source, sink) =
                register_session_counting_sink(&context.router, context.family, session_id);
            subscribe_stream(
                &context,
                &source,
                subscribe_destination,
                session_id,
                notify_pattern,
            );
            sink
        })
        .collect();

    let iterations = ctx.measure_workload(|| {
        for (route, expected_offset) in publish_routes.iter().zip(expected_offsets.iter()) {
            let stream_session = begin_stream(&context, route);

            let append_frame =
                build_stream_append(stream_session, expected_offset.get(), b"fanout event");
            let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
            let append_response = request(&context, route, append_msg_type, append_payload);
            assert_stream_success("append", route, &append_response);

            let commit_frame = build_stream_commit(stream_session, STREAM_SYNC_COMMIT_MODE);
            let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
            let commit_response = request(&context, route, commit_msg_type, commit_payload);
            assert_stream_success("commit", route, &commit_response);

            expected_offset.set(expected_offset.get().saturating_add(1));
        }
    });
    stress_config::record_completed(ctx, publish_routes.len() as u64 * iterations);
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_offset_tracking_overhead(ctx: &mut StressContext) {
    ctx.parameter("scenario", "offset_tracking");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("batch_size", "single_last_read");

    let context = setup_stream_sink();
    let route = "stream://bench/system/offset/append";
    let session_id = begin_stream(&context, route);
    let append_frame = build_stream_append(session_id, 0, b"offset event");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    let _ = request(&context, route, append_msg_type, append_payload);
    let commit_frame = build_stream_commit(session_id, 0);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
    let _ = request(&context, route, commit_msg_type, commit_payload);

    let last_frame = build_stream_last(route);
    let (last_msg_type, last_payload) = extract_single_tlv_field(&last_frame);

    let iterations = ctx.measure_workload(|| {
        let _ = request(&context, route, last_msg_type, last_payload.clone());
    });
    stress_config::record_completed(ctx, iterations);
}

stress_main!();
