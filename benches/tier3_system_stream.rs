// Stream domain tier 3 system benchmarks using live domain sinks.
//
// Append, read, batch write, multi-area concurrent, and offset tracking.
// These benches drive the same in-proc sink/runtime path as the live server.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via set_elements(count)

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_last,
    build_stream_read, build_stream_subscribe, create_bench_stream_sink, extract_single_tlv_field,
    parse_stream_session_id, register_session_queue_sink, route_frame, FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;

const CLIENT_SESSION_ID: u64 = 1;

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
    let responses = context.inbox.drain();
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("stream response")
}

fn request_with_session(
    context: &StreamBenchContext,
    destination: &str,
    session_id: u64,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_frame(
        context.router.as_ref(),
        &context.source,
        destination,
        session_id,
        ChannelId::Pub,
        msg_type,
        payload,
        context.family,
    )
    .expect("stream route");
    let responses = context.inbox.drain();
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("stream response")
}

fn begin_stream(context: &StreamBenchContext, route: &str, expected_offset: u64) -> u64 {
    let begin_frame = build_stream_begin(route, expected_offset);
    let (msg_type, payload) = extract_single_tlv_field(&begin_frame);
    let response = request(context, route, msg_type, payload);
    parse_stream_session_id(response.as_ref()).expect("stream session id")
}

fn subscribe_stream(context: &StreamBenchContext, route: &str, session_id: u64, pattern: &str) {
    let subscribe_frame = build_stream_subscribe(pattern);
    let (msg_type, payload) = extract_single_tlv_field(&subscribe_frame);
    let _ = request_with_session(context, route, session_id, msg_type, payload);
}

#[stress_test]
fn should_complete_append_sustained_load(ctx: &mut StressContext) {
    ctx.tag("scenario", "sustained_append");

    let context = setup_stream_sink();
    let route = "stream://bench/system/append/append";
    let session_id = begin_stream(&context, route, 0);
    let append_frame = build_stream_append(session_id, b"sustained append event");
    let (msg_type, payload) = extract_single_tlv_field(&append_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(3), || {
        let _ = request(&context, route, msg_type, payload.clone());
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_read_scan_throughput(ctx: &mut StressContext) {
    ctx.tag("scenario", "read_scan");

    let context = setup_stream_sink();
    let route = "stream://bench/system/read/read";
    let session_id = begin_stream(&context, route, 0);
    let append_frame = build_stream_append(session_id, b"read event");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    for _ in 0..100 {
        let _ = request(&context, route, append_msg_type, append_payload.clone());
    }
    let commit_frame = build_stream_commit(session_id, 0);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
    let _ = request(&context, route, commit_msg_type, commit_payload);

    let read_frame = build_stream_read(route, 0);
    let (read_msg_type, read_payload) = extract_single_tlv_field(&read_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(3), || {
        let _ = request(&context, route, read_msg_type, read_payload.clone());
    });
    ctx.set_elements(100 * iterations as u64);
}

#[stress_test]
fn should_complete_batch_write_operations(ctx: &mut StressContext) {
    ctx.tag("scenario", "batch_write");

    let context = setup_stream_sink();
    let route = "stream://bench/system/batch/append";
    let session_id = begin_stream(&context, route, 0);
    let append_frame = build_stream_append(session_id, b"batch event");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(3), || {
        for _ in 0..100 {
            let _ = request(&context, route, append_msg_type, append_payload.clone());
        }
    });
    ctx.set_elements(100 * iterations as u64);
}

#[stress_test]
fn should_complete_multiarea_concurrent_writes(ctx: &mut StressContext) {
    ctx.tag("scenario", "multiarea_writes");

    let context = setup_stream_sink();
    let routes: Vec<String> = (0..10)
        .map(|i| format!("stream://bench/system/area{}/append", i))
        .collect();
    let append_requests: Vec<(String, u16, Bytes)> = routes
        .iter()
        .map(|route| {
            let session_id = begin_stream(&context, route, 0);
            let append_frame = build_stream_append(session_id, b"concurrent write");
            let (msg_type, payload) = extract_single_tlv_field(&append_frame);
            (route.clone(), msg_type, payload)
        })
        .collect();

    let iterations = ctx.measure_for(std::time::Duration::from_secs(3), || {
        for (route, msg_type, payload) in &append_requests {
            let _ = request(&context, route, *msg_type, payload.clone());
        }
    });
    ctx.set_elements(10 * iterations as u64);
}

#[stress_test]
fn should_complete_publish_fanout_with_subscribers(ctx: &mut StressContext) {
    ctx.tag("scenario", "publish_fanout");

    let context = setup_stream_sink();
    let route = "stream://bench/system/fanout/append";

    for session_id in 2..18 {
        subscribe_stream(&context, route, session_id, route);
    }

    let mut commits = Vec::with_capacity(10);
    for _ in 0..10 {
        let stream_session = begin_stream(&context, route, 0);
        let append_frame = build_stream_append(stream_session, b"fanout event");
        let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
        let _ = request(&context, route, append_msg_type, append_payload);
        let commit_frame = build_stream_commit(stream_session, 0);
        let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
        commits.push((stream_session, commit_msg_type, commit_payload));
    }

    let iterations = ctx.measure_for(std::time::Duration::from_secs(3), || {
        for (session_id, msg_type, payload) in &commits {
            let _ = request_with_session(&context, route, *session_id, *msg_type, payload.clone());
        }
    });
    ctx.set_elements(10 * iterations as u64);
}

#[stress_test]
fn should_complete_offset_tracking_overhead(ctx: &mut StressContext) {
    ctx.tag("scenario", "offset_tracking");

    let context = setup_stream_sink();
    let route = "stream://bench/system/offset/append";
    let session_id = begin_stream(&context, route, 0);
    let append_frame = build_stream_append(session_id, b"offset event");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    let _ = request(&context, route, append_msg_type, append_payload);
    let commit_frame = build_stream_commit(session_id, 0);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
    let _ = request(&context, route, commit_msg_type, commit_payload);

    let last_frame = build_stream_last(route);
    let (last_msg_type, last_payload) = extract_single_tlv_field(&last_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(3), || {
        let _ = request(&context, route, last_msg_type, last_payload.clone());
    });
    ctx.set_elements(iterations as u64);
}

stress_main!();
