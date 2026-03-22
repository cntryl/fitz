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
    build_stream_read, create_bench_stream_sink, extract_single_tlv_field, parse_stream_session_id,
    register_session_queue_sink, route_frame, FrameQueueSink,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;

const CLIENT_SESSION_ID: u64 = 1;

fn setup_stream_sink() -> (Arc<Router>, RouteFamily, RouteAddress, Arc<FrameQueueSink>) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_stream_sink(router.clone());
    router.register_domain_pattern("stream", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, CLIENT_SESSION_ID);
    (router, family, source, inbox)
}

fn request(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    inbox: &Arc<FrameQueueSink>,
    destination: &str,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_frame(
        router.as_ref(),
        source,
        destination,
        CLIENT_SESSION_ID,
        ChannelId::Pub,
        msg_type,
        payload,
        family,
    )
    .expect("stream route");
    let responses = inbox.drain();
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("stream response")
}

fn begin_stream(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    inbox: &Arc<FrameQueueSink>,
    route: &str,
    expected_offset: u64,
) -> u64 {
    let begin_frame = build_stream_begin(route, expected_offset);
    let (msg_type, payload) = extract_single_tlv_field(&begin_frame);
    let response = request(router, family, source, inbox, route, msg_type, payload);
    parse_stream_session_id(response.as_ref()).expect("stream session id")
}

#[stress_test]
fn should_complete_append_sustained_load(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "sustained_append");

    let (router, family, source, inbox) = setup_stream_sink();
    let route = "stream://bench/system/append/append";
    let session_id = begin_stream(&router, family, &source, &inbox, route, 0);
    let append_frame = build_stream_append(session_id, b"sustained append event");
    let (msg_type, payload) = extract_single_tlv_field(&append_frame);

    ctx.measure(|| {
        let _ = request(
            &router,
            family,
            &source,
            &inbox,
            route,
            msg_type,
            payload.clone(),
        );
    });
}

#[stress_test]
fn should_complete_read_scan_throughput(ctx: &mut StressContext) {
    ctx.set_elements(100);
    ctx.tag("scenario", "read_scan");

    let (router, family, source, inbox) = setup_stream_sink();
    let route = "stream://bench/system/read/read";
    let session_id = begin_stream(&router, family, &source, &inbox, route, 0);
    let append_frame = build_stream_append(session_id, b"read event");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    for _ in 0..100 {
        let _ = request(
            &router,
            family,
            &source,
            &inbox,
            route,
            append_msg_type,
            append_payload.clone(),
        );
    }
    let commit_frame = build_stream_commit(session_id, 0);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
    let _ = request(
        &router,
        family,
        &source,
        &inbox,
        route,
        commit_msg_type,
        commit_payload,
    );

    let read_frame = build_stream_read(route, 0);
    let (read_msg_type, read_payload) = extract_single_tlv_field(&read_frame);

    ctx.measure(|| {
        let _ = request(
            &router,
            family,
            &source,
            &inbox,
            route,
            read_msg_type,
            read_payload.clone(),
        );
    });
}

#[stress_test]
fn should_complete_batch_write_operations(ctx: &mut StressContext) {
    ctx.set_elements(100);
    ctx.tag("scenario", "batch_write");

    let (router, family, source, inbox) = setup_stream_sink();
    let route = "stream://bench/system/batch/append";
    let session_id = begin_stream(&router, family, &source, &inbox, route, 0);
    let append_frame = build_stream_append(session_id, b"batch event");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);

    ctx.measure(|| {
        for _ in 0..100 {
            let _ = request(
                &router,
                family,
                &source,
                &inbox,
                route,
                append_msg_type,
                append_payload.clone(),
            );
        }
    });
}

#[stress_test]
fn should_complete_multiarea_concurrent_writes(ctx: &mut StressContext) {
    ctx.set_elements(10);
    ctx.tag("scenario", "multiarea_writes");

    let (router, family, source, inbox) = setup_stream_sink();
    let routes: Vec<String> = (0..10)
        .map(|i| format!("stream://bench/system/area{}/append", i))
        .collect();
    let append_requests: Vec<(String, u16, Bytes)> = routes
        .iter()
        .map(|route| {
            let session_id = begin_stream(&router, family, &source, &inbox, route, 0);
            let append_frame = build_stream_append(session_id, b"concurrent write");
            let (msg_type, payload) = extract_single_tlv_field(&append_frame);
            (route.clone(), msg_type, payload)
        })
        .collect();

    ctx.measure(|| {
        for (route, msg_type, payload) in &append_requests {
            let _ = request(
                &router,
                family,
                &source,
                &inbox,
                route,
                *msg_type,
                payload.clone(),
            );
        }
    });
}

#[stress_test]
fn should_complete_offset_tracking_overhead(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "offset_tracking");

    let (router, family, source, inbox) = setup_stream_sink();
    let route = "stream://bench/system/offset/append";
    let session_id = begin_stream(&router, family, &source, &inbox, route, 0);
    let append_frame = build_stream_append(session_id, b"offset event");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    let _ = request(
        &router,
        family,
        &source,
        &inbox,
        route,
        append_msg_type,
        append_payload,
    );
    let commit_frame = build_stream_commit(session_id, 0);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
    let _ = request(
        &router,
        family,
        &source,
        &inbox,
        route,
        commit_msg_type,
        commit_payload,
    );

    let last_frame = build_stream_last(route);
    let (last_msg_type, last_payload) = extract_single_tlv_field(&last_frame);

    ctx.measure(|| {
        let _ = request(
            &router,
            family,
            &source,
            &inbox,
            route,
            last_msg_type,
            last_payload.clone(),
        );
    });
}

stress_main!();
