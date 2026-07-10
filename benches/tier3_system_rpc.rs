#![allow(deprecated)]
//! RPC domain tier 3 system benchmarks using the live RPC domain sink.
//!
//! Measures the real in-proc path: requester frame -> `RpcDomainSink`
//! -> worker inbox delivery -> worker response frame -> requester inbox.

#[path = "stress_config.rs"]
mod stress_config;

use stress_config::StressContextExt;

use bytes::Bytes;
use cntryl_stress::{black_box, stress, stress_main, StressContext};
use fitz::benchkit::{
    build_rpc_request, build_rpc_response_frame, build_rpc_subscribe, create_bench_rpc_sink,
    extract_single_tlv_field, register_session_queue_sink, route_frame, FrameQueueSink,
};
use fitz::domains::rpc::protocol::RpcMessage;
use fitz::protocol::frame::ChannelId;
use fitz::protocol::rpc_codec::parse_request;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;

const ROUTE_STR: &str = "rpc://bench/system/route";
const REQUESTER_SESSION_ID: u64 = 1;
const PENDING_CARDINALITY_CYCLES_PER_ITERATION: u64 = 400;

fn configure_pending_cycle_measurement(ctx: &mut StressContext) {
    ctx.parameter("completed_unit", "pending_cycles");
    ctx.parameter("logical_unit", "pending_cycle");
}

type WorkerHandle = (u64, RouteAddress, Arc<FrameQueueSink>);

struct RequestFrameRing {
    frames: Vec<(u16, Bytes)>,
    next: usize,
}

impl RequestFrameRing {
    fn new(route: &str, payload: &[u8], count: usize) -> Self {
        let frames = (0..count)
            .map(|_| {
                let frame = build_rpc_request(route, payload);
                extract_single_tlv_field(&frame)
            })
            .collect();

        Self { frames, next: 0 }
    }

    fn next_frame(&mut self) -> (u16, Bytes) {
        let (msg_type, payload) = &self.frames[self.next];
        self.next = (self.next + 1) % self.frames.len();
        (*msg_type, payload.clone())
    }
}

fn assert_requester_received_worker_responses(
    frames: Vec<fitz::protocol::frame_context::FrameContext>,
    expected_count: usize,
) {
    let response_count = frames
        .into_iter()
        .filter(|frame| frame.msg_type.as_u16() == 303)
        .count();

    assert_eq!(
        response_count, expected_count,
        "expected requester inbox to contain {expected_count} worker responses"
    );
}

fn setup_rpc_sink() -> (Arc<Router>, RouteFamily, RouteAddress, Arc<FrameQueueSink>) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_rpc_sink(router.clone());
    router.register_domain_pattern("rpc", sink as Arc<dyn MailboxSink>);
    let (requester_source, requester_inbox) =
        register_session_queue_sink(&router, family, REQUESTER_SESSION_ID);
    (router, family, requester_source, requester_inbox)
}

fn register_worker_for_route(
    router: &Arc<Router>,
    family: RouteFamily,
    session_id: u64,
    route: &str,
) -> WorkerHandle {
    let (worker_source, worker_inbox) = register_session_queue_sink(router, family, session_id);
    let subscribe_frame = build_rpc_subscribe(route);
    let (msg_type, payload) = extract_single_tlv_field(&subscribe_frame);
    route_frame(
        router.as_ref(),
        &worker_source,
        route,
        session_id,
        ChannelId::Rpc,
        msg_type,
        payload,
        family,
    )
    .expect("rpc subscribe");
    let _ = worker_inbox.drain();
    (session_id, worker_source, worker_inbox)
}

fn register_worker(router: &Arc<Router>, family: RouteFamily, session_id: u64) -> WorkerHandle {
    register_worker_for_route(router, family, session_id, ROUTE_STR)
}

fn dispatch_request_to_route(
    router: &Arc<Router>,
    family: RouteFamily,
    requester_source: &RouteAddress,
    route: &str,
    request_msg_type: u16,
    request_payload: Bytes,
) {
    route_frame(
        router.as_ref(),
        requester_source,
        route,
        REQUESTER_SESSION_ID,
        ChannelId::Rpc,
        request_msg_type,
        request_payload,
        family,
    )
    .expect("rpc request");
}

fn dispatch_request(
    router: &Arc<Router>,
    family: RouteFamily,
    requester_source: &RouteAddress,
    request_msg_type: u16,
    request_payload: Bytes,
) {
    dispatch_request_to_route(
        router,
        family,
        requester_source,
        ROUTE_STR,
        request_msg_type,
        request_payload,
    );
}

fn route_worker_frame_to_route(
    router: &Arc<Router>,
    family: RouteFamily,
    worker_session_id: u64,
    worker_source: &RouteAddress,
    route: &str,
    frame: &[u8],
) {
    let (msg_type, payload) = extract_single_tlv_field(frame);
    route_frame(
        router.as_ref(),
        worker_source,
        route,
        worker_session_id,
        ChannelId::Rpc,
        msg_type,
        payload,
        family,
    )
    .expect("rpc worker frame");
}

fn drain_request_correlation(
    worker_inbox: &Arc<FrameQueueSink>,
    family: RouteFamily,
) -> uuid::Uuid {
    worker_inbox
        .drain()
        .into_iter()
        .find_map(|frame| match frame.msg_type.as_u16() {
            302 => match parse_request(&frame, &frame.payload, family) {
                Ok(RpcMessage::Request(request)) => Some(request.correlation_id),
                _ => None,
            },
            _ => None,
        })
        .expect("rpc dispatched request")
}

fn measure_pending_cardinality_steady_state(
    ctx: &mut StressContext,
    name: &str,
    pending_count: usize,
    per_iteration_cycles: u64,
) {
    assert!(pending_count > 0, "pending_count must be at least one");

    ctx.parameter("scenario", "pending_cardinality_steady_state");
    ctx.parameter("measurement_scope", "routed_pending");
    ctx.parameter("operation", "response_replenish_cycle");
    ctx.parameter("batch_size", format!("{per_iteration_cycles}_cycles"));
    ctx.parameter("worker_count", "1");
    ctx.parameter("pending_count", pending_count.to_string());
    configure_pending_cycle_measurement(ctx);

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers = vec![register_worker(&router, family, 4_000)];
    let (worker_session_id, worker_source, worker_inbox) = &workers[0];
    let mut request_ring =
        RequestFrameRing::new(ROUTE_STR, b"pending cardinality payload", pending_count + 1);

    for _ in 0..pending_count {
        let (request_msg_type, request_payload) = request_ring.next_frame();
        dispatch_request(
            &router,
            family,
            &requester_source,
            request_msg_type,
            request_payload,
        );
    }

    let mut current_correlation_id = drain_request_correlation(worker_inbox, family);
    let _ = requester_inbox.drain();

    let iterations = ctx.measure_workload(name, || {
        for _ in 0..per_iteration_cycles {
            route_worker_frame_to_route(
                &router,
                family,
                *worker_session_id,
                worker_source,
                ROUTE_STR,
                &build_rpc_response_frame(current_correlation_id, b"pending cardinality payload"),
            );
            assert_requester_received_worker_responses(requester_inbox.drain(), 1);

            let (request_msg_type, request_payload) = request_ring.next_frame();
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload,
            );
            current_correlation_id = drain_request_correlation(worker_inbox, family);
        }
        black_box(&workers);
    });
    stress_config::record_completed(ctx, per_iteration_cycles * iterations);
}

#[stress(tier = 3)]
fn should_complete_pending_cardinality_steady_state_1000(ctx: &mut StressContext) {
    measure_pending_cardinality_steady_state(
        ctx,
        "complete_pending_cardinality_steady_state_1000",
        1000,
        PENDING_CARDINALITY_CYCLES_PER_ITERATION,
    );
}

stress_main!();
