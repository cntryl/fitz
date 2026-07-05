#![allow(deprecated)]
//! RPC domain tier 3 system benchmarks using the live RPC domain sink.
//!
//! Measures the real in-proc path: requester frame -> `RpcDomainSink`
//! -> worker inbox delivery -> worker response frame -> requester inbox.

#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_rpc_request, build_rpc_response_frame, build_rpc_subscribe, create_bench_rpc_sink,
    extract_single_tlv_field, register_session_queue_sink, route_frame, FrameQueueSink,
};
use fitz::domains::rpc::protocol::RpcMessage;
use fitz::protocol::frame::ChannelId;
use fitz::protocol::rpc_codec::parse_request;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::hint::black_box;
use std::sync::Arc;

const ROUTE_STR: &str = "rpc://bench/system/route";
const REQUESTER_SESSION_ID: u64 = 1;
const REQUEST_FRAME_RING_SIZE: usize = 2048;
const MULTI_ROUTE_REQUEST_FRAME_RING_SIZE: usize = 256;
const MULTI_ROUTE_COUNT: usize = 64;

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

fn build_route_set(route_count: usize) -> Vec<String> {
    (0..route_count)
        .map(|index| format!("rpc://bench/system/route/{index}"))
        .collect()
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

fn service_worker_for_route(
    router: &Arc<Router>,
    family: RouteFamily,
    route: &str,
    worker_session_id: u64,
    worker_source: &RouteAddress,
    worker_inbox: &Arc<FrameQueueSink>,
) -> usize {
    let mut responses = 0usize;

    loop {
        let frames = worker_inbox.drain();
        if frames.is_empty() {
            break;
        }

        let mut handled_request = false;
        for frame in frames {
            if frame.msg_type.as_u16() == 302 {
                handled_request = true;
                if let Ok(RpcMessage::Request(request)) =
                    parse_request(&frame, &frame.payload, family)
                {
                    route_worker_frame_to_route(
                        router,
                        family,
                        worker_session_id,
                        worker_source,
                        route,
                        &build_rpc_response_frame(request.correlation_id, request.body.as_ref()),
                    );
                    responses += 1;
                }
            }
        }

        if !handled_request {
            break;
        }
    }

    responses
}

fn service_expected_worker(
    router: &Arc<Router>,
    family: RouteFamily,
    workers: &[WorkerHandle],
    next_worker_index: &mut usize,
) -> usize {
    let (session_id, source, inbox) = &workers[*next_worker_index];
    *next_worker_index = (*next_worker_index + 1) % workers.len();
    service_worker_for_route(router, family, ROUTE_STR, *session_id, source, inbox)
}

fn service_worker_on_route(
    router: &Arc<Router>,
    family: RouteFamily,
    route: &str,
    worker: &WorkerHandle,
) -> usize {
    let (session_id, source, inbox) = worker;
    service_worker_for_route(router, family, route, *session_id, source, inbox)
}

fn cleanup_expected_worker_request_for_route(
    router: &Arc<Router>,
    family: RouteFamily,
    route: &str,
    workers: &[WorkerHandle],
    next_worker_index: &mut usize,
) {
    let (worker_session_id, worker_source, worker_inbox) = &workers[*next_worker_index];
    *next_worker_index = (*next_worker_index + 1) % workers.len();

    let correlation_id = worker_inbox
        .drain()
        .into_iter()
        .find_map(|frame| match frame.msg_type.as_u16() {
            302 => match parse_request(&frame, &frame.payload, family) {
                Ok(RpcMessage::Request(request)) => Some(request.correlation_id),
                _ => None,
            },
            _ => None,
        })
        .expect("rpc dispatched request");

    route_worker_frame_to_route(
        router,
        family,
        *worker_session_id,
        worker_source,
        route,
        &build_rpc_response_frame(correlation_id, b"cleanup"),
    );
}

fn cleanup_expected_worker_request(
    router: &Arc<Router>,
    family: RouteFamily,
    workers: &[WorkerHandle],
    next_worker_index: &mut usize,
) {
    cleanup_expected_worker_request_for_route(
        router,
        family,
        ROUTE_STR,
        workers,
        next_worker_index,
    );
}

fn cleanup_worker_request_on_route(
    router: &Arc<Router>,
    family: RouteFamily,
    route: &str,
    worker: &WorkerHandle,
) {
    let (worker_session_id, worker_source, worker_inbox) = worker;
    let correlation_id = worker_inbox
        .drain()
        .into_iter()
        .find_map(|frame| match frame.msg_type.as_u16() {
            302 => match parse_request(&frame, &frame.payload, family) {
                Ok(RpcMessage::Request(request)) => Some(request.correlation_id),
                _ => None,
            },
            _ => None,
        })
        .expect("rpc dispatched request");

    route_worker_frame_to_route(
        router,
        family,
        *worker_session_id,
        worker_source,
        route,
        &build_rpc_response_frame(correlation_id, b"cleanup"),
    );
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

fn measure_full_roundtrip_scaling(
    ctx: &mut StressContext,
    worker_count: usize,
    per_iteration_requests: u64,
    scenario: &'static str,
) {
    ctx.parameter("scenario", scenario);
    ctx.parameter("measurement_scope", "routed_roundtrip");
    ctx.parameter("operation", "dispatch_service_response");
    ctx.parameter("batch_size", format!("{per_iteration_requests}_roundtrips"));
    ctx.parameter("worker_count", worker_count.to_string());

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers: Vec<WorkerHandle> = (0..worker_count)
        .map(|index| register_worker(&router, family, 1_000 + index as u64))
        .collect();
    let mut request_ring =
        RequestFrameRing::new(ROUTE_STR, b"scaling payload", REQUEST_FRAME_RING_SIZE);
    let mut next_worker_index = 0usize;

    let iterations = ctx.measure_workload(|| {
        for _ in 0..per_iteration_requests {
            let (request_msg_type, request_payload) = request_ring.next_frame();
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload,
            );
            let _ = service_expected_worker(&router, family, &workers, &mut next_worker_index);
            assert_requester_received_worker_responses(requester_inbox.drain(), 1);
        }
        black_box(&workers);
    });
    stress_config::record_completed(ctx, per_iteration_requests * iterations);
}

fn measure_dispatch_only_scaling(
    ctx: &mut StressContext,
    worker_count: usize,
    per_iteration_requests: u64,
    scenario: &'static str,
) {
    ctx.parameter("scenario", scenario);
    ctx.parameter("measurement_scope", "routed_dispatch_only");
    ctx.parameter("operation", "dispatch_response_cleanup");
    ctx.parameter("batch_size", format!("{per_iteration_requests}_dispatches"));
    ctx.parameter("worker_count", worker_count.to_string());

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers: Vec<WorkerHandle> = (0..worker_count)
        .map(|index| register_worker(&router, family, 1_000 + index as u64))
        .collect();
    let mut request_ring =
        RequestFrameRing::new(ROUTE_STR, b"scaling payload", REQUEST_FRAME_RING_SIZE);
    let mut next_worker_index = 0usize;

    let iterations = ctx.measure_workload(|| {
        for _ in 0..per_iteration_requests {
            let (request_msg_type, request_payload) = request_ring.next_frame();
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload,
            );
            cleanup_expected_worker_request(&router, family, &workers, &mut next_worker_index);
            let _ = requester_inbox.drain();
        }
        black_box(&workers);
    });
    stress_config::record_completed(ctx, per_iteration_requests * iterations);
}

fn measure_multi_route_full_roundtrip_scaling(
    ctx: &mut StressContext,
    route_count: usize,
    per_iteration_requests: u64,
    scenario: &'static str,
) {
    ctx.parameter("scenario", scenario);
    ctx.parameter("measurement_scope", "routed_roundtrip");
    ctx.parameter("operation", "dispatch_service_response");
    ctx.parameter("batch_size", format!("{per_iteration_requests}_roundtrips"));
    ctx.parameter("worker_count", route_count.to_string());
    ctx.parameter("route_count", route_count.to_string());

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let rpc_routes = build_route_set(route_count);
    let workers: Vec<WorkerHandle> = rpc_routes
        .iter()
        .enumerate()
        .map(|(index, route)| {
            register_worker_for_route(&router, family, 2_000 + index as u64, route)
        })
        .collect();
    let mut request_rings: Vec<RequestFrameRing> = rpc_routes
        .iter()
        .map(|route| {
            RequestFrameRing::new(
                route,
                b"multi route scaling payload",
                MULTI_ROUTE_REQUEST_FRAME_RING_SIZE,
            )
        })
        .collect();
    let mut next_route_index = 0usize;

    let iterations = ctx.measure_workload(|| {
        for _ in 0..per_iteration_requests {
            let route_index = next_route_index;
            next_route_index = (next_route_index + 1) % rpc_routes.len();

            let (request_msg_type, request_payload) = request_rings[route_index].next_frame();
            dispatch_request_to_route(
                &router,
                family,
                &requester_source,
                &rpc_routes[route_index],
                request_msg_type,
                request_payload,
            );
            let _ = service_worker_on_route(
                &router,
                family,
                &rpc_routes[route_index],
                &workers[route_index],
            );
            assert_requester_received_worker_responses(requester_inbox.drain(), 1);
        }
        black_box((&rpc_routes, &workers));
    });
    stress_config::record_completed(ctx, per_iteration_requests * iterations);
}

fn measure_multi_route_dispatch_only_scaling(
    ctx: &mut StressContext,
    route_count: usize,
    per_iteration_requests: u64,
    scenario: &'static str,
) {
    ctx.parameter("scenario", scenario);
    ctx.parameter("measurement_scope", "routed_dispatch_only");
    ctx.parameter("operation", "dispatch_response_cleanup");
    ctx.parameter("batch_size", format!("{per_iteration_requests}_dispatches"));
    ctx.parameter("worker_count", route_count.to_string());
    ctx.parameter("route_count", route_count.to_string());

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let rpc_routes = build_route_set(route_count);
    let workers: Vec<WorkerHandle> = rpc_routes
        .iter()
        .enumerate()
        .map(|(index, route)| {
            register_worker_for_route(&router, family, 3_000 + index as u64, route)
        })
        .collect();
    let mut request_rings: Vec<RequestFrameRing> = rpc_routes
        .iter()
        .map(|route| {
            RequestFrameRing::new(
                route,
                b"multi route scaling payload",
                MULTI_ROUTE_REQUEST_FRAME_RING_SIZE,
            )
        })
        .collect();
    let mut next_route_index = 0usize;

    let iterations = ctx.measure_workload(|| {
        for _ in 0..per_iteration_requests {
            let route_index = next_route_index;
            next_route_index = (next_route_index + 1) % rpc_routes.len();

            let (request_msg_type, request_payload) = request_rings[route_index].next_frame();
            dispatch_request_to_route(
                &router,
                family,
                &requester_source,
                &rpc_routes[route_index],
                request_msg_type,
                request_payload,
            );
            cleanup_worker_request_on_route(
                &router,
                family,
                &rpc_routes[route_index],
                &workers[route_index],
            );
            let _ = requester_inbox.drain();
        }
        black_box((&rpc_routes, &workers));
    });
    stress_config::record_completed(ctx, per_iteration_requests * iterations);
}

fn measure_pending_cardinality_steady_state(
    ctx: &mut StressContext,
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

    let iterations = ctx.measure_workload(|| {
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

#[stress_test(tier = 3)]
fn should_complete_request_dispatch_sustained(ctx: &mut StressContext) {
    const ITERS: u64 = 1000;
    ctx.parameter("scenario", "sustained_dispatch");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("operation", "dispatch_service_response");
    ctx.parameter("batch_size", "1000_roundtrips");
    ctx.parameter("worker_count", "1");

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers = vec![register_worker(&router, family, 100)];
    let mut request_ring =
        RequestFrameRing::new(ROUTE_STR, b"rpc request payload", REQUEST_FRAME_RING_SIZE);
    let mut next_worker_index = 0usize;

    let iterations = ctx.measure_workload(|| {
        for _ in 0..ITERS {
            let (request_msg_type, request_payload) = request_ring.next_frame();
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload,
            );
            let _ = service_expected_worker(&router, family, &workers, &mut next_worker_index);
            assert_requester_received_worker_responses(requester_inbox.drain(), 1);
        }
        black_box(&workers);
    });
    stress_config::record_completed(ctx, ITERS * iterations);
}

#[stress_test(tier = 3)]
fn should_complete_single_response_throughput(ctx: &mut StressContext) {
    const ITERS: u64 = 1000;
    ctx.parameter("scenario", "single_response_throughput");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("operation", "dispatch_service_response");
    ctx.parameter("batch_size", "1000_roundtrips");
    ctx.parameter("worker_count", "1");

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers = vec![register_worker(&router, family, 101)];
    let mut request_ring =
        RequestFrameRing::new(ROUTE_STR, b"streaming request", REQUEST_FRAME_RING_SIZE);
    let mut next_worker_index = 0usize;

    let iterations = ctx.measure_workload(|| {
        for _ in 0..ITERS {
            let (request_msg_type, request_payload) = request_ring.next_frame();
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload,
            );
            let _ = service_expected_worker(&router, family, &workers, &mut next_worker_index);
            assert_requester_received_worker_responses(requester_inbox.drain(), 1);
        }
        black_box(&workers);
    });
    stress_config::record_completed(ctx, ITERS * iterations);
}

#[stress_test(tier = 3)]
fn should_complete_worker_pool_scaling_64_workers(ctx: &mut StressContext) {
    measure_full_roundtrip_scaling(ctx, 64, 500, "scaling_64_full_roundtrip");
}

#[stress_test(tier = 3)]
fn should_complete_worker_pool_scaling_256_workers(ctx: &mut StressContext) {
    measure_full_roundtrip_scaling(ctx, 256, 200, "scaling_256_full_roundtrip");
}

#[stress_test(tier = 3)]
fn should_complete_multi_route_worker_pool_scaling_64_routes(ctx: &mut StressContext) {
    measure_multi_route_full_roundtrip_scaling(
        ctx,
        MULTI_ROUTE_COUNT,
        512,
        "scaling_64_routes_full_roundtrip",
    );
}

#[stress_test(tier = 3)]
fn should_complete_worker_pool_dispatch_only_scaling_64_workers(ctx: &mut StressContext) {
    measure_dispatch_only_scaling(ctx, 64, 500, "scaling_64_dispatch_only");
}

#[stress_test(tier = 3)]
fn should_complete_worker_pool_dispatch_only_scaling_256_workers(ctx: &mut StressContext) {
    measure_dispatch_only_scaling(ctx, 256, 200, "scaling_256_dispatch_only");
}

#[stress_test(tier = 3)]
fn should_complete_multi_route_worker_pool_dispatch_only_scaling_64_routes(
    ctx: &mut StressContext,
) {
    measure_multi_route_dispatch_only_scaling(
        ctx,
        MULTI_ROUTE_COUNT,
        512,
        "scaling_64_routes_dispatch_only",
    );
}

#[stress_test(tier = 3)]
fn should_complete_pending_cardinality_steady_state_1(ctx: &mut StressContext) {
    measure_pending_cardinality_steady_state(ctx, 1, 100);
}

#[stress_test(tier = 3)]
fn should_complete_pending_cardinality_steady_state_64(ctx: &mut StressContext) {
    measure_pending_cardinality_steady_state(ctx, 64, 100);
}

#[stress_test(tier = 3)]
fn should_complete_pending_cardinality_steady_state_256(ctx: &mut StressContext) {
    measure_pending_cardinality_steady_state(ctx, 256, 100);
}

#[stress_test(tier = 3)]
fn should_complete_pending_cardinality_steady_state_1000(ctx: &mut StressContext) {
    measure_pending_cardinality_steady_state(ctx, 1000, 100);
}

#[stress_test(tier = 3)]
fn should_complete_steady_state_request_tracking(ctx: &mut StressContext) {
    const ITERS: u64 = 1000;
    ctx.parameter("scenario", "steady_state_tracking");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("operation", "dispatch_service_response");
    ctx.parameter("batch_size", "1000_roundtrips");
    ctx.parameter("worker_count", "1");

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers = vec![register_worker(&router, family, 102)];
    let mut request_ring =
        RequestFrameRing::new(ROUTE_STR, b"concurrent request", REQUEST_FRAME_RING_SIZE);
    let mut next_worker_index = 0usize;

    let iterations = ctx.measure_workload(|| {
        for _ in 0..ITERS {
            let (request_msg_type, request_payload) = request_ring.next_frame();
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload,
            );
            let _ = service_expected_worker(&router, family, &workers, &mut next_worker_index);
            assert_requester_received_worker_responses(requester_inbox.drain(), 1);
        }
        black_box(&workers);
    });
    stress_config::record_completed(ctx, ITERS * iterations);
}

#[stress_test(tier = 3)]
fn should_complete_short_roundtrip_batch(ctx: &mut StressContext) {
    ctx.parameter("scenario", "short_roundtrip_batch");
    ctx.parameter("measurement_scope", "routed_system");
    ctx.parameter("operation", "dispatch_service_response");
    ctx.parameter("batch_size", "10_roundtrips");
    ctx.parameter("worker_count", "1");

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers = vec![register_worker(&router, family, 103)];
    let mut request_ring =
        RequestFrameRing::new(ROUTE_STR, b"mixed workload", REQUEST_FRAME_RING_SIZE);
    let mut next_worker_index = 0usize;

    let iterations = ctx.measure_workload(|| {
        for _ in 0..10 {
            let (request_msg_type, request_payload) = request_ring.next_frame();
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload,
            );
            let _ = service_expected_worker(&router, family, &workers, &mut next_worker_index);
            assert_requester_received_worker_responses(requester_inbox.drain(), 1);
        }
        black_box(&workers);
    });
    stress_config::record_completed(ctx, 10 * iterations);
}

stress_main!();
