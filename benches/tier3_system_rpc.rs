//! RPC domain tier 3 system benchmarks using the live RPC domain sink.
//!
//! Measures the real in-proc path: requester frame -> `RpcDomainSink`
//! -> worker inbox delivery -> worker response frame -> requester inbox.

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use criterion::black_box;
use fitz::benchkit::{
    build_rpc_request, build_rpc_subscribe, create_bench_rpc_sink, extract_single_tlv_field,
    register_session_queue_sink, route_frame, FrameQueueSink,
};
use fitz::domains::rpc::protocol::{RpcMessage, RpcResponse};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::rpc_codec::{encode_response_message, parse_request};
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use std::sync::Arc;

const ROUTE_STR: &str = "rpc://bench/system/route";
const REQUESTER_SESSION_ID: u64 = 1;

type WorkerHandle = (u64, RouteAddress, Arc<FrameQueueSink>);

fn setup_rpc_sink() -> (Arc<Router>, RouteFamily, RouteAddress, Arc<FrameQueueSink>) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_rpc_sink(router.clone());
    router.register_domain_pattern("rpc", sink as Arc<dyn MailboxSink>);
    let (requester_source, requester_inbox) =
        register_session_queue_sink(&router, family, REQUESTER_SESSION_ID);
    (router, family, requester_source, requester_inbox)
}

fn register_worker(router: &Arc<Router>, family: RouteFamily, session_id: u64) -> WorkerHandle {
    let (worker_source, worker_inbox) = register_session_queue_sink(router, family, session_id);
    let subscribe_frame = build_rpc_subscribe(ROUTE_STR);
    let (msg_type, payload) = extract_single_tlv_field(&subscribe_frame);
    route_frame(
        router.as_ref(),
        &worker_source,
        ROUTE_STR,
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

fn dispatch_request(
    router: &Arc<Router>,
    family: RouteFamily,
    requester_source: &RouteAddress,
    request_msg_type: u16,
    request_payload: Bytes,
) {
    route_frame(
        router.as_ref(),
        requester_source,
        ROUTE_STR,
        REQUESTER_SESSION_ID,
        ChannelId::Rpc,
        request_msg_type,
        request_payload,
        family,
    )
    .expect("rpc request");
}

fn service_worker(
    router: &Arc<Router>,
    family: RouteFamily,
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
            match frame.msg_type.as_u16() {
                302 => {
                    handled_request = true;
                    if let Ok(RpcMessage::Request(request)) =
                        parse_request(&frame, &frame.payload, family)
                    {
                        let response =
                            RpcResponse::single(request.correlation_id, request.body.clone());
                        dispatch_worker_response(
                            router,
                            family,
                            worker_session_id,
                            worker_source,
                            Bytes::from(encode_response_message(&response)),
                        );
                        responses += 1;
                    }
                }
                304 => {}
                _ => {}
            }
        }

        if !handled_request {
            break;
        }
    }

    responses
}

fn dispatch_worker_response(
    router: &Arc<Router>,
    family: RouteFamily,
    worker_session_id: u64,
    worker_source: &RouteAddress,
    response_payload: Bytes,
) {
    route_frame(
        router.as_ref(),
        worker_source,
        ROUTE_STR,
        worker_session_id,
        ChannelId::Rpc,
        303,
        response_payload,
        family,
    )
    .expect("rpc response");
}

fn service_workers(router: &Arc<Router>, family: RouteFamily, workers: &[WorkerHandle]) -> usize {
    let mut total = 0usize;
    loop {
        let mut round = 0usize;
        for (session_id, source, inbox) in workers {
            round += service_worker(router, family, *session_id, source, inbox);
        }
        if round == 0 {
            break;
        }
        total += round;
    }
    total
}

#[stress_test]
fn should_complete_request_dispatch_sustained(ctx: &mut StressContext) {
    const ITERS: u64 = 1000;
    ctx.tag("scenario", "sustained_dispatch");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "1000_roundtrips");
    ctx.tag("worker_count", "1");

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers = vec![register_worker(&router, family, 100)];
    let request_frame = build_rpc_request(ROUTE_STR, b"rpc request payload");
    let (request_msg_type, request_payload) = extract_single_tlv_field(&request_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        for _ in 0..ITERS {
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload.clone(),
            );
            let _ = service_workers(&router, family, &workers);
            let _ = requester_inbox.drain();
        }
        black_box(&workers);
    });
    ctx.set_elements(ITERS * iterations as u64);
}

#[stress_test]
fn should_complete_response_streaming_throughput(ctx: &mut StressContext) {
    const ITERS: u64 = 1000;
    ctx.tag("scenario", "response_streaming");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "1000_roundtrips");
    ctx.tag("worker_count", "1");

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers = vec![register_worker(&router, family, 101)];
    let request_frame = build_rpc_request(ROUTE_STR, b"streaming request");
    let (request_msg_type, request_payload) = extract_single_tlv_field(&request_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        for _ in 0..ITERS {
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload.clone(),
            );
            let _ = service_workers(&router, family, &workers);
            let _ = requester_inbox.drain();
        }
        black_box(&workers);
    });
    ctx.set_elements(ITERS * iterations as u64);
}

#[stress_test]
fn should_complete_worker_pool_scaling_64_workers(ctx: &mut StressContext) {
    const ITERS: u64 = 500;
    ctx.tag("scenario", "scaling_64");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "500_roundtrips");
    ctx.tag("worker_count", "64");

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers: Vec<WorkerHandle> = (0..64)
        .map(|i| register_worker(&router, family, 1_000 + i))
        .collect();
    let request_frame = build_rpc_request(ROUTE_STR, b"scaling payload");
    let (request_msg_type, request_payload) = extract_single_tlv_field(&request_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        for _ in 0..ITERS {
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload.clone(),
            );
            let _ = service_workers(&router, family, &workers);
            let _ = requester_inbox.drain();
        }
        black_box(&workers);
    });
    ctx.set_elements(ITERS * iterations as u64);
}

#[stress_test]
fn should_complete_worker_pool_scaling_256_workers(ctx: &mut StressContext) {
    const ITERS: u64 = 200;
    ctx.tag("scenario", "scaling_256");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "200_roundtrips");
    ctx.tag("worker_count", "256");

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers: Vec<WorkerHandle> = (0..256)
        .map(|i| register_worker(&router, family, 2_000 + i))
        .collect();
    let request_frame = build_rpc_request(ROUTE_STR, b"scaling payload");
    let (request_msg_type, request_payload) = extract_single_tlv_field(&request_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        for _ in 0..ITERS {
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload.clone(),
            );
            let _ = service_workers(&router, family, &workers);
            let _ = requester_inbox.drain();
        }
        black_box(&workers);
    });
    ctx.set_elements(ITERS * iterations as u64);
}

#[stress_test]
fn should_complete_concurrent_request_tracking(ctx: &mut StressContext) {
    const ITERS: u64 = 1000;
    ctx.tag("scenario", "concurrent_tracking");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "1000_roundtrips");
    ctx.tag("worker_count", "1");

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers = vec![register_worker(&router, family, 102)];
    let request_frame = build_rpc_request(ROUTE_STR, b"concurrent request");
    let (request_msg_type, request_payload) = extract_single_tlv_field(&request_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        for _ in 0..ITERS {
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload.clone(),
            );
            let _ = service_workers(&router, family, &workers);
            let _ = requester_inbox.drain();
        }
        black_box(&workers);
    });
    ctx.set_elements(ITERS * iterations as u64);
}

#[stress_test]
fn should_complete_mixed_request_response_workflow(ctx: &mut StressContext) {
    ctx.tag("scenario", "mixed_workload");
    ctx.tag("measurement_scope", "routed_system");
    ctx.tag("batch_size", "10_roundtrips");
    ctx.tag("worker_count", "1");

    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let workers = vec![register_worker(&router, family, 103)];
    let request_frame = build_rpc_request(ROUTE_STR, b"mixed workload");
    let (request_msg_type, request_payload) = extract_single_tlv_field(&request_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        for _ in 0..10 {
            dispatch_request(
                &router,
                family,
                &requester_source,
                request_msg_type,
                request_payload.clone(),
            );
            let _ = service_workers(&router, family, &workers);
            let _ = requester_inbox.drain();
        }
        black_box(&workers);
    });
    ctx.set_elements(10 * iterations as u64);
}

stress_main!();
