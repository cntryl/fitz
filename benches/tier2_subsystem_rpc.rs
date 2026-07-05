#![allow(deprecated)]
use bytes::Bytes;
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_rpc_request, build_rpc_response_frame, build_rpc_subscribe, create_bench_rpc_sink,
    extract_single_tlv_field, register_session_queue_sink, route_frame, FrameQueueSink,
};
use fitz::domains::rpc::protocol::RpcMessage;
use fitz::protocol::frame::ChannelId;
use fitz::protocol::rpc_codec::parse_request;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

const ROUTE_STR: &str = "rpc://bench/subsystem/route";
const REQUESTER_SESSION_ID: u64 = 1;
const REQUEST_FRAME_RING_SIZE: usize = 4096;
const DISPATCH_BATCH_SIZE: usize = 8192;

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

fn route_address(family: RouteFamily, route: &str) -> RouteAddress {
    RouteAddress::new(family, Route::new(route))
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn route_frame_to_address(
    router: &Router,
    source: &RouteAddress,
    destination: &RouteAddress,
    session_id: u64,
    msg_type: u16,
    payload: Bytes,
    family: RouteFamily,
) {
    route_frame(
        router,
        source,
        destination.route().as_str(),
        session_id,
        ChannelId::Rpc,
        msg_type,
        payload,
        family,
    )
    .expect("rpc route should succeed");
}

fn setup_rpc_sink() -> (Arc<Router>, RouteFamily, RouteAddress, Arc<FrameQueueSink>) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink: Arc<dyn MailboxSink> = create_bench_rpc_sink(router.clone());
    router.register_domain_pattern("rpc", sink);
    let (requester_source, requester_inbox) =
        register_session_queue_sink(&router, family, REQUESTER_SESSION_ID);
    (router, family, requester_source, requester_inbox)
}

fn register_worker_for_destination(
    router: &Arc<Router>,
    family: RouteFamily,
    session_id: u64,
    destination: &RouteAddress,
) -> WorkerHandle {
    let (worker_source, worker_inbox) = register_session_queue_sink(router, family, session_id);
    let subscribe_frame = build_rpc_subscribe(destination.route().as_str());
    let (msg_type, payload) = extract_single_tlv_field(&subscribe_frame);
    route_frame_to_address(
        router.as_ref(),
        &worker_source,
        destination,
        session_id,
        msg_type,
        payload,
        family,
    );
    worker_inbox.drain_after_count(1, Duration::from_secs(1));
    (session_id, worker_source, worker_inbox)
}

fn dispatch_request_to_destination(
    router: &Arc<Router>,
    family: RouteFamily,
    requester_source: &RouteAddress,
    destination: &RouteAddress,
    request_msg_type: u16,
    request_payload: Bytes,
) {
    route_frame_to_address(
        router.as_ref(),
        requester_source,
        destination,
        REQUESTER_SESSION_ID,
        request_msg_type,
        request_payload,
        family,
    );
}

fn route_worker_frame_to_destination(
    router: &Arc<Router>,
    family: RouteFamily,
    worker_session_id: u64,
    worker_source: &RouteAddress,
    destination: &RouteAddress,
    msg_type: u16,
    payload: Bytes,
) {
    route_frame_to_address(
        router.as_ref(),
        worker_source,
        destination,
        worker_session_id,
        msg_type,
        payload,
        family,
    );
}

fn drain_request_correlation(
    worker_inbox: &Arc<FrameQueueSink>,
    family: RouteFamily,
) -> uuid::Uuid {
    worker_inbox
        .drain_after_count(1, Duration::from_secs(1))
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

fn cleanup_worker_request_on_destination(
    router: &Arc<Router>,
    family: RouteFamily,
    destination: &RouteAddress,
    worker: &WorkerHandle,
) {
    let (worker_session_id, worker_source, worker_inbox) = worker;
    let correlation_id = drain_request_correlation(worker_inbox, family);
    let (response_msg_type, response_payload) =
        extract_single_tlv_field(&build_rpc_response_frame(correlation_id, b"cleanup"));
    route_worker_frame_to_destination(
        router,
        family,
        *worker_session_id,
        worker_source,
        destination,
        response_msg_type,
        response_payload,
    );
}

#[allow(clippy::too_many_lines)]
fn dispatch_response_cleanup_workers(ctx: &mut StressContext, worker_count: usize) {
    let (router, family, requester_source, requester_inbox) = setup_rpc_sink();
    let destination = route_address(family, ROUTE_STR);
    let workers = (0..worker_count)
        .map(|index| {
            register_worker_for_destination(
                &router,
                family,
                40_000_u64.saturating_add(usize_to_u64_saturating(index)),
                &destination,
            )
        })
        .collect::<Vec<_>>();
    let mut request_ring =
        RequestFrameRing::new(ROUTE_STR, b"dispatch payload", REQUEST_FRAME_RING_SIZE);
    let mut next_worker_index = 0usize;

    tier2_stress::measure_iterations(
        ctx,
        "dispatch_response_cleanup_workers",
        usize_to_u64_saturating(DISPATCH_BATCH_SIZE),
        || {
            for _ in 0..DISPATCH_BATCH_SIZE {
                let (request_msg_type, request_payload) = request_ring.next_frame();
                dispatch_request_to_destination(
                    &router,
                    family,
                    &requester_source,
                    &destination,
                    request_msg_type,
                    black_box(request_payload),
                );
                cleanup_worker_request_on_destination(
                    &router,
                    family,
                    &destination,
                    &workers[next_worker_index],
                );
                next_worker_index = (next_worker_index + 1) % workers.len();
                requester_inbox.clear();
            }
        },
    );
}

#[stress(
    tier = 2,
    name = "dispatch_response_cleanup_8192_ops_1_workers_primary"
)]
fn should_dispatch_response_cleanup_8192_ops_1_workers_primary(ctx: &mut StressContext) {
    dispatch_response_cleanup_workers(ctx, 1);
}

#[stress(
    tier = 2,
    name = "dispatch_response_cleanup_8192_ops_64_workers_primary"
)]
fn should_dispatch_response_cleanup_8192_ops_64_workers_primary(ctx: &mut StressContext) {
    dispatch_response_cleanup_workers(ctx, 64);
}

#[stress(
    tier = 2,
    name = "dispatch_response_cleanup_8192_ops_256_workers_primary"
)]
fn should_dispatch_response_cleanup_8192_ops_256_workers_primary(ctx: &mut StressContext) {
    dispatch_response_cleanup_workers(ctx, 256);
}

stress_main!();
