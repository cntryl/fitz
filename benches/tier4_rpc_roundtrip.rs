#[path = "tier4_rpc_support.rs"]
mod tier4_rpc_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_rpc_support::{
    assert_requester_inbox_contains_worker_response, build_network_request_frame_ring,
    complete_roundtrip_tcp, complete_roundtrip_ws, try_parse_rpc_request_payload_parts,
    NetworkRequestFrame,
};
use crate::tier4_support::{
    measure_operations, tag_dimensions, LayerKind, StorageProfile, Tier4Dimensions,
};
use bytes::Bytes;
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_rpc_subscribe_with_max_concurrent, create_bench_rpc_sink, extract_single_tlv_field,
    register_session_queue_sink, route_frame, shared_bench_runtime, FrameQueueSink,
};
use fitz::domains::rpc::sink::RpcDomainSink;
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use std::time::Instant;

const SERVICE_ROUTE: &str = "rpc://tier4/service";
const REQUESTER_SESSION_ID: u64 = 1;
const WORKER_SESSION_ID: u64 = 2;
const RESPONSE_TIMEOUT_MS: u64 = 5_000;
const REQUEST_FRAME_RING_SIZE: usize = 512;
const RPC_PAYLOAD_SIZE: usize = 1_024;
const WORKER_MAX_CONCURRENT: u32 = 64;

fn dimensions(layer: LayerKind) -> Tier4Dimensions<'static> {
    Tier4Dimensions {
        domain: "rpc",
        scenario: "request_response_roundtrip",
        storage_profile: StorageProfile::Memory,
        layer,
        write_mode: "not_applicable",
        payload_size: RPC_PAYLOAD_SIZE,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count: 1,
        filter_selectivity: "not_applicable",
        client_count: 1,
        workload_mix: "request_response",
        completed_unit: "request_response_roundtrip",
        gate_class: "regression_gate",
    }
}

fn tag_roundtrip(ctx: &mut StressContext, layer: LayerKind) {
    tag_dimensions(ctx, &dimensions(layer));
    ctx.parameter("worker_count", 1);
    ctx.parameter("inflight_per_client", 1);
    ctx.parameter("completion_mode", "validated_response_wait");
}

struct InProcessExchange {
    request: NetworkRequestFrame,
    request_message_type: u16,
    request_payload: Bytes,
    response_message_type: u16,
    response_payload: Bytes,
}

struct InProcessRpcFixture {
    sink: Arc<RpcDomainSink>,
    router: Arc<Router>,
    family: RouteFamily,
    requester_source: RouteAddress,
    requester_inbox: Arc<FrameQueueSink>,
    worker_source: RouteAddress,
    worker_inbox: Arc<FrameQueueSink>,
    exchanges: Vec<InProcessExchange>,
    next_exchange: usize,
}

impl InProcessRpcFixture {
    fn new(payload: &[u8]) -> Self {
        let family = RouteFamily::new(1);
        let router = Arc::new(Router::new());
        let sink = create_bench_rpc_sink(router.clone());
        router.register_domain_pattern("rpc", sink.clone() as Arc<dyn MailboxSink>);
        let (requester_source, requester_inbox) =
            register_session_queue_sink(&router, family, REQUESTER_SESSION_ID);
        let (worker_source, worker_inbox) =
            register_session_queue_sink(&router, family, WORKER_SESSION_ID);
        let subscribe =
            build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, WORKER_MAX_CONCURRENT);
        let (message_type, subscribe_payload) = extract_single_tlv_field(&subscribe);
        route_frame(
            &router,
            &worker_source,
            SERVICE_ROUTE,
            WORKER_SESSION_ID,
            ChannelId::Rpc,
            message_type,
            subscribe_payload,
            family,
        )
        .expect("register in-process RPC worker");
        let _ = worker_inbox.drain();

        let exchanges = build_network_request_frame_ring(
            SERVICE_ROUTE,
            payload,
            family,
            REQUEST_FRAME_RING_SIZE,
        )
        .into_iter()
        .map(|request| {
            let (request_message_type, request_payload) = extract_single_tlv_field(&request.frame);
            let (response_message_type, response_payload) =
                extract_single_tlv_field(&request.response_frame);
            InProcessExchange {
                request,
                request_message_type,
                request_payload,
                response_message_type,
                response_payload,
            }
        })
        .collect();
        Self {
            sink,
            router,
            family,
            requester_source,
            requester_inbox,
            worker_source,
            worker_inbox,
            exchanges,
            next_exchange: 0,
        }
    }

    fn complete(&mut self, encoded: bool) -> std::time::Duration {
        let exchange = &self.exchanges[self.next_exchange];
        self.next_exchange = (self.next_exchange + 1) % self.exchanges.len();
        let started = Instant::now();
        let (request_message_type, request_payload) = if encoded {
            parse_tlv(&exchange.request.frame)
        } else {
            (
                exchange.request_message_type,
                exchange.request_payload.clone(),
            )
        };
        route_frame(
            &self.router,
            &self.requester_source,
            SERVICE_ROUTE,
            REQUESTER_SESSION_ID,
            ChannelId::Rpc,
            request_message_type,
            request_payload,
            self.family,
        )
        .expect("route in-process RPC request");
        assert_worker_received(&self.worker_inbox, &exchange.request);
        let (response_message_type, response_payload) = if encoded {
            parse_tlv(&exchange.request.response_frame)
        } else {
            (
                exchange.response_message_type,
                exchange.response_payload.clone(),
            )
        };
        route_frame(
            &self.router,
            &self.worker_source,
            SERVICE_ROUTE,
            WORKER_SESSION_ID,
            ChannelId::Rpc,
            response_message_type,
            response_payload,
            self.family,
        )
        .expect("route in-process RPC response");
        assert_requester_inbox_contains_worker_response(
            self.requester_inbox.drain(),
            exchange.request.correlation_id,
            exchange.request.body.as_ref(),
        );
        started.elapsed()
    }
}

impl Drop for InProcessRpcFixture {
    fn drop(&mut self) {
        self.sink.stop();
    }
}

fn parse_tlv(frame: &[u8]) -> (u16, Bytes) {
    let mut parser = TlvFrameParser::new(frame);
    let (message_type, payload) = parser.next_field_ref().expect("one RPC TLV field");
    assert!(
        parser.next_field_ref().is_none(),
        "expected one RPC TLV field"
    );
    (message_type, Bytes::copy_from_slice(payload))
}

fn assert_worker_received(inbox: &FrameQueueSink, expected: &NetworkRequestFrame) {
    let frame = inbox
        .drain()
        .into_iter()
        .find(|frame| frame.msg_type.as_u16() == 302)
        .expect("RPC worker request delivery");
    let (correlation_id, body) =
        try_parse_rpc_request_payload_parts(&frame.payload).expect("valid RPC worker request");
    assert_eq!(correlation_id, expected.correlation_id);
    assert_eq!(body, expected.body);
}

#[stress(tier = 4)]
fn should_measure_direct_request_response_roundtrip(ctx: &mut StressContext) {
    tag_roundtrip(ctx, LayerKind::Direct);
    let payload = vec![0xC3; RPC_PAYLOAD_SIZE];
    let mut fixture = InProcessRpcFixture::new(&payload);
    measure_operations(ctx, "direct_request_response_roundtrip", 1, |latencies| {
        latencies.push(fixture.complete(false));
    });
}

#[stress(tier = 4)]
fn should_measure_encoded_request_response_roundtrip(ctx: &mut StressContext) {
    tag_roundtrip(ctx, LayerKind::Encoded);
    let payload = vec![0xC3; RPC_PAYLOAD_SIZE];
    let mut fixture = InProcessRpcFixture::new(&payload);
    measure_operations(ctx, "encoded_request_response_roundtrip", 1, |latencies| {
        latencies.push(fixture.complete(true));
    });
}

fn measure_tcp_roundtrip(ctx: &mut StressContext) {
    tag_roundtrip(ctx, LayerKind::Tcp);
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start RPC TCP server");
    let subscribe = build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, WORKER_MAX_CONCURRENT);
    let payload = vec![0xC3; RPC_PAYLOAD_SIZE];
    let exchanges = build_network_request_frame_ring(
        SERVICE_ROUTE,
        &payload,
        RouteFamily::new(1),
        REQUEST_FRAME_RING_SIZE,
    );
    let mut worker = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect RPC TCP worker");
    runtime
        .block_on(worker.request(&subscribe, RESPONSE_TIMEOUT_MS))
        .expect("register RPC TCP worker");
    let mut requester = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect RPC TCP requester");
    let mut next = 0usize;
    measure_operations(ctx, "tcp_request_response_roundtrip", 1, |latencies| {
        let started = Instant::now();
        runtime
            .block_on(complete_roundtrip_tcp(
                &mut requester,
                &mut worker,
                &exchanges[next],
                RESPONSE_TIMEOUT_MS,
            ))
            .expect("complete RPC TCP roundtrip");
        next = (next + 1) % exchanges.len();
        latencies.push(started.elapsed());
    });
    runtime
        .block_on(worker.close())
        .expect("close RPC TCP worker");
    runtime
        .block_on(requester.close())
        .expect("close RPC TCP requester");
    runtime
        .block_on(server.shutdown())
        .expect("shutdown RPC TCP server");
}

fn measure_ws_roundtrip(ctx: &mut StressContext) {
    tag_roundtrip(ctx, LayerKind::WebSocket);
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start RPC WS server");
    let subscribe = build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, WORKER_MAX_CONCURRENT);
    let payload = vec![0xC3; RPC_PAYLOAD_SIZE];
    let exchanges = build_network_request_frame_ring(
        SERVICE_ROUTE,
        &payload,
        RouteFamily::new(1),
        REQUEST_FRAME_RING_SIZE,
    );
    let mut worker = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect RPC WS worker");
    runtime
        .block_on(worker.request(&subscribe, RESPONSE_TIMEOUT_MS))
        .expect("register RPC WS worker");
    let mut requester = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect RPC WS requester");
    let mut next = 0usize;
    measure_operations(ctx, "ws_request_response_roundtrip", 1, |latencies| {
        let started = Instant::now();
        runtime
            .block_on(complete_roundtrip_ws(
                &mut requester,
                &mut worker,
                &exchanges[next],
                RESPONSE_TIMEOUT_MS,
            ))
            .expect("complete RPC WS roundtrip");
        next = (next + 1) % exchanges.len();
        latencies.push(started.elapsed());
    });
    runtime
        .block_on(worker.close())
        .expect("close RPC WS worker");
    runtime
        .block_on(requester.close())
        .expect("close RPC WS requester");
    runtime
        .block_on(server.shutdown())
        .expect("shutdown RPC WS server");
}

#[stress(tier = 4)]
fn should_measure_tcp_request_response_roundtrip(ctx: &mut StressContext) {
    measure_tcp_roundtrip(ctx);
}

#[stress(tier = 4)]
fn should_measure_ws_request_response_roundtrip(ctx: &mut StressContext) {
    measure_ws_roundtrip(ctx);
}

stress_main!();
