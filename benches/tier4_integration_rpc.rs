//! RPC domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Layers: direct, encoded (codec decode path), tcp, websocket, multiclient (concurrent).
//! RPC tier4 tests full request -> worker dispatch -> response over the wire where applicable.

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_rpc_ack_frame, build_rpc_request, build_rpc_response_frame, build_rpc_subscribe,
    create_bench_rpc_sink, extract_single_tlv_field, parse_rpc_response,
    register_session_queue_sink, route_frame, shared_bench_runtime, FrameQueueSink,
};
use fitz::domains::rpc::protocol::{RpcMessage, RpcResponse};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::frame_context::FrameContext;
use fitz::protocol::rpc_codec::encode_response_message;
use fitz::protocol::rpc_codec::parse_request;
use fitz::protocol::tlv::MessageType;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;

const SERVICE_ROUTE: &str = "rpc://tier4/service";
const REQUESTER_SESSION_ID: u64 = 1;
const WORKER_SESSION_ID: u64 = 2;

fn setup_rpc_sink() -> (
    Arc<Router>,
    RouteFamily,
    RouteAddress,
    Arc<FrameQueueSink>,
    RouteAddress,
    Arc<FrameQueueSink>,
) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_rpc_sink(router.clone());
    router.register_domain_pattern("rpc", sink as Arc<dyn MailboxSink>);

    let (requester_source, requester_inbox) =
        register_session_queue_sink(&router, family, REQUESTER_SESSION_ID);
    let (worker_source, worker_inbox) =
        register_session_queue_sink(&router, family, WORKER_SESSION_ID);

    let subscribe_frame = build_rpc_subscribe(SERVICE_ROUTE);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);
    route_frame(
        router.as_ref(),
        &worker_source,
        SERVICE_ROUTE,
        WORKER_SESSION_ID,
        ChannelId::Rpc,
        subscribe_msg_type,
        subscribe_payload,
        family,
    )
    .expect("rpc subscribe");
    let _ = worker_inbox.drain();

    (
        router,
        family,
        requester_source,
        requester_inbox,
        worker_source,
        worker_inbox,
    )
}

fn service_worker(
    router: &Arc<Router>,
    family: RouteFamily,
    worker_source: &RouteAddress,
    worker_inbox: &Arc<FrameQueueSink>,
) {
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
                    if let Ok(RpcMessage::Request(req)) =
                        parse_request(&frame, &frame.payload, family)
                    {
                        let response = RpcResponse::single(req.correlation_id, req.body.clone());
                        route_frame(
                            router.as_ref(),
                            worker_source,
                            SERVICE_ROUTE,
                            WORKER_SESSION_ID,
                            ChannelId::Rpc,
                            303,
                            Bytes::from(encode_response_message(&response)),
                            family,
                        )
                        .expect("rpc response");
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
}

#[stress_test]
fn should_complete_direct_request(ctx: &mut StressContext) {
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "request_response");
    ctx.tag("measurement_scope", "direct_inproc");
    ctx.tag("batch_size", "single_roundtrip");
    ctx.tag("worker_count", "1");

    let (router, family, requester_source, requester_inbox, worker_source, worker_inbox) =
        setup_rpc_sink();
    let request_frame = build_rpc_request(SERVICE_ROUTE, b"ping");
    let (request_msg_type, request_payload) = extract_single_tlv_field(&request_frame);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        route_frame(
            router.as_ref(),
            &requester_source,
            SERVICE_ROUTE,
            REQUESTER_SESSION_ID,
            ChannelId::Rpc,
            request_msg_type,
            request_payload.clone(),
            family,
        )
        .expect("rpc request");
        service_worker(&router, family, &worker_source, &worker_inbox);
        let _ = requester_inbox.drain();
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_encoded_request(ctx: &mut StressContext) {
    ctx.tag("layer", "encoded");
    ctx.tag("scenario", "request_response");
    ctx.tag("measurement_scope", "encoded_inproc");
    ctx.tag("batch_size", "single_roundtrip");
    ctx.tag("worker_count", "1");

    let (router, family, requester_source, requester_inbox, worker_source, worker_inbox) =
        setup_rpc_sink();
    let request_frame = build_rpc_request(SERVICE_ROUTE, b"ping");

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let request_frame = request_frame.clone();
        let mut parser = TlvFrameParser::new(&request_frame);
        let (msg_type, payload) = parser.next_field().expect("one field");
        route_frame(
            router.as_ref(),
            &requester_source,
            SERVICE_ROUTE,
            REQUESTER_SESSION_ID,
            ChannelId::Rpc,
            msg_type,
            Bytes::from(payload),
            family,
        )
        .expect("rpc request");
        service_worker(&router, family, &worker_source, &worker_inbox);
        let _ = requester_inbox.drain();
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_tcp_request_response(ctx: &mut StressContext) {
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "request_response");
    ctx.tag("measurement_scope", "tcp_e2e");
    ctx.tag("batch_size", "single_roundtrip");
    ctx.tag("worker_count", "1");

    let subscribe_frame = build_rpc_subscribe("rpc://tier4/worker");
    let request_frame = build_rpc_request("rpc://tier4/service", b"ping");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

    let mut worker_client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect worker");
    runtime
        .block_on(worker_client.send_frame(&subscribe_frame))
        .expect("subscribe");
    let _ = runtime.block_on(worker_client.recv_frame(2000)); // subscribe ack

    let mut requester_client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect requester");

    let worker_handle = {
        let rt = shared_bench_runtime();
        rt.spawn(async move {
            loop {
                let frame = match worker_client.recv_frame(5000).await {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                let mut parser = TlvFrameParser::new(&frame);
                if let Some((msg_type, payload)) = parser.next_field() {
                    if msg_type == 302 {
                        let frame_ctx = FrameContext::new(
                            0,
                            ChannelId::Rpc,
                            MessageType::new(302),
                            Bytes::from(payload.clone()),
                            RouteFamily::new(1),
                        );
                        if let Ok(RpcMessage::Request(req)) =
                            parse_request(&frame_ctx, &payload, RouteFamily::new(1))
                        {
                            let resp_frame =
                                build_rpc_response_frame(req.correlation_id, &req.body);
                            let ack_frame = build_rpc_ack_frame(req.correlation_id);
                            let _ = worker_client.send_frame(&resp_frame).await;
                            let _ = worker_client.send_frame(&ack_frame).await;
                        }
                    }
                }
            }
        })
    };

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let response = runtime
            .block_on(requester_client.request(&request_frame, 2000))
            .expect("request response");
        let (_msg_type, _status, _data) = parse_rpc_response(&response);
    });
    ctx.set_elements(iterations as u64);

    worker_handle.abort();
}

#[stress_test]
fn should_complete_ws_request_response(ctx: &mut StressContext) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "request_response");
    ctx.tag("measurement_scope", "ws_e2e");
    ctx.tag("batch_size", "single_roundtrip");
    ctx.tag("worker_count", "1");

    let subscribe_frame = build_rpc_subscribe("rpc://tier4/worker");
    let request_frame = build_rpc_request("rpc://tier4/service", b"ping");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

    let mut worker_client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect worker ws");
    runtime
        .block_on(worker_client.send_frame(&subscribe_frame))
        .expect("subscribe");
    let _ = runtime.block_on(worker_client.recv_frame(2000));

    let mut requester_client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect requester ws");

    let worker_handle = {
        let rt = shared_bench_runtime();
        rt.spawn(async move {
            loop {
                let frame = match worker_client.recv_frame(5000).await {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                let mut parser = TlvFrameParser::new(&frame);
                if let Some((msg_type, payload)) = parser.next_field() {
                    if msg_type == 302 {
                        let frame_ctx = FrameContext::new(
                            0,
                            ChannelId::Rpc,
                            MessageType::new(302),
                            Bytes::from(payload.clone()),
                            RouteFamily::new(1),
                        );
                        if let Ok(RpcMessage::Request(req)) =
                            parse_request(&frame_ctx, &payload, RouteFamily::new(1))
                        {
                            let resp_frame =
                                build_rpc_response_frame(req.correlation_id, &req.body);
                            let ack_frame = build_rpc_ack_frame(req.correlation_id);
                            let _ = worker_client.send_frame(&resp_frame).await;
                            let _ = worker_client.send_frame(&ack_frame).await;
                        }
                    }
                }
            }
        })
    };

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let response = runtime
            .block_on(requester_client.request(&request_frame, 2000))
            .expect("request response");
        let (_msg_type, _status, _data) = parse_rpc_response(&response);
    });
    ctx.set_elements(iterations as u64);

    worker_handle.abort();
}

#[stress_test]
fn should_complete_multiclient_concurrent_requests(ctx: &mut StressContext) {
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_requests");
    ctx.tag("measurement_scope", "ws_multiclient_e2e");
    ctx.tag("batch_size", "10_clients_1_roundtrip_each");
    ctx.tag("client_count", "10");
    ctx.tag("worker_count", "1");

    let subscribe_frame = build_rpc_subscribe("rpc://tier4/worker");
    let request_frame = build_rpc_request("rpc://tier4/service", b"ping");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

    let mut worker_client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect worker ws");
    runtime
        .block_on(worker_client.send_frame(&subscribe_frame))
        .expect("subscribe");
    let _ = runtime.block_on(worker_client.recv_frame(2000));

    let clients: Vec<std::sync::Arc<tokio::sync::Mutex<TestWebSocketClient>>> = (0..10)
        .map(|_| {
            let c = runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect ws");
            std::sync::Arc::new(tokio::sync::Mutex::new(c))
        })
        .collect();

    let worker_handle = {
        let rt = shared_bench_runtime();
        rt.spawn(async move {
            loop {
                let frame = match worker_client.recv_frame(5000).await {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                let mut parser = TlvFrameParser::new(&frame);
                if let Some((msg_type, payload)) = parser.next_field() {
                    if msg_type == 302 {
                        let frame_ctx = FrameContext::new(
                            0,
                            ChannelId::Rpc,
                            MessageType::new(302),
                            Bytes::from(payload.clone()),
                            RouteFamily::new(1),
                        );
                        if let Ok(RpcMessage::Request(req)) =
                            parse_request(&frame_ctx, &payload, RouteFamily::new(1))
                        {
                            let resp_frame =
                                build_rpc_response_frame(req.correlation_id, &req.body);
                            let ack_frame = build_rpc_ack_frame(req.correlation_id);
                            let _ = worker_client.send_frame(&resp_frame).await;
                            let _ = worker_client.send_frame(&ack_frame).await;
                        }
                    }
                }
            }
        })
    };

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let frame = request_frame.clone();
        runtime.block_on(futures::future::join_all(clients.iter().map(|arc| {
            let arc = arc.clone();
            let f = frame.clone();
            async move {
                let mut c = arc.lock().await;
                let response = c.request(&f, 2000).await.expect("request");
                let _ = parse_rpc_response(&response);
            }
        })));
    });
    ctx.set_elements(10 * iterations as u64);

    worker_handle.abort();
}

stress_main!();
