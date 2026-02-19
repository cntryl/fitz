//! RPC domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Layers: direct, encoded (codec decode path), tcp, websocket, multiclient (concurrent).
//! RPC tier4 tests full request → worker dispatch → response over the wire where applicable.

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_rpc_ack_frame, build_rpc_request, build_rpc_response_frame, build_rpc_subscribe,
    parse_rpc_response, shared_bench_runtime,
};
use fitz::domains::rpc::protocol::{RpcMessage, RpcRequest};
use fitz::domains::rpc::rpc_route_actor::RpcRouteActor;
use fitz::prelude::Actor;
use fitz::protocol::frame::ChannelId;
use fitz::protocol::frame_context::FrameContext;
use fitz::protocol::rpc_codec::parse_request;
use fitz::protocol::tlv::MessageType;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{addr, make_router, TestClient, TestServer, TestSink, TestWebSocketClient};
use std::sync::Arc;
use uuid::Uuid;

fn setup_rpc_actor() -> (RpcRouteActor, Context<RpcRouteActor>) {
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let worker = addr("rpc://tier4/worker");
    router.register(worker.clone(), sink.clone());

    let reply = addr("inbox://session/1");
    router.register(reply.clone(), sink);

    let router = Arc::new(router);
    let ctx = Context::new(addr("rpc://tier4/service"), router);

    (RpcRouteActor::new(RouteFamily::new(1)), ctx)
}

#[stress_test]
fn should_complete_direct_request(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "request");

    let (mut actor, mut actor_ctx) = setup_rpc_actor();
    let subscribe = RpcMessage::Subscribe {
        worker_addr: RouteAddress::new(RouteFamily::new(1), Route::new("rpc://tier4/worker")),
    };
    actor.receive(subscribe, &mut actor_ctx);

    let req = RpcRequest::new(
        RouteFamily::new(1),
        Uuid::new_v4(),
        Route::new("rpc://tier4/service"),
        Route::new("inbox://session/1"),
        Bytes::from_static(b"ping"),
    );

    ctx.measure(|| {
        actor.receive(RpcMessage::Request(req.clone()), &mut actor_ctx);
    });
}

#[stress_test]
fn should_complete_encoded_request(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "encoded");
    ctx.tag("scenario", "request");

    let (mut actor, mut actor_ctx) = setup_rpc_actor();
    let subscribe = RpcMessage::Subscribe {
        worker_addr: RouteAddress::new(RouteFamily::new(1), Route::new("rpc://tier4/worker")),
    };
    actor.receive(subscribe, &mut actor_ctx);

    let family = RouteFamily::new(1);
    let request_frame = build_rpc_request("rpc://tier4/service", b"ping");

    ctx.measure(|| {
        let mut parser = TlvFrameParser::new(request_frame.clone());
        let (msg_type, payload) = parser.next_field().expect("one field");
        let frame_ctx = FrameContext::new(
            0,
            ChannelId::Rpc,
            MessageType::new(msg_type),
            Bytes::from(payload.clone()),
            family,
        );
        let msg = parse_request(&frame_ctx, &payload, family).expect("parse");
        actor.receive(msg, &mut actor_ctx);
    });
}

#[stress_test]
fn should_complete_tcp_request_response(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "network_roundtrip");

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
                let mut parser = TlvFrameParser::new(frame);
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

    ctx.measure(|| {
        let response = runtime
            .block_on(requester_client.request(&request_frame, 2000))
            .expect("request response");
        let (_msg_type, _status, _data) = parse_rpc_response(&response);
    });

    worker_handle.abort();
}

#[stress_test]
fn should_complete_ws_request_response(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "network_roundtrip");

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
                let mut parser = TlvFrameParser::new(frame);
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

    ctx.measure(|| {
        let response = runtime
            .block_on(requester_client.request(&request_frame, 2000))
            .expect("request response");
        let (_msg_type, _status, _data) = parse_rpc_response(&response);
    });

    worker_handle.abort();
}

#[stress_test]
fn should_complete_multiclient_concurrent_requests(ctx: &mut StressContext) {
    ctx.set_elements(10);
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_subscribe");

    let subscribe_frame = build_rpc_subscribe("rpc://tier4/worker");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

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

    ctx.measure(|| {
        let frame = subscribe_frame.clone();
        let results: Vec<_> =
            runtime.block_on(futures::future::join_all(clients.iter().map(|arc| {
                let arc = arc.clone();
                let f = frame.clone();
                async move {
                    let mut c = arc.lock().await;
                    c.send_frame(&f).await?;
                    c.recv_frame(2000).await
                }
            })));
        for r in results {
            let _ = r.expect("request");
        }
    });
}

stress_main!();
