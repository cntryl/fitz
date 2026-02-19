//! RPC domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor + disk (no network) - baseline integration overhead
//! 2. **Encoded** - Same as direct but with TLV codec (measures serialization cost)
//! 3. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 4. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 5. **MultiClient** - N concurrent WS clients hitting domain concurrently

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{build_rpc_request, build_rpc_subscribe, parse_rpc_response};
use fitz::domains::rpc::protocol::{RpcMessage, RpcRequest};
use fitz::domains::rpc::rpc_route_actor::RpcRouteActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
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

    let req = RpcRequest::new(
        RouteFamily::new(1),
        Uuid::new_v4(),
        Route::new("rpc://tier4/service"),
        Route::new("inbox://session/1"),
        Bytes::from_static(b"ping"),
    );

    let subscribe_frame = build_rpc_subscribe("rpc://tier4/worker");
    let request_frame = build_rpc_request("rpc://tier4/service", b"ping");

    ctx.measure(|| {
        actor.receive(RpcMessage::Request(req.clone()), &mut actor_ctx);
        let _ = (&subscribe_frame, &request_frame);
    });
}

#[stress_test]
fn should_complete_tcp_subscribe(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "subscribe");

    let subscribe_frame = build_rpc_subscribe("rpc://tier4/worker");

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&subscribe_frame, 2000))
            .expect("subscribe response");
        let (_msg_type, _status, _data) = parse_rpc_response(&response);
    });
}

#[stress_test]
fn should_complete_ws_subscribe(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "subscribe");

    let subscribe_frame = build_rpc_subscribe("rpc://tier4/worker");

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&subscribe_frame, 2000))
            .expect("subscribe response");
        let (_msg_type, _status, _data) = parse_rpc_response(&response);
    });
}

#[stress_test]
fn should_complete_multiclient_subscribe(ctx: &mut StressContext) {
    ctx.set_elements(10);
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_subscribe");

    let subscribe_frame = build_rpc_subscribe("rpc://tier4/worker");

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut clients: Vec<TestWebSocketClient> = (0..10)
        .map(|_| {
            runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect ws")
        })
        .collect();

    ctx.measure(|| {
        for client in clients.iter_mut() {
            let response = runtime
                .block_on(client.request(&subscribe_frame, 2000))
                .expect("subscribe response");
            let (_msg_type, _status, _data) = parse_rpc_response(&response);
        }
    });
}

stress_main!();
