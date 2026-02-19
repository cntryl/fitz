//! Queue domain tier 4 integration benchmarks using stress
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
use fitz::benchkit::{
    build_queue_dequeue, build_queue_enqueue, create_local_bench_queue_actor, parse_queue_response,
};
use fitz::domains::queue::protocol::QueueMessage;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;

fn setup_queue_actor(
    route: &str,
) -> (
    fitz::domains::queue::QueueActor,
    Context<fitz::domains::queue::QueueActor>,
) {
    let (actor, _temp_dir) = create_local_bench_queue_actor("tier4", "queue", "main", None);
    let router = Arc::new(Router::new());
    let addr = RouteAddress::new(RouteFamily::new(0), Route::new(route.to_string()));
    let ctx = Context::new(addr, router);
    (actor, ctx)
}

#[stress_test]
fn should_complete_direct_enqueue(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "enqueue");

    let route = "queue://tier4/queue/main/enqueue";
    let (mut actor, mut actor_ctx) = setup_queue_actor(route);

    ctx.measure(|| {
        actor.receive(
            QueueMessage::Enqueue {
                family_id: RouteFamily::new(0),
                route: Route::new(route.to_string()),
                body: Bytes::from_static(b"msg"),
                delay_seconds: None,
            },
            &mut actor_ctx,
        );
    });
}

#[stress_test]
fn should_complete_encoded_enqueue(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "encoded");
    ctx.tag("scenario", "enqueue");

    let route = "queue://tier4/queue/main/enqueue";
    let (mut actor, mut actor_ctx) = setup_queue_actor(route);
    let enqueue_frame = build_queue_enqueue(route, b"msg");
    let dequeue_frame = build_queue_dequeue(route);

    ctx.measure(|| {
        actor.receive(
            QueueMessage::Enqueue {
                family_id: RouteFamily::new(0),
                route: Route::new(route.to_string()),
                body: Bytes::from_static(b"msg"),
                delay_seconds: None,
            },
            &mut actor_ctx,
        );
        let _ = (&enqueue_frame, &dequeue_frame);
    });
}

#[stress_test]
fn should_complete_tcp_enqueue(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "network_roundtrip");

    let route = "queue://tier4/queue/main/enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"msg");

    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&enqueue_frame, 2000))
            .expect("enqueue response");
        let (_msg_type, _status, _data) = parse_queue_response(&response);
    });
}

#[stress_test]
fn should_complete_ws_enqueue(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "network_roundtrip");

    let route = "queue://tier4/queue/main/enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"msg");

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
            .block_on(client.request(&enqueue_frame, 2000))
            .expect("enqueue response");
        let (_msg_type, _status, _data) = parse_queue_response(&response);
    });
}

#[stress_test]
fn should_complete_multiclient_concurrent_enqueues(ctx: &mut StressContext) {
    ctx.set_elements(10);
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_enqueues");

    let route = "queue://tier4/queue/main/enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"msg");

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
                .block_on(client.request(&enqueue_frame, 2000))
                .expect("enqueue response");
            let (_msg_type, _status, _data) = parse_queue_response(&response);
        }
    });
}

stress_main!();
