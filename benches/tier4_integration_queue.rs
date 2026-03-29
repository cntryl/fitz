//! Queue domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor in-process using the same in-memory storage mode as TestServer
//! 2. **Encoded** - Same as direct but with TLV codec (measures serialization cost)
//! 3. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 4. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 5. **MultiClient** - N concurrent WS clients hitting domain concurrently

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_queue_enqueue, create_bench_queue_actor, parse_queue_response, shared_bench_runtime,
};
use fitz::domains::queue::protocol::QueueMessage;
use fitz::prelude::Actor;
use fitz::protocol::queue_codec::parse_request as queue_parse_request;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;

fn setup_queue_actor(
    route: &str,
) -> (
    fitz::domains::queue::QueueActor,
    Context<fitz::domains::queue::QueueActor>,
) {
    let family = RouteFamily::new(1);
    let actor = create_bench_queue_actor("tier4", "queue", "main", None);
    let router = Arc::new(Router::new());
    let addr = RouteAddress::new(family, Route::new(route));
    let ctx = Context::new(addr, router);
    (actor, ctx)
}

#[stress_test]
fn should_complete_direct_enqueue(ctx: &mut StressContext) {
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "enqueue");
    ctx.tag("measurement_scope", "direct_inproc");
    ctx.tag("batch_size", "single_enqueue");

    let route = "queue://tier4/queue/main/enqueue";
    let family = RouteFamily::new(1);
    let (mut actor, mut actor_ctx) = setup_queue_actor(route);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        actor.receive(
            QueueMessage::Send {
                family_id: family,
                route: Route::new(route),
                body: Bytes::from_static(b"msg"),
                delay_seconds: None,
            },
            &mut actor_ctx,
        );
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_encoded_enqueue(ctx: &mut StressContext) {
    ctx.tag("layer", "encoded");
    ctx.tag("scenario", "enqueue");
    ctx.tag("measurement_scope", "encoded_inproc");
    ctx.tag("batch_size", "single_enqueue");

    let route = "queue://tier4/queue/main/enqueue";
    let (mut actor, mut actor_ctx) = setup_queue_actor(route);
    let enqueue_frame = build_queue_enqueue(route, b"msg");
    let family = RouteFamily::new(1);

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let mut parser = TlvFrameParser::new(&enqueue_frame);
        let (msg_type, payload) = parser.next_field_ref().expect("enqueue field");
        let msg = queue_parse_request(msg_type, family, payload).expect("parse enqueue");
        actor.receive(msg, &mut actor_ctx);
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_tcp_enqueue(ctx: &mut StressContext) {
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "enqueue");
    ctx.tag("measurement_scope", "tcp_e2e");
    ctx.tag("batch_size", "single_enqueue");

    let route = "queue://tier4/queue/main/enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"msg");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let response = runtime
            .block_on(client.request(&enqueue_frame, 2000))
            .expect("enqueue response");
        let (_msg_type, _status, _data) = parse_queue_response(&response);
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_ws_enqueue(ctx: &mut StressContext) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "enqueue");
    ctx.tag("measurement_scope", "ws_e2e");
    ctx.tag("batch_size", "single_enqueue");

    let route = "queue://tier4/queue/main/enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"msg");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let response = runtime
            .block_on(client.request(&enqueue_frame, 2000))
            .expect("enqueue response");
        let (_msg_type, _status, _data) = parse_queue_response(&response);
    });
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_multiclient_concurrent_enqueues(ctx: &mut StressContext) {
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_enqueues");
    ctx.tag("measurement_scope", "ws_multiclient_e2e");
    ctx.tag("batch_size", "10_clients_1_enqueue_each");
    ctx.tag("client_count", "10");

    let route = "queue://tier4/queue/main/enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"msg");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let clients: Vec<Arc<Mutex<TestWebSocketClient>>> = (0..10)
        .map(|_| {
            let c = runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect ws");
            Arc::new(Mutex::new(c))
        })
        .collect();

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _results: Vec<_> =
            runtime.block_on(futures::future::join_all(clients.iter().map(|arc| {
                let arc = arc.clone();
                let frame = enqueue_frame.clone();
                async move {
                    let mut c = arc.lock().await;
                    let response = c.request(&frame, 2000).await.expect("enqueue");
                    let _ = parse_queue_response(&response);
                }
            })));
    });
    ctx.set_elements(10 * iterations as u64);
}

stress_main!();
