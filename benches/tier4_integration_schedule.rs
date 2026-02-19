//! Schedule domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor (no network) - baseline
//! 2. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 3. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 4. **MultiClient** - N concurrent WS clients (real concurrency)

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_schedule_create, create_local_bench_store, parse_schedule_response, shared_bench_runtime,
};
use fitz::domains::schedule::actor::ScheduleActor;
use fitz::domains::schedule::protocol::ScheduleMessage;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;

fn make_schedule_ctx() -> Context<ScheduleActor> {
    let router = Router::new();
    let addr = RouteAddress::new(RouteFamily::new(1), Route::new("schedule://tier4/job1"));
    Context::new(addr, Arc::new(router))
}

#[stress_test]
fn should_complete_direct_create(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "create");

    let (db, _temp_dir) = create_local_bench_store();
    let mut actor = ScheduleActor::new(
        RouteFamily::new(1),
        db,
        cntryl_midge::WriteOptions::buffered(),
    );
    let mut actor_ctx = make_schedule_ctx();

    ctx.measure(|| {
        actor.receive(
            ScheduleMessage::Create {
                route: "schedule://tier4/job1".to_string(),
                cron: "0 * * * *".to_string(),
                payload: Bytes::from_static(b"payload"),
            },
            &mut actor_ctx,
        );
    });
}

#[stress_test]
fn should_complete_tcp_create(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "network_roundtrip");

    let create_frame = build_schedule_create("schedule://tier4/job1", "0 * * * *", b"payload");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&create_frame, 2000))
            .expect("create response");
        let (_msg_type, _status, _data) = parse_schedule_response(&response);
    });
}

#[stress_test]
fn should_complete_ws_create(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "network_roundtrip");

    let create_frame = build_schedule_create("schedule://tier4/job1", "0 * * * *", b"payload");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&create_frame, 2000))
            .expect("create response");
        let (_msg_type, _status, _data) = parse_schedule_response(&response);
    });
}

#[stress_test]
fn should_complete_multiclient_creates(ctx: &mut StressContext) {
    ctx.set_elements(10);
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_creates");

    let create_frame = build_schedule_create("schedule://tier4/job1", "0 * * * *", b"payload");

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

    ctx.measure(|| {
        let _results: Vec<_> =
            runtime.block_on(futures::future::join_all(clients.iter().map(|arc| {
                let arc = arc.clone();
                let frame = create_frame.clone();
                async move {
                    let mut c = arc.lock().await;
                    let response = c.request(&frame, 2000).await.expect("create");
                    let _ = parse_schedule_response(&response);
                }
            })));
    });
}

stress_main!();
