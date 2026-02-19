//! Lease domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor (no network) - baseline
//! 2. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 3. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 4. **MultiClient** - N concurrent WS clients (real concurrency)

use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_lease_acquire_immediate, build_lease_release, parse_lease_response,
    parse_lease_token_response, shared_bench_runtime,
};
use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;

fn make_ctx() -> Context<LeaseActor> {
    let router = Router::new();
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("lease://tier4/locks/session"),
    );
    Context::new(addr, Arc::new(router))
}

#[stress_test]
fn should_complete_direct_acquire(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "acquire");

    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut actor_ctx = make_ctx();

    ctx.measure(|| {
        actor.receive(
            LeaseMessage::Acquire {
                family_id: RouteFamily::new(1),
                route: Route::new("lease://tier4/locks/primary"),
                owner_id: "owner1".to_string(),
                ttl_secs: 30,
                wait_seconds: 0,
            },
            &mut actor_ctx,
        );
    });
}

#[stress_test]
fn should_complete_tcp_acquire_release(ctx: &mut StressContext) {
    ctx.set_elements(2);
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "network_roundtrip");

    let acquire_frame = build_lease_acquire_immediate("lease://tier4/locks/primary", "owner1", 30);

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&acquire_frame, 2000))
            .expect("acquire response");
        let (_msg_type, _status, data) = parse_lease_response(&response);
        let token = parse_lease_token_response(&data).expect("lease token");

        let release_frame = build_lease_release("lease://tier4/locks/primary", "owner1", token);
        let _ = runtime
            .block_on(client.request(&release_frame, 2000))
            .expect("release response");
    });
}

#[stress_test]
fn should_complete_ws_acquire_release(ctx: &mut StressContext) {
    ctx.set_elements(2);
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "network_roundtrip");

    let acquire_frame = build_lease_acquire_immediate("lease://tier4/locks/primary", "owner1", 30);

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
            .block_on(client.request(&acquire_frame, 2000))
            .expect("acquire response");
        let (_msg_type, _status, data) = parse_lease_response(&response);
        let token = parse_lease_token_response(&data).expect("lease token");

        let release_frame = build_lease_release("lease://tier4/locks/primary", "owner1", token);
        let _ = runtime
            .block_on(client.request(&release_frame, 2000))
            .expect("release response");
    });
}

#[stress_test]
fn should_complete_multiclient_acquire_release(ctx: &mut StressContext) {
    ctx.set_elements(10);
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_clients");

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
        let _results: Vec<_> = runtime.block_on(futures::future::join_all(
            clients.iter().enumerate().map(|(idx, arc)| {
                let arc = arc.clone();
                async move {
                    let owner = format!("owner{}", idx);
                    // Each client uses a distinct lease so all acquires succeed under concurrency.
                    let route = format!("lease://tier4/locks/primary_{}", idx);
                    let acquire_frame = build_lease_acquire_immediate(&route, &owner, 30);
                    let mut c = arc.lock().await;
                    let response = c.request(&acquire_frame, 2000).await.expect("acquire");
                    let (_msg_type, _status, data) = parse_lease_response(&response);
                    let token = parse_lease_token_response(&data).expect("lease token");
                    let release_frame = build_lease_release(&route, &owner, token);
                    c.request(&release_frame, 2000).await.expect("release");
                }
            }),
        ));
    });
}

stress_main!();
