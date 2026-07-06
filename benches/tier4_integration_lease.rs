//! Lease domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor (no network) - baseline
//! 2. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 3. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 4. **`MultiClient`** - N concurrent WS clients (real concurrency)

#[path = "stress_config.rs"]
mod stress_config;

use stress_config::StressContextExt;

use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_lease_acquire_immediate, build_lease_release, parse_lease_response,
    parse_lease_token_response, shared_bench_runtime, DirectLeaseAcquireRelease,
};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;

const DIRECT_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION: u64 = 64;
const TCP_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION: u64 = 16;
const WS_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION: u64 = 16;

fn shutdown_lease_test_server(runtime: &tokio::runtime::Runtime, server: TestServer) {
    runtime
        .block_on(server.shutdown())
        .expect("shutdown lease bench server");
}

fn close_tcp_client(runtime: &tokio::runtime::Runtime, client: TestClient) {
    runtime
        .block_on(client.close())
        .expect("close lease tcp bench client");
}

fn close_ws_client(runtime: &tokio::runtime::Runtime, client: &mut TestWebSocketClient) {
    runtime
        .block_on(client.close())
        .expect("close lease websocket bench client");
}

fn close_ws_clients(
    runtime: &tokio::runtime::Runtime,
    clients: &[Arc<Mutex<TestWebSocketClient>>],
) {
    runtime.block_on(async {
        for client in clients {
            client
                .lock()
                .await
                .close()
                .await
                .expect("close lease multiclient websocket bench client");
        }
    });
}

#[stress(tier = 4)]
fn should_complete_direct_acquire_release(ctx: &mut StressContext) {
    ctx.parameter("layer", "direct");
    ctx.parameter("scenario", "acquire_release");
    ctx.parameter("measurement_scope", "direct_inproc");
    ctx.parameter(
        "batch_size",
        format!("{DIRECT_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION}_acquire_release_roundtrips"),
    );

    let driver = DirectLeaseAcquireRelease::new(
        RouteFamily::new(1),
        "lease://tier4/locks/primary",
        1,
        "owner1",
        30,
    );

    let iterations = ctx.measure_workload("complete_direct_acquire_release", || {
        for _ in 0..DIRECT_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION {
            driver.complete_roundtrip();
        }
    });
    stress_config::record_completed(
        ctx,
        2 * DIRECT_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION * iterations,
    );
}

#[stress(tier = 4)]
fn should_complete_tcp_acquire_release(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "acquire_release");
    ctx.parameter("measurement_scope", "tcp_e2e");
    ctx.parameter(
        "batch_size",
        format!("{TCP_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION}_acquire_release_roundtrips"),
    );

    let acquire_frame = build_lease_acquire_immediate("lease://tier4/locks/primary", "owner1", 30);

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    let iterations = ctx.measure_workload("complete_tcp_acquire_release", || {
        for _ in 0..TCP_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION {
            let response = runtime
                .block_on(client.request(&acquire_frame, 2000))
                .expect("acquire response");
            let (_msg_type, _status, data) = parse_lease_response(&response);
            let token = parse_lease_token_response(&data).expect("lease token");

            let release_frame = build_lease_release("lease://tier4/locks/primary", "owner1", token);
            let _ = runtime
                .block_on(client.request(&release_frame, 2000))
                .expect("release response");
        }
    });
    stress_config::record_completed(
        ctx,
        2 * TCP_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION * iterations,
    );
    close_tcp_client(runtime, client);
    shutdown_lease_test_server(runtime, server);
}

#[stress(tier = 4)]
fn should_complete_ws_acquire_release(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "acquire_release");
    ctx.parameter("measurement_scope", "ws_e2e");
    ctx.parameter(
        "batch_size",
        format!("{WS_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION}_acquire_release_roundtrips"),
    );

    let acquire_frame = build_lease_acquire_immediate("lease://tier4/locks/primary", "owner1", 30);

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    let iterations = ctx.measure_workload("complete_ws_acquire_release", || {
        for _ in 0..WS_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION {
            let response = runtime
                .block_on(client.request(&acquire_frame, 2000))
                .expect("acquire response");
            let (_msg_type, _status, data) = parse_lease_response(&response);
            let token = parse_lease_token_response(&data).expect("lease token");

            let release_frame = build_lease_release("lease://tier4/locks/primary", "owner1", token);
            let _ = runtime
                .block_on(client.request(&release_frame, 2000))
                .expect("release response");
        }
    });
    stress_config::record_completed(
        ctx,
        2 * WS_ACQUIRE_RELEASE_ROUNDS_PER_ITERATION * iterations,
    );
    close_ws_client(runtime, &mut client);
    shutdown_lease_test_server(runtime, server);
}

#[stress(tier = 4)]
fn should_complete_multiclient_acquire_release(ctx: &mut StressContext) {
    ctx.parameter("layer", "multiclient");
    ctx.parameter("scenario", "concurrent_acquire_release");
    ctx.parameter("measurement_scope", "ws_multiclient_e2e");
    ctx.parameter("batch_size", "10_clients_acquire_release");
    ctx.parameter("client_count", "10");

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

    let iterations = ctx.measure_workload("complete_multiclient_acquire_release", || {
        let _results: Vec<_> = runtime.block_on(futures::future::join_all(
            clients.iter().enumerate().map(|(idx, arc)| {
                let arc = arc.clone();
                async move {
                    let owner = format!("owner{idx}");
                    // Each client uses a distinct lease so all acquires succeed under concurrency.
                    let route = format!("lease://tier4/locks/primary_{idx}");
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
    stress_config::record_completed(ctx, 20 * iterations);
    close_ws_clients(runtime, &clients);
    drop(clients);
    shutdown_lease_test_server(runtime, server);
}

stress_main!();
