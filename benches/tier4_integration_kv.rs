//! KV domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor + disk (no network). In-process baseline; no TLV/network.
//! 2. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 3. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 4. **MultiClient** - N concurrent WS clients hitting domain concurrently
//!
//! Each test measures a single operation with all setup/teardown outside the measurement loop.
//! Target: ops/sec via set_elements(count), reveals performance cliffs at each layer.

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_kv_begin, build_kv_put, build_kv_rollback, create_local_bench_store, parse_kv_response,
    parse_kv_tx_id, shared_bench_runtime,
};
use fitz::domains::kv::{KvActor, KvMessage, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};

#[stress_test]
fn should_complete_direct_begin_put_rollback(ctx: &mut StressContext) {
    ctx.set_elements(3); // begin + put + rollback
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "transaction_sequence");

    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = KvActor::new(store);

    ctx.measure(|| {
        let response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: "tier4".to_string(),
            area: "kv".to_string(),
            resource: "direct".to_string(),
            mode: TxMode::ReadWrite,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });
        let tx_id = match response {
            fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
            _ => return,
        };

        actor.handle(KvMessage::Put {
            tx_id,
            route_family: RouteFamily::new(1),
            resource: "direct".to_string(),
            key: Bytes::from_static(b"key1"),
            value: Bytes::from_static(b"value1"),
        });

        actor.handle(KvMessage::Rollback { tx_id });
    });
}

#[stress_test]
fn should_complete_tcp_begin_put_rollback(ctx: &mut StressContext) {
    ctx.set_elements(3);
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "network_roundtrip");

    let route = "kv://tier4/kv/tcp";
    let begin_frame = build_kv_begin(route, 1, 0);
    let key = b"key1";
    let value = b"value1";

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    ctx.measure(|| {
        let response = runtime
            .block_on(client.request(&begin_frame, 2000))
            .expect("begin response");
        let (_msg_type, _status, data) = parse_kv_response(&response);
        let tx_id = parse_kv_tx_id(&data).expect("tx_id");

        let put_frame = build_kv_put(tx_id, route, key, value);
        runtime
            .block_on(client.request(&put_frame, 2000))
            .expect("put response");

        let rollback_frame = build_kv_rollback(tx_id, route);
        runtime
            .block_on(client.request(&rollback_frame, 2000))
            .expect("rollback response");
    });
}

#[stress_test]
fn should_complete_ws_begin_put_rollback(ctx: &mut StressContext) {
    ctx.set_elements(3);
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "network_roundtrip");

    let route = "kv://tier4/kv/ws";
    let begin_frame = build_kv_begin(route, 1, 0);
    let key = b"key1";
    let value = b"value1";

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
            .block_on(client.request(&begin_frame, 2000))
            .expect("begin response");
        let (_msg_type, _status, data) = parse_kv_response(&response);
        let tx_id = parse_kv_tx_id(&data).expect("tx_id");

        let put_frame = build_kv_put(tx_id, route, key, value);
        runtime
            .block_on(client.request(&put_frame, 2000))
            .expect("put response");

        let rollback_frame = build_kv_rollback(tx_id, route);
        runtime
            .block_on(client.request(&rollback_frame, 2000))
            .expect("rollback response");
    });
}

#[stress_test]
fn should_complete_multiclient_concurrent_transactions(ctx: &mut StressContext) {
    ctx.set_elements(50);
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_transactions");

    let route = "kv://tier4/kv/multiclient";
    let begin_frame = build_kv_begin(route, 1, 0);
    let key = b"key1";
    let value = b"value1";

    let runtime = shared_bench_runtime();
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
                .block_on(client.request(&begin_frame, 2000))
                .expect("begin response");
            let (_msg_type, _status, data) = parse_kv_response(&response);
            let tx_id = parse_kv_tx_id(&data).expect("tx_id");

            let put_frame = build_kv_put(tx_id, route, key, value);
            runtime
                .block_on(client.request(&put_frame, 2000))
                .expect("put response");

            let rollback_frame = build_kv_rollback(tx_id, route);
            runtime
                .block_on(client.request(&rollback_frame, 2000))
                .expect("rollback response");
        }
    });
}

stress_main!();
