//! KV domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests five integration levels:
//! 1. **Direct** - Domain actor + disk (no network). In-process baseline; no TLV/network.
//! 2. **Encoded** - TLV frames built outside; in measure decode (KV codec) and call actor.
//! 3. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 4. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 5. **MultiClient** - N concurrent WS clients hitting domain concurrently (real concurrency).
//!
//! Each test measures a single operation with all setup/teardown outside the measurement loop.
//! Target: ops/sec via set_elements(count), reveals performance cliffs at each layer.

#[macro_use]
#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_test, StressContext};
use fitz::benchkit::{
    build_kv_begin, build_kv_put, build_kv_rollback, create_local_bench_store, parse_kv_response,
    parse_kv_tx_id, shared_bench_runtime,
};
use fitz::domains::kv::{KvActor, KvMessage, KvResponse, TxMode};
use fitz::protocol::kv_codec::parse_request as kv_parse_request;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;

#[stress_test]
fn should_complete_direct_begin_put_rollback(ctx: &mut StressContext) {
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "transaction_sequence");
    ctx.tag("measurement_scope", "direct_inproc");
    ctx.tag("batch_size", "3_ops");

    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = KvActor::new(store);

    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
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
    ctx.set_elements(3 * iterations as u64); // begin + put + rollback
}

#[stress_test]
fn should_complete_encoded_begin_put_rollback(ctx: &mut StressContext) {
    ctx.tag("layer", "encoded");
    ctx.tag("scenario", "transaction_sequence");
    ctx.tag("measurement_scope", "encoded_inproc");
    ctx.tag("batch_size", "3_ops");

    let route = "kv://tier4/kv/encoded";
    let begin_frame = build_kv_begin(route, 1, 0);
    let key = b"key1";
    let value = b"value1";
    let family = RouteFamily::new(1);

    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = KvActor::new(store);

    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        let begin_frame = begin_frame.clone();
        let mut parser = TlvFrameParser::new(&begin_frame);
        let (msg_type, payload) = parser.next_field().expect("begin field");
        let msg = kv_parse_request(msg_type, family, &payload).expect("parse begin");
        let tx_id = match actor.handle(msg) {
            KvResponse::BeginOk { tx_id } => tx_id,
            _ => return,
        };

        let put_frame = build_kv_put(tx_id, route, key, value);
        let mut parser = TlvFrameParser::new(&put_frame);
        let (msg_type, payload) = parser.next_field().expect("put field");
        let msg = kv_parse_request(msg_type, family, &payload).expect("parse put");
        actor.handle(msg);

        let rollback_frame = build_kv_rollback(tx_id, route);
        let mut parser = TlvFrameParser::new(&rollback_frame);
        let (msg_type, payload) = parser.next_field().expect("rollback field");
        let msg = kv_parse_request(msg_type, family, &payload).expect("parse rollback");
        actor.handle(msg);
    });
    ctx.set_elements(3 * iterations as u64);
}

#[stress_test]
fn should_complete_tcp_begin_put_rollback(ctx: &mut StressContext) {
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "transaction_sequence");
    ctx.tag("measurement_scope", "tcp_e2e");
    ctx.tag("batch_size", "3_ops");

    let route = "kv://tier4/kv/tcp";
    let begin_frame = build_kv_begin(route, 1, 0);
    let key = b"key1";
    let value = b"value1";

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
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
    ctx.set_elements(3 * iterations as u64);
}

#[stress_test]
fn should_complete_ws_begin_put_rollback(ctx: &mut StressContext) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "transaction_sequence");
    ctx.tag("measurement_scope", "ws_e2e");
    ctx.tag("batch_size", "3_ops");

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

    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
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
    ctx.set_elements(3 * iterations as u64);
}

#[stress_test]
fn should_complete_multiclient_concurrent_transactions(ctx: &mut StressContext) {
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", "concurrent_transactions");
    ctx.tag("measurement_scope", "ws_multiclient_e2e");
    ctx.tag("batch_size", "10_clients_3_ops_each");
    ctx.tag("client_count", "10");

    let route = "kv://tier4/kv/multiclient";
    let begin_frame = build_kv_begin(route, 1, 0);
    let key = b"key1".to_vec();
    let value = b"value1".to_vec();
    let route_owned = route.to_string();

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

    let iterations = ctx.measure_for(stress_config::BenchConfig::default().measure_duration, || {
        let _results: Vec<_> =
            runtime.block_on(futures::future::join_all(clients.iter().map(|arc| {
                let arc = arc.clone();
                let begin = begin_frame.clone();
                let r = route_owned.clone();
                let k = key.clone();
                let v = value.clone();
                async move {
                    let mut c = arc.lock().await;
                    let response = c.request(&begin, 2000).await.expect("begin");
                    let (_msg_type, _status, data) = parse_kv_response(&response);
                    let tx_id = parse_kv_tx_id(&data).expect("tx_id");
                    let put_frame = build_kv_put(tx_id, &r, &k, &v);
                    c.request(&put_frame, 2000).await.expect("put");
                    let rollback_frame = build_kv_rollback(tx_id, &r);
                    c.request(&rollback_frame, 2000).await.expect("rollback");
                }
            })));
    });
    ctx.set_elements(30 * iterations as u64);
}

stress_main_with_env!();
