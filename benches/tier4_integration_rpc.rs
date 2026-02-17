//! RPC domain tier 4 integration benchmarks
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests two integration levels:
//! 1. **TCP** - Full TCP stack: encode → socket → server → decode → actor → encode → socket
//! 2. **WebSocket** - Full WS stack: encode → WS frame → server → decode → actor → encode → WS frame
//!
//! This reveals where performance cliffs occur in RPC request/response workflows.
//! (Direct actor testing skipped - see tier3 for actor-level benchmarks)

use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::transport::{TestClient, TestServer, TestWebSocketClient};
use std::time::Duration;
use uuid::Uuid;

#[path = "config.rs"]
mod config;

// ============================================================================
// TLV ENCODING HELPERS
// ============================================================================

/// Encode an RPC subscribe request (worker registration)
fn encode_subscribe_request(route_family: RouteFamily, route_str: &str, worker_id: u64) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 300; // SUBSCRIBE

    buf.put_u8(msg_type as u8);

    // RouteFamily
    buf.put_u8(1);
    buf.put_u16(8);
    buf.put_u64(route_family.as_u64());

    // Route
    let route_bytes = route_str.as_bytes();
    buf.put_u8(2);
    buf.put_u16(route_bytes.len() as u16);
    buf.put_slice(route_bytes);

    // Worker ID
    buf.put_u8(3);
    buf.put_u16(8);
    buf.put_u64(worker_id);

    buf.to_vec()
}

/// Encode an RPC request (call to worker)
fn encode_rpc_request(
    route_family: RouteFamily,
    route_str: &str,
    request_id: Uuid,
    body: &[u8],
) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 302; // RPC_REQUEST

    if msg_type <= 254 {
        buf.put_u8(msg_type as u8);
    } else {
        buf.put_u8(0xFF);
        buf.put_u16(msg_type);
    }

    // RouteFamily
    buf.put_u8(1);
    buf.put_u16(8);
    buf.put_u64(route_family.as_u64());

    // Route
    let route_bytes = route_str.as_bytes();
    buf.put_u8(2);
    buf.put_u16(route_bytes.len() as u16);
    buf.put_slice(route_bytes);

    // Request ID (UUID)
    buf.put_u8(4);
    buf.put_u16(16);
    buf.put_slice(request_id.as_bytes());

    // Body
    buf.put_u8(5);
    buf.put_u16(body.len() as u16);
    buf.put_slice(body);

    buf.to_vec()
}

/// Encode an RPC response (worker responding to request)
fn encode_rpc_response(request_id: Uuid, response_body: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 303; // RPC_RESPONSE

    if msg_type <= 254 {
        buf.put_u8(msg_type as u8);
    } else {
        buf.put_u8(0xFF);
        buf.put_u16(msg_type);
    }

    // Request ID
    buf.put_u8(4);
    buf.put_u16(16);
    buf.put_slice(request_id.as_bytes());

    // Response body
    buf.put_u8(6);
    buf.put_u16(response_body.len() as u16);
    buf.put_slice(response_body);

    buf.to_vec()
}

// ============================================================================
// TCP INTEGRATION BENCHMARKS - Full socket stack
// ============================================================================

fn bench_tcp_subscribe_request_response(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server, mut client) = runtime.block_on(async {
        let server = TestServer::start().await.unwrap();
        let client = server.connect().await.unwrap();
        (server, client)
    });

    let mut group = c.benchmark_group("rpc_tcp_request_response");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("tcp_subscribe_request_response", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let route_family = RouteFamily::new(1);
                let route_str = "rpc://realm/area/service";
                let worker_id = 1;
                let request_id = Uuid::new_v4();

                // Subscribe
                let subscribe_frame = encode_subscribe_request(route_family, route_str, worker_id);
                let _ = client.request(&subscribe_frame, 5000).await.unwrap();

                // Request
                let request_frame =
                    encode_rpc_request(route_family, route_str, request_id, b"test_request");
                let _ = client.request(&request_frame, 5000).await.unwrap();

                // Response
                let response_frame = encode_rpc_response(request_id, b"test_response");
                let _ = black_box(client.request(&response_frame, 5000).await.unwrap());
            })
        })
    });

    group.finish();
    drop(client);
    drop(server);
}

// ============================================================================
// WEBSOCKET INTEGRATION BENCHMARKS - Full WS framing stack
// ============================================================================

fn bench_ws_subscribe_request_response(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server, mut ws_client) = runtime.block_on(async {
        let server = TestServer::start().await.unwrap();
        let ws_client = server.connect_ws().await.unwrap();
        (server, ws_client)
    });

    let mut group = c.benchmark_group("rpc_ws_request_response");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("ws_subscribe_request_response", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let route_family = RouteFamily::new(1);
                let route_str = "rpc://realm/area/service";
                let worker_id = 1;
                let request_id = Uuid::new_v4();

                // Subscribe
                let subscribe_frame = encode_subscribe_request(route_family, route_str, worker_id);
                let _ = ws_client.request(&subscribe_frame, 5000).await.unwrap();

                // Request
                let request_frame =
                    encode_rpc_request(route_family, route_str, request_id, b"test_request");
                let _ = ws_client.request(&request_frame, 5000).await.unwrap();

                // Response
                let response_frame = encode_rpc_response(request_id, b"test_response");
                let _ = black_box(ws_client.request(&response_frame, 5000).await.unwrap());
            })
        })
    });

    group.finish();
    drop(ws_client);
    drop(server);
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_tcp_subscribe_request_response,
        bench_ws_subscribe_request_response,
}
criterion_main!(benches);
