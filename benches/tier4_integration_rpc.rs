//! RPC domain tier 4 integration benchmarks
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests three integration levels:
//! 1. **Direct** - Domain actor (no network) - baseline integration overhead
//! 2. **TCP** - Full TCP stack: encode → socket → server → decode → actor → encode → socket
//! 3. **WebSocket** - Full WS stack: encode → WS frame → server → decode → actor → encode → WS frame
//!
//! This reveals where performance cliffs occur in RPC request/response workflows.

use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput};
use fitz::domains::rpc::protocol::{RpcMessage, RpcRequest, RpcResponse};
use fitz::domains::rpc::rpc_route_actor::RpcRouteActor;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::transport::{TestServer, TestClient, TestWebSocketClient};
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
// DIRECT INTEGRATION BENCHMARKS - Domain actor only (baseline)
// ============================================================================

fn bench_direct_subscribe_request_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_direct_request_response");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3)); // subscribe + request + response
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(500));

    group.bench_function("direct_subscribe_request_response", |b| {
        b.iter_batched(
            || RpcRouteActor::new(RouteFamily::new(1)),
            |mut actor| {
                let route_family = RouteFamily::new(1);
                let route_str = "rpc://realm/area/service";
                let worker_id = 1;
                let request_id = Uuid::new_v4();

                // Subscribe (worker registration)
                let subscribe_msg = RpcMessage::Subscribe {
                    family_id: route_family,
                    route: route_str.to_string(),
                    subscriber_session_id: worker_id,
                };
                let subscribe_addr = RouteAddress::from_str(route_str).unwrap();
                let subscribe_env = Envelope::new(subscribe_addr.clone(), subscribe_msg);
                let _ = actor.receive(subscribe_env);

                // RPC Request
                let rpc_request = RpcRequest {
                    request_id,
                    timeout_ms: Some(5000),
                    body: b"test_request".to_vec(),
                };
                let request_msg = RpcMessage::Request {
                    family_id: route_family,
                    route: route_str.to_string(),
                    req: rpc_request,
                };
                let request_env = Envelope::new(subscribe_addr.clone(), request_msg);
                let _ = actor.receive(request_env);

                // RPC Response (worker responding)
                let rpc_response = RpcResponse {
                    request_id,
                    body: b"test_response".to_vec(),
                };
                let response_msg = RpcMessage::Response {
                    response: rpc_response,
                };
                let response_env = Envelope::new(subscribe_addr, response_msg);
                let _ = black_box(actor.receive(response_env));
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// TCP INTEGRATION BENCHMARKS - Full socket stack
// ============================================================================

fn bench_tcp_subscribe_request_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_tcp_request_response");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("tcp_subscribe_request_response", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();

        b.to_async(&rt).iter_batched(
            || {
                let rt_handle = tokio::runtime::Handle::current();
                rt_handle.block_on(async {
                    let server = TestServer::start().await.unwrap();
                    let client = server.connect().await.unwrap();
                    (server, client)
                })
            },
            |(server, mut client)| async move {
                let route_family = RouteFamily::new(1);
                let route_str = "rpc://realm/area/service";
                let worker_id = 1;
                let request_id = Uuid::new_v4();

                // Subscribe
                let subscribe_frame =
                    encode_subscribe_request(route_family, route_str, worker_id);
                let _ = client.request(&subscribe_frame, 5000).await.unwrap();

                // Request
                let request_frame =
                    encode_rpc_request(route_family, route_str, request_id, b"test_request");
                let _ = client.request(&request_frame, 5000).await.unwrap();

                // Response
                let response_frame = encode_rpc_response(request_id, b"test_response");
                let _ = black_box(client.request(&response_frame, 5000).await.unwrap());

                drop(server);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// WEBSOCKET INTEGRATION BENCHMARKS - Full WS framing stack
// ============================================================================

fn bench_ws_subscribe_request_response(c: &mut Criterion) {
    let mut group = c.benchmark_group("rpc_ws_request_response");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("ws_subscribe_request_response", |b| {
        let rt = tokio::runtime::Runtime::new().unwrap();

        b.to_async(&rt).iter_batched(
            || {
                let rt_handle = tokio::runtime::Handle::current();
                rt_handle.block_on(async {
                    let server = TestServer::start().await.unwrap();
                    let ws_client = server.connect_ws().await.unwrap();
                    (server, ws_client)
                })
            },
            |(server, mut ws_client)| async move {
                let route_family = RouteFamily::new(1);
                let route_str = "rpc://realm/area/service";
                let worker_id = 1;
                let request_id = Uuid::new_v4();

                // Subscribe
                let subscribe_frame =
                    encode_subscribe_request(route_family, route_str, worker_id);
                let _ = ws_client.request(&subscribe_frame, 5000).await.unwrap();

                // Request
                let request_frame =
                    encode_rpc_request(route_family, route_str, request_id, b"test_request");
                let _ = ws_client.request(&request_frame, 5000).await.unwrap();

                // Response
                let response_frame = encode_rpc_response(request_id, b"test_response");
                let _ = black_box(
                    ws_client
                        .request(&response_frame, 5000)
                        .await
                        .unwrap()
                );

                drop(server);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_direct_subscribe_request_response,
        bench_tcp_subscribe_request_response,
        bench_ws_subscribe_request_response,
}
criterion_main!(benches);
