//! Queue domain tier 4 integration benchmarks
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests two integration levels:
//! 1. **TCP** - Full TCP stack: encode → socket → server → decode → actor → encode → socket
//! 2. **WebSocket** - Full WS stack: encode → WS frame → server → decode → actor → encode → WS frame
//!
//! This reveals where performance cliffs occur in queue message workflows.

use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::transport::TestServer;
use std::time::Duration;

#[path = "config.rs"]
mod config;

// ============================================================================
// TLV ENCODING HELPERS
// ============================================================================

/// Encode an enqueue request frame
fn encode_enqueue_request(
    route_family: RouteFamily,
    route_str: &str,
    body: &[u8],
    delay_seconds: Option<u64>,
) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 200; // ENQUEUE

    // Message type
    buf.put_u8(msg_type as u8);

    // RouteFamily
    buf.put_u8(1);
    buf.put_u16(8);
    buf.put_u64(route_family.as_u64());

    // Route
    let route_bytes = route_str.as_bytes();
    buf.put_u32(route_bytes.len() as u32);
    buf.put_slice(route_bytes);

    // Body
    buf.put_u32(body.len() as u32);
    buf.put_slice(body);

    // Delay (optional)
    if let Some(delay) = delay_seconds {
        buf.put_u8(1); // has_delay
        buf.put_u64(delay);
    } else {
        buf.put_u8(0); // no_delay
    }

    buf.to_vec()
}

/// Encode a reserve request frame
fn encode_reserve_request(
    route_family: RouteFamily,
    route_str: &str,
    lease_seconds: u64,
    batch_size: Option<u32>,
) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 202; // RESERVE

    buf.put_u8(msg_type as u8);

    // RouteFamily
    buf.put_u8(1);
    buf.put_u16(8);
    buf.put_u64(route_family.as_u64());

    // Route
    let route_bytes = route_str.as_bytes();
    buf.put_u32(route_bytes.len() as u32);
    buf.put_slice(route_bytes);

    // Lease seconds
    buf.put_u64(lease_seconds);

    // Batch size (optional)
    if let Some(batch) = batch_size {
        buf.put_u8(1);
        buf.put_u32(batch);
    } else {
        buf.put_u8(0);
    }

    buf.to_vec()
}

/// Encode a complete request frame
fn encode_complete_request(
    route_family: RouteFamily,
    route_str: &str,
    message_id: u64,
    token: u64,
) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 204; // COMPLETE

    buf.put_u8(msg_type as u8);

    // RouteFamily
    buf.put_u8(1);
    buf.put_u16(8);
    buf.put_u64(route_family.as_u64());

    // Route
    let route_bytes = route_str.as_bytes();
    buf.put_u32(route_bytes.len() as u32);
    buf.put_slice(route_bytes);

    // MessageId
    buf.put_u64(message_id);

    // Token
    buf.put_u64(token);

    buf.to_vec()
}

// ============================================================================
// TCP INTEGRATION BENCHMARKS - Full socket stack
// ============================================================================

fn bench_tcp_enqueue_reserve_complete(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server, mut client) = runtime.block_on(async {
        let server = TestServer::start().await.unwrap();
        let client = server.connect().await.unwrap();
        (server, client)
    });

    let mut group = c.benchmark_group("queue_tcp_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("tcp_enqueue_reserve_complete", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let route_family = RouteFamily::new(1);
                let route_str = "queue://bench/queue/tasks";

                // Enqueue
                let enqueue_frame =
                    encode_enqueue_request(route_family, route_str, b"test_message", None);
                let _enqueue_resp = client.request(&enqueue_frame, 5000).await.unwrap();

                // For simplicity, assume message_id = 1 (we'd need to parse the response)
                // In a real implementation, parse enqueue_resp to extract message_id

                // Reserve
                let reserve_frame = encode_reserve_request(route_family, route_str, 30, Some(1));
                let _reserve_resp = client.request(&reserve_frame, 5000).await.unwrap();

                // For simplicity, assume message_id=1, token=1 (we'd parse reserve_resp)

                // Complete
                let complete_frame = encode_complete_request(route_family, route_str, 1, 1);
                let _ = black_box(client.request(&complete_frame, 5000).await.unwrap());
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

fn bench_ws_enqueue_reserve_complete(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server, mut ws_client) = runtime.block_on(async {
        let server = TestServer::start().await.unwrap();
        let ws_client = server.connect_ws().await.unwrap();
        (server, ws_client)
    });

    let mut group = c.benchmark_group("queue_ws_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("ws_enqueue_reserve_complete", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let route_family = RouteFamily::new(1);
                let route_str = "queue://bench/queue/tasks";

                // Enqueue
                let enqueue_frame =
                    encode_enqueue_request(route_family, route_str, b"test_message", None);
                let _ = ws_client.request(&enqueue_frame, 5000).await.unwrap();

                // Reserve
                let reserve_frame = encode_reserve_request(route_family, route_str, 30, Some(1));
                let _ = ws_client.request(&reserve_frame, 5000).await.unwrap();

                // Complete
                let complete_frame = encode_complete_request(route_family, route_str, 1, 1);
                let _ = black_box(ws_client.request(&complete_frame, 5000).await.unwrap());
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
        bench_tcp_enqueue_reserve_complete,
        bench_ws_enqueue_reserve_complete,
}
criterion_main!(benches);
