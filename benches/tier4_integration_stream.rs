//! Stream domain tier 4 integration benchmarks
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests two integration levels:
//! 1. **TCP** - Full TCP stack: encode → socket → server → decode → actor → encode → socket
//! 2. **WebSocket** - Full WS stack: encode → WS frame → server → decode → actor → encode → WS frame
//!
//! This reveals where performance cliffs occur in append-only stream workflows.
//! (Direct actor testing skipped - requires complex storage setup, see tier3 for actor-level benchmarks)

use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::transport::{TestClient, TestServer, TestWebSocketClient};
use std::time::Duration;

#[path = "config.rs"]
mod config;

// ============================================================================
// TLV ENCODING HELPERS
// ============================================================================

/// Encode a stream begin request
fn encode_begin_request(route_family: RouteFamily, route_str: &str, mode: u8) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 600; // BEGIN

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

    // Mode (0=create, 1=append)
    buf.put_u8(3);
    buf.put_u16(1);
    buf.put_u8(mode);

    buf.to_vec()
}

/// Encode a stream append request
fn encode_append_request(session_id: u64, data: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 601; // APPEND

    if msg_type <= 254 {
        buf.put_u8(msg_type as u8);
    } else {
        buf.put_u8(0xFF);
        buf.put_u16(msg_type);
    }

    // Session ID
    buf.put_u8(4);
    buf.put_u16(8);
    buf.put_u64(session_id);

    // Data
    buf.put_u8(5);
    buf.put_u16(data.len() as u16);
    buf.put_slice(data);

    buf.to_vec()
}

/// Encode a stream commit request
fn encode_commit_request(session_id: u64) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 602; // COMMIT

    if msg_type <= 254 {
        buf.put_u8(msg_type as u8);
    } else {
        buf.put_u8(0xFF);
        buf.put_u16(msg_type);
    }

    // Session ID
    buf.put_u8(4);
    buf.put_u16(8);
    buf.put_u64(session_id);

    buf.to_vec()
}

/// Encode a stream read request
fn encode_read_request(
    route_family: RouteFamily,
    route_str: &str,
    offset: u64,
    limit: u32,
) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 604; // READ

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

    // Offset
    buf.put_u8(6);
    buf.put_u16(8);
    buf.put_u64(offset);

    // Limit
    buf.put_u8(7);
    buf.put_u16(4);
    buf.put_u32(limit);

    buf.to_vec()
}

// ============================================================================
// TCP INTEGRATION BENCHMARKS - Full socket stack
// ============================================================================

fn bench_tcp_begin_append_commit(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server, mut client) = runtime.block_on(async {
        let server = TestServer::start().await.unwrap();
        let client = server.connect().await.unwrap();
        (server, client)
    });

    let mut group = c.benchmark_group("stream_tcp_ingest_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("tcp_begin_append_commit", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let route_family = RouteFamily::new(1);
                let route_str = "stream://realm/area/events";

                // Begin (mode=0 for Create)
                let begin_frame = encode_begin_request(route_family, route_str, 0);
                let _ = client.request(&begin_frame, 5000).await.unwrap();

                // For simplicity, assume session_id = 1
                let session_id = 1;

                // Append
                let append_frame = encode_append_request(session_id, b"test_chunk");
                let _ = client.request(&append_frame, 5000).await.unwrap();

                // Commit
                let commit_frame = encode_commit_request(session_id);
                let _ = black_box(client.request(&commit_frame, 5000).await.unwrap());
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

fn bench_ws_begin_append_commit(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server, mut ws_client) = runtime.block_on(async {
        let server = TestServer::start().await.unwrap();
        let ws_client = server.connect_ws().await.unwrap();
        (server, ws_client)
    });

    let mut group = c.benchmark_group("stream_ws_ingest_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("ws_begin_append_commit", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let route_family = RouteFamily::new(1);
                let route_str = "stream://realm/area/events";

                // Begin
                let begin_frame = encode_begin_request(route_family, route_str, 0);
                let _ = ws_client.request(&begin_frame, 5000).await.unwrap();

                let session_id = 1;

                // Append
                let append_frame = encode_append_request(session_id, b"test_chunk");
                let _ = ws_client.request(&append_frame, 5000).await.unwrap();

                // Commit
                let commit_frame = encode_commit_request(session_id);
                let _ = black_box(ws_client.request(&commit_frame, 5000).await.unwrap());
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
        bench_tcp_begin_append_commit,
        bench_ws_begin_append_commit,
}
criterion_main!(benches);
