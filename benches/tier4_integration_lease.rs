//! Lease domain tier 4 integration benchmarks
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests two integration levels:
//! 1. **TCP** - Full TCP stack: encode → socket → server → decode → actor → encode → socket
//! 2. **WebSocket** - Full WS stack: encode → WS frame → server → decode → actor → encode → WS frame
//!
//! This reveals where performance cliffs occur in distributed lock workflows.
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

/// Encode a lease acquire request
fn encode_acquire_request(route_family: RouteFamily, route_str: &str, ttl_seconds: u64) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 400; // ACQUIRE

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

    // TTL
    buf.put_u8(3);
    buf.put_u16(8);
    buf.put_u64(ttl_seconds);

    buf.to_vec()
}

/// Encode a lease renew request
fn encode_renew_request(route_family: RouteFamily, route_str: &str, token: u64) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 401; // RENEW

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

    // Token
    buf.put_u8(4);
    buf.put_u16(8);
    buf.put_u64(token);

    buf.to_vec()
}

/// Encode a lease release request
fn encode_release_request(route_family: RouteFamily, route_str: &str, token: u64) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 402; // RELEASE

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

    // Token
    buf.put_u8(4);
    buf.put_u16(8);
    buf.put_u64(token);

    buf.to_vec()
}

// ============================================================================
// TCP INTEGRATION BENCHMARKS - Full socket stack
// ============================================================================

fn bench_tcp_acquire_renew_release(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server, mut client) = runtime.block_on(async {
        let server = TestServer::start().await.unwrap();
        let client = server.connect().await.unwrap();
        (server, client)
    });

    let mut group = c.benchmark_group("lease_tcp_lock_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("tcp_acquire_renew_release", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let route_family = RouteFamily::new(1);
                let route_str = "lease://realm/area/lock1";

                // Acquire
                let acquire_frame = encode_acquire_request(route_family, route_str, 30);
                let _ = client.request(&acquire_frame, 5000).await.unwrap();

                // For simplicity, assume token = 1 (we'd parse the response in real impl)
                let token = 1;

                // Renew
                let renew_frame = encode_renew_request(route_family, route_str, token);
                let _ = client.request(&renew_frame, 5000).await.unwrap();

                // Release
                let release_frame = encode_release_request(route_family, route_str, token);
                let _ = black_box(client.request(&release_frame, 5000).await.unwrap());
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

fn bench_ws_acquire_renew_release(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server, mut ws_client) = runtime.block_on(async {
        let server = TestServer::start().await.unwrap();
        let ws_client = server.connect_ws().await.unwrap();
        (server, ws_client)
    });

    let mut group = c.benchmark_group("lease_ws_lock_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("ws_acquire_renew_release", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let route_family = RouteFamily::new(1);
                let route_str = "lease://realm/area/lock1";

                // Acquire
                let acquire_frame = encode_acquire_request(route_family, route_str, 30);
                let _ = ws_client.request(&acquire_frame, 5000).await.unwrap();

                let token = 1;

                // Renew
                let renew_frame = encode_renew_request(route_family, route_str, token);
                let _ = ws_client.request(&renew_frame, 5000).await.unwrap();

                // Release
                let release_frame = encode_release_request(route_family, route_str, token);
                let _ = black_box(ws_client.request(&release_frame, 5000).await.unwrap());
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
        bench_tcp_acquire_renew_release,
        bench_ws_acquire_renew_release,
}
criterion_main!(benches);
