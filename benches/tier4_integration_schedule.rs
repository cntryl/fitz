//! Schedule domain tier 4 integration benchmarks
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests two integration levels:
//! 1. **TCP** - Full TCP stack: encode → socket → server → decode → actor → encode → socket
//! 2. **WebSocket** - Full WS stack: encode → WS frame → server → decode → actor → encode → WS frame
//!
//! This reveals where performance cliffs occur in scheduled task workflows.
//! (Direct actor testing skipped - requires complex storage setup, see tier3 for actor-level benchmarks)

use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::transport::{TestServer, TestClient, TestWebSocketClient};
use std::time::Duration;

#[path = "config.rs"]
mod config;

// ============================================================================
// TLV ENCODING HELPERS
// ============================================================================

/// Encode a schedule create request
fn encode_create_request(
    route_family: RouteFamily,
    route_str: &str,
    delay_seconds: u64,
    recurring: bool,
) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 700; // CREATE

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

    // Delay
    buf.put_u8(3);
    buf.put_u16(8);
    buf.put_u64(delay_seconds);

    // Recurring flag
    buf.put_u8(4);
    buf.put_u16(1);
    buf.put_u8(if recurring { 1 } else { 0 });

    buf.to_vec()
}

/// Encode a schedule cancel request
fn encode_cancel_request(route_family: RouteFamily, route_str: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 701; // CANCEL

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

    buf.to_vec()
}

/// Encode a schedule list request
fn encode_list_request(route_family: RouteFamily) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 702; // LIST

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

    buf.to_vec()
}

// ============================================================================
// TCP INTEGRATION BENCHMARKS - Full socket stack
// ============================================================================

fn bench_tcp_create_cancel(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_tcp_task_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(2));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("tcp_create_cancel", |b| {
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
                let route_str = "schedule://realm/area/task1";

                // Create
                let create_frame = encode_create_request(route_family, route_str, 60, false);
                let _ = client.request(&create_frame, 5000).await.unwrap();

                // Cancel
                let cancel_frame = encode_cancel_request(route_family, route_str);
                let _ = black_box(client.request(&cancel_frame, 5000).await.unwrap());

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

fn bench_ws_create_cancel(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_ws_task_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(2));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("ws_create_cancel", |b| {
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
                let route_str = "schedule://realm/area/task1";

                // Create
                let create_frame = encode_create_request(route_family, route_str, 60, false);
                let _ = ws_client.request(&create_frame, 5000).await.unwrap();

                // Cancel
                let cancel_frame = encode_cancel_request(route_family, route_str);
                let _ = black_box(
                    ws_client
                        .request(&cancel_frame, 5000)
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
        bench_tcp_create_cancel,
        bench_ws_create_cancel,
}
criterion_main!(benches);
