//! Notice domain tier 4 integration benchmarks
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests three integration levels:
//! 1. **Direct** - Domain actor (no network) - baseline integration overhead
//! 2. **TCP** - Full TCP stack: encode → socket → server → decode → actor → encode → socket
//! 3. **WebSocket** - Full WS stack: encode → WS frame → server → decode → actor → encode → WS frame
//!
//! This reveals where performance cliffs occur in pub/sub operations.

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

/// Encode a subscribe request frame with TLV format
fn encode_subscribe_request(
    route_family: RouteFamily,
    route_str: &str,
    subscriber_id: u64,
) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 501; // SUBSCRIBE

    // Message type (may use escape sequence if > 254)
    if msg_type <= 254 {
        buf.put_u8(msg_type as u8);
    } else {
        buf.put_u8(0xFF);
        buf.put_u16(msg_type);
    }

    // RouteFamily
    buf.put_u8(1); // Tag: RouteFamily
    buf.put_u16(8); // Length
    buf.put_u64(route_family.as_u64());

    // Route string
    let route_bytes = route_str.as_bytes();
    buf.put_u8(2); // Tag: Route
    buf.put_u16(route_bytes.len() as u16);
    buf.put_slice(route_bytes);

    // Subscriber ID
    buf.put_u8(3); // Tag: SubscriberId
    buf.put_u16(8);
    buf.put_u64(subscriber_id);

    buf.to_vec()
}

/// Encode a publish request frame with TLV format
fn encode_publish_request(route_family: RouteFamily, route_str: &str, payload: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 500; // PUBLISH

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

    // Route string
    let route_bytes = route_str.as_bytes();
    buf.put_u8(2);
    buf.put_u16(route_bytes.len() as u16);
    buf.put_slice(route_bytes);

    // Payload
    buf.put_u8(4); // Tag: Payload
    buf.put_u16(payload.len() as u16);
    buf.put_slice(payload);

    buf.to_vec()
}

/// Encode an unsubscribe request frame
fn encode_unsubscribe_request(
    route_family: RouteFamily,
    route_str: &str,
    subscriber_id: u64,
) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let msg_type: u16 = 502; // UNSUBSCRIBE

    if msg_type <= 254 {
        buf.put_u8(msg_type as u8);
    } else {
        buf.put_u8(0xFF);
        buf.put_u16(msg_type);
    }

    buf.put_u8(1);
    buf.put_u16(8);
    buf.put_u64(route_family.as_u64());

    let route_bytes = route_str.as_bytes();
    buf.put_u8(2);
    buf.put_u16(route_bytes.len() as u16);
    buf.put_slice(route_bytes);

    buf.put_u8(3);
    buf.put_u16(8);
    buf.put_u64(subscriber_id);

    buf.to_vec()
}

// ============================================================================
// TCP INTEGRATION BENCHMARKS - Full socket stack
// ============================================================================

fn bench_tcp_subscribe_publish_unsubscribe(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server, mut client) = runtime.block_on(async {
        let server = TestServer::start().await.unwrap();
        let client = server.connect().await.unwrap();
        (server, client)
    });

    let mut group = c.benchmark_group("notice_tcp_pubsub_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("tcp_subscribe_publish_unsubscribe", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let route_family = RouteFamily::new(1);
                let route_str = "notice://realm/area/events";
                let subscriber_id = 1;

                // Subscribe
                let subscribe_frame =
                    encode_subscribe_request(route_family, route_str, subscriber_id);
                let _ = client.request(&subscribe_frame, 5000).await.unwrap();

                // Publish
                let publish_frame = encode_publish_request(route_family, route_str, b"test_event");
                let _ = client.request(&publish_frame, 5000).await.unwrap();

                // Unsubscribe
                let unsubscribe_frame =
                    encode_unsubscribe_request(route_family, route_str, subscriber_id);
                let _ = black_box(client.request(&unsubscribe_frame, 5000).await.unwrap());
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

fn bench_ws_subscribe_publish_unsubscribe(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (server, mut ws_client) = runtime.block_on(async {
        let server = TestServer::start().await.unwrap();
        let ws_client = server.connect_ws().await.unwrap();
        (server, ws_client)
    });

    let mut group = c.benchmark_group("notice_ws_pubsub_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("ws_subscribe_publish_unsubscribe", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let route_family = RouteFamily::new(1);
                let route_str = "notice://realm/area/events";
                let subscriber_id = 1;

                // Subscribe
                let subscribe_frame =
                    encode_subscribe_request(route_family, route_str, subscriber_id);
                let _ = ws_client.request(&subscribe_frame, 5000).await.unwrap();

                // Publish
                let publish_frame = encode_publish_request(route_family, route_str, b"test_event");
                let _ = ws_client.request(&publish_frame, 5000).await.unwrap();

                // Unsubscribe
                let unsubscribe_frame =
                    encode_unsubscribe_request(route_family, route_str, subscriber_id);
                let _ = black_box(ws_client.request(&unsubscribe_frame, 5000).await.unwrap());
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
        bench_tcp_subscribe_publish_unsubscribe,
        bench_ws_subscribe_publish_unsubscribe,
}
criterion_main!(benches);
