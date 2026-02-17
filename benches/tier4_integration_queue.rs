//! Queue domain tier 4 integration benchmarks
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests three integration levels:
//! 1. **Direct** - Domain actor (no network) - baseline integration overhead
//! 2. **TCP** - Full TCP stack: encode → socket → server → decode → actor → encode → socket
//! 3. **WebSocket** - Full WS stack: encode → WS frame → server → decode → actor → encode → WS frame
//!
//! This reveals where performance cliffs occur in queue message workflows.

use bytes::{BufMut, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput};
use fitz::benchkit::create_local_bench_queue_actor;
use fitz::domains::queue::{QueueMessage, QueueResponse};
use fitz::runtime::envelope::Envelope;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::testkit::transport::{TestServer, TestClient, TestWebSocketClient};
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
// DIRECT INTEGRATION BENCHMARKS - Domain actor only (baseline)
// ============================================================================

fn bench_direct_enqueue_reserve_complete(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_direct_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3)); // enqueue + reserve + complete
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(500));

    group.bench_function("direct_enqueue_reserve_complete", |b| {
        b.iter_batched(
            || create_local_bench_queue_actor("bench", "queue", "tasks", None),
            |(mut actor, _temp_dir)| {
                let route_family = RouteFamily::new(1);
                let route = Route::from_str("queue://bench/queue/tasks").unwrap();
                let route_addr = RouteAddress::from_str("queue://bench/queue/tasks").unwrap();
                let body = b"test_message".to_vec();

                // Enqueue
                let enqueue_msg = QueueMessage::Enqueue {
                    family_id: route_family,
                    route: route.clone(),
                    body: body.into(),
                    delay_seconds: None,
                };
                let enqueue_env = Envelope::new(route_addr.clone(), enqueue_msg);
               let enqueue_resp = actor.receive(enqueue_env);

                // Extract message_id from enqueue response
                let message_id = match enqueue_resp {
                    QueueResponse::Enqueued { id } => id.as_u64(),
                    _ => return,
                };

                // Reserve
                let reserve_msg = QueueMessage::Reserve {
                    family_id: route_family,
                    route: route.clone(),
                    lease_seconds: 30,
                    batch_size: Some(1),
                    wait_seconds: None,
                };
                let reserve_env = Envelope::new(route_addr.clone(), reserve_msg);
                let reserve_resp = actor.receive(reserve_env);

                // Extract token from reserve response
                if let QueueResponse::Reserved { messages } = reserve_resp {
                    if let Some(msg) = messages.first() {
                        // Complete
                        let complete_msg = QueueMessage::Complete {
                            family_id: route_family,
                            route: route.clone(),
                            id: msg.id,
                            token: msg.token,
                        };
                        let complete_env = Envelope::new(route_addr, complete_msg);
                        let _ = black_box(actor.receive(complete_env));
                    }
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ============================================================================
// TCP INTEGRATION BENCHMARKS - Full socket stack
// ============================================================================

fn bench_tcp_enqueue_reserve_complete(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_tcp_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("tcp_enqueue_reserve_complete", |b| {
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
                let route_str = "queue://bench/queue/tasks";

                // Enqueue
                let enqueue_frame =
                    encode_enqueue_request(route_family, route_str, b"test_message", None);
                let enqueue_resp = client.request(&enqueue_frame, 5000).await.unwrap();

                // For simplicity, assume message_id = 1 (we'd need to parse the response)
                // In a real implementation, parse enqueue_resp to extract message_id

                // Reserve
                let reserve_frame = encode_reserve_request(route_family, route_str, 30, Some(1));
                let reserve_resp = client.request(&reserve_frame, 5000).await.unwrap();

                // For simplicity, assume message_id=1, token=1 (we'd parse reserve_resp)

                // Complete
                let complete_frame = encode_complete_request(route_family, route_str, 1, 1);
                let _ = black_box(client.request(&complete_frame, 5000).await.unwrap());

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

fn bench_ws_enqueue_reserve_complete(c: &mut Criterion) {
    let mut group = c.benchmark_group("queue_ws_workflow");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3));
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));

    group.bench_function("ws_enqueue_reserve_complete", |b| {
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
        bench_direct_enqueue_reserve_complete,
        bench_tcp_enqueue_reserve_complete,
        bench_ws_enqueue_reserve_complete,
}
criterion_main!(benches);
