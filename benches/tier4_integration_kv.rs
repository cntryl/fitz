//! KV domain tier 4 integration benchmarks
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests three integration levels:
//! 1. **Direct** - Domain actor + disk (no network) - baseline integration overhead
//! 2. **TCP** - Full TCP stack: encode → socket → server → decode → actor → encode → socket
//! 3. **WebSocket** - Full WS stack: encode → WS frame → server → decode → actor → encode → WS frame
//!
//! This reveals where performance cliffs occur:
//! - tier1 hotpath: ~100ns (pure sync operations)
//! - tier2 subsystem: ~1µs (component integration)
//! - tier3 system: ~10µs (domain + plumbing)
//! - tier4 direct: ~100µs (+ disk I/O, no network)
//! - tier4 tcp: ~Xms (+ socket overhead)
//! - tier4 ws: ~Yms (+ WebSocket framing overhead)

use bytes::{BufMut, Bytes};
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::create_local_bench_store;
use fitz::domains::kv::{KvActor, KvMessage, TxMode};
use fitz::protocol::kv_codec::msg_type;
use fitz::runtime::routing::RouteFamily;
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn bench_full_pipeline_put(c: &mut Criterion) {
    // Complete pipeline: Create actor, begin, put, rollback
    // Measures total overhead vs hotpath
    let mut group = c.benchmark_group("kv_integration_full_pipeline_put");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));

    group.bench_function("create_actor_begin_put_rollback", |b| {
        b.iter_batched(
            create_local_bench_store,
            |(store, _temp_dir)| {
                let mut actor = KvActor::new(store);

                let response = actor.handle(KvMessage::Begin {
                    route_family: RouteFamily::new(1),
                    realm: "integration".to_string(),
                    area: "kv".to_string(),
                    resource: "full_pipeline".to_string(),
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
                    resource: "full_pipeline".to_string(),
                    key: Bytes::from_static(b"integration_key"),
                    value: Bytes::from_static(b"integration_value_with_some_length"),
                });

                actor.handle(KvMessage::Rollback { tx_id });
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_full_pipeline_transaction_sequence(c: &mut Criterion) {
    // Realistic transaction sequence: Begin, get, put, delete, rollback
    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = KvActor::new(store);

    // Setup some initial data
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "integration".to_string(),
        area: "kv".to_string(),
        resource: "initial".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id_setup = match response {
        fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
        _ => return,
    };

    actor.handle(KvMessage::Put {
        tx_id: tx_id_setup,
        route_family: RouteFamily::new(1),
        resource: "initial".to_string(),
        key: Bytes::from_static(b"existing_key"),
        value: Bytes::from_static(b"existing_value"),
    });

    actor.handle(KvMessage::Rollback { tx_id: tx_id_setup });

    let mut group = c.benchmark_group("kv_integration_full_pipeline_sequence");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(1));

    group.bench_function("begin_get_put_delete_rollback_full_cycle", |b| {
        b.iter(|| {
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "cycle".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };

            // Read existing
            actor.handle(KvMessage::Get {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "initial".to_string(),
                key: Bytes::from_static(b"existing_key"),
            });

            // Add new
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "cycle".to_string(),
                key: Bytes::from_static(b"new_key"),
                value: Bytes::from_static(b"new_value"),
            });

            // Delete existing
            actor.handle(KvMessage::Delete {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "initial".to_string(),
                key: Bytes::from_static(b"existing_key"),
            });

            // Cleanup
            actor.handle(KvMessage::Rollback { tx_id });
        })
    });

    group.finish();
}

fn bench_multi_resource_transaction(c: &mut Criterion) {
    // Transactions spanning multiple resources within same realm/area
    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_integration_multi_resource");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(3));

    group.bench_function("single_tx_3_resources", |b| {
        b.iter(|| {
            // All resources use same family for simplicity
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "r1".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };

            // Operations on resource 1
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "r1".to_string(),
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            });

            // Operations on resource 2 (same transaction context)
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "r2".to_string(),
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            });

            // Operations on resource 3
            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(1),
                resource: "r3".to_string(),
                key: Bytes::from_static(b"k3"),
                value: Bytes::from_static(b"v3"),
            });

            actor.handle(KvMessage::Rollback { tx_id });
        })
    });

    group.finish();
}

fn bench_cross_family_transaction_sequence(c: &mut Criterion) {
    // Separate transactions on different families within same benchmark iteration
    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = KvActor::new(store);

    let mut group = c.benchmark_group("kv_integration_cross_family_sequence");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));
    group.throughput(Throughput::Elements(3));

    group.bench_function("3_separate_family_transactions", |b| {
        b.iter(|| {
            // Transaction on family 1
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(1),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "tx1".to_string(),
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
                resource: "tx1".to_string(),
                key: Bytes::from_static(b"k1"),
                value: Bytes::from_static(b"v1"),
            });

            actor.handle(KvMessage::Rollback { tx_id });

            // Transaction on family 2
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(2),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "tx2".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };

            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(2),
                resource: "tx2".to_string(),
                key: Bytes::from_static(b"k2"),
                value: Bytes::from_static(b"v2"),
            });

            actor.handle(KvMessage::Rollback { tx_id });

            // Transaction on family 3
            let response = actor.handle(KvMessage::Begin {
                route_family: RouteFamily::new(3),
                realm: "integration".to_string(),
                area: "kv".to_string(),
                resource: "tx3".to_string(),
                mode: TxMode::ReadWrite,
                write_options: cntryl_midge::WriteOptions::buffered(),
            });
            let tx_id = match response {
                fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
                _ => return,
            };

            actor.handle(KvMessage::Put {
                tx_id,
                route_family: RouteFamily::new(3),
                resource: "tx3".to_string(),
                key: Bytes::from_static(b"k3"),
                value: Bytes::from_static(b"v3"),
            });

            actor.handle(KvMessage::Rollback { tx_id });
        })
    });

    group.finish();
}

// ============================================================================
// TCP INTEGRATION BENCHMARKS - Full network stack
// ============================================================================

fn bench_tcp_begin_put_rollback(c: &mut Criterion) {
    // Setup: Start server and connect TCP client
    let runtime = tokio::runtime::Runtime::new().unwrap();
    
    let server = runtime.block_on(async {
        fitz::testkit::TestServer::start().await.unwrap()
    });

    let mut client = runtime.block_on(async {
        server.connect().await.unwrap()
    });

    // Precompute request frames
    let begin_frame = encode_begin_request(
        RouteFamily::new(1),
        "tcp-bench",
        "kv",
        "transactions",
        TxMode::ReadWrite,
    );

    let mut group = c.benchmark_group("kv_integration_tcp");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements(1));

    group.bench_function("tcp_begin_put_rollback_roundtrip", |b| {
        b.iter(|| {
            runtime.block_on(async {
                // Send BEGIN request
                let response = client.request(&begin_frame, 5000).await.unwrap();
                let tx_id = parse_begin_response(&response).unwrap();

                // Send PUT request
                let put_frame = encode_put_request(
                    tx_id,
                    RouteFamily::new(1),
                    "transactions",
                    b"tcp_key",
                    b"tcp_value_with_some_length",
                );
                client.request(&put_frame, 5000).await.unwrap();

                // Send ROLLBACK request
                let rollback_frame = encode_rollback_request(tx_id);
                client.request(&rollback_frame, 5000).await.unwrap();
            })
        })
    });

    group.finish();
    drop(client);
    drop(server);
}

// ============================================================================
// WEBSOCKET INTEGRATION BENCHMARKS - Full WebSocket stack
// ============================================================================

fn bench_ws_begin_put_rollback(c: &mut Criterion) {
    // Setup: Start server and connect WebSocket client
    let runtime = tokio::runtime::Runtime::new().unwrap();
    
    let server = runtime.block_on(async {
        fitz::testkit::TestServer::start().await.unwrap()
    });

    let mut ws_client = runtime.block_on(async {
        server.connect_ws().await.unwrap()
    });

    // Precompute request frames (same as TCP, different framing)
    let begin_frame = encode_begin_request(
        RouteFamily::new(1),
        "ws-bench",
        "kv",
        "transactions",
        TxMode::ReadWrite,
    );

    let mut group = c.benchmark_group("kv_integration_ws");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(2));
    group.throughput(Throughput::Elements(1));

    group.bench_function("ws_begin_put_rollback_roundtrip", |b| {
        b.iter(|| {
            runtime.block_on(async {
                // Send BEGIN request
                let response = ws_client.request(&begin_frame, 5000).await.unwrap();
                let tx_id = parse_begin_response(&response).unwrap();

                // Send PUT request
                let put_frame = encode_put_request(
                    tx_id,
                    RouteFamily::new(1),
                    "transactions",
                    b"ws_key",
                    b"ws_value_with_some_length",
                );
                ws_client.request(&put_frame, 5000).await.unwrap();

                // Send ROLLBACK request
                let rollback_frame = encode_rollback_request(tx_id);
                ws_client.request(&rollback_frame, 5000).await.unwrap();
            })
        })
    });

    group.finish();
    drop(ws_client);
    drop(server);
}

// ============================================================================
// TLV ENCODING HELPERS (match server protocol exactly)
// ============================================================================

fn encode_begin_request(
    route_family: RouteFamily,
    realm: &str,
    area: &str,
    resource: &str,
    mode: TxMode,
) -> Vec<u8> {
    let mut buf = Vec::new();
    
    // Message type
    buf.put_u16(msg_type::BEGIN);
    
    // Payload: [u8 mode][u32 route_len][route]
    buf.put_u8(match mode {
        TxMode::ReadOnly => 0,
        TxMode::ReadWrite => 1,
    });
    
    let route = format!("kv://{}/{}/{}", realm, area, resource);
    buf.put_u32(route.len() as u32);
    buf.extend_from_slice(route.as_bytes());
    
    buf
}

fn encode_put_request(
    tx_id: u64,
    route_family: RouteFamily,
    resource: &str,
    key: &[u8],
    value: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    
    // Message type
    buf.put_u16(msg_type::PUT);
    
    // Payload: [u64 tx_id][u32 route_len][route][u32 key_len][key][u32 value_len][value]
    buf.put_u64(tx_id);
    
    let route = format!("kv://{}", resource);
    buf.put_u32(route.len() as u32);
    buf.extend_from_slice(route.as_bytes());
    
    buf.put_u32(key.len() as u32);
    buf.extend_from_slice(key);
    
    buf.put_u32(value.len() as u32);
    buf.extend_from_slice(value);
    
    buf
}

fn encode_rollback_request(tx_id: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    
    // Message type
    buf.put_u16(msg_type::ROLLBACK);
    
    // Payload: [u64 tx_id]
    buf.put_u64(tx_id);
    
    buf
}

fn parse_begin_response(response: &[u8]) -> Result<u64, String> {
    if response.is_empty() {
        return Err("Empty response".to_string());
    }
    
    let status = response[0];
    if status != 0 {
        return Err("BEGIN failed".to_string());
    }
    
    if response.len() < 9 {
        return Err("Response too short".to_string());
    }
    
    let tx_id = u64::from_be_bytes([
        response[1],
        response[2],
        response[3],
        response[4],
        response[5],
        response[6],
        response[7],
        response[8],
    ]);
    
    Ok(tx_id)
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_full_pipeline_put,
        bench_full_pipeline_transaction_sequence,
        bench_multi_resource_transaction,
        bench_cross_family_transaction_sequence,
        bench_tcp_begin_put_rollback,
        bench_ws_begin_put_rollback
}
criterion_main!(benches);
