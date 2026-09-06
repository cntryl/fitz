#![allow(dead_code)] // KV targets select focused lifecycle and contention helpers.

use crate::tier4_support::{
    measure_operations, tag_dimensions, LayerKind, StorageProfile, Tier4Dimensions, TransportKind,
};
use bytes::Bytes;
use cntryl_stress::StressContext;
use fitz::benchkit::{
    build_kv_begin, build_kv_commit, build_kv_put, create_local_bench_store,
    create_write_heavy_bench_store, parse_kv_response, parse_kv_tx_id, shared_bench_runtime,
};
use fitz::domains::kv::{KvActor, KvMessage, KvResourceScope, KvResponse, TxMode};
use fitz::protocol::kv_codec::parse_request as parse_kv_request;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use futures_util::future::join_all;
use std::time::Instant;

pub(crate) const ROUTE: &str = "kv://tier4/state/resource";
pub(crate) const PAYLOAD_SIZE: usize = 256;
const TIMEOUT_MS: u64 = 5_000;
const MAX_TX_RETRIES: usize = 512;
const STATE_RING_LEN: usize = 32;

pub(crate) fn dimensions(
    storage: StorageProfile,
    layer: LayerKind,
    write_mode: &'static str,
) -> Tier4Dimensions<'static> {
    Tier4Dimensions {
        domain: "kv",
        scenario: "begin_put_commit_lifecycle",
        storage_profile: storage,
        layer,
        write_mode,
        payload_size: PAYLOAD_SIZE,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count: 1,
        filter_selectivity: "not_applicable",
        client_count: 1,
        workload_mix: "write_only",
        completed_unit: "transaction_lifecycle",
        gate_class: if storage == StorageProfile::Memory {
            "regression_gate"
        } else {
            "storage_characterization"
        },
    }
}

struct DirectKvActor {
    actor: KvActor,
    _temp_dir: Option<tempfile::TempDir>,
}

impl DirectKvActor {
    fn new(storage: StorageProfile) -> Self {
        let (store, temp_dir) = match storage {
            StorageProfile::Memory => (create_write_heavy_bench_store(), None),
            StorageProfile::LocalDisk => {
                let (store, temp_dir) = create_local_bench_store();
                (store, Some(temp_dir))
            }
        };
        Self {
            actor: KvActor::new(store),
            _temp_dir: temp_dir,
        }
    }
}

fn direct_actor(storage: StorageProfile) -> DirectKvActor {
    DirectKvActor::new(storage)
}

struct DirectLifecycleState {
    actor: DirectKvActor,
    keys: [Bytes; STATE_RING_LEN],
    values: [Bytes; STATE_RING_LEN],
    next: usize,
}

impl DirectLifecycleState {
    fn new(storage: StorageProfile) -> Self {
        Self {
            actor: direct_actor(storage),
            keys: std::array::from_fn(|index| Bytes::from(format!("key-{index}"))),
            values: std::array::from_fn(|index| {
                Bytes::from(vec![
                    u8::try_from(index).expect("state-ring index fits u8");
                    PAYLOAD_SIZE
                ])
            }),
            next: 0,
        }
    }

    fn complete(&mut self, commit: bool) {
        let key = self.keys[self.next].clone();
        let value = self.values[self.next].clone();
        self.next = (self.next + 1) % STATE_RING_LEN;
        direct_lifecycle(&mut self.actor, commit, key, value);
    }
}

fn direct_lifecycle(actor: &mut DirectKvActor, commit: bool, key: Bytes, value: Bytes) {
    let scope = KvResourceScope::new(RouteFamily::new(1), "tier4", "state", "resource");
    let begin = actor.actor.handle(KvMessage::Begin {
        scope: scope.clone(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::sync().into(),
    });
    let KvResponse::BeginOk { tx_id } = begin else {
        panic!("KV begin failed: {begin:?}")
    };
    assert!(matches!(
        actor.actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key,
            value,
        }),
        KvResponse::PutOk
    ));
    let response = if commit {
        actor.actor.handle(KvMessage::Commit { tx_id, scope })
    } else {
        actor.actor.handle(KvMessage::Rollback { tx_id, scope })
    };
    assert!(
        matches!(response, KvResponse::CommitOk | KvResponse::RollbackOk),
        "KV lifecycle failed: {response:?}"
    );
}

pub(crate) fn measure_direct(
    ctx: &mut StressContext,
    storage: StorageProfile,
    commit: bool,
    measurement: &'static str,
) {
    let write_mode = if commit { "sync" } else { "rollback" };
    let mut d = dimensions(storage, LayerKind::Direct, write_mode);
    d.scenario = if commit {
        "begin_put_sync_commit"
    } else {
        "begin_put_rollback"
    };
    tag_dimensions(ctx, &d);
    let mut state = DirectLifecycleState::new(storage);
    state.complete(commit);
    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        state.complete(commit);
        latencies.push(started.elapsed());
    });
}

pub(crate) struct EncodedState {
    actor: DirectKvActor,
    begin: Vec<u8>,
    put: Vec<u8>,
    commit: Vec<u8>,
    rollback: Vec<u8>,
    sequence: u64,
}

impl EncodedState {
    pub(crate) fn new(storage: StorageProfile) -> Self {
        Self {
            actor: direct_actor(storage),
            begin: build_kv_begin(ROUTE, 1, 1),
            put: build_kv_put(0, ROUTE, b"key", &vec![0xA5; PAYLOAD_SIZE]),
            commit: build_kv_commit(0, ROUTE),
            rollback: fitz::benchkit::build_kv_rollback(0, ROUTE),
            sequence: 0,
        }
    }
    fn set_tx(frame: &mut [u8], tx_id: u64) {
        let offset = if frame.first().copied() == Some(0xFF) {
            5
        } else {
            3
        };
        frame[offset..offset + 8].copy_from_slice(&tx_id.to_be_bytes());
    }
    fn dispatch(actor: &mut DirectKvActor, frame: &[u8]) -> KvResponse {
        let mut parser = TlvFrameParser::new(frame);
        let (message_type, payload) = parser.next_field_ref().expect("KV field");
        let message =
            parse_kv_request(message_type, RouteFamily::new(1), payload).expect("KV decode");
        actor.actor.handle(message)
    }
    fn complete(&mut self, commit: bool) {
        let KvResponse::BeginOk { tx_id } = Self::dispatch(&mut self.actor, &self.begin) else {
            panic!("KV encoded begin failed")
        };
        Self::set_tx(&mut self.put, tx_id);
        assert!(matches!(
            Self::dispatch(&mut self.actor, &self.put),
            KvResponse::PutOk
        ));
        let frame = if commit {
            &mut self.commit
        } else {
            &mut self.rollback
        };
        Self::set_tx(frame, tx_id);
        let response = Self::dispatch(&mut self.actor, frame);
        assert!(
            matches!(response, KvResponse::CommitOk | KvResponse::RollbackOk),
            "KV encoded lifecycle failed: {response:?}"
        );
        self.sequence += 1;
    }
}

pub(crate) fn measure_encoded(
    ctx: &mut StressContext,
    storage: StorageProfile,
    commit: bool,
    measurement: &'static str,
) {
    let mut d = dimensions(
        storage,
        LayerKind::Encoded,
        if commit { "sync" } else { "rollback" },
    );
    d.scenario = if commit {
        "begin_put_sync_commit"
    } else {
        "begin_put_rollback"
    };
    tag_dimensions(ctx, &d);
    let mut state = EncodedState::new(storage);
    state.complete(commit);
    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        state.complete(commit);
        latencies.push(started.elapsed());
    });
}

enum Client {
    Tcp(TestClient),
    WebSocket(Box<TestWebSocketClient>),
}
impl Client {
    async fn request(&mut self, frame: &[u8]) -> Vec<u8> {
        match self {
            Self::Tcp(c) => c.request(frame, TIMEOUT_MS).await,
            Self::WebSocket(c) => c.request(frame, TIMEOUT_MS).await,
        }
        .expect("KV response")
    }
    async fn close(self) {
        match self {
            Self::Tcp(c) => c.close().await.expect("close KV TCP"),
            Self::WebSocket(mut c) => c.close().await.expect("close KV WS"),
        }
    }
}

struct WireState {
    begin: Vec<u8>,
    put: Vec<u8>,
    commit: Vec<u8>,
    sequence: u64,
}
impl WireState {
    fn new(route: &str) -> Self {
        Self {
            begin: build_kv_begin(route, 1, 1),
            put: build_kv_put(0, route, b"key", &vec![0xA5; PAYLOAD_SIZE]),
            commit: build_kv_commit(0, route),
            sequence: 0,
        }
    }
    async fn complete(&mut self, client: &mut Client) {
        let begin = client.request(&self.begin).await;
        let (_type, status, data) = parse_kv_response(&begin);
        assert_eq!(status, 0, "KV begin status");
        let tx_id = parse_kv_tx_id(&data).expect("KV tx id");
        EncodedState::set_tx(&mut self.put, tx_id);
        EncodedState::set_tx(&mut self.commit, tx_id);
        let put = client.request(&self.put).await;
        let (_, status, _) = parse_kv_response(&put);
        assert_eq!(status, 0, "KV put status");
        let commit = client.request(&self.commit).await;
        let (_, status, _) = parse_kv_response(&commit);
        assert_eq!(status, 0, "KV commit status");
        self.sequence += 1;
    }

    async fn complete_retry(&mut self, client: &mut Client) -> u64 {
        for attempt in 0..MAX_TX_RETRIES {
            let begin = client.request(&self.begin).await;
            let (_type, status, data) = parse_kv_response(&begin);
            if status != 0 {
                tokio::task::yield_now().await;
                continue;
            }
            let tx_id = parse_kv_tx_id(&data).expect("KV contention tx id");
            EncodedState::set_tx(&mut self.put, tx_id);
            EncodedState::set_tx(&mut self.commit, tx_id);
            let put = client.request(&self.put).await;
            let (_, put_status, _) = parse_kv_response(&put);
            if put_status != 0 {
                tokio::task::yield_now().await;
                continue;
            }
            let commit = client.request(&self.commit).await;
            let (_, commit_status, _) = parse_kv_response(&commit);
            if commit_status == 0 {
                self.sequence += 1;
                return u64::try_from(attempt).expect("retry attempt should fit u64");
            }
            tokio::task::yield_now().await;
        }

        panic!("KV contention lifecycle did not commit after {MAX_TX_RETRIES} attempts");
    }
}

pub(crate) fn measure_transport(
    ctx: &mut StressContext,
    storage: StorageProfile,
    transport: TransportKind,
    measurement: &'static str,
) {
    let layer = LayerKind::from(transport);
    let d = dimensions(storage, layer, "sync");
    tag_dimensions(ctx, &d);
    let runtime = shared_bench_runtime();
    let temp =
        (storage == StorageProfile::LocalDisk).then(|| tempfile::tempdir().expect("KV temp dir"));
    let server = match &temp {
        Some(dir) => runtime
            .block_on(TestServer::start_with_local_storage(
                dir.path().to_string_lossy().into_owned(),
            ))
            .expect("KV local server"),
        None => runtime
            .block_on(TestServer::start_with_write_heavy_memory())
            .expect("KV memory server"),
    };
    let mut client = runtime
        .block_on(async {
            match transport {
                TransportKind::Tcp => TestClient::new(server.tcp_addr).await.map(Client::Tcp),
                TransportKind::WebSocket => {
                    TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
                        .await
                        .map(|client| Client::WebSocket(Box::new(client)))
                }
            }
        })
        .expect("KV client");
    let mut state = WireState::new(ROUTE);
    runtime.block_on(state.complete(&mut client));
    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        runtime.block_on(state.complete(&mut client));
        latencies.push(started.elapsed());
    });
    runtime.block_on(client.close());
    runtime.block_on(server.shutdown()).expect("KV shutdown");
    drop(temp);
}

pub(crate) fn measure_contention(
    ctx: &mut StressContext,
    transport: TransportKind,
    measurement: &'static str,
) {
    let d = Tier4Dimensions {
        domain: "kv",
        scenario: "multiclient_transaction_contention",
        storage_profile: StorageProfile::Memory,
        layer: if transport == TransportKind::Tcp {
            LayerKind::TcpMultiClient
        } else {
            LayerKind::WebSocketMultiClient
        },
        write_mode: "sync",
        payload_size: PAYLOAD_SIZE,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count: 1,
        filter_selectivity: "not_applicable",
        client_count: 4,
        workload_mix: "write_contention",
        completed_unit: "transaction_lifecycle",
        gate_class: "characterization",
    };
    tag_dimensions(ctx, &d);
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start_with_write_heavy_memory())
        .expect("KV contention server");
    let mut clients = runtime
        .block_on(async {
            join_all((0..4).map(|_| async {
                match transport {
                    TransportKind::Tcp => TestClient::new(server.tcp_addr).await.map(Client::Tcp),
                    TransportKind::WebSocket => {
                        TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
                            .await
                            .map(|client| Client::WebSocket(Box::new(client)))
                    }
                }
            }))
            .await
        })
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let mut states = (0..clients.len())
        .map(|index| WireState::new(&format!("kv://tier4/contention/resource-{index}")))
        .collect::<Vec<_>>();
    let mut retry_count = 0_u64;
    measure_operations(ctx, measurement, 4, |latencies| {
        let observed =
            runtime.block_on(async {
                join_all(clients.iter_mut().zip(states.iter_mut()).map(
                    |(client, state)| async move {
                        let started = Instant::now();
                        let retries = state.complete_retry(client).await;
                        (started.elapsed(), retries)
                    },
                ))
                .await
            });
        for (latency, retries) in observed {
            latencies.push(latency);
            retry_count = retry_count.saturating_add(retries);
        }
    });
    ctx.metadata("optimistic_commit_retries", retry_count.to_string());
    runtime.block_on(async {
        join_all(clients.into_iter().map(Client::close)).await;
    });
    runtime
        .block_on(server.shutdown())
        .expect("KV contention shutdown");
}
