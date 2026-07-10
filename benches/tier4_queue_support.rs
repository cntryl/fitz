#![allow(dead_code)] // Standalone Queue targets use focused subsets of this support API.

use crate::tier4_support::{
    measure_operations, tag_dimensions, StorageProfile, Tier4Dimensions, TransportKind,
};
use bytes::Bytes;
use cntryl_stress::StressContext;
use fitz::benchkit::{
    build_queue_complete, build_queue_dequeue, build_queue_enqueue, create_local_bench_store,
    create_write_heavy_bench_store, shared_bench_runtime,
};
use fitz::domains::queue::{QueueActor, QueueKey, QueueMessage, QueueResponse};
use fitz::protocol::error_codes::decode_error_body;
use fitz::protocol::queue_codec::parse_request as parse_queue_request;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient, TlvFrameParser};
use futures_util::future::join_all;
use std::time::Instant;

pub(crate) const CANONICAL_PAYLOAD_SIZE: usize = 1_024;
pub(crate) const CANONICAL_ROUTE: &str = "queue://tier4/work/main";
const RESPONSE_TIMEOUT_MS: u64 = 5_000;
const SESSION_ID: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueueWriteMode {
    Sync,
    Buffered,
}

impl QueueWriteMode {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Buffered => "buffered",
        }
    }

    fn options(self) -> cntryl_midge::WriteOptions {
        match self {
            Self::Sync => cntryl_midge::WriteOptions::sync(),
            Self::Buffered => cntryl_midge::WriteOptions::buffered(),
        }
    }
}

pub(crate) fn dimensions(
    scenario: &'static str,
    storage_profile: StorageProfile,
    layer: crate::tier4_support::LayerKind,
    write_mode: &'static str,
    payload_size: usize,
    client_count: usize,
    workload_mix: &'static str,
    completed_unit: &'static str,
    gate_class: &'static str,
) -> Tier4Dimensions<'static> {
    Tier4Dimensions {
        domain: "queue",
        scenario,
        storage_profile,
        layer,
        write_mode,
        payload_size,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count: 1,
        filter_selectivity: "not_applicable",
        client_count,
        workload_mix,
        completed_unit,
        gate_class,
    }
}

struct QueueActorFixture {
    actor: QueueActor,
    _temp_dir: Option<tempfile::TempDir>,
}

impl QueueActorFixture {
    fn new(storage: StorageProfile, write_mode: QueueWriteMode) -> Self {
        let (store, temp_dir) = match storage {
            StorageProfile::Memory => (create_write_heavy_bench_store(), None),
            StorageProfile::LocalDisk => {
                let (store, temp_dir) = create_local_bench_store();
                (store, Some(temp_dir))
            }
        };
        let family = RouteFamily::new(1);
        let key = QueueKey {
            family,
            realm: "tier4".to_string(),
            area: "work".to_string(),
            resource: "main".to_string(),
        };
        let actor = QueueActor::new_with_write_options(
            family,
            key,
            store,
            None,
            fitz::utils::idempotency::default_dedup_store(),
            write_mode.options(),
        );
        Self {
            actor,
            _temp_dir: temp_dir,
        }
    }
}

fn complete_direct_lifecycle(actor: &mut QueueActor, payload: &Bytes, session_id: u64) {
    let QueueResponse::Sent { id } = actor.handle_send(payload.clone(), None) else {
        panic!("Queue direct enqueue must succeed");
    };
    let QueueResponse::Received { messages } =
        actor.handle_receive_for_session(session_id, 30, None)
    else {
        panic!("Queue direct reserve must succeed");
    };
    assert_eq!(
        messages.len(),
        1,
        "Queue lifecycle must reserve one message"
    );
    let reserved = &messages[0];
    assert_eq!(
        reserved.id, id,
        "Queue lifecycle reserved the wrong message"
    );
    assert_eq!(&reserved.body, payload, "Queue lifecycle changed the body");
    assert!(matches!(
        actor.handle_ack_for_session(session_id, reserved.id, reserved.token),
        QueueResponse::Acked
    ));
}

pub(crate) fn measure_direct_lifecycle(
    ctx: &mut StressContext,
    dimensions: Tier4Dimensions<'static>,
    write_mode: QueueWriteMode,
    measurement: &'static str,
) {
    tag_dimensions(ctx, &dimensions);
    let mut fixture = QueueActorFixture::new(dimensions.storage_profile, write_mode);
    let payload = Bytes::from(vec![0x51; dimensions.payload_size]);
    complete_direct_lifecycle(&mut fixture.actor, &payload, SESSION_ID);

    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        complete_direct_lifecycle(&mut fixture.actor, &payload, SESSION_ID);
        latencies.push(started.elapsed());
    });
}

struct EncodedLifecycleState {
    fixture: QueueActorFixture,
    enqueue_frame: Vec<u8>,
    reserve_frame: Vec<u8>,
    complete_frame: MutableQueueCompleteFrame,
}

impl EncodedLifecycleState {
    fn new(storage: StorageProfile, write_mode: QueueWriteMode, payload_size: usize) -> Self {
        Self {
            fixture: QueueActorFixture::new(storage, write_mode),
            enqueue_frame: build_queue_enqueue(CANONICAL_ROUTE, &vec![0x52; payload_size]),
            reserve_frame: build_queue_dequeue(CANONICAL_ROUTE),
            complete_frame: MutableQueueCompleteFrame::new(CANONICAL_ROUTE),
        }
    }

    fn complete(&mut self) {
        let QueueResponse::Sent { id } =
            dispatch_encoded(&mut self.fixture.actor, SESSION_ID, &self.enqueue_frame)
        else {
            panic!("Queue encoded enqueue must succeed");
        };
        let QueueResponse::Received { messages } =
            dispatch_encoded(&mut self.fixture.actor, SESSION_ID, &self.reserve_frame)
        else {
            panic!("Queue encoded reserve must succeed");
        };
        assert_eq!(
            messages.len(),
            1,
            "Queue lifecycle must reserve one message"
        );
        let reserved = &messages[0];
        assert_eq!(
            reserved.id, id,
            "Queue lifecycle reserved the wrong message"
        );
        self.complete_frame
            .set(reserved.id.as_u64(), reserved.token);
        assert!(matches!(
            dispatch_encoded(
                &mut self.fixture.actor,
                SESSION_ID,
                self.complete_frame.as_slice(),
            ),
            QueueResponse::Acked
        ));
    }
}

pub(crate) fn measure_encoded_lifecycle(
    ctx: &mut StressContext,
    dimensions: Tier4Dimensions<'static>,
    write_mode: QueueWriteMode,
    measurement: &'static str,
) {
    tag_dimensions(ctx, &dimensions);
    let mut state = EncodedLifecycleState::new(
        dimensions.storage_profile,
        write_mode,
        dimensions.payload_size,
    );
    state.complete();

    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        state.complete();
        latencies.push(started.elapsed());
    });
}

fn dispatch_encoded(actor: &mut QueueActor, session_id: u64, frame: &[u8]) -> QueueResponse {
    let mut parser = TlvFrameParser::new(frame);
    let (message_type, payload) = parser.next_field_ref().expect("one Queue TLV field");
    assert!(
        parser.next_field_ref().is_none(),
        "expected one Queue field"
    );
    match parse_queue_request(message_type, RouteFamily::new(1), payload)
        .expect("decode Queue request")
    {
        QueueMessage::Send {
            body,
            delay_seconds,
            ..
        } => actor.handle_send(body, delay_seconds),
        QueueMessage::Receive {
            inflight_seconds,
            batch_size,
            ..
        } => actor.handle_receive_for_session(session_id, inflight_seconds, batch_size),
        QueueMessage::Ack { id, token, .. } => actor.handle_ack_for_session(session_id, id, token),
        QueueMessage::Extend { .. } | QueueMessage::InflightExpired { .. } => {
            panic!("unexpected Queue lifecycle operation")
        }
    }
}

pub(crate) struct MutableQueueCompleteFrame {
    frame: Vec<u8>,
    message_id_offset: usize,
}

impl MutableQueueCompleteFrame {
    fn new(route: &str) -> Self {
        let frame = build_queue_complete(route, 0, 0);
        let payload_offset = tlv_payload_offset(&frame);
        let route_len = usize::try_from(u32::from_be_bytes(
            frame[payload_offset..payload_offset + 4]
                .try_into()
                .expect("Queue complete route length"),
        ))
        .expect("Queue route length should fit usize");
        let message_id_offset = payload_offset + 4 + route_len;
        assert!(
            frame.len() >= message_id_offset + 16,
            "Queue complete frame must contain message id and token"
        );
        Self {
            frame,
            message_id_offset,
        }
    }

    fn set(&mut self, message_id: u64, token: u64) {
        self.frame[self.message_id_offset..self.message_id_offset + 8]
            .copy_from_slice(&message_id.to_be_bytes());
        self.frame[self.message_id_offset + 8..self.message_id_offset + 16]
            .copy_from_slice(&token.to_be_bytes());
    }

    fn as_slice(&self) -> &[u8] {
        &self.frame
    }
}

fn tlv_payload_offset(frame: &[u8]) -> usize {
    match frame.first().copied() {
        Some(0xFF) => 5,
        Some(_) => 3,
        None => panic!("Queue frame must not be empty"),
    }
}

pub(crate) enum QueueBenchClient {
    Tcp(TestClient),
    WebSocket(Box<TestWebSocketClient>),
}

impl QueueBenchClient {
    async fn connect(server: &TestServer, transport: TransportKind) -> Self {
        match transport {
            TransportKind::Tcp => Self::Tcp(
                TestClient::new(server.tcp_addr)
                    .await
                    .expect("connect Queue TCP client"),
            ),
            TransportKind::WebSocket => Self::WebSocket(Box::new(
                TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
                    .await
                    .expect("connect Queue WebSocket client"),
            )),
        }
    }

    async fn request(&mut self, frame: &[u8]) -> Vec<u8> {
        match self {
            Self::Tcp(client) => client.request(frame, RESPONSE_TIMEOUT_MS).await,
            Self::WebSocket(client) => client.request(frame, RESPONSE_TIMEOUT_MS).await,
        }
        .expect("Queue request response")
    }

    async fn close(self) {
        match self {
            Self::Tcp(client) => client.close().await.expect("close Queue TCP client"),
            Self::WebSocket(mut client) => {
                client.close().await.expect("close Queue WebSocket client")
            }
        }
    }
}

struct WireLifecycleState {
    enqueue_frame: Vec<u8>,
    reserve_frame: Vec<u8>,
    complete_frame: MutableQueueCompleteFrame,
}

impl WireLifecycleState {
    fn new(route: &str, payload_size: usize) -> Self {
        Self {
            enqueue_frame: build_queue_enqueue(route, &vec![0x53; payload_size]),
            reserve_frame: build_queue_dequeue(route),
            complete_frame: MutableQueueCompleteFrame::new(route),
        }
    }

    async fn complete(&mut self, client: &mut QueueBenchClient, require_own_message: bool) {
        let enqueue_response = client.request(&self.enqueue_frame).await;
        let message_id = parse_enqueue_response(&enqueue_response);
        let reserve_response = client.request(&self.reserve_frame).await;
        let (reserved_id, token) = parse_reserve_response(&reserve_response);
        if require_own_message {
            assert_eq!(reserved_id, message_id, "Queue reserved the wrong message");
        }
        self.complete_frame.set(reserved_id, token);
        let complete_response = client.request(self.complete_frame.as_slice()).await;
        let payload = assert_success_response(&complete_response, 204, 1);
        assert_eq!(payload, [0], "Queue complete response payload");
    }
}

pub(crate) fn measure_transport_lifecycle(
    ctx: &mut StressContext,
    dimensions: Tier4Dimensions<'static>,
    transport: TransportKind,
    measurement: &'static str,
) {
    tag_dimensions(ctx, &dimensions);
    with_transport_clients(
        dimensions.storage_profile,
        transport,
        1,
        |runtime, clients| {
            let mut state = WireLifecycleState::new(CANONICAL_ROUTE, dimensions.payload_size);
            runtime.block_on(state.complete(&mut clients[0], true));
            measure_operations(ctx, measurement, 1, |latencies| {
                let started = Instant::now();
                runtime.block_on(state.complete(&mut clients[0], true));
                latencies.push(started.elapsed());
            });
        },
    );
}

pub(crate) fn measure_concurrent_lifecycles(
    ctx: &mut StressContext,
    dimensions: Tier4Dimensions<'static>,
    transport: TransportKind,
    measurement: &'static str,
) {
    tag_dimensions(ctx, &dimensions);
    with_transport_clients(
        dimensions.storage_profile,
        transport,
        dimensions.client_count,
        |runtime, clients| {
            let mut states = (0..clients.len())
                .map(|_| WireLifecycleState::new(CANONICAL_ROUTE, dimensions.payload_size))
                .collect::<Vec<_>>();
            runtime.block_on(async {
                join_all(
                    clients
                        .iter_mut()
                        .zip(states.iter_mut())
                        .map(|(client, state)| state.complete(client, false)),
                )
                .await;
            });

            let logical_operations =
                u64::try_from(clients.len()).expect("Queue client count should fit u64");
            measure_operations(ctx, measurement, logical_operations, |latencies| {
                let observed = runtime.block_on(async {
                    join_all(clients.iter_mut().zip(states.iter_mut()).map(
                        |(client, state)| async move {
                            let started = Instant::now();
                            state.complete(client, false).await;
                            started.elapsed()
                        },
                    ))
                    .await
                });
                latencies.extend(observed);
            });
        },
    );
}

fn with_transport_clients<R>(
    storage: StorageProfile,
    transport: TransportKind,
    client_count: usize,
    run: impl FnOnce(&tokio::runtime::Runtime, &mut [QueueBenchClient]) -> R,
) -> R {
    let runtime = shared_bench_runtime();
    let temp_dir = (storage == StorageProfile::LocalDisk)
        .then(|| tempfile::tempdir().expect("create Queue benchmark directory"));
    let server = match &temp_dir {
        Some(dir) => runtime
            .block_on(TestServer::start_with_local_storage(
                dir.path().to_string_lossy().into_owned(),
            ))
            .expect("start local-disk Queue benchmark server"),
        None => runtime
            .block_on(TestServer::start_with_write_heavy_memory())
            .expect("start write-heavy memory Queue benchmark server"),
    };
    let mut clients = runtime.block_on(async {
        join_all((0..client_count).map(|_| QueueBenchClient::connect(&server, transport))).await
    });
    let result = run(runtime, &mut clients);
    runtime.block_on(async {
        for client in clients {
            client.close().await;
        }
    });
    runtime
        .block_on(server.shutdown())
        .expect("shutdown Queue benchmark server");
    drop(temp_dir);
    result
}

fn parse_enqueue_response(frame: &[u8]) -> u64 {
    let payload = assert_success_response(frame, 200, 9);
    assert_eq!(payload.len(), 9, "Queue enqueue response length");
    u64::from_be_bytes(payload[1..9].try_into().expect("Queue message id"))
}

fn parse_reserve_response(frame: &[u8]) -> (u64, u64) {
    let payload = assert_success_response(frame, 202, 25);
    let count = u32::from_be_bytes(payload[1..5].try_into().expect("Queue reserve count"));
    assert_eq!(count, 1, "Queue lifecycle must reserve one message");
    let message_id = u64::from_be_bytes(payload[5..13].try_into().expect("Queue message id"));
    let token = u64::from_be_bytes(payload[13..21].try_into().expect("Queue inflight token"));
    let body_len = usize::try_from(u32::from_be_bytes(
        payload[21..25].try_into().expect("Queue body length"),
    ))
    .expect("Queue body length should fit usize");
    assert_eq!(payload.len(), 25 + body_len, "Queue reserve body length");
    (message_id, token)
}

fn assert_success_response(frame: &[u8], expected_message_type: u16, minimum_len: usize) -> &[u8] {
    let mut parser = TlvFrameParser::new(frame);
    let (message_type, payload) = parser.next_field_ref().expect("Queue response field");
    assert!(
        parser.next_field_ref().is_none(),
        "expected one Queue response"
    );
    assert_eq!(message_type, expected_message_type, "Queue response type");
    assert!(payload.len() >= minimum_len, "Queue response is too short");
    if payload.first().copied() != Some(0) {
        let message = decode_error_body(payload)
            .map(|(_, message)| message)
            .unwrap_or_else(|_| "malformed Queue error response".to_string());
        panic!("Queue request failed: {message}");
    }
    payload
}
