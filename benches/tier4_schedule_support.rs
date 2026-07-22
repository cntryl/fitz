#![allow(dead_code)] // Standalone Schedule targets use focused subsets of this support API.

use crate::tier4_support::{
    measure_operations, tag_dimensions, StorageProfile, Tier4Dimensions, TransportKind,
};
use bytes::Bytes;
use cntryl_stress::StressContext;
use fitz::benchkit::{
    build_schedule_create, build_schedule_create_batch, create_local_bench_store,
    create_write_heavy_bench_store, shared_bench_runtime,
};
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::protocol::error_codes::decode_error_body;
use fitz::protocol::frame::ChannelId;
use fitz::protocol::frame_context::FrameContext;
use fitz::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use fitz::protocol::schedule_codec::{encode_response, parse_request as parse_schedule_request};
use fitz::protocol::tlv::MessageType;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::session::SessionId;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder, TlvFrameParser};
use futures_util::future::join_all;
use std::time::Instant;

pub(crate) const CANONICAL_PAYLOAD_SIZE: usize = 1_024;
pub(crate) const CANONICAL_ROUTE: &str = "schedule://tier4/jobs/main/run";
pub(crate) const CREATE_BATCH_WIDTH: usize = 32;
const RESPONSE_TIMEOUT_MS: u64 = 5_000;
const SESSION_ID: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScheduleWriteMode {
    Sync,
    Buffered,
}

impl ScheduleWriteMode {
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

#[allow(clippy::too_many_arguments)] // Shared row declarations stay readable at call sites.
pub(crate) fn dimensions(
    scenario: &'static str,
    storage_profile: StorageProfile,
    layer: crate::tier4_support::LayerKind,
    write_mode: &'static str,
    payload_size: usize,
    route_count: usize,
    client_count: usize,
    workload_mix: &'static str,
    completed_unit: &'static str,
    gate_class: &'static str,
) -> Tier4Dimensions<'static> {
    Tier4Dimensions {
        domain: "schedule",
        scenario,
        storage_profile,
        layer,
        write_mode,
        payload_size,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count,
        filter_selectivity: "not_applicable",
        client_count,
        workload_mix,
        completed_unit,
        gate_class,
    }
}

struct ScheduleActorFixture {
    actor: ScheduleActor,
    _temp_dir: Option<tempfile::TempDir>,
}

impl ScheduleActorFixture {
    fn new(storage: StorageProfile, write_mode: ScheduleWriteMode) -> Self {
        let (store, temp_dir) = match storage {
            StorageProfile::Memory => (create_write_heavy_bench_store(), None),
            StorageProfile::LocalDisk => {
                let (store, temp_dir) = create_local_bench_store();
                (store, Some(temp_dir))
            }
        };
        Self {
            actor: ScheduleActor::new(RouteFamily::new(1), store, write_mode.options()),
            _temp_dir: temp_dir,
        }
    }
}

struct DirectLifecycleState {
    fixture: ScheduleActorFixture,
    payloads: [Bytes; 2],
    next_payload: usize,
}

impl DirectLifecycleState {
    fn new(storage: StorageProfile, write_mode: ScheduleWriteMode, payload_size: usize) -> Self {
        Self {
            fixture: ScheduleActorFixture::new(storage, write_mode),
            payloads: [
                Bytes::from(vec![0x61; payload_size]),
                Bytes::from(vec![0x62; payload_size]),
            ],
            next_payload: 0,
        }
    }

    fn complete(&mut self) {
        let payload = self.payloads[self.next_payload].clone();
        self.next_payload ^= 1;
        assert!(matches!(
            self.fixture.actor.handle(ScheduleMessage::Create {
                route: CANONICAL_ROUTE.to_string(),
                cron: "* * * * *".to_string(),
                delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
                payload: payload.clone(),
            }),
            ScheduleResponse::Ok
        ));
        self.fixture.actor.bench_prepare_scan(1);
        let claims = self.fixture.actor.bench_claim_due_fires();
        assert_eq!(claims.len(), 1, "Schedule lifecycle must claim one fire");
        assert_eq!(claims[0].route, CANONICAL_ROUTE);
        assert_eq!(claims[0].payload, payload);
        let delivered = [(claims[0].fire_ms, claims[0].route.clone())];
        let (acked, _) = self
            .fixture
            .actor
            .bench_ack_pending_fire_claims(&delivered)
            .expect("ack Schedule pending fire");
        assert_eq!(acked, 1, "Schedule lifecycle must ack one fire");
        assert!(
            self.fixture
                .actor
                .bench_pending_claimed_occurrences_for_publish()
                .is_empty(),
            "Schedule lifecycle must not leave a pending claim"
        );
    }
}

pub(crate) fn measure_direct_lifecycle(
    ctx: &mut StressContext,
    dimensions: Tier4Dimensions<'static>,
    write_mode: ScheduleWriteMode,
    measurement: &'static str,
) {
    tag_dimensions(ctx, &dimensions);
    let mut state = DirectLifecycleState::new(
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

struct EncodedLifecycleState {
    fixture: ScheduleActorFixture,
    frames: [Vec<u8>; 2],
    next_frame: usize,
}

impl EncodedLifecycleState {
    fn new(storage: StorageProfile, write_mode: ScheduleWriteMode, payload_size: usize) -> Self {
        Self {
            fixture: ScheduleActorFixture::new(storage, write_mode),
            frames: [
                build_schedule_create(CANONICAL_ROUTE, "* * * * *", &vec![0x63; payload_size]),
                build_schedule_create(CANONICAL_ROUTE, "* * * * *", &vec![0x64; payload_size]),
            ],
            next_frame: 0,
        }
    }

    fn complete(&mut self) {
        let frame = &self.frames[self.next_frame];
        self.next_frame ^= 1;
        let message = decode_schedule_request(frame);
        let ScheduleMessage::Create { payload, .. } = &message else {
            panic!("expected Schedule create message");
        };
        let expected_payload = payload.clone();
        let response = self.fixture.actor.handle(message);
        assert!(matches!(response, ScheduleResponse::Ok));
        assert_eq!(
            encode_response(&response),
            vec![0],
            "encoded Schedule response"
        );
        self.fixture.actor.bench_prepare_scan(1);
        let claims = self.fixture.actor.bench_claim_due_fires();
        assert_eq!(claims.len(), 1, "Schedule lifecycle must claim one fire");
        assert_eq!(claims[0].route, CANONICAL_ROUTE);
        assert_eq!(claims[0].payload, expected_payload);
        let delivered = [(claims[0].fire_ms, claims[0].route.clone())];
        let (acked, _) = self
            .fixture
            .actor
            .bench_ack_pending_fire_claims(&delivered)
            .expect("ack encoded Schedule pending fire");
        assert_eq!(acked, 1, "Schedule lifecycle must ack one fire");
    }
}

pub(crate) fn measure_encoded_lifecycle(
    ctx: &mut StressContext,
    dimensions: Tier4Dimensions<'static>,
    write_mode: ScheduleWriteMode,
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

fn decode_schedule_request(frame: &[u8]) -> ScheduleMessage {
    let mut parser = TlvFrameParser::new(frame);
    let (message_type, payload) = parser.next_field_ref().expect("one Schedule field");
    assert!(
        parser.next_field_ref().is_none(),
        "expected one Schedule field"
    );
    let family = RouteFamily::new(1);
    let payload = Bytes::copy_from_slice(payload);
    let frame_context = FrameContext::new(
        SESSION_ID,
        ChannelId::Pub,
        MessageType::new(message_type),
        payload.clone(),
        family,
    );
    parse_schedule_request(
        &frame_context,
        &payload,
        family,
        SessionId(SESSION_ID),
        RouteAddress::new(family, Route::new("session://tier4/schedule")),
    )
    .expect("decode Schedule request")
}

pub(crate) enum ScheduleBenchClient {
    Tcp(TestClient),
    WebSocket(Box<TestWebSocketClient>),
}

impl ScheduleBenchClient {
    async fn connect(server: &TestServer, transport: TransportKind) -> Self {
        match transport {
            TransportKind::Tcp => Self::Tcp(
                TestClient::new(server.tcp_addr)
                    .await
                    .expect("connect Schedule TCP client"),
            ),
            TransportKind::WebSocket => Self::WebSocket(Box::new(
                TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
                    .await
                    .expect("connect Schedule WebSocket client"),
            )),
        }
    }

    async fn request(&mut self, frame: &[u8]) -> Vec<u8> {
        match self {
            Self::Tcp(client) => client.request(frame, RESPONSE_TIMEOUT_MS).await,
            Self::WebSocket(client) => client.request(frame, RESPONSE_TIMEOUT_MS).await,
        }
        .expect("Schedule request response")
    }

    async fn recv_frame(&mut self) -> Vec<u8> {
        match self {
            Self::Tcp(client) => client.recv_frame(RESPONSE_TIMEOUT_MS).await,
            Self::WebSocket(client) => client.recv_frame(RESPONSE_TIMEOUT_MS).await,
        }
        .expect("Schedule delivery")
    }

    async fn close(self) {
        match self {
            Self::Tcp(client) => client.close().await.expect("close Schedule TCP client"),
            Self::WebSocket(mut client) => client
                .close()
                .await
                .expect("close Schedule WebSocket client"),
        }
    }
}

struct WireLifecycleState {
    frames: [Vec<u8>; 2],
    payloads: [Vec<u8>; 2],
    next_frame: usize,
}

impl WireLifecycleState {
    fn new(payload_size: usize) -> Self {
        let payloads = [vec![0x65; payload_size], vec![0x66; payload_size]];
        let frames = [
            build_schedule_create(CANONICAL_ROUTE, "* * * * *", &payloads[0]),
            build_schedule_create(CANONICAL_ROUTE, "* * * * *", &payloads[1]),
        ];
        Self {
            frames,
            payloads,
            next_frame: 0,
        }
    }

    async fn complete(
        &mut self,
        writer: &mut ScheduleBenchClient,
        subscriber: &mut ScheduleBenchClient,
        server: &TestServer,
    ) {
        let frame_index = self.next_frame;
        self.next_frame ^= 1;
        let response = writer.request(&self.frames[frame_index]).await;
        assert_schedule_success(&response, 700);
        server
            .force_schedule_scan_for_tests(1)
            .expect("force Schedule due scan");
        let delivery = subscriber.recv_frame().await;
        assert_schedule_delivery(&delivery, &self.payloads[frame_index]);
        assert_eq!(
            server.runtime.schedule_pending_fire_claims(),
            0,
            "Schedule lifecycle must durably acknowledge its fire"
        );
        assert_eq!(
            server.runtime.schedule_pending_ack_retries(),
            0,
            "Schedule lifecycle must not leave an ack retry"
        );
        assert_eq!(
            server.runtime.schedule_ack_failures(),
            0,
            "Schedule lifecycle must not hide an ack failure"
        );
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
        2,
        |runtime, server, clients| {
            let subscribe = build_schedule_subscribe(CANONICAL_ROUTE);
            let response = runtime.block_on(clients[1].request(&subscribe));
            assert_schedule_subscribe(&response);
            let mut state = WireLifecycleState::new(dimensions.payload_size);
            let (writer_slice, subscriber_slice) = clients.split_at_mut(1);
            let writer = &mut writer_slice[0];
            let subscriber = &mut subscriber_slice[0];
            runtime.block_on(state.complete(writer, subscriber, server));
            measure_operations(ctx, measurement, 1, |latencies| {
                let started = Instant::now();
                runtime.block_on(state.complete(writer, subscriber, server));
                latencies.push(started.elapsed());
            });
        },
    );
}

struct CreateState {
    frames: [Vec<u8>; 2],
    next_frame: usize,
}

impl CreateState {
    fn new(route: &str, payload_size: usize) -> Self {
        Self {
            frames: [
                build_schedule_create(route, "0 * * * *", &vec![0x67; payload_size]),
                build_schedule_create(route, "0 * * * *", &vec![0x68; payload_size]),
            ],
            next_frame: 0,
        }
    }

    async fn create(&mut self, client: &mut ScheduleBenchClient) {
        let response = client.request(&self.frames[self.next_frame]).await;
        self.next_frame ^= 1;
        assert_schedule_success(&response, 700);
    }
}

pub(crate) fn measure_concurrent_creates(
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
        |runtime, _server, clients| {
            let mut states = (0..clients.len())
                .map(|index| {
                    CreateState::new(
                        &format!("schedule://tier4/concurrent/resource-{index}/run"),
                        dimensions.payload_size,
                    )
                })
                .collect::<Vec<_>>();
            runtime.block_on(async {
                join_all(
                    clients
                        .iter_mut()
                        .zip(states.iter_mut())
                        .map(|(client, state)| state.create(client)),
                )
                .await;
            });
            let logical_operations =
                u64::try_from(clients.len()).expect("Schedule client count should fit u64");
            measure_operations(ctx, measurement, logical_operations, |latencies| {
                let observed = runtime.block_on(async {
                    join_all(clients.iter_mut().zip(states.iter_mut()).map(
                        |(client, state)| async move {
                            let started = Instant::now();
                            state.create(client).await;
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

pub(crate) fn measure_batch_create(
    ctx: &mut StressContext,
    dimensions: Tier4Dimensions<'static>,
    transport: TransportKind,
    measurement: &'static str,
) {
    tag_dimensions(ctx, &dimensions);
    ctx.parameter("batch_width", CREATE_BATCH_WIDTH);
    with_transport_clients(
        dimensions.storage_profile,
        transport,
        1,
        |runtime, _server, clients| {
            let routes = (0..CREATE_BATCH_WIDTH)
                .map(|index| format!("schedule://tier4/batch/resource-{index}/run"))
                .collect::<Vec<_>>();
            let payloads = [
                vec![0x69; dimensions.payload_size],
                vec![0x6A; dimensions.payload_size],
            ];
            let frames = payloads.map(|payload| {
                let entries = routes
                    .iter()
                    .map(|route| (route.as_str(), "0 * * * *", payload.as_slice()))
                    .collect::<Vec<_>>();
                build_schedule_create_batch(&entries)
            });
            let mut next_frame = 0usize;
            let response = runtime.block_on(clients[0].request(&frames[next_frame]));
            assert_schedule_success(&response, 706);
            next_frame ^= 1;
            measure_operations(
                ctx,
                measurement,
                u64::try_from(CREATE_BATCH_WIDTH).expect("batch width should fit u64"),
                |latencies| {
                    let started = Instant::now();
                    let response = runtime.block_on(clients[0].request(&frames[next_frame]));
                    let elapsed = started.elapsed();
                    next_frame ^= 1;
                    assert_schedule_success(&response, 706);
                    latencies.extend(std::iter::repeat_n(elapsed, CREATE_BATCH_WIDTH));
                },
            );
        },
    );
}

fn with_transport_clients<R>(
    storage: StorageProfile,
    transport: TransportKind,
    client_count: usize,
    run: impl FnOnce(&tokio::runtime::Runtime, &TestServer, &mut [ScheduleBenchClient]) -> R,
) -> R {
    let runtime = shared_bench_runtime();
    let temp_dir = (storage == StorageProfile::LocalDisk)
        .then(|| tempfile::tempdir().expect("create Schedule benchmark directory"));
    let server = match &temp_dir {
        Some(dir) => runtime
            .block_on(TestServer::start_with_local_storage(
                dir.path().to_string_lossy().into_owned(),
            ))
            .expect("start local-disk Schedule benchmark server"),
        None => runtime
            .block_on(TestServer::start_with_write_heavy_memory())
            .expect("start write-heavy memory Schedule benchmark server"),
    };
    let mut clients = runtime.block_on(async {
        join_all((0..client_count).map(|_| ScheduleBenchClient::connect(&server, transport))).await
    });
    let result = run(runtime, &server, &mut clients);
    runtime.block_on(async {
        for client in clients {
            client.close().await;
        }
    });
    runtime
        .block_on(server.shutdown())
        .expect("shutdown Schedule benchmark server");
    drop(temp_dir);
    result
}

fn build_schedule_subscribe(route: &str) -> Vec<u8> {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(703, &encoder.finish());
    builder.build()
}

fn assert_schedule_success(frame: &[u8], expected_message_type: u16) {
    let payload = schedule_response_payload(frame, expected_message_type);
    if payload.first().copied() != Some(0) {
        let message = decode_error_body(payload).map_or_else(
            |_| "malformed Schedule error response".to_string(),
            |(_, message)| message,
        );
        panic!("Schedule request failed: {message}");
    }
    assert_eq!(payload, [0], "Schedule success response payload");
}

fn assert_schedule_subscribe(frame: &[u8]) {
    let payload = schedule_response_payload(frame, 703);
    let mut decoder = PayloadDecoder::new(payload);
    assert_eq!(decoder.get_u8().expect("Schedule subscribe status"), 0);
    let subscription_id = decoder
        .get_optional_u64()
        .expect("Schedule subscription id")
        .expect("Schedule subscription id must be present");
    assert!(
        subscription_id > 0,
        "Schedule subscription id must be nonzero"
    );
    assert!(
        decoder.is_complete(),
        "Schedule subscribe has trailing bytes"
    );
}

fn schedule_response_payload(frame: &[u8], expected_message_type: u16) -> &[u8] {
    let mut parser = TlvFrameParser::new(frame);
    let (message_type, payload) = parser.next_field_ref().expect("Schedule response field");
    assert!(
        parser.next_field_ref().is_none(),
        "expected one Schedule response"
    );
    assert_eq!(
        message_type, expected_message_type,
        "Schedule response type"
    );
    payload
}

fn assert_schedule_delivery(frame: &[u8], expected_payload: &[u8]) {
    let mut parser = TlvFrameParser::new(frame);
    let (message_type, payload) = parser.next_field_ref().expect("Schedule delivery field");
    assert!(
        parser.next_field_ref().is_none(),
        "expected one Schedule delivery"
    );
    assert_eq!(message_type, 705, "Schedule delivery type");
    let mut decoder = PayloadDecoder::new(payload);
    let subscription_id = decoder.get_u64().expect("Schedule subscription id");
    assert!(
        subscription_id > 0,
        "Schedule subscription id must be nonzero"
    );
    let body = decoder.get_bytes().expect("Schedule delivery body");
    assert_eq!(body.as_ref(), expected_payload, "Schedule delivery body");
    assert!(
        decoder.is_complete(),
        "Schedule delivery has trailing bytes"
    );
}
