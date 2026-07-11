#![allow(dead_code)] // Lease targets select focused lifecycle and contention helpers.

use crate::tier4_support::{
    measure_operations, tag_dimensions, LayerKind, StorageProfile, Tier4Dimensions, TransportKind,
};
use bytes::Bytes;
use cntryl_stress::StressContext;
use fitz::benchkit::{
    build_lease_acquire_immediate, build_lease_release, create_bench_lease_sink,
    extract_single_tlv_field, register_session_queue_sink, route_frame_to_address,
    shared_bench_runtime, DirectLeaseAcquireRelease, FrameQueueSink,
};
use fitz::domains::lease::sink::LeaseDomainSink;
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use futures_util::future::join_all;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) const CLIENT_COUNT: usize = 4;
pub(crate) const LEASE_ROUTE: &str = "lease://tier4/locks/primary";
const OWNER_SESSION_ID: u64 = 1;
const RESPONSE_TIMEOUT_MS: u64 = 5_000;

pub(crate) fn dimensions(
    layer: LayerKind,
    scenario: &'static str,
    client_count: usize,
    completed_unit: &'static str,
    gate_class: &'static str,
) -> Tier4Dimensions<'static> {
    Tier4Dimensions {
        domain: "lease",
        scenario,
        storage_profile: StorageProfile::Memory,
        layer,
        write_mode: "not_applicable",
        payload_size: 0,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count: 1,
        filter_selectivity: "not_applicable",
        client_count,
        workload_mix: if client_count == 1 {
            "acquire_release"
        } else {
            "ownership_contention"
        },
        completed_unit,
        gate_class,
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum AcquireDecision {
    Acquired(u64),
    HeldByOther,
}

fn parse_acquire_body(body: &[u8]) -> AcquireDecision {
    assert!(!body.is_empty(), "Lease acquire response must not be empty");
    if body[0] == 0 {
        assert_eq!(body.len(), 10, "Lease acquire success body length");
        assert_eq!(body[1], 0, "Lease acquire must return Acquired");
        return AcquireDecision::Acquired(u64::from_be_bytes(
            body[2..10].try_into().expect("Lease fencing token bytes"),
        ));
    }

    let (code, message) =
        fitz::protocol::error_codes::decode_error_body(body).expect("decode Lease acquire error");
    assert_eq!(
        code,
        fitz::protocol::error_codes::lease::ERR_LEASE_HELD,
        "unexpected Lease acquire error: {message}"
    );
    AcquireDecision::HeldByOther
}

fn assert_release_body(body: &[u8]) {
    assert_eq!(body, [0], "Lease release must return exact success body");
}

fn network_body(response: &[u8], expected_type: u16) -> Bytes {
    let mut parser = TlvFrameParser::new(response);
    let (message_type, body) = parser.next_field_ref().expect("Lease response field");
    assert_eq!(
        message_type, expected_type,
        "unexpected Lease response type"
    );
    assert!(
        parser.next_field_ref().is_none(),
        "expected one Lease response"
    );
    Bytes::copy_from_slice(body)
}

struct MutableReleaseFrame {
    frame: Vec<u8>,
    token_offset: usize,
}

impl MutableReleaseFrame {
    fn new(route: &str, owner: &str) -> Self {
        let frame = build_lease_release(route, owner, 0);
        let payload_offset = if frame.first().copied() == Some(0xFF) {
            5
        } else {
            3
        };
        let token_offset = payload_offset + 4 + route.len() + 4 + owner.len();
        assert!(
            frame.len() >= token_offset + 8,
            "Lease release token offset"
        );
        Self {
            frame,
            token_offset,
        }
    }

    fn set_token(&mut self, token: u64) {
        self.frame[self.token_offset..self.token_offset + 8].copy_from_slice(&token.to_be_bytes());
    }

    fn as_slice(&self) -> &[u8] {
        &self.frame
    }
}

pub(crate) enum LeaseClient {
    Tcp(TestClient),
    WebSocket(Box<TestWebSocketClient>),
}

impl LeaseClient {
    async fn connect(server: &TestServer, transport: TransportKind) -> Self {
        match transport {
            TransportKind::Tcp => Self::Tcp(
                TestClient::new(server.tcp_addr)
                    .await
                    .expect("connect Lease TCP client"),
            ),
            TransportKind::WebSocket => Self::WebSocket(Box::new(
                TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
                    .await
                    .expect("connect Lease WebSocket client"),
            )),
        }
    }

    async fn request(&mut self, frame: &[u8]) -> Vec<u8> {
        match self {
            Self::Tcp(client) => client.request(frame, RESPONSE_TIMEOUT_MS).await,
            Self::WebSocket(client) => client.request(frame, RESPONSE_TIMEOUT_MS).await,
        }
        .expect("Lease response")
    }

    async fn close(self) {
        match self {
            Self::Tcp(client) => client.close().await.expect("close Lease TCP client"),
            Self::WebSocket(mut client) => {
                client.close().await.expect("close Lease WebSocket client");
            }
        }
    }
}

struct WireLifecycle {
    acquire: Vec<u8>,
    release: MutableReleaseFrame,
}

impl WireLifecycle {
    fn new(route: &str, owner: &str) -> Self {
        Self {
            acquire: build_lease_acquire_immediate(route, owner, 30),
            release: MutableReleaseFrame::new(route, owner),
        }
    }

    async fn acquire(&self, client: &mut LeaseClient) -> AcquireDecision {
        let response = client.request(&self.acquire).await;
        parse_acquire_body(&network_body(&response, 400))
    }

    async fn release(&mut self, client: &mut LeaseClient, token: u64) {
        self.release.set_token(token);
        let response = client.request(self.release.as_slice()).await;
        assert_release_body(&network_body(&response, 402));
    }

    async fn complete(&mut self, client: &mut LeaseClient) {
        let AcquireDecision::Acquired(token) = self.acquire(client).await else {
            panic!("uncontended Lease acquire must succeed");
        };
        self.release(client, token).await;
    }
}

pub(crate) fn measure_direct(ctx: &mut StressContext, measurement: &'static str) {
    tag_dimensions(
        ctx,
        &dimensions(
            LayerKind::Direct,
            "acquire_release_lifecycle",
            1,
            "acquire_release_lifecycle",
            "characterization",
        ),
    );
    let driver = DirectLeaseAcquireRelease::new(
        RouteFamily::new(1),
        LEASE_ROUTE,
        OWNER_SESSION_ID,
        "owner-direct",
        30,
    );
    driver.complete_roundtrip();
    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        driver.complete_roundtrip();
        latencies.push(started.elapsed());
    });
}

struct EncodedFixture {
    router: Arc<Router>,
    sink: Arc<LeaseDomainSink>,
    source: RouteAddress,
    destination: RouteAddress,
    inbox: Arc<FrameQueueSink>,
}

impl EncodedFixture {
    fn new() -> Self {
        let family = RouteFamily::new(1);
        let router = Arc::new(Router::new());
        let sink = create_bench_lease_sink(router.clone());
        let mailbox: Arc<dyn MailboxSink> = sink.clone();
        router.register_domain_pattern("lease", mailbox);
        let (source, inbox) = register_session_queue_sink(&router, family, OWNER_SESSION_ID);
        Self {
            router,
            sink,
            source,
            destination: RouteAddress::new(family, Route::new(LEASE_ROUTE)),
            inbox,
        }
    }

    fn request(&self, frame: &[u8], expected_type: u16) -> Bytes {
        let (message_type, payload) = extract_single_tlv_field(frame);
        assert_eq!(message_type, expected_type, "encoded Lease request type");
        route_frame_to_address(
            &self.router,
            &self.source,
            &self.destination,
            OWNER_SESSION_ID,
            ChannelId::Lease,
            message_type,
            payload,
        )
        .expect("route encoded Lease request");
        let mut responses = self.inbox.drain_after_count(1, Duration::from_secs(5));
        assert_eq!(responses.len(), 1, "one encoded Lease response");
        let response = responses.pop().expect("encoded Lease response");
        assert_eq!(response.msg_type.as_u16(), expected_type);
        response.payload
    }

    fn shutdown(self) {
        self.sink.stop();
        self.router.clear();
    }
}

struct EncodedLifecycle {
    acquire: Vec<u8>,
    release: MutableReleaseFrame,
}

impl EncodedLifecycle {
    fn new() -> Self {
        Self {
            acquire: build_lease_acquire_immediate(LEASE_ROUTE, "owner-encoded", 30),
            release: MutableReleaseFrame::new(LEASE_ROUTE, "owner-encoded"),
        }
    }

    fn complete(&mut self, fixture: &EncodedFixture) {
        let AcquireDecision::Acquired(token) =
            parse_acquire_body(&fixture.request(&self.acquire, 400))
        else {
            panic!("encoded Lease acquire must succeed");
        };
        self.release.set_token(token);
        assert_release_body(&fixture.request(self.release.as_slice(), 402));
    }
}

pub(crate) fn measure_encoded(ctx: &mut StressContext, measurement: &'static str) {
    tag_dimensions(
        ctx,
        &dimensions(
            LayerKind::Encoded,
            "acquire_release_lifecycle",
            1,
            "acquire_release_lifecycle",
            "characterization",
        ),
    );
    let fixture = EncodedFixture::new();
    let mut lifecycle = EncodedLifecycle::new();
    lifecycle.complete(&fixture);
    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        lifecycle.complete(&fixture);
        latencies.push(started.elapsed());
    });
    fixture.shutdown();
}

pub(crate) fn measure_transport(
    ctx: &mut StressContext,
    transport: TransportKind,
    measurement: &'static str,
) {
    tag_dimensions(
        ctx,
        &dimensions(
            LayerKind::from(transport),
            "acquire_release_lifecycle",
            1,
            "acquire_release_lifecycle",
            "regression_gate",
        ),
    );
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start Lease server");
    let mut client = runtime.block_on(LeaseClient::connect(&server, transport));
    let mut lifecycle = WireLifecycle::new(LEASE_ROUTE, "owner-transport");
    runtime.block_on(lifecycle.complete(&mut client));
    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        runtime.block_on(lifecycle.complete(&mut client));
        latencies.push(started.elapsed());
    });
    runtime.block_on(client.close());
    runtime
        .block_on(server.shutdown())
        .expect("shutdown Lease server");
}

async fn contention_wave(clients: &mut [LeaseClient], lifecycles: &mut [WireLifecycle]) {
    let decisions = join_all(
        clients
            .iter_mut()
            .zip(lifecycles.iter())
            .map(|(client, lifecycle)| lifecycle.acquire(client)),
    )
    .await;
    let winners = decisions
        .iter()
        .enumerate()
        .filter_map(|(index, decision)| match decision {
            AcquireDecision::Acquired(token) => Some((index, *token)),
            AcquireDecision::HeldByOther => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(winners.len(), 1, "exactly one Lease contention winner");
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == AcquireDecision::HeldByOther)
            .count(),
        clients.len() - 1,
        "all losing Lease acquires must be HeldByOther"
    );
    let (winner, token) = winners[0];
    lifecycles[winner]
        .release(&mut clients[winner], token)
        .await;
}

pub(crate) fn measure_contention(
    ctx: &mut StressContext,
    transport: TransportKind,
    measurement: &'static str,
) {
    let layer = match transport {
        TransportKind::Tcp => LayerKind::TcpMultiClient,
        TransportKind::WebSocket => LayerKind::WebSocketMultiClient,
    };
    tag_dimensions(
        ctx,
        &dimensions(
            layer,
            "same_route_ownership_contention",
            CLIENT_COUNT,
            "contention_wave",
            "characterization",
        ),
    );
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start Lease contention server");
    let mut clients = runtime.block_on(async {
        join_all((0..CLIENT_COUNT).map(|_| LeaseClient::connect(&server, transport))).await
    });
    let mut lifecycles = (0..CLIENT_COUNT)
        .map(|index| WireLifecycle::new(LEASE_ROUTE, &format!("owner-{index}")))
        .collect::<Vec<_>>();
    runtime.block_on(contention_wave(&mut clients, &mut lifecycles));
    measure_operations(ctx, measurement, 1, |latencies| {
        let started = Instant::now();
        runtime.block_on(contention_wave(&mut clients, &mut lifecycles));
        latencies.push(started.elapsed());
    });
    runtime.block_on(async {
        join_all(clients.into_iter().map(LeaseClient::close)).await;
    });
    runtime
        .block_on(server.shutdown())
        .expect("shutdown Lease contention server");
}
