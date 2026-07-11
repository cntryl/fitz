#![allow(dead_code)] // Workload targets share this transport fixture selectively.

use crate::tier4_stream_support::{
    assert_stream_notify, measure_operations, MutableAppendFrame, MutableCommitFrame, ReadScope,
    RowDimensions, StorageProfile, TransportKind, RESPONSE_TIMEOUT_MS, STREAM_SYNC_COMMIT_MODE,
    WIRE_READ_PAGE_LIMIT,
};
use cntryl_stress::StressContext;
use fitz::benchkit::{
    build_stream_begin, build_stream_commit, build_stream_read_with_limit, build_stream_subscribe,
    parse_stream_read_record_count, parse_stream_response, parse_stream_session_id,
    shared_bench_runtime,
};
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use futures_util::future::join_all;
use std::time::Instant;

const APPENDS_PER_OPEN_SESSION: usize = 8_192;
const OPEN_APPEND_SESSION_COUNT: usize = 64;

pub(crate) enum StreamBenchClient {
    Tcp(TestClient),
    WebSocket(Box<TestWebSocketClient>),
}

impl StreamBenchClient {
    async fn connect(
        server: &TestServer,
        transport: TransportKind,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match transport {
            TransportKind::Tcp => TestClient::new(server.tcp_addr).await.map(Self::Tcp),
            TransportKind::WebSocket => {
                TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
                    .await
                    .map(|client| Self::WebSocket(Box::new(client)))
            }
        }
    }

    pub(crate) async fn request(
        &mut self,
        frame: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        match self {
            Self::Tcp(client) => client.request(frame, RESPONSE_TIMEOUT_MS).await,
            Self::WebSocket(client) => client.request(frame, RESPONSE_TIMEOUT_MS).await,
        }
    }

    pub(crate) async fn recv_frame(
        &mut self,
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        match self {
            Self::Tcp(client) => client.recv_frame(timeout_ms).await,
            Self::WebSocket(client) => client.recv_frame(timeout_ms).await,
        }
    }

    async fn close(self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Tcp(client) => client.close().await,
            Self::WebSocket(mut client) => client.close().await,
        }
    }
}

pub(crate) fn with_transport_clients<R>(
    storage: StorageProfile,
    transport: TransportKind,
    client_count: usize,
    run: impl FnOnce(&tokio::runtime::Runtime, &TestServer, &mut [StreamBenchClient]) -> R,
) -> R {
    let runtime = shared_bench_runtime();
    let temp_dir = (storage == StorageProfile::LocalDisk)
        .then(|| tempfile::tempdir().expect("create Stream benchmark directory"));
    let server = match &temp_dir {
        Some(dir) => runtime
            .block_on(TestServer::start_with_local_storage(
                dir.path().to_string_lossy().into_owned(),
            ))
            .expect("start local-disk Stream benchmark server"),
        None => runtime
            .block_on(TestServer::start_with_write_heavy_memory())
            .expect("start memory Stream benchmark server"),
    };
    let mut clients = (0..client_count)
        .map(|_| {
            runtime
                .block_on(StreamBenchClient::connect(&server, transport))
                .expect("connect Stream benchmark client")
        })
        .collect::<Vec<_>>();

    let result = run(runtime, &server, &mut clients);

    for client in clients {
        runtime
            .block_on(client.close())
            .expect("close Stream benchmark client");
    }
    runtime
        .block_on(server.shutdown())
        .expect("shutdown Stream benchmark server");
    drop(temp_dir);
    result
}

pub(crate) fn with_transport_client<R>(
    storage: StorageProfile,
    transport: TransportKind,
    run: impl FnOnce(&tokio::runtime::Runtime, &TestServer, &mut StreamBenchClient) -> R,
) -> R {
    with_transport_clients(storage, transport, 1, |runtime, server, clients| {
        run(runtime, server, &mut clients[0])
    })
}

pub(crate) async fn request_success(client: &mut StreamBenchClient, frame: &[u8]) -> Vec<u8> {
    let response = client
        .request(frame)
        .await
        .expect("Stream request response");
    assert_stream_success(&response);
    response
}

fn stream_error_message(response: &[u8]) -> String {
    let (_message_type, _status, payload) = parse_stream_response(response);
    fitz::protocol::error_codes::decode_error_body(&payload).map_or_else(
        |_| "Stream request failed".to_string(),
        |(_, message)| message,
    )
}

pub(crate) async fn request_read_count(
    client: &mut StreamBenchClient,
    frame: &[u8],
    expected_count: usize,
) {
    let response = request_success(client, frame).await;
    let count = parse_stream_read_record_count(&response).expect("Stream read record count");
    assert_eq!(count, expected_count, "unexpected Stream read count");
}

pub(crate) async fn request_read_pages(client: &mut StreamBenchClient, pages: &[(Vec<u8>, usize)]) {
    for (frame, expected_count) in pages {
        request_read_count(client, frame, *expected_count).await;
    }
}

pub(crate) fn read_pages(route: &str, total_limit: usize) -> Vec<(Vec<u8>, usize)> {
    let mut pages = Vec::new();
    let mut from_offset = 0usize;
    while from_offset < total_limit {
        let page_limit = WIRE_READ_PAGE_LIMIT.min(total_limit - from_offset);
        pages.push((
            build_stream_read_with_limit(
                route,
                u64::try_from(from_offset).expect("read offset should fit u64"),
                u64::try_from(page_limit).expect("page limit should fit u64"),
            ),
            page_limit,
        ));
        from_offset += page_limit;
    }
    pages
}

pub(crate) async fn begin_session(client: &mut StreamBenchClient, begin_frame: &[u8]) -> u64 {
    let response = request_success(client, begin_frame).await;
    let (_message_type, _status, payload) = parse_stream_response(&response);
    parse_stream_session_id(&payload).expect("Stream session id")
}

pub(crate) async fn seed_route(
    client: &mut StreamBenchClient,
    route: &str,
    event_count: usize,
    payload: &[u8],
) {
    let begin_frame = build_stream_begin(route);
    let session_id = begin_session(client, &begin_frame).await;
    for offset in 0..event_count {
        let append_frame = fitz::benchkit::build_stream_append(
            session_id,
            u64::try_from(offset).expect("offset should fit u64"),
            payload,
        );
        request_success(client, &append_frame).await;
    }
    request_success(
        client,
        &build_stream_commit(session_id, STREAM_SYNC_COMMIT_MODE),
    )
    .await;
}

pub(crate) async fn seed_scope(
    client: &mut StreamBenchClient,
    realm: &str,
    scope: ReadScope,
    history_depth: usize,
    route_count: usize,
    payload: &[u8],
) {
    match scope {
        ReadScope::None => panic!("cannot seed a write-only read scope"),
        ReadScope::Resource => {
            seed_route(
                client,
                &format!("stream://{realm}/orders/resource-0"),
                history_depth,
                payload,
            )
            .await;
        }
        ReadScope::Area => {
            seed_routes(client, realm, "orders", history_depth, route_count, payload).await;
        }
        ReadScope::Realm => {
            for route_index in 0..route_count {
                let area = format!("area-{route_index}");
                let route = format!("stream://{realm}/{area}/resource-{route_index}");
                let records = records_for_route(history_depth, route_count, route_index);
                seed_route(client, &route, records, payload).await;
            }
        }
    }
}

async fn seed_routes(
    client: &mut StreamBenchClient,
    realm: &str,
    area: &str,
    total_records: usize,
    route_count: usize,
    payload: &[u8],
) {
    for route_index in 0..route_count {
        let records = records_for_route(total_records, route_count, route_index);
        let route = format!("stream://{realm}/{area}/resource-{route_index}");
        seed_route(client, &route, records, payload).await;
    }
}

fn records_for_route(total: usize, routes: usize, index: usize) -> usize {
    let base = total / routes;
    base + usize::from(index < total % routes)
}

pub(crate) struct OpenAppendState {
    frame: MutableAppendFrame,
    next_offset: u64,
}

impl OpenAppendState {
    pub(crate) async fn prepare(
        client: &mut StreamBenchClient,
        route: &str,
        payload: &[u8],
    ) -> Self {
        let session_id = begin_session(client, &build_stream_begin(route)).await;
        Self {
            frame: MutableAppendFrame::new(session_id, 0, payload),
            next_offset: 0,
        }
    }

    pub(crate) async fn append(&mut self, client: &mut StreamBenchClient) {
        self.frame.set_expected_offset(self.next_offset);
        request_success(client, self.frame.as_slice()).await;
        self.next_offset = self.next_offset.saturating_add(1);
    }
}

pub(crate) struct WriteLifecycleState {
    begin_frame: Vec<u8>,
    append_frame: MutableAppendFrame,
    commit_frame: MutableCommitFrame,
    next_offset: u64,
}

impl WriteLifecycleState {
    pub(crate) fn new(route: &str, payload: &[u8]) -> Self {
        Self {
            begin_frame: build_stream_begin(route),
            append_frame: MutableAppendFrame::new(0, 0, payload),
            commit_frame: MutableCommitFrame::new(0, STREAM_SYNC_COMMIT_MODE),
            next_offset: 0,
        }
    }

    pub(crate) async fn complete(&mut self, client: &mut StreamBenchClient) {
        let session_id = begin_session(client, &self.begin_frame).await;
        self.append_frame.set_session_id(session_id);
        self.append_frame.set_expected_offset(self.next_offset);
        request_success(client, self.append_frame.as_slice()).await;
        self.commit_frame.set_session_id(session_id);
        let mut attempts = 0_u32;
        loop {
            let response = client
                .request(self.commit_frame.as_slice())
                .await
                .expect("Stream commit response");
            let (_message_type, status, _payload) = parse_stream_response(&response);
            if status == 0 {
                break;
            }

            let message = stream_error_message(&response);
            if !(message.contains("conflict")
                || message.contains("Concurrency")
                || message.contains("retry"))
            {
                panic!("Stream commit failed: {message}");
            }
            attempts += 1;
            assert!(attempts < 1_000, "Stream commit retry limit exceeded");
            tokio::task::yield_now().await;
        }
        self.next_offset = self.next_offset.saturating_add(1);
    }
}

pub(crate) fn measure_append_open_session(
    ctx: &mut StressContext,
    dimensions: RowDimensions<'_>,
    transport: TransportKind,
    measurement: &'static str,
) {
    crate::tier4_stream_support::tag_row(ctx, &dimensions);
    let storage = dimensions.storage_profile;
    with_transport_client(storage, transport, |runtime, _server, client| {
        let route = format!(
            "stream://tier4-append-{}/{}/resource",
            storage.label(),
            transport.label()
        );
        let payload = vec![0xA5; dimensions.payload_size];
        let mut append_states = (0..OPEN_APPEND_SESSION_COUNT)
            .map(|index| {
                runtime.block_on(OpenAppendState::prepare(
                    client,
                    &format!("{route}-{index}"),
                    &payload,
                ))
            })
            .collect::<Vec<_>>();
        let mut append_count = 0usize;
        measure_operations(ctx, measurement, 1, |latencies| {
            let session_index = append_count / APPENDS_PER_OPEN_SESSION;
            let append = append_states
                .get_mut(session_index)
                .expect("pre-opened append sessions should cover one sample");
            let started = Instant::now();
            runtime.block_on(append.append(client));
            latencies.push(started.elapsed());
            append_count += 1;
        });
    });
}

pub(crate) fn measure_exact_replay(
    ctx: &mut StressContext,
    dimensions: RowDimensions<'_>,
    transport: TransportKind,
    measurement: &'static str,
) {
    crate::tier4_stream_support::tag_row(ctx, &dimensions);
    let storage = dimensions.storage_profile;
    with_transport_client(storage, transport, |runtime, _server, client| {
        let realm = format!("tier4-read-{}-{}", storage.label(), transport.label());
        let payload = vec![0x5A; dimensions.payload_size];
        runtime.block_on(seed_scope(
            client,
            &realm,
            ReadScope::Resource,
            dimensions.history_depth,
            1,
            &payload,
        ));
        let pages = read_pages(&ReadScope::Resource.route(&realm), dimensions.read_limit);
        ctx.parameter("wire_page_limit", WIRE_READ_PAGE_LIMIT);
        ctx.parameter("wire_page_count", pages.len());
        runtime.block_on(request_read_pages(client, &pages));

        measure_operations(ctx, measurement, 1, |latencies| {
            let started = Instant::now();
            runtime.block_on(request_read_pages(client, &pages));
            latencies.push(started.elapsed());
        });
    });
}

pub(crate) fn measure_write_lifecycle(
    ctx: &mut StressContext,
    dimensions: RowDimensions<'_>,
    transport: TransportKind,
    measurement: &'static str,
) {
    crate::tier4_stream_support::tag_row(ctx, &dimensions);
    let storage = dimensions.storage_profile;
    with_transport_client(storage, transport, |runtime, _server, client| {
        let route = format!(
            "stream://tier4-write-{}/{}/resource",
            storage.label(),
            transport.label()
        );
        let payload = vec![0xC3; dimensions.payload_size];
        let mut lifecycle = WriteLifecycleState::new(&route, &payload);
        measure_operations(ctx, measurement, 1, |latencies| {
            let started = Instant::now();
            runtime.block_on(lifecycle.complete(client));
            latencies.push(started.elapsed());
        });
    });
}

pub(crate) fn measure_concurrent_exact_replay(
    ctx: &mut StressContext,
    dimensions: RowDimensions<'_>,
    transport: TransportKind,
    measurement: &'static str,
) {
    crate::tier4_stream_support::tag_row(ctx, &dimensions);
    let storage = dimensions.storage_profile;
    with_transport_clients(
        storage,
        transport,
        dimensions.client_count,
        |runtime, _server, clients| {
            let realm = format!(
                "tier4-concurrent-read-{}-{}-{}",
                storage.label(),
                transport.label(),
                dimensions.client_count
            );
            let payload = vec![0x5A; dimensions.payload_size];
            runtime.block_on(seed_scope(
                &mut clients[0],
                &realm,
                ReadScope::Resource,
                dimensions.history_depth,
                1,
                &payload,
            ));
            let pages = read_pages(&ReadScope::Resource.route(&realm), dimensions.read_limit);
            ctx.parameter("wire_page_limit", WIRE_READ_PAGE_LIMIT);
            ctx.parameter("wire_page_count", pages.len());

            measure_operations(
                ctx,
                measurement,
                u64::try_from(dimensions.client_count).expect("client count should fit u64"),
                |latencies| {
                    let per_client = runtime.block_on(async {
                        join_all(clients.iter_mut().map(|client| {
                            let pages = &pages;
                            async move {
                                let started = Instant::now();
                                request_read_pages(client, pages).await;
                                started.elapsed()
                            }
                        }))
                        .await
                    });
                    latencies.extend(per_client);
                },
            );
        },
    );
}

pub(crate) fn measure_concurrent_write_lifecycle(
    ctx: &mut StressContext,
    dimensions: RowDimensions<'_>,
    transport: TransportKind,
    measurement: &'static str,
) {
    crate::tier4_stream_support::tag_row(ctx, &dimensions);
    let storage = dimensions.storage_profile;
    with_transport_clients(
        storage,
        transport,
        dimensions.client_count,
        |runtime, _server, clients| {
            let payload = vec![0xC3; dimensions.payload_size];
            let mut states = (0..dimensions.client_count)
                .map(|index| {
                    WriteLifecycleState::new(
                        &format!(
                            "stream://tier4-concurrent-write-{}/{}/resource-{index}",
                            storage.label(),
                            transport.label()
                        ),
                        &payload,
                    )
                })
                .collect::<Vec<_>>();

            measure_operations(
                ctx,
                measurement,
                u64::try_from(dimensions.client_count).expect("client count should fit u64"),
                |latencies| {
                    let per_client = runtime.block_on(async {
                        join_all(clients.iter_mut().zip(states.iter_mut()).map(
                            |(client, state)| async move {
                                let started = Instant::now();
                                state.complete(client).await;
                                started.elapsed()
                            },
                        ))
                        .await
                    });
                    latencies.extend(per_client);
                },
            );
        },
    );
}

pub(crate) async fn subscribe(client: &mut StreamBenchClient, pattern: &str) {
    request_success(client, &build_stream_subscribe(pattern)).await;
}

pub(crate) async fn delivery_confirmed_commit(
    writer: &mut StreamBenchClient,
    subscriber: &mut StreamBenchClient,
    lifecycle: &mut WriteLifecycleState,
    expected_route: &str,
) {
    lifecycle.complete(writer).await;
    let notify = subscriber
        .recv_frame(RESPONSE_TIMEOUT_MS)
        .await
        .expect("Stream notification");
    assert_stream_notify(&notify, expected_route);
}

fn assert_stream_success(response: &[u8]) {
    let (_message_type, status, payload) = parse_stream_response(response);
    if status != 0 {
        let message = fitz::protocol::error_codes::decode_error_body(&payload).map_or_else(
            |error| format!("could not decode Stream error: {error}"),
            |(_code, message)| message,
        );
        panic!("Stream request must succeed: {message}");
    }
}
