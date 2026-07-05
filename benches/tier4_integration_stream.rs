//! Stream domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Tests four integration levels:
//! 1. **Direct** - Domain actor (no network) - baseline
//! 2. **TCP** - Full TCP stack: encode -> socket -> server -> decode -> actor -> encode -> socket
//! 3. **WebSocket** - Full WS stack: encode -> WS frame -> server -> decode -> actor -> encode -> WS frame
//! 4. **`MultiClient`** - N concurrent WS clients (real concurrency)

#[path = "stress_config.rs"]
mod stress_config;

use stress_config::StressContextExt;

use bytes::Bytes;
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_stream_append, build_stream_begin, build_stream_commit, build_stream_read,
    create_bench_stream_sink, extract_single_tlv_field, parse_stream_read_record_count,
    parse_stream_response, parse_stream_session_id, register_session_queue_sink, route_frame,
    shared_bench_runtime,
};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

const DIRECT_CLIENT_SESSION_ID: u64 = 1;
const STREAM_SYNC_COMMIT_MODE: u8 = 1;
const STREAM_APPEND_MSG_TYPE: u16 = 601;
const STREAM_ROLLBACK_MSG_TYPE: u16 = 603;
const DIRECT_APPEND_EVENTS_PER_SESSION: u64 = 10_000;
const MULTICLIENT_APPEND_ROUNDS_PER_ITERATION: u64 = 4;

struct DirectStreamBenchContext {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<fitz::benchkit::FrameQueueSink>,
}

fn setup_direct_stream_context() -> DirectStreamBenchContext {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_stream_sink(router.clone());
    router.register_domain_pattern("stream", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, DIRECT_CLIENT_SESSION_ID);
    DirectStreamBenchContext {
        router,
        family,
        source,
        inbox,
    }
}

fn shutdown_stream_test_server(runtime: &tokio::runtime::Runtime, server: TestServer) {
    runtime
        .block_on(server.shutdown())
        .expect("shutdown stream bench server");
}

fn close_tcp_client(runtime: &tokio::runtime::Runtime, client: TestClient) {
    runtime
        .block_on(client.close())
        .expect("close stream tcp bench client");
}

fn close_ws_client(runtime: &tokio::runtime::Runtime, client: &mut TestWebSocketClient) {
    runtime
        .block_on(client.close())
        .expect("close stream websocket bench client");
}

fn close_ws_clients(
    runtime: &tokio::runtime::Runtime,
    clients: &[Arc<Mutex<TestWebSocketClient>>],
) {
    runtime.block_on(async {
        for client in clients {
            client
                .lock()
                .await
                .close()
                .await
                .expect("close stream multiclient websocket bench client");
        }
    });
}

fn direct_request(
    context: &DirectStreamBenchContext,
    destination: &str,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_frame(
        context.router.as_ref(),
        &context.source,
        destination,
        DIRECT_CLIENT_SESSION_ID,
        ChannelId::Pub,
        msg_type,
        payload,
        context.family,
    )
    .expect("stream route");
    let responses = context.inbox.drain_after_count(1, Duration::from_secs(1));
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("stream response")
}

fn direct_begin_stream(context: &DirectStreamBenchContext, route: &str) -> u64 {
    let begin_frame = build_stream_begin(route);
    let (msg_type, payload) = extract_single_tlv_field(&begin_frame);
    let response = direct_request(context, route, msg_type, payload);
    parse_stream_session_id(response.as_ref()).expect("stream session id")
}

fn build_stream_append_payload(session_id: u64, expected_offset: u64, body: &[u8]) -> Bytes {
    let mut payload = Vec::with_capacity(8 + 8 + 4 + body.len() + 1);
    payload.extend_from_slice(&session_id.to_be_bytes());
    payload.extend_from_slice(&expected_offset.to_be_bytes());
    payload.extend_from_slice(
        &u32::try_from(body.len())
            .expect("stream append body length fits u32")
            .to_be_bytes(),
    );
    payload.extend_from_slice(body);
    payload.push(0);
    Bytes::from(payload)
}

fn build_stream_session_payload(session_id: u64) -> Bytes {
    Bytes::copy_from_slice(&session_id.to_be_bytes())
}

fn assert_stream_direct_success(payload: &[u8], operation: &str) {
    assert_eq!(
        payload.first().copied(),
        Some(0),
        "{operation} response must succeed"
    );
}

fn direct_rollback_stream(context: &DirectStreamBenchContext, route: &str, session_id: u64) {
    let response = direct_request(
        context,
        route,
        STREAM_ROLLBACK_MSG_TYPE,
        build_stream_session_payload(session_id),
    );
    assert_stream_direct_success(response.as_ref(), "direct rollback");
}

fn direct_seed_stream_route(
    context: &DirectStreamBenchContext,
    route: &str,
    event_count: usize,
    body: &'static [u8],
) {
    let session_id = direct_begin_stream(context, route);
    for expected_offset in 0..event_count as u64 {
        let append_frame = build_stream_append(session_id, expected_offset, body);
        let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
        let _ = direct_request(context, route, append_msg_type, append_payload);
    }

    let commit_frame = build_stream_commit(session_id, STREAM_SYNC_COMMIT_MODE);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
    let _ = direct_request(context, route, commit_msg_type, commit_payload);
}

fn direct_prepare_validated_read(
    context: &DirectStreamBenchContext,
    route: &str,
    expected_count: usize,
) -> (u16, Bytes) {
    let read_frame = build_stream_read(route, 0);
    let (msg_type, payload) = extract_single_tlv_field(&read_frame);
    let response = direct_request(context, route, msg_type, payload.clone());
    let count = fitz::benchkit::count_stream_read_records_from_payload(response.as_ref())
        .expect("direct stream read count");
    assert_eq!(
        count, expected_count,
        "unexpected direct read count for {route}"
    );
    (msg_type, payload)
}

async fn tcp_seed_stream_route(
    client: &mut TestClient,
    route: &str,
    event_count: usize,
    body: &'static [u8],
) {
    let begin_response = client
        .request(&build_stream_begin(route), 2000)
        .await
        .expect("begin response");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");

    for expected_offset in 0..event_count as u64 {
        let append_frame = build_stream_append(session_id, expected_offset, body);
        let _ = client
            .request(&append_frame, 2000)
            .await
            .expect("append response");
    }

    let commit_frame = build_stream_commit(session_id, STREAM_SYNC_COMMIT_MODE);
    let _ = client
        .request(&commit_frame, 2000)
        .await
        .expect("commit response");
}

async fn ws_seed_stream_route(
    client: &mut TestWebSocketClient,
    route: &str,
    event_count: usize,
    body: &'static [u8],
) {
    let begin_response = client
        .request(&build_stream_begin(route), 2000)
        .await
        .expect("begin response");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");

    for expected_offset in 0..event_count as u64 {
        let append_frame = build_stream_append(session_id, expected_offset, body);
        let _ = client
            .request(&append_frame, 2000)
            .await
            .expect("append response");
    }

    let commit_frame = build_stream_commit(session_id, STREAM_SYNC_COMMIT_MODE);
    let _ = client
        .request(&commit_frame, 2000)
        .await
        .expect("commit response");
}

#[stress(tier = 4)]
fn should_complete_direct_append(ctx: &mut StressContext) {
    ctx.parameter("layer", "direct");
    ctx.parameter("scenario", "append");
    ctx.parameter("measurement_scope", "direct_inproc");
    ctx.parameter("batch_size", "single_append");

    let context = setup_direct_stream_context();
    let route = "stream://tier4/stream/direct/append";
    let mut session_id = direct_begin_stream(&context, route);
    let mut expected_offset = 0u64;

    let iterations = ctx.measure_workload("complete_direct_append", || {
        if expected_offset == DIRECT_APPEND_EVENTS_PER_SESSION {
            direct_rollback_stream(&context, route, session_id);
            session_id = direct_begin_stream(&context, route);
            expected_offset = 0;
        }

        let response = direct_request(
            &context,
            route,
            STREAM_APPEND_MSG_TYPE,
            build_stream_append_payload(session_id, expected_offset, b"event"),
        );
        assert_stream_direct_success(response.as_ref(), "measured direct append");
        expected_offset = expected_offset
            .checked_add(1)
            .expect("direct append expected offset overflow");
    });
    stress_config::record_completed(ctx, iterations);
}

#[stress(tier = 4)]
fn should_complete_direct_area_wildcard_read(ctx: &mut StressContext) {
    ctx.parameter("layer", "direct");
    ctx.parameter("scenario", "read_area_wildcard");
    ctx.parameter("measurement_scope", "direct_inproc");
    ctx.parameter("batch_size", "100_events_scanned");

    let context = setup_direct_stream_context();
    direct_seed_stream_route(
        &context,
        "stream://tier4/stream-area/orders",
        50,
        b"area event",
    );
    direct_seed_stream_route(
        &context,
        "stream://tier4/stream-area/audits",
        50,
        b"area event",
    );

    let read_route = "stream://tier4/stream-area/*";
    let (read_msg_type, read_payload) = direct_prepare_validated_read(&context, read_route, 100);

    let iterations = ctx.measure_workload("complete_direct_area_wildcard_read", || {
        let _ = direct_request(&context, read_route, read_msg_type, read_payload.clone());
    });
    stress_config::record_completed(ctx, 100 * iterations);
}

#[stress(tier = 4)]
fn should_complete_direct_resource_read(ctx: &mut StressContext) {
    ctx.parameter("layer", "direct");
    ctx.parameter("scenario", "read_resource_exact");
    ctx.parameter("measurement_scope", "direct_inproc");
    ctx.parameter("batch_size", "100_events_scanned");

    let context = setup_direct_stream_context();
    let read_route = "stream://tier4/resource/orders";
    direct_seed_stream_route(&context, read_route, 100, b"resource event");

    let (read_msg_type, read_payload) = direct_prepare_validated_read(&context, read_route, 100);

    let iterations = ctx.measure_workload("complete_direct_resource_read", || {
        let _ = direct_request(&context, read_route, read_msg_type, read_payload.clone());
    });
    stress_config::record_completed(ctx, 100 * iterations);
}

#[stress(tier = 4)]
fn should_complete_direct_realm_wildcard_read(ctx: &mut StressContext) {
    ctx.parameter("layer", "direct");
    ctx.parameter("scenario", "read_realm_wildcard");
    ctx.parameter("measurement_scope", "direct_inproc");
    ctx.parameter("batch_size", "100_events_scanned");

    let context = setup_direct_stream_context();
    direct_seed_stream_route(&context, "stream://tier4/events/orders", 50, b"realm event");
    direct_seed_stream_route(&context, "stream://tier4/audit/ledger", 50, b"realm event");

    let read_route = "stream://tier4/*/*";
    let (read_msg_type, read_payload) = direct_prepare_validated_read(&context, read_route, 100);

    let iterations = ctx.measure_workload("complete_direct_realm_wildcard_read", || {
        let _ = direct_request(&context, read_route, read_msg_type, read_payload.clone());
    });
    stress_config::record_completed(ctx, 100 * iterations);
}

#[stress(tier = 4)]
fn should_complete_tcp_append(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "append");
    ctx.parameter("measurement_scope", "tcp_e2e");
    ctx.parameter("batch_size", "single_append");

    let route = "stream://tier4/stream/tcp/append";
    let begin_frame = build_stream_begin(route);

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    let begin_response = runtime
        .block_on(client.request(&begin_frame, 2000))
        .expect("begin response");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");
    let append_frame = build_stream_append(session_id, 0, b"event");

    let iterations = ctx.measure_workload("complete_tcp_append", || {
        let _ = runtime
            .block_on(client.request(&append_frame, 2000))
            .expect("append response");
    });
    stress_config::record_completed(ctx, iterations);
    close_tcp_client(runtime, client);
    shutdown_stream_test_server(runtime, server);
}

#[stress(tier = 4)]
fn should_complete_tcp_resource_read(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "read_resource_exact");
    ctx.parameter("measurement_scope", "tcp_e2e");
    ctx.parameter("batch_size", "100_events_scanned");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    let read_route = "stream://tier4/resource/orders";
    runtime.block_on(async {
        tcp_seed_stream_route(&mut client, read_route, 100, b"resource event").await;
    });

    let read_frame = build_stream_read(read_route, 0);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("resource read response");
    let count = parse_stream_read_record_count(&validated_response).expect("resource read count");
    assert_eq!(count, 100, "unexpected tcp resource read count");

    let iterations = ctx.measure_workload("complete_tcp_resource_read", || {
        let _ = runtime
            .block_on(client.request(&read_frame, 2000))
            .expect("resource read response");
    });
    stress_config::record_completed(ctx, 100 * iterations);
    close_tcp_client(runtime, client);
    shutdown_stream_test_server(runtime, server);
}

#[stress(tier = 4)]
fn should_complete_tcp_area_wildcard_read(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "read_area_wildcard");
    ctx.parameter("measurement_scope", "tcp_e2e");
    ctx.parameter("batch_size", "100_events_scanned");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    runtime.block_on(async {
        tcp_seed_stream_route(
            &mut client,
            "stream://tier4/stream-area/orders",
            50,
            b"area event",
        )
        .await;
        tcp_seed_stream_route(
            &mut client,
            "stream://tier4/stream-area/audits",
            50,
            b"area event",
        )
        .await;
    });

    let read_frame = build_stream_read("stream://tier4/stream-area/*", 0);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("area read response");
    let count = parse_stream_read_record_count(&validated_response).expect("area read count");
    assert_eq!(count, 100, "unexpected tcp area read count");

    let iterations = ctx.measure_workload("complete_tcp_area_wildcard_read", || {
        let _ = runtime
            .block_on(client.request(&read_frame, 2000))
            .expect("area read response");
    });
    stress_config::record_completed(ctx, 100 * iterations);
    close_tcp_client(runtime, client);
    shutdown_stream_test_server(runtime, server);
}

#[stress(tier = 4)]
fn should_complete_tcp_realm_wildcard_read(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "read_realm_wildcard");
    ctx.parameter("measurement_scope", "tcp_e2e");
    ctx.parameter("batch_size", "100_events_scanned");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect tcp");

    runtime.block_on(async {
        tcp_seed_stream_route(
            &mut client,
            "stream://tier4/events/orders",
            50,
            b"realm event",
        )
        .await;
        tcp_seed_stream_route(
            &mut client,
            "stream://tier4/audit/ledger",
            50,
            b"realm event",
        )
        .await;
    });

    let read_frame = build_stream_read("stream://tier4/*/*", 0);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("realm read response");
    let count = parse_stream_read_record_count(&validated_response).expect("realm read count");
    assert_eq!(count, 100, "unexpected tcp realm read count");

    let iterations = ctx.measure_workload("complete_tcp_realm_wildcard_read", || {
        let _ = runtime
            .block_on(client.request(&read_frame, 2000))
            .expect("realm read response");
    });
    stress_config::record_completed(ctx, 100 * iterations);
    close_tcp_client(runtime, client);
    shutdown_stream_test_server(runtime, server);
}

#[stress(tier = 4)]
fn should_complete_ws_append(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "append");
    ctx.parameter("measurement_scope", "ws_e2e");
    ctx.parameter("batch_size", "single_append");

    let route = "stream://tier4/stream/ws/append";
    let begin_frame = build_stream_begin(route);

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    let begin_response = runtime
        .block_on(client.request(&begin_frame, 2000))
        .expect("begin response");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");
    let append_frame = build_stream_append(session_id, 0, b"event");

    let iterations = ctx.measure_workload("complete_ws_append", || {
        let _ = runtime
            .block_on(client.request(&append_frame, 2000))
            .expect("append response");
    });
    stress_config::record_completed(ctx, iterations);
    close_ws_client(runtime, &mut client);
    shutdown_stream_test_server(runtime, server);
}

#[stress(tier = 4)]
fn should_complete_ws_resource_read(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "read_resource_exact");
    ctx.parameter("measurement_scope", "ws_e2e");
    ctx.parameter("batch_size", "100_events_scanned");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    let read_route = "stream://tier4/resource/orders";
    runtime.block_on(async {
        ws_seed_stream_route(&mut client, read_route, 100, b"resource event").await;
    });

    let read_frame = build_stream_read(read_route, 0);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("resource read response");
    let count = parse_stream_read_record_count(&validated_response).expect("resource read count");
    assert_eq!(count, 100, "unexpected ws resource read count");

    let iterations = ctx.measure_workload("complete_ws_resource_read", || {
        let _ = runtime
            .block_on(client.request(&read_frame, 2000))
            .expect("resource read response");
    });
    stress_config::record_completed(ctx, 100 * iterations);
    close_ws_client(runtime, &mut client);
    shutdown_stream_test_server(runtime, server);
}

#[stress(tier = 4)]
fn should_complete_ws_area_wildcard_read(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "read_area_wildcard");
    ctx.parameter("measurement_scope", "ws_e2e");
    ctx.parameter("batch_size", "100_events_scanned");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    runtime.block_on(async {
        ws_seed_stream_route(
            &mut client,
            "stream://tier4/stream-area/orders",
            50,
            b"area event",
        )
        .await;
        ws_seed_stream_route(
            &mut client,
            "stream://tier4/stream-area/audits",
            50,
            b"area event",
        )
        .await;
    });

    let read_frame = build_stream_read("stream://tier4/stream-area/*", 0);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("area read response");
    let count = parse_stream_read_record_count(&validated_response).expect("area read count");
    assert_eq!(count, 100, "unexpected ws area read count");

    let iterations = ctx.measure_workload("complete_ws_area_wildcard_read", || {
        let _ = runtime
            .block_on(client.request(&read_frame, 2000))
            .expect("area read response");
    });
    stress_config::record_completed(ctx, 100 * iterations);
    close_ws_client(runtime, &mut client);
    shutdown_stream_test_server(runtime, server);
}

#[stress(tier = 4)]
fn should_complete_ws_realm_wildcard_read(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "read_realm_wildcard");
    ctx.parameter("measurement_scope", "ws_e2e");
    ctx.parameter("batch_size", "100_events_scanned");

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect ws");

    runtime.block_on(async {
        ws_seed_stream_route(
            &mut client,
            "stream://tier4/events/orders",
            50,
            b"realm event",
        )
        .await;
        ws_seed_stream_route(
            &mut client,
            "stream://tier4/audit/ledger",
            50,
            b"realm event",
        )
        .await;
    });

    let read_frame = build_stream_read("stream://tier4/*/*", 0);
    let validated_response = runtime
        .block_on(client.request(&read_frame, 2000))
        .expect("realm read response");
    let count = parse_stream_read_record_count(&validated_response).expect("realm read count");
    assert_eq!(count, 100, "unexpected ws realm read count");

    let iterations = ctx.measure_workload("complete_ws_realm_wildcard_read", || {
        let _ = runtime
            .block_on(client.request(&read_frame, 2000))
            .expect("realm read response");
    });
    stress_config::record_completed(ctx, 100 * iterations);
    close_ws_client(runtime, &mut client);
    shutdown_stream_test_server(runtime, server);
}

#[stress(tier = 4)]
fn should_complete_multiclient_appends(ctx: &mut StressContext) {
    ctx.parameter("layer", "multiclient");
    ctx.parameter("scenario", "concurrent_appends");
    ctx.parameter("measurement_scope", "ws_multiclient_e2e");
    ctx.parameter("batch_size", "10_clients_1_append_each");
    ctx.parameter("client_count", "10");

    let begin_frames: Vec<Vec<u8>> = (0..10)
        .map(|index| build_stream_begin(&format!("stream://tier4/stream/multi-{index}/append")))
        .collect();

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");
    let clients: Vec<Arc<Mutex<TestWebSocketClient>>> = (0..10)
        .map(|_| {
            let c = runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect ws");
            Arc::new(Mutex::new(c))
        })
        .collect();

    let append_frames: Vec<Vec<u8>> = runtime.block_on(futures::future::join_all(
        clients.iter().zip(begin_frames.iter()).map(|(arc, begin)| {
            let arc = arc.clone();
            let begin = begin.clone();
            async move {
                let mut c = arc.lock().await;
                let response = c.request(&begin, 2000).await.expect("begin");
                let (_msg_type, _status, data) = parse_stream_response(&response);
                let session_id = parse_stream_session_id(&data).expect("session_id");
                build_stream_append(session_id, 0, b"event")
            }
        }),
    ));

    let iterations = ctx.measure_workload("complete_multiclient_appends", || {
        for _ in 0..MULTICLIENT_APPEND_ROUNDS_PER_ITERATION {
            let _results: Vec<_> = runtime.block_on(futures::future::join_all(
                clients
                    .iter()
                    .zip(append_frames.iter())
                    .map(|(arc, frame)| {
                        let arc = arc.clone();
                        let frame = frame.clone();
                        async move {
                            let mut c = arc.lock().await;
                            c.request(&frame, 2000).await.expect("append");
                        }
                    }),
            ));
        }
    });
    stress_config::record_completed(
        ctx,
        10 * MULTICLIENT_APPEND_ROUNDS_PER_ITERATION * iterations,
    );
    close_ws_clients(runtime, &clients);
    drop(clients);
    shutdown_stream_test_server(runtime, server);
}

stress_main!();
