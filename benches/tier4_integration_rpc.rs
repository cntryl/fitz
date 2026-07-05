//! RPC domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Layers: direct, encoded (codec decode path), tcp, websocket, multiclient (concurrent).
//! RPC tier4 tests full request -> worker dispatch -> response over the wire where applicable.

#[path = "stress_config.rs"]
mod stress_config;

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_rpc_request, build_rpc_response_frame_bytes, build_rpc_subscribe_with_max_concurrent,
    create_bench_rpc_sink, extract_single_tlv_field, register_session_queue_sink, route_frame,
    shared_bench_runtime, FrameQueueSink,
};
use fitz::domains::rpc::protocol::RpcResponse;
use fitz::protocol::frame::ChannelId;
use fitz::protocol::rpc_codec::encode_response_message;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::{mpsc as std_mpsc, Arc};
use std::time::Duration;
use uuid::Uuid;

const SERVICE_ROUTE: &str = "rpc://tier4/service";
const REQUESTER_SESSION_ID: u64 = 1;
const WORKER_SESSION_ID: u64 = 2;
const RESPONSE_TIMEOUT_MS: u64 = 2_000;
const MULTICLIENT_COUNT: usize = 10;
const MULTICLIENT_REQUEST_FRAME_RING_SIZE: usize = 512;
const WS_ROUNDTRIPS_PER_ITERATION: usize = 32;
const TIER4_WORKER_MAX_CONCURRENT: u32 = 32;

struct NetworkRequestFrame {
    frame: Bytes,
    correlation_id: Uuid,
    body: Bytes,
}

struct RpcRequestParts {
    correlation_id: Uuid,
    body: Bytes,
}

struct RpcResponseParts {
    correlation_id: Uuid,
    seq: u64,
    body: Bytes,
    stream_end: bool,
}

struct RpcRequesterDriver {
    command_tx: tokio::sync::mpsc::UnboundedSender<usize>,
    handle: tokio::task::JoinHandle<()>,
}

fn read_u32(input: &[u8], offset: &mut usize) -> Option<u32> {
    let end = offset.checked_add(4)?;
    let bytes = input.get(*offset..end)?;
    *offset = end;
    Some(u32::from_be_bytes(bytes.try_into().ok()?))
}

fn read_u64(input: &[u8], offset: &mut usize) -> Option<u64> {
    let end = offset.checked_add(8)?;
    let bytes = input.get(*offset..end)?;
    *offset = end;
    Some(u64::from_be_bytes(bytes.try_into().ok()?))
}

fn read_len_prefixed_range(input: &[u8], offset: &mut usize) -> Option<(usize, usize)> {
    let len = usize::try_from(read_u32(input, offset)?).ok()?;
    let start = *offset;
    let end = start.checked_add(len)?;
    input.get(start..end)?;
    *offset = end;
    Some((start, end))
}

fn read_uuid_field(input: &[u8], offset: &mut usize) -> Option<Uuid> {
    let end = offset.checked_add(16)?;
    let uuid_bytes = input.get(*offset..end)?;
    *offset = end;
    let uuid_array: [u8; 16] = uuid_bytes.try_into().ok()?;
    Some(Uuid::from_bytes(uuid_array))
}

fn single_tlv_payload_range(frame: &[u8]) -> Option<(u16, usize, usize)> {
    const ESCAPE_MARKER: u8 = 0xFF;

    let mut offset = 0usize;
    let msg_type = if *frame.get(offset)? == ESCAPE_MARKER {
        let end = offset.checked_add(3)?;
        let bytes = frame.get(offset + 1..end)?;
        offset = end;
        u16::from_be_bytes(bytes.try_into().ok()?)
    } else {
        let value = u16::from(*frame.get(offset)?);
        offset += 1;
        value
    };

    let len_end = offset.checked_add(2)?;
    let len_bytes = frame.get(offset..len_end)?;
    offset = len_end;
    let len = usize::from(u16::from_be_bytes(len_bytes.try_into().ok()?));
    let end = offset.checked_add(len)?;
    frame.get(offset..end)?;
    Some((msg_type, offset, end))
}

fn try_parse_rpc_request_payload_parts(payload: &Bytes) -> Option<RpcRequestParts> {
    let mut offset = 0usize;
    let correlation_id = read_uuid_field(payload.as_ref(), &mut offset)?;
    read_len_prefixed_range(payload.as_ref(), &mut offset)?;
    let (body_start, body_end) = read_len_prefixed_range(payload.as_ref(), &mut offset)?;
    if offset != payload.len() {
        return None;
    }

    Some(RpcRequestParts {
        correlation_id,
        body: payload.slice(body_start..body_end),
    })
}

fn try_parse_rpc_request_frame_parts(frame: &Bytes) -> Option<RpcRequestParts> {
    let (msg_type, payload_start, payload_end) = single_tlv_payload_range(frame.as_ref())?;
    if msg_type != 302 {
        return None;
    }

    let payload = frame.slice(payload_start..payload_end);
    try_parse_rpc_request_payload_parts(&payload)
}

fn try_parse_rpc_response_payload_parts(payload: &Bytes) -> Option<RpcResponseParts> {
    let mut offset = 0usize;
    let correlation_id = read_uuid_field(payload.as_ref(), &mut offset)?;
    let seq = read_u64(payload.as_ref(), &mut offset)?;
    let flags = *payload.get(offset)?;
    offset += 1;
    let (body_start, body_end) = read_len_prefixed_range(payload.as_ref(), &mut offset)?;
    if offset != payload.len() {
        return None;
    }

    Some(RpcResponseParts {
        correlation_id,
        seq,
        body: payload.slice(body_start..body_end),
        stream_end: flags & 0x01 != 0,
    })
}

fn try_parse_rpc_response_frame_parts(frame: &Bytes) -> Option<RpcResponseParts> {
    let (msg_type, payload_start, payload_end) = single_tlv_payload_range(frame.as_ref())?;
    if msg_type != 303 {
        return None;
    }

    let payload = frame.slice(payload_start..payload_end);
    try_parse_rpc_response_payload_parts(&payload)
}

fn build_network_request_frame(
    route: &str,
    payload: &[u8],
    _family: RouteFamily,
) -> NetworkRequestFrame {
    let frame = Bytes::from(build_rpc_request(route, payload));
    let request = try_parse_rpc_request_frame_parts(&frame).expect("rpc request frame");

    NetworkRequestFrame {
        frame,
        correlation_id: request.correlation_id,
        body: request.body,
    }
}

fn build_network_request_frame_ring(
    route: &str,
    payload: &[u8],
    family: RouteFamily,
    count: usize,
) -> Vec<NetworkRequestFrame> {
    (0..count)
        .map(|_| build_network_request_frame(route, payload, family))
        .collect()
}

fn assert_rpc_worker_response(
    response: &RpcResponseParts,
    expected_correlation_id: Uuid,
    expected_body: &[u8],
) {
    validate_rpc_worker_response(response, expected_correlation_id, expected_body)
        .expect("valid rpc worker response");
}

fn validate_rpc_worker_response(
    response: &RpcResponseParts,
    expected_correlation_id: Uuid,
    expected_body: &[u8],
) -> Result<(), String> {
    if response.correlation_id != expected_correlation_id {
        return Err(format!(
            "unexpected rpc correlation id: expected {expected_correlation_id}, got {}",
            response.correlation_id
        ));
    }
    if response.seq != 0 {
        return Err(format!(
            "single-response bench should emit seq 0, got {}",
            response.seq
        ));
    }
    if response.body.as_ref() != expected_body {
        return Err(format!(
            "unexpected rpc response body: expected {expected_body:?}, got {:?}",
            response.body.as_ref()
        ));
    }
    if !response.stream_end {
        return Err("single-response bench should end the stream".to_string());
    }

    Ok(())
}

fn assert_requester_inbox_contains_worker_response(
    frames: Vec<fitz::protocol::frame_context::FrameContext>,
    _family: RouteFamily,
    expected_correlation_id: Uuid,
    expected_body: &[u8],
) {
    for frame in frames {
        if frame.msg_type.as_u16() != 303 {
            continue;
        }

        if let Some(response) = try_parse_rpc_response_payload_parts(&frame.payload) {
            assert_rpc_worker_response(&response, expected_correlation_id, expected_body);
            return;
        }

        panic!("failed to parse rpc response frame");
    }

    panic!("expected worker rpc response in requester inbox");
}

async fn try_request_until_worker_response_tcp(
    client: &mut TestClient,
    request_frame: &NetworkRequestFrame,
    _family: RouteFamily,
) -> Result<(), String> {
    client
        .send_frame_bytes(request_frame.frame.clone())
        .await
        .map_err(|error| format!("send rpc request: {error}"))?;

    for _ in 0..4 {
        let frame = client
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(|error| format!("receive rpc response: {error}"))?;
        if let Some(response) = try_parse_rpc_response_frame_parts(&frame) {
            validate_rpc_worker_response(
                &response,
                request_frame.correlation_id,
                request_frame.body.as_ref(),
            )?;
            return Ok(());
        }
    }

    Err("expected worker rpc response frame over tcp".to_string())
}

async fn request_until_worker_response_tcp(
    client: &mut TestClient,
    request_frame: &NetworkRequestFrame,
    family: RouteFamily,
) {
    try_request_until_worker_response_tcp(client, request_frame, family)
        .await
        .expect("rpc tcp worker response");
}

async fn try_request_until_worker_response_ws(
    client: &mut TestWebSocketClient,
    request_frame: &NetworkRequestFrame,
    _family: RouteFamily,
) -> Result<(), String> {
    client
        .send_frame_bytes(request_frame.frame.clone())
        .await
        .map_err(|error| format!("send rpc request: {error}"))?;

    for _ in 0..4 {
        let frame = client
            .recv_frame_bytes_without_timeout()
            .await
            .map_err(|error| format!("receive rpc response: {error}"))?;
        if let Some(response) = try_parse_rpc_response_frame_parts(&frame) {
            validate_rpc_worker_response(
                &response,
                request_frame.correlation_id,
                request_frame.body.as_ref(),
            )?;
            return Ok(());
        }
    }

    Err("expected worker rpc response frame over websocket".to_string())
}

async fn request_until_worker_response_ws(
    client: &mut TestWebSocketClient,
    request_frame: &NetworkRequestFrame,
    family: RouteFamily,
) {
    try_request_until_worker_response_ws(client, request_frame, family)
        .await
        .expect("rpc websocket worker response");
}

fn spawn_rpc_ws_workers(
    worker_clients: Vec<TestWebSocketClient>,
    _family: RouteFamily,
) -> Vec<tokio::task::JoinHandle<()>> {
    worker_clients
        .into_iter()
        .map(|mut worker_client| {
            let rt = shared_bench_runtime();
            rt.spawn(async move {
                loop {
                    let Ok(frame) = worker_client.recv_frame_bytes_without_timeout().await else {
                        break;
                    };

                    if let Some(req) = try_parse_rpc_request_frame_parts(&frame) {
                        let resp_frame =
                            build_rpc_response_frame_bytes(req.correlation_id, req.body);
                        let _ = worker_client.send_frame_bytes(resp_frame).await;
                    }
                }
            })
        })
        .collect()
}

fn spawn_rpc_ws_requesters(
    clients: Vec<TestWebSocketClient>,
    request_frames: Vec<Vec<NetworkRequestFrame>>,
    family: RouteFamily,
) -> (
    Vec<RpcRequesterDriver>,
    std_mpsc::Receiver<Result<usize, String>>,
) {
    let (completion_tx, completion_rx) = std_mpsc::channel();
    let drivers = clients
        .into_iter()
        .zip(request_frames)
        .enumerate()
        .map(|(requester_id, (mut client, frames))| {
            let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
            let completion_tx = completion_tx.clone();
            let rt = shared_bench_runtime();
            let handle = rt.spawn(async move {
                while let Some(request_index) = command_rx.recv().await {
                    let result = tokio::time::timeout(
                        Duration::from_millis(RESPONSE_TIMEOUT_MS),
                        try_request_until_worker_response_ws(
                            &mut client,
                            &frames[request_index],
                            family,
                        ),
                    )
                    .await
                    .map_err(|_| format!("requester {requester_id} rpc response timeout"))
                    .and_then(|inner| {
                        inner
                            .map(|()| requester_id)
                            .map_err(|error| format!("requester {requester_id}: {error}"))
                    });

                    if completion_tx.send(result).is_err() {
                        break;
                    }
                }
                let _ = client.close().await;
            });
            RpcRequesterDriver { command_tx, handle }
        })
        .collect();
    drop(completion_tx);

    (drivers, completion_rx)
}

fn request_all_multiclient_ws(
    drivers: &[RpcRequesterDriver],
    completion_rx: &std_mpsc::Receiver<Result<usize, String>>,
    request_index: usize,
) {
    assert_eq!(
        drivers.len(),
        MULTICLIENT_COUNT,
        "expected exactly {MULTICLIENT_COUNT} requester drivers"
    );

    for driver in drivers {
        driver
            .command_tx
            .send(request_index)
            .expect("requester driver is running");
    }

    for _ in 0..drivers.len() {
        match completion_rx.recv_timeout(Duration::from_millis(RESPONSE_TIMEOUT_MS)) {
            Ok(Ok(_requester_id)) => {}
            Ok(Err(error)) => panic!("{error}"),
            Err(error) => panic!("multiclient rpc response timeout: {error}"),
        }
    }
}

fn measure_multiclient_concurrent_requests(
    ctx: &mut StressContext,
    worker_count: usize,
    scenario: &'static str,
) {
    ctx.parameter("layer", "multiclient");
    ctx.parameter("scenario", scenario);
    ctx.parameter("measurement_scope", "ws_multiclient_e2e");
    ctx.parameter("batch_size", "10_clients_1_roundtrip_each");
    ctx.parameter("client_count", MULTICLIENT_COUNT.to_string());
    ctx.parameter("worker_count", worker_count.to_string());

    let family = RouteFamily::new(1);
    let subscribe_frame =
        build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, TIER4_WORKER_MAX_CONCURRENT);

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

    let mut worker_clients: Vec<TestWebSocketClient> = (0..worker_count)
        .map(|_| {
            let mut worker_client = runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect worker ws");
            runtime
                .block_on(worker_client.send_frame(&subscribe_frame))
                .expect("subscribe");
            let _ = runtime.block_on(worker_client.recv_frame(RESPONSE_TIMEOUT_MS));
            worker_client
        })
        .collect();
    let active_worker_client = worker_clients.remove(0);
    let idle_worker_clients = worker_clients;

    let clients: Vec<TestWebSocketClient> = (0..MULTICLIENT_COUNT)
        .map(|_| {
            runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect ws")
        })
        .collect();
    let request_frames: Vec<Vec<NetworkRequestFrame>> = (0..MULTICLIENT_COUNT)
        .map(|_| {
            build_network_request_frame_ring(
                SERVICE_ROUTE,
                b"ping",
                family,
                MULTICLIENT_REQUEST_FRAME_RING_SIZE,
            )
        })
        .collect();
    let mut next_request_index = 0usize;
    let worker_handles = spawn_rpc_ws_workers(vec![active_worker_client], family);
    let (requester_drivers, completion_rx) =
        spawn_rpc_ws_requesters(clients, request_frames, family);

    let iterations = ctx.measure_workload(|| {
        let request_index = next_request_index;
        next_request_index = (next_request_index + 1) % MULTICLIENT_REQUEST_FRAME_RING_SIZE;

        request_all_multiclient_ws(&requester_drivers, &completion_rx, request_index);
    });
    stress_config::record_completed(ctx, MULTICLIENT_COUNT as u64 * iterations);

    for driver in requester_drivers {
        drop(driver.command_tx);
        runtime
            .block_on(driver.handle)
            .expect("requester driver should stop cleanly");
    }

    for worker_handle in worker_handles {
        worker_handle.abort();
        let _ = runtime.block_on(worker_handle);
    }

    for mut idle_worker_client in idle_worker_clients {
        let _ = runtime.block_on(idle_worker_client.close());
    }

    runtime
        .block_on(server.shutdown())
        .expect("shutdown server");
}

fn setup_rpc_sink() -> (
    Arc<Router>,
    RouteFamily,
    RouteAddress,
    Arc<FrameQueueSink>,
    RouteAddress,
    Arc<FrameQueueSink>,
) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_rpc_sink(router.clone());
    router.register_domain_pattern("rpc", sink as Arc<dyn MailboxSink>);

    let (requester_source, requester_inbox) =
        register_session_queue_sink(&router, family, REQUESTER_SESSION_ID);
    let (worker_source, worker_inbox) =
        register_session_queue_sink(&router, family, WORKER_SESSION_ID);

    let subscribe_frame =
        build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, TIER4_WORKER_MAX_CONCURRENT);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);
    route_frame(
        router.as_ref(),
        &worker_source,
        SERVICE_ROUTE,
        WORKER_SESSION_ID,
        ChannelId::Rpc,
        subscribe_msg_type,
        subscribe_payload,
        family,
    )
    .expect("rpc subscribe");
    let _ = worker_inbox.drain();

    (
        router,
        family,
        requester_source,
        requester_inbox,
        worker_source,
        worker_inbox,
    )
}

fn service_worker(
    router: &Arc<Router>,
    family: RouteFamily,
    worker_source: &RouteAddress,
    worker_inbox: &Arc<FrameQueueSink>,
) {
    loop {
        let frames = worker_inbox.drain();
        if frames.is_empty() {
            break;
        }

        let mut handled_request = false;
        for frame in frames {
            if frame.msg_type.as_u16() == 302 {
                handled_request = true;
                if let Some(req) = try_parse_rpc_request_payload_parts(&frame.payload) {
                    let response = RpcResponse::single(req.correlation_id, req.body);
                    route_frame(
                        router.as_ref(),
                        worker_source,
                        SERVICE_ROUTE,
                        WORKER_SESSION_ID,
                        ChannelId::Rpc,
                        303,
                        Bytes::from(encode_response_message(&response)),
                        family,
                    )
                    .expect("rpc response");
                }
            }
        }

        if !handled_request {
            break;
        }
    }
}

#[stress_test(tier = 4)]
fn should_complete_direct_request(ctx: &mut StressContext) {
    ctx.parameter("layer", "direct");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "direct_inproc");
    ctx.parameter("batch_size", "single_roundtrip");
    ctx.parameter("worker_count", "1");

    let request = build_network_request_frame(SERVICE_ROUTE, b"ping", RouteFamily::new(1));
    let (router, family, requester_source, requester_inbox, worker_source, worker_inbox) =
        setup_rpc_sink();
    let (request_msg_type, request_payload) = extract_single_tlv_field(&request.frame);

    let iterations = ctx.measure_workload(|| {
        route_frame(
            router.as_ref(),
            &requester_source,
            SERVICE_ROUTE,
            REQUESTER_SESSION_ID,
            ChannelId::Rpc,
            request_msg_type,
            request_payload.clone(),
            family,
        )
        .expect("rpc request");
        service_worker(&router, family, &worker_source, &worker_inbox);
        assert_requester_inbox_contains_worker_response(
            requester_inbox.drain(),
            family,
            request.correlation_id,
            request.body.as_ref(),
        );
    });
    stress_config::record_completed(ctx, iterations);
}

#[stress_test(tier = 4)]
fn should_complete_encoded_request(ctx: &mut StressContext) {
    ctx.parameter("layer", "encoded");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "encoded_inproc");
    ctx.parameter("batch_size", "single_roundtrip");
    ctx.parameter("worker_count", "1");

    let request = build_network_request_frame(SERVICE_ROUTE, b"ping", RouteFamily::new(1));
    let (router, family, requester_source, requester_inbox, worker_source, worker_inbox) =
        setup_rpc_sink();
    let request_frame = &request.frame;

    let iterations = ctx.measure_workload(|| {
        let mut parser = TlvFrameParser::new(request_frame);
        let (msg_type, payload) = parser.next_field_ref().expect("one field");
        route_frame(
            router.as_ref(),
            &requester_source,
            SERVICE_ROUTE,
            REQUESTER_SESSION_ID,
            ChannelId::Rpc,
            msg_type,
            Bytes::copy_from_slice(payload),
            family,
        )
        .expect("rpc request");
        service_worker(&router, family, &worker_source, &worker_inbox);
        assert_requester_inbox_contains_worker_response(
            requester_inbox.drain(),
            family,
            request.correlation_id,
            request.body.as_ref(),
        );
    });
    stress_config::record_completed(ctx, iterations);
}

#[stress_test(tier = 4)]
fn should_complete_tcp_request_response(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "tcp_e2e");
    ctx.parameter("batch_size", "single_roundtrip");
    ctx.parameter("worker_count", "1");

    let family = RouteFamily::new(1);
    let subscribe_frame =
        build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, TIER4_WORKER_MAX_CONCURRENT);
    let request_frame = build_network_request_frame(SERVICE_ROUTE, b"ping", family);

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

    let mut worker_client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect worker");
    runtime
        .block_on(worker_client.send_frame(&subscribe_frame))
        .expect("subscribe");
    let _ = runtime.block_on(worker_client.recv_frame(RESPONSE_TIMEOUT_MS)); // subscribe ack

    let mut requester_client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect requester");

    let worker_handle = {
        let rt = shared_bench_runtime();
        rt.spawn(async move {
            loop {
                let Ok(frame) = worker_client.recv_frame_bytes_without_timeout().await else {
                    break;
                };
                if let Some(req) = try_parse_rpc_request_frame_parts(&frame) {
                    let resp_frame = build_rpc_response_frame_bytes(req.correlation_id, req.body);
                    let _ = worker_client.send_frame_bytes(resp_frame).await;
                }
            }
        })
    };

    let iterations = ctx.measure_workload(|| {
        runtime
            .block_on(async {
                tokio::time::timeout(
                    Duration::from_millis(RESPONSE_TIMEOUT_MS),
                    request_until_worker_response_tcp(
                        &mut requester_client,
                        &request_frame,
                        family,
                    ),
                )
                .await
            })
            .expect("rpc tcp response timeout");
    });
    stress_config::record_completed(ctx, iterations);

    worker_handle.abort();
    let _ = runtime.block_on(worker_handle);
    runtime
        .block_on(requester_client.close())
        .expect("close requester tcp");
    runtime
        .block_on(server.shutdown())
        .expect("shutdown server");
}

#[stress_test(tier = 4)]
fn should_complete_ws_request_response(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "ws_e2e");
    ctx.parameter("batch_size", "single_roundtrip");
    ctx.parameter("worker_count", "1");

    let family = RouteFamily::new(1);
    let subscribe_frame =
        build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, TIER4_WORKER_MAX_CONCURRENT);
    let request_frames = build_network_request_frame_ring(
        SERVICE_ROUTE,
        b"ping",
        family,
        MULTICLIENT_REQUEST_FRAME_RING_SIZE,
    );

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

    let mut worker_client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect worker ws");
    runtime
        .block_on(worker_client.send_frame(&subscribe_frame))
        .expect("subscribe");
    let _ = runtime.block_on(worker_client.recv_frame(RESPONSE_TIMEOUT_MS));

    let mut requester_client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect requester ws");

    let worker_handle = {
        let rt = shared_bench_runtime();
        rt.spawn(async move {
            loop {
                let Ok(frame) = worker_client.recv_frame_bytes_without_timeout().await else {
                    break;
                };
                if let Some(req) = try_parse_rpc_request_frame_parts(&frame) {
                    let resp_frame = build_rpc_response_frame_bytes(req.correlation_id, req.body);
                    let _ = worker_client.send_frame_bytes(resp_frame).await;
                }
            }
        })
    };

    let mut next_request_index = 0usize;
    let iterations = ctx.measure_workload(|| {
        for _ in 0..WS_ROUNDTRIPS_PER_ITERATION {
            let request_frame = &request_frames[next_request_index];
            next_request_index = (next_request_index + 1) % request_frames.len();
            runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_millis(RESPONSE_TIMEOUT_MS),
                        request_until_worker_response_ws(
                            &mut requester_client,
                            request_frame,
                            family,
                        ),
                    )
                    .await
                })
                .expect("rpc websocket response timeout");
        }
    });
    stress_config::record_completed(ctx, WS_ROUNDTRIPS_PER_ITERATION as u64 * iterations);

    worker_handle.abort();
    let _ = runtime.block_on(worker_handle);

    runtime
        .block_on(requester_client.close())
        .expect("close requester ws");
    runtime
        .block_on(server.shutdown())
        .expect("shutdown server");
}

#[stress_test(tier = 4)]
fn should_complete_multiclient_concurrent_requests(ctx: &mut StressContext) {
    measure_multiclient_concurrent_requests(ctx, 1, "concurrent_requests");
}

#[stress_test(tier = 4)]
fn should_complete_multiclient_concurrent_requests_4_workers(ctx: &mut StressContext) {
    measure_multiclient_concurrent_requests(ctx, 4, "concurrent_requests");
}

#[stress_test(tier = 4)]
fn should_complete_multiclient_concurrent_requests_8_workers(ctx: &mut StressContext) {
    measure_multiclient_concurrent_requests(ctx, 8, "concurrent_requests");
}

stress_main!();
