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
    build_rpc_request, build_rpc_response_frame, build_rpc_subscribe, create_bench_rpc_sink,
    extract_single_tlv_field, register_session_queue_sink, route_frame, shared_bench_runtime,
    FrameQueueSink,
};
use fitz::domains::rpc::protocol::{RpcMessage, RpcRequest, RpcResponse};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::frame_context::FrameContext;
use fitz::protocol::rpc_codec::encode_response_message;
use fitz::protocol::rpc_codec::parse_request;
use fitz::protocol::tlv::MessageType;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

const SERVICE_ROUTE: &str = "rpc://tier4/service";
const REQUESTER_SESSION_ID: u64 = 1;
const WORKER_SESSION_ID: u64 = 2;
const RESPONSE_TIMEOUT_MS: u64 = 2_000;
const MULTICLIENT_COUNT: usize = 10;
const MULTICLIENT_REQUEST_FRAME_RING_SIZE: usize = 512;

type SharedWsClient = Arc<Mutex<TestWebSocketClient>>;

struct NetworkRequestFrame {
    frame: Vec<u8>,
    correlation_id: Uuid,
    body: Bytes,
}

fn try_parse_rpc_request_frame(frame: &[u8], family: RouteFamily) -> Option<RpcRequest> {
    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser.next_field_ref()?;
    if msg_type != 302 {
        return None;
    }

    let frame_ctx = FrameContext::new(
        REQUESTER_SESSION_ID,
        ChannelId::Rpc,
        MessageType::new(msg_type),
        Bytes::new(),
        family,
    );

    match parse_request(&frame_ctx, payload, family) {
        Ok(RpcMessage::Request(request)) => Some(request),
        _ => None,
    }
}

fn build_network_request_frame(
    route: &str,
    payload: &[u8],
    family: RouteFamily,
) -> NetworkRequestFrame {
    let frame = build_rpc_request(route, payload);
    let request = try_parse_rpc_request_frame(&frame, family).expect("rpc request frame");

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

fn try_parse_rpc_worker_response_frame(frame: &[u8], family: RouteFamily) -> Option<RpcResponse> {
    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser.next_field_ref()?;
    if msg_type != 303 {
        return None;
    }

    let frame_ctx = FrameContext::new(
        REQUESTER_SESSION_ID,
        ChannelId::Rpc,
        MessageType::new(msg_type),
        Bytes::new(),
        family,
    );

    match parse_request(&frame_ctx, payload, family) {
        Ok(RpcMessage::Response(response)) => Some(response),
        _ => None,
    }
}

fn assert_rpc_worker_response(
    response: &RpcResponse,
    expected_correlation_id: Uuid,
    expected_body: &[u8],
) {
    assert_eq!(
        response.correlation_id, expected_correlation_id,
        "unexpected rpc correlation id"
    );
    assert_eq!(response.seq, 0, "single-response bench should emit seq 0");
    assert_eq!(
        response.body.as_ref(),
        expected_body,
        "unexpected rpc response body"
    );
    assert!(
        response.stream_end,
        "single-response bench should end the stream"
    );
}

fn assert_requester_inbox_contains_worker_response(
    frames: Vec<FrameContext>,
    family: RouteFamily,
    expected_correlation_id: Uuid,
    expected_body: &[u8],
) {
    for frame in frames {
        if frame.msg_type.as_u16() != 303 {
            continue;
        }

        match parse_request(&frame, &frame.payload, family) {
            Ok(RpcMessage::Response(response)) => {
                assert_rpc_worker_response(&response, expected_correlation_id, expected_body);
                return;
            }
            Ok(other) => panic!("expected rpc response frame, found {other:?}"),
            Err(error) => panic!("failed to parse rpc response frame: {error}"),
        }
    }

    panic!("expected worker rpc response in requester inbox");
}

async fn request_until_worker_response_tcp(
    client: &mut TestClient,
    request_frame: &NetworkRequestFrame,
    family: RouteFamily,
) {
    client
        .send_frame(&request_frame.frame)
        .await
        .expect("send rpc request");

    for _ in 0..4 {
        let frame = client
            .recv_frame(RESPONSE_TIMEOUT_MS)
            .await
            .expect("receive rpc response");
        if let Some(response) = try_parse_rpc_worker_response_frame(&frame, family) {
            assert_rpc_worker_response(
                &response,
                request_frame.correlation_id,
                request_frame.body.as_ref(),
            );
            return;
        }
    }

    panic!("expected worker rpc response frame over tcp");
}

async fn request_until_worker_response_ws(
    client: &mut TestWebSocketClient,
    request_frame: &NetworkRequestFrame,
    family: RouteFamily,
) {
    client
        .send_frame(&request_frame.frame)
        .await
        .expect("send rpc request");

    for _ in 0..4 {
        let frame = client
            .recv_frame(RESPONSE_TIMEOUT_MS)
            .await
            .expect("receive rpc response");
        if let Some(response) = try_parse_rpc_worker_response_frame(&frame, family) {
            assert_rpc_worker_response(
                &response,
                request_frame.correlation_id,
                request_frame.body.as_ref(),
            );
            return;
        }
    }

    panic!("expected worker rpc response frame over websocket");
}

fn spawn_rpc_ws_workers(
    worker_clients: Vec<TestWebSocketClient>,
    family: RouteFamily,
) -> Vec<tokio::task::JoinHandle<()>> {
    worker_clients
        .into_iter()
        .map(|mut worker_client| {
            let rt = shared_bench_runtime();
            rt.spawn(async move {
                loop {
                    let frame = match worker_client.recv_frame(RESPONSE_TIMEOUT_MS).await {
                        Ok(frame) => frame,
                        Err(_) => continue,
                    };

                    if let Some(req) = try_parse_rpc_request_frame(&frame, family) {
                        let resp_frame =
                            build_rpc_response_frame(req.correlation_id, req.body.as_ref());
                        let _ = worker_client.send_frame(&resp_frame).await;
                    }
                }
            })
        })
        .collect()
}

fn measure_multiclient_concurrent_requests(
    ctx: &mut StressContext,
    worker_count: usize,
    scenario: &'static str,
) {
    ctx.tag("layer", "multiclient");
    ctx.tag("scenario", scenario);
    ctx.tag("measurement_scope", "ws_multiclient_e2e");
    ctx.tag("batch_size", "10_clients_1_roundtrip_each");
    ctx.tag("client_count", MULTICLIENT_COUNT.to_string());
    ctx.tag("worker_count", worker_count.to_string());

    let family = RouteFamily::new(1);
    let subscribe_frame = build_rpc_subscribe(SERVICE_ROUTE);

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

    let worker_clients: Vec<TestWebSocketClient> = (0..worker_count)
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

    let clients: Vec<SharedWsClient> = (0..MULTICLIENT_COUNT)
        .map(|_| {
            let client = runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect ws");
            Arc::new(Mutex::new(client))
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
    let worker_handles = spawn_rpc_ws_workers(worker_clients, family);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            let request_index = next_request_index;
            next_request_index = (next_request_index + 1) % MULTICLIENT_REQUEST_FRAME_RING_SIZE;

            runtime.block_on(futures::future::join_all(
                clients
                    .iter()
                    .zip(request_frames.iter())
                    .map(|(client, frames)| {
                        let request_frame = &frames[request_index];
                        async move {
                            let mut ws_client = client.lock().await;
                            request_until_worker_response_ws(&mut ws_client, request_frame, family)
                                .await;
                        }
                    }),
            ));
        },
    );
    ctx.set_elements(MULTICLIENT_COUNT as u64 * iterations as u64);

    for worker_handle in worker_handles {
        worker_handle.abort();
    }

    let _closed: Vec<_> = runtime.block_on(futures::future::join_all(clients.iter().map(
        |client| async move {
            let mut ws_client = client.lock().await;
            ws_client.close().await.expect("close ws client");
        },
    )));
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

    let subscribe_frame = build_rpc_subscribe(SERVICE_ROUTE);
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
            match frame.msg_type.as_u16() {
                302 => {
                    handled_request = true;
                    if let Ok(RpcMessage::Request(req)) =
                        parse_request(&frame, &frame.payload, family)
                    {
                        let response = RpcResponse::single(req.correlation_id, req.body.clone());
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
                304 => {}
                _ => {}
            }
        }

        if !handled_request {
            break;
        }
    }
}

#[stress_test]
fn should_complete_direct_request(ctx: &mut StressContext) {
    ctx.tag("layer", "direct");
    ctx.tag("scenario", "request_response");
    ctx.tag("measurement_scope", "direct_inproc");
    ctx.tag("batch_size", "single_roundtrip");
    ctx.tag("worker_count", "1");

    let request = build_network_request_frame(SERVICE_ROUTE, b"ping", RouteFamily::new(1));
    let (router, family, requester_source, requester_inbox, worker_source, worker_inbox) =
        setup_rpc_sink();
    let (request_msg_type, request_payload) = extract_single_tlv_field(&request.frame);

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
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
        },
    );
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_encoded_request(ctx: &mut StressContext) {
    ctx.tag("layer", "encoded");
    ctx.tag("scenario", "request_response");
    ctx.tag("measurement_scope", "encoded_inproc");
    ctx.tag("batch_size", "single_roundtrip");
    ctx.tag("worker_count", "1");

    let request = build_network_request_frame(SERVICE_ROUTE, b"ping", RouteFamily::new(1));
    let (router, family, requester_source, requester_inbox, worker_source, worker_inbox) =
        setup_rpc_sink();
    let request_frame = &request.frame;

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
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
        },
    );
    ctx.set_elements(iterations as u64);
}

#[stress_test]
fn should_complete_tcp_request_response(ctx: &mut StressContext) {
    ctx.tag("layer", "tcp");
    ctx.tag("scenario", "request_response");
    ctx.tag("measurement_scope", "tcp_e2e");
    ctx.tag("batch_size", "single_roundtrip");
    ctx.tag("worker_count", "1");

    let family = RouteFamily::new(1);
    let subscribe_frame = build_rpc_subscribe(SERVICE_ROUTE);
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
                let frame = match worker_client.recv_frame(RESPONSE_TIMEOUT_MS).await {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                if let Some(req) = try_parse_rpc_request_frame(&frame, family) {
                    let resp_frame =
                        build_rpc_response_frame(req.correlation_id, req.body.as_ref());
                    let _ = worker_client.send_frame(&resp_frame).await;
                }
            }
        })
    };

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            runtime.block_on(request_until_worker_response_tcp(
                &mut requester_client,
                &request_frame,
                family,
            ));
        },
    );
    ctx.set_elements(iterations as u64);

    worker_handle.abort();
}

#[stress_test]
fn should_complete_ws_request_response(ctx: &mut StressContext) {
    ctx.tag("layer", "websocket");
    ctx.tag("scenario", "request_response");
    ctx.tag("measurement_scope", "ws_e2e");
    ctx.tag("batch_size", "single_roundtrip");
    ctx.tag("worker_count", "1");

    let family = RouteFamily::new(1);
    let subscribe_frame = build_rpc_subscribe(SERVICE_ROUTE);
    let request_frame = build_network_request_frame(SERVICE_ROUTE, b"ping", family);

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
                let frame = match worker_client.recv_frame(RESPONSE_TIMEOUT_MS).await {
                    Ok(f) => f,
                    Err(_) => continue,
                };
                if let Some(req) = try_parse_rpc_request_frame(&frame, family) {
                    let resp_frame =
                        build_rpc_response_frame(req.correlation_id, req.body.as_ref());
                    let _ = worker_client.send_frame(&resp_frame).await;
                }
            }
        })
    };

    let iterations = ctx.measure_for(
        stress_config::BenchConfig::default().measure_duration,
        || {
            runtime.block_on(request_until_worker_response_ws(
                &mut requester_client,
                &request_frame,
                family,
            ));
        },
    );
    ctx.set_elements(iterations as u64);

    worker_handle.abort();

    runtime
        .block_on(requester_client.close())
        .expect("close requester ws");
}

#[stress_test]
fn should_complete_multiclient_concurrent_requests(ctx: &mut StressContext) {
    measure_multiclient_concurrent_requests(ctx, 1, "concurrent_requests");
}

#[stress_test]
fn should_complete_multiclient_concurrent_requests_4_workers(ctx: &mut StressContext) {
    measure_multiclient_concurrent_requests(ctx, 4, "concurrent_requests");
}

#[stress_test]
fn should_complete_multiclient_concurrent_requests_8_workers(ctx: &mut StressContext) {
    measure_multiclient_concurrent_requests(ctx, 8, "concurrent_requests");
}

stress_main!();
