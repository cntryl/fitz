//! RPC domain tier 4 integration benchmarks using stress
//!
//! **TIER 4 GOAL: Identify E2E performance cliffs**
//!
//! Layers: direct, encoded (codec decode path), tcp, websocket, multiclient (concurrent).
//! RPC tier4 tests full request -> worker dispatch -> response over the wire where applicable.

#[path = "stress_config.rs"]
mod stress_config;
#[path = "tier4_rpc_support.rs"]
mod tier4_rpc_support;

use stress_config::StressContextExt;
use tier4_rpc_support::{
    assert_requester_inbox_contains_worker_response, build_network_request_frame,
    build_network_request_frame_ring, complete_pipelined_requests_tcp,
    complete_pipelined_requests_ws, request_all_multiclient_ws, request_until_worker_response_tcp,
    request_until_worker_response_ws, spawn_rpc_tcp_workers, spawn_rpc_ws_requesters,
    spawn_rpc_ws_workers, try_parse_rpc_request_payload_parts, NetworkRequestFrame,
};

use bytes::Bytes;
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{
    build_rpc_subscribe_with_max_concurrent, create_bench_rpc_sink, extract_single_tlv_field,
    register_session_queue_sink, route_frame, shared_bench_runtime, FrameQueueSink,
};
use fitz::domains::rpc::protocol::RpcResponse;
use fitz::protocol::frame::ChannelId;
use fitz::protocol::rpc_codec::encode_response_message;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use futures_util::future::try_join_all;
use std::sync::Arc;
use std::time::Duration;

const SERVICE_ROUTE: &str = "rpc://tier4/service";
const REQUESTER_SESSION_ID: u64 = 1;
const WORKER_SESSION_ID: u64 = 2;
const RESPONSE_TIMEOUT_MS: u64 = 2_000;
const DIRECT_ROUNDTRIPS_PER_ITERATION: u64 = 64;
const ENCODED_ROUNDTRIPS_PER_ITERATION: u64 = 64;
const MULTICLIENT_COUNT: usize = 10;
const MULTICLIENT_ROUNDS_PER_ITERATION: usize = 4;
const MULTICLIENT_REQUEST_FRAME_RING_SIZE: usize = 512;
const TCP_ROUNDTRIPS_PER_ITERATION: u64 = 16;
const WS_ROUNDTRIPS_PER_ITERATION: usize = 32;
const TIER4_WORKER_MAX_CONCURRENT: u32 = 32;
const PIPELINED_SINGLE_INFLIGHT: usize = 128;
const PIPELINED_CONCURRENT_INFLIGHT_PER_CLIENT: usize = 32;
const PIPELINED_CONCURRENT_CLIENT_COUNT: usize = 10;
const PIPELINED_CONCURRENT_WORKER_COUNT: usize = 4;
const PIPELINED_RESPONSE_TIMEOUT_MS: u64 = 10_000;

fn measure_multiclient_concurrent_requests(
    ctx: &mut StressContext,
    name: &str,
    worker_count: usize,
    scenario: &'static str,
) {
    ctx.parameter("layer", "multiclient");
    ctx.parameter("scenario", scenario);
    ctx.parameter("measurement_scope", "ws_multiclient_e2e");
    ctx.parameter("mode", "sync_concurrent");
    ctx.parameter("completion_mode", "response_wait");
    ctx.parameter("completed_unit", "roundtrips");
    ctx.parameter("inflight_per_client", "1");
    ctx.parameter(
        "batch_size",
        format!("10_clients_{MULTICLIENT_ROUNDS_PER_ITERATION}_roundtrips_each"),
    );
    ctx.parameter("client_count", MULTICLIENT_COUNT.to_string());
    ctx.parameter("worker_count", worker_count.to_string());

    let family = RouteFamily::new(1);
    let subscribe_frame =
        build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, TIER4_WORKER_MAX_CONCURRENT);

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
    let worker_handles = spawn_rpc_ws_workers(worker_clients);
    let (requester_drivers, completion_rx) =
        spawn_rpc_ws_requesters(clients, request_frames, RESPONSE_TIMEOUT_MS);

    let iterations = ctx.measure_workload(name, || {
        for _ in 0..MULTICLIENT_ROUNDS_PER_ITERATION {
            let request_index = next_request_index;
            next_request_index = (next_request_index + 1) % MULTICLIENT_REQUEST_FRAME_RING_SIZE;

            request_all_multiclient_ws(
                &requester_drivers,
                &completion_rx,
                request_index,
                RESPONSE_TIMEOUT_MS,
            );
        }
    });
    stress_config::record_completed(
        ctx,
        (MULTICLIENT_COUNT * MULTICLIENT_ROUNDS_PER_ITERATION) as u64 * iterations,
    );

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
                if let Some((correlation_id, body)) =
                    try_parse_rpc_request_payload_parts(&frame.payload)
                {
                    let response = RpcResponse::single(correlation_id, body);
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

#[stress(tier = 4)]
fn should_complete_direct_request(ctx: &mut StressContext) {
    ctx.parameter("layer", "direct");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "direct_inproc");
    ctx.parameter("mode", "sync_single_inflight");
    ctx.parameter("completion_mode", "response_wait");
    ctx.parameter("completed_unit", "roundtrips");
    ctx.parameter("inflight_per_client", "1");
    ctx.parameter(
        "batch_size",
        format!("{DIRECT_ROUNDTRIPS_PER_ITERATION}_roundtrips"),
    );
    ctx.parameter("worker_count", "1");

    let request = build_network_request_frame(SERVICE_ROUTE, b"ping", RouteFamily::new(1));
    let (router, family, requester_source, requester_inbox, worker_source, worker_inbox) =
        setup_rpc_sink();
    let (request_msg_type, request_payload) = extract_single_tlv_field(&request.frame);

    let iterations = ctx.measure_workload("complete_direct_request", || {
        for _ in 0..DIRECT_ROUNDTRIPS_PER_ITERATION {
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
                request.correlation_id,
                request.body.as_ref(),
            );
        }
    });
    stress_config::record_completed(ctx, DIRECT_ROUNDTRIPS_PER_ITERATION * iterations);
}

#[stress(tier = 4)]
fn should_complete_encoded_request(ctx: &mut StressContext) {
    ctx.parameter("layer", "encoded");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "encoded_inproc");
    ctx.parameter("mode", "sync_single_inflight");
    ctx.parameter("completion_mode", "response_wait");
    ctx.parameter("completed_unit", "roundtrips");
    ctx.parameter("inflight_per_client", "1");
    ctx.parameter(
        "batch_size",
        format!("{ENCODED_ROUNDTRIPS_PER_ITERATION}_roundtrips"),
    );
    ctx.parameter("worker_count", "1");

    let request = build_network_request_frame(SERVICE_ROUTE, b"ping", RouteFamily::new(1));
    let (router, family, requester_source, requester_inbox, worker_source, worker_inbox) =
        setup_rpc_sink();
    let request_frame = &request.frame;

    let iterations = ctx.measure_workload("complete_encoded_request", || {
        for _ in 0..ENCODED_ROUNDTRIPS_PER_ITERATION {
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
                request.correlation_id,
                request.body.as_ref(),
            );
        }
    });
    stress_config::record_completed(ctx, ENCODED_ROUNDTRIPS_PER_ITERATION * iterations);
}

#[stress(tier = 4)]
fn should_complete_tcp_request_response(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "tcp_e2e");
    ctx.parameter("mode", "sync_single_inflight");
    ctx.parameter("completion_mode", "response_wait");
    ctx.parameter("completed_unit", "roundtrips");
    ctx.parameter("inflight_per_client", "1");
    ctx.parameter("client_count", "1");
    ctx.parameter(
        "batch_size",
        format!("{TCP_ROUNDTRIPS_PER_ITERATION}_roundtrips"),
    );
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
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect worker");
    runtime
        .block_on(worker_client.send_frame(&subscribe_frame))
        .expect("subscribe");
    let _ = runtime.block_on(worker_client.recv_frame(RESPONSE_TIMEOUT_MS)); // subscribe ack

    let mut requester_client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect requester");

    let worker_handles = spawn_rpc_tcp_workers(vec![worker_client]);

    let mut next_request_index = 0usize;
    let iterations = ctx.measure_workload("complete_tcp_request_response", || {
        runtime
            .block_on(async {
                tokio::time::timeout(Duration::from_millis(RESPONSE_TIMEOUT_MS), async {
                    for _ in 0..TCP_ROUNDTRIPS_PER_ITERATION {
                        let request_frame = &request_frames[next_request_index];
                        next_request_index = (next_request_index + 1) % request_frames.len();
                        request_until_worker_response_tcp(&mut requester_client, request_frame)
                            .await;
                    }
                })
                .await
            })
            .expect("rpc tcp response timeout");
    });
    stress_config::record_completed(ctx, TCP_ROUNDTRIPS_PER_ITERATION * iterations);

    for worker_handle in worker_handles {
        worker_handle.abort();
        let _ = runtime.block_on(worker_handle);
    }
    runtime
        .block_on(requester_client.close())
        .expect("close requester tcp");
    runtime
        .block_on(server.shutdown())
        .expect("shutdown server");
}

#[stress(tier = 4)]
fn should_complete_ws_request_response(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "ws_e2e");
    ctx.parameter("mode", "sync_single_inflight");
    ctx.parameter("completion_mode", "response_wait");
    ctx.parameter("completed_unit", "roundtrips");
    ctx.parameter("inflight_per_client", "1");
    ctx.parameter("client_count", "1");
    ctx.parameter(
        "batch_size",
        format!("{WS_ROUNDTRIPS_PER_ITERATION}_roundtrips"),
    );
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

    let worker_handles = spawn_rpc_ws_workers(vec![worker_client]);

    let mut next_request_index = 0usize;
    let iterations = ctx.measure_workload("complete_ws_request_response", || {
        for _ in 0..WS_ROUNDTRIPS_PER_ITERATION {
            let request_frame = &request_frames[next_request_index];
            next_request_index = (next_request_index + 1) % request_frames.len();
            runtime
                .block_on(async {
                    tokio::time::timeout(
                        Duration::from_millis(RESPONSE_TIMEOUT_MS),
                        request_until_worker_response_ws(&mut requester_client, request_frame),
                    )
                    .await
                })
                .expect("rpc websocket response timeout");
        }
    });
    stress_config::record_completed(ctx, WS_ROUNDTRIPS_PER_ITERATION as u64 * iterations);

    for worker_handle in worker_handles {
        worker_handle.abort();
        let _ = runtime.block_on(worker_handle);
    }

    runtime
        .block_on(requester_client.close())
        .expect("close requester ws");
    runtime
        .block_on(server.shutdown())
        .expect("shutdown server");
}

#[stress(tier = 4)]
fn should_complete_tcp_pipelined_request_response(ctx: &mut StressContext) {
    ctx.parameter("layer", "tcp");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "tcp_pipelined_e2e");
    ctx.parameter("mode", "async_pipelined");
    ctx.parameter("completion_mode", "response_wait");
    ctx.parameter("completed_unit", "roundtrips");
    ctx.parameter("inflight_per_client", PIPELINED_SINGLE_INFLIGHT.to_string());
    ctx.parameter("client_count", "1");
    ctx.parameter("worker_count", "1");
    ctx.parameter(
        "batch_size",
        format!("{PIPELINED_SINGLE_INFLIGHT}_roundtrips"),
    );

    let family = RouteFamily::new(1);
    let subscribe_frame =
        build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, TIER4_WORKER_MAX_CONCURRENT);
    let request_frames =
        build_network_request_frame_ring(SERVICE_ROUTE, b"ping", family, PIPELINED_SINGLE_INFLIGHT);

    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

    let mut worker_client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect worker");
    runtime
        .block_on(worker_client.send_frame(&subscribe_frame))
        .expect("subscribe");
    let _ = runtime.block_on(worker_client.recv_frame(RESPONSE_TIMEOUT_MS));

    let mut requester_client = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect requester");
    let worker_handles = spawn_rpc_tcp_workers(vec![worker_client]);

    let iterations = ctx.measure_workload("complete_tcp_pipelined_request_response", || {
        runtime
            .block_on(complete_pipelined_requests_tcp(
                &mut requester_client,
                &request_frames,
                PIPELINED_RESPONSE_TIMEOUT_MS,
            ))
            .expect("tcp pipelined rpc responses");
    });
    stress_config::record_completed(ctx, PIPELINED_SINGLE_INFLIGHT as u64 * iterations);

    for worker_handle in worker_handles {
        worker_handle.abort();
        let _ = runtime.block_on(worker_handle);
    }
    runtime
        .block_on(requester_client.close())
        .expect("close requester tcp");
    runtime
        .block_on(server.shutdown())
        .expect("shutdown server");
}

#[stress(tier = 4)]
fn should_complete_ws_pipelined_request_response(ctx: &mut StressContext) {
    ctx.parameter("layer", "websocket");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "ws_pipelined_e2e");
    ctx.parameter("mode", "async_pipelined");
    ctx.parameter("completion_mode", "response_wait");
    ctx.parameter("completed_unit", "roundtrips");
    ctx.parameter("inflight_per_client", PIPELINED_SINGLE_INFLIGHT.to_string());
    ctx.parameter("client_count", "1");
    ctx.parameter("worker_count", "1");
    ctx.parameter(
        "batch_size",
        format!("{PIPELINED_SINGLE_INFLIGHT}_roundtrips"),
    );

    let family = RouteFamily::new(1);
    let subscribe_frame =
        build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, TIER4_WORKER_MAX_CONCURRENT);
    let request_frames =
        build_network_request_frame_ring(SERVICE_ROUTE, b"ping", family, PIPELINED_SINGLE_INFLIGHT);

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
    let worker_handles = spawn_rpc_ws_workers(vec![worker_client]);

    let iterations = ctx.measure_workload("complete_ws_pipelined_request_response", || {
        runtime
            .block_on(complete_pipelined_requests_ws(
                &mut requester_client,
                &request_frames,
                PIPELINED_RESPONSE_TIMEOUT_MS,
            ))
            .expect("websocket pipelined rpc responses");
    });
    stress_config::record_completed(ctx, PIPELINED_SINGLE_INFLIGHT as u64 * iterations);

    for worker_handle in worker_handles {
        worker_handle.abort();
        let _ = runtime.block_on(worker_handle);
    }
    runtime
        .block_on(requester_client.close())
        .expect("close requester ws");
    runtime
        .block_on(server.shutdown())
        .expect("shutdown server");
}

#[stress(tier = 4)]
fn should_complete_tcp_multiclient_pipelined_requests(ctx: &mut StressContext) {
    ctx.parameter("layer", "multiclient");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "tcp_multiclient_pipelined_e2e");
    ctx.parameter("mode", "concurrent_pipelined");
    ctx.parameter("completion_mode", "response_wait");
    ctx.parameter("completed_unit", "roundtrips");
    ctx.parameter(
        "inflight_per_client",
        PIPELINED_CONCURRENT_INFLIGHT_PER_CLIENT.to_string(),
    );
    ctx.parameter(
        "client_count",
        PIPELINED_CONCURRENT_CLIENT_COUNT.to_string(),
    );
    ctx.parameter(
        "worker_count",
        PIPELINED_CONCURRENT_WORKER_COUNT.to_string(),
    );
    ctx.parameter(
        "batch_size",
        format!(
            "{PIPELINED_CONCURRENT_CLIENT_COUNT}_clients_{PIPELINED_CONCURRENT_INFLIGHT_PER_CLIENT}_roundtrips_each"
        ),
    );

    let family = RouteFamily::new(1);
    let subscribe_frame =
        build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, TIER4_WORKER_MAX_CONCURRENT);
    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

    let worker_clients: Vec<TestClient> = (0..PIPELINED_CONCURRENT_WORKER_COUNT)
        .map(|_| {
            let mut worker_client = runtime
                .block_on(TestClient::new(server.tcp_addr))
                .expect("connect worker");
            runtime
                .block_on(worker_client.send_frame(&subscribe_frame))
                .expect("subscribe");
            let _ = runtime.block_on(worker_client.recv_frame(RESPONSE_TIMEOUT_MS));
            worker_client
        })
        .collect();
    let mut clients: Vec<TestClient> = (0..PIPELINED_CONCURRENT_CLIENT_COUNT)
        .map(|_| {
            runtime
                .block_on(TestClient::new(server.tcp_addr))
                .expect("connect requester")
        })
        .collect();
    let request_batches: Vec<Vec<NetworkRequestFrame>> = (0..PIPELINED_CONCURRENT_CLIENT_COUNT)
        .map(|_| {
            build_network_request_frame_ring(
                SERVICE_ROUTE,
                b"ping",
                family,
                PIPELINED_CONCURRENT_INFLIGHT_PER_CLIENT,
            )
        })
        .collect();
    let worker_handles = spawn_rpc_tcp_workers(worker_clients);

    let iterations = ctx.measure_workload("complete_tcp_multiclient_pipelined_requests", || {
        runtime
            .block_on(async {
                try_join_all(clients.iter_mut().zip(request_batches.iter()).map(
                    |(client, request_frames)| {
                        complete_pipelined_requests_tcp(
                            client,
                            request_frames,
                            PIPELINED_RESPONSE_TIMEOUT_MS,
                        )
                    },
                ))
                .await
            })
            .expect("tcp multiclient pipelined rpc responses");
    });
    stress_config::record_completed(
        ctx,
        (PIPELINED_CONCURRENT_CLIENT_COUNT * PIPELINED_CONCURRENT_INFLIGHT_PER_CLIENT) as u64
            * iterations,
    );

    for worker_handle in worker_handles {
        worker_handle.abort();
        let _ = runtime.block_on(worker_handle);
    }
    for client in clients {
        runtime
            .block_on(client.close())
            .expect("close requester tcp");
    }
    runtime
        .block_on(server.shutdown())
        .expect("shutdown server");
}

#[stress(tier = 4)]
fn should_complete_ws_multiclient_pipelined_requests(ctx: &mut StressContext) {
    ctx.parameter("layer", "multiclient");
    ctx.parameter("scenario", "request_response");
    ctx.parameter("measurement_scope", "ws_multiclient_pipelined_e2e");
    ctx.parameter("mode", "concurrent_pipelined");
    ctx.parameter("completion_mode", "response_wait");
    ctx.parameter("completed_unit", "roundtrips");
    ctx.parameter(
        "inflight_per_client",
        PIPELINED_CONCURRENT_INFLIGHT_PER_CLIENT.to_string(),
    );
    ctx.parameter(
        "client_count",
        PIPELINED_CONCURRENT_CLIENT_COUNT.to_string(),
    );
    ctx.parameter(
        "worker_count",
        PIPELINED_CONCURRENT_WORKER_COUNT.to_string(),
    );
    ctx.parameter(
        "batch_size",
        format!(
            "{PIPELINED_CONCURRENT_CLIENT_COUNT}_clients_{PIPELINED_CONCURRENT_INFLIGHT_PER_CLIENT}_roundtrips_each"
        ),
    );

    let family = RouteFamily::new(1);
    let subscribe_frame =
        build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, TIER4_WORKER_MAX_CONCURRENT);
    let runtime = shared_bench_runtime();
    let server = runtime.block_on(TestServer::start()).expect("start server");

    let worker_clients: Vec<TestWebSocketClient> = (0..PIPELINED_CONCURRENT_WORKER_COUNT)
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
    let mut clients: Vec<TestWebSocketClient> = (0..PIPELINED_CONCURRENT_CLIENT_COUNT)
        .map(|_| {
            runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect requester ws")
        })
        .collect();
    let request_batches: Vec<Vec<NetworkRequestFrame>> = (0..PIPELINED_CONCURRENT_CLIENT_COUNT)
        .map(|_| {
            build_network_request_frame_ring(
                SERVICE_ROUTE,
                b"ping",
                family,
                PIPELINED_CONCURRENT_INFLIGHT_PER_CLIENT,
            )
        })
        .collect();
    let worker_handles = spawn_rpc_ws_workers(worker_clients);

    let iterations = ctx.measure_workload("complete_ws_multiclient_pipelined_requests", || {
        runtime
            .block_on(async {
                try_join_all(clients.iter_mut().zip(request_batches.iter()).map(
                    |(client, request_frames)| {
                        complete_pipelined_requests_ws(
                            client,
                            request_frames,
                            PIPELINED_RESPONSE_TIMEOUT_MS,
                        )
                    },
                ))
                .await
            })
            .expect("websocket multiclient pipelined rpc responses");
    });
    stress_config::record_completed(
        ctx,
        (PIPELINED_CONCURRENT_CLIENT_COUNT * PIPELINED_CONCURRENT_INFLIGHT_PER_CLIENT) as u64
            * iterations,
    );

    for worker_handle in worker_handles {
        worker_handle.abort();
        let _ = runtime.block_on(worker_handle);
    }
    for mut client in clients {
        runtime
            .block_on(client.close())
            .expect("close requester ws");
    }
    runtime
        .block_on(server.shutdown())
        .expect("shutdown server");
}

#[stress(tier = 4)]
fn should_complete_multiclient_concurrent_requests(ctx: &mut StressContext) {
    measure_multiclient_concurrent_requests(
        ctx,
        "complete_multiclient_concurrent_requests",
        1,
        "concurrent_requests",
    );
}

#[stress(tier = 4)]
fn should_complete_multiclient_concurrent_requests_4_workers(ctx: &mut StressContext) {
    measure_multiclient_concurrent_requests(
        ctx,
        "complete_multiclient_concurrent_requests_4_workers",
        4,
        "concurrent_requests",
    );
}

#[stress(tier = 4)]
fn should_complete_multiclient_concurrent_requests_8_workers(ctx: &mut StressContext) {
    measure_multiclient_concurrent_requests(
        ctx,
        "complete_multiclient_concurrent_requests_8_workers",
        8,
        "concurrent_requests",
    );
}

stress_main!();
