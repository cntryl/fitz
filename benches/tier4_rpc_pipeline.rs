#[path = "tier4_rpc_support.rs"]
mod tier4_rpc_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_rpc_support::{
    build_network_request_frame_ring, complete_pipelined_requests_tcp_with_latencies,
    complete_pipelined_requests_ws_with_latencies, prebuilt_response_frames,
    service_pipelined_requests_tcp, service_pipelined_requests_ws,
};
use crate::tier4_support::{
    measure_operations, tag_dimensions, LayerKind, StorageProfile, Tier4Dimensions,
};
use cntryl_stress::{stress, stress_main, StressContext};
use fitz::benchkit::{build_rpc_subscribe_with_max_concurrent, shared_bench_runtime};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{TestClient, TestServer, TestWebSocketClient};
use futures_util::future::try_join_all;

const SERVICE_ROUTE: &str = "rpc://tier4/pipeline";
const PIPELINE_PAYLOAD_SIZE: usize = 1_024;
const PIPELINE_TIMEOUT_MS: u64 = 10_000;
const WORKER_MAX_CONCURRENT: u32 = 1_024;

fn dimensions(layer: LayerKind, client_count: usize) -> Tier4Dimensions<'static> {
    Tier4Dimensions {
        domain: "rpc",
        scenario: "validated_pipelined_responses",
        storage_profile: StorageProfile::Memory,
        layer,
        write_mode: "not_applicable",
        payload_size: PIPELINE_PAYLOAD_SIZE,
        history_depth: 0,
        read_limit: 0,
        read_scope: "none",
        route_count: 1,
        filter_selectivity: "not_applicable",
        client_count,
        workload_mix: "concurrent_pipelined_request_response",
        completed_unit: "validated_response",
        gate_class: if client_count == 1 {
            "characterization"
        } else {
            "regression_gate"
        },
    }
}

fn tag_pipeline(
    ctx: &mut StressContext,
    layer: LayerKind,
    client_count: usize,
    inflight_per_client: usize,
) {
    tag_dimensions(ctx, &dimensions(layer, client_count));
    ctx.parameter("worker_count", 1);
    ctx.parameter("inflight_per_client", inflight_per_client);
    ctx.parameter(
        "responses_per_iteration",
        client_count * inflight_per_client,
    );
    ctx.parameter("completion_mode", "all_correlations_and_bodies_validated");
}

fn measure_tcp_pipeline(
    ctx: &mut StressContext,
    client_count: usize,
    inflight_per_client: usize,
    measurement: &'static str,
) {
    let layer = if client_count == 1 {
        LayerKind::Tcp
    } else {
        LayerKind::TcpMultiClient
    };
    tag_pipeline(ctx, layer, client_count, inflight_per_client);
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start RPC TCP pipeline server");
    let subscribe = build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, WORKER_MAX_CONCURRENT);
    let mut worker = runtime
        .block_on(TestClient::new(server.tcp_addr))
        .expect("connect RPC TCP pipeline worker");
    runtime
        .block_on(worker.request(&subscribe, PIPELINE_TIMEOUT_MS))
        .expect("register RPC TCP pipeline worker");
    let mut clients = (0..client_count)
        .map(|_| {
            runtime
                .block_on(TestClient::new(server.tcp_addr))
                .expect("connect RPC TCP pipeline requester")
        })
        .collect::<Vec<_>>();
    let payload = vec![0xD7; PIPELINE_PAYLOAD_SIZE];
    let request_batches = (0..client_count)
        .map(|_| {
            build_network_request_frame_ring(
                SERVICE_ROUTE,
                &payload,
                RouteFamily::new(1),
                inflight_per_client,
            )
        })
        .collect::<Vec<_>>();
    let responses = prebuilt_response_frames(&request_batches);
    let operations = client_count * inflight_per_client;
    let logical_operations = u64::try_from(operations).expect("RPC operation count fits u64");

    measure_operations(ctx, measurement, logical_operations, |latencies| {
        let per_client = runtime
            .block_on(async {
                let service = service_pipelined_requests_tcp(
                    &mut worker,
                    &responses,
                    operations,
                    PIPELINE_TIMEOUT_MS,
                );
                let requests = try_join_all(clients.iter_mut().zip(&request_batches).map(
                    |(client, batch)| {
                        complete_pipelined_requests_tcp_with_latencies(
                            client,
                            batch,
                            PIPELINE_TIMEOUT_MS,
                        )
                    },
                ));
                let (_, observations) = tokio::try_join!(service, requests)?;
                Ok::<_, String>(observations)
            })
            .expect("complete RPC TCP pipeline");
        latencies.extend(per_client.into_iter().flatten());
    });

    runtime
        .block_on(worker.close())
        .expect("close RPC TCP pipeline worker");
    for client in clients {
        runtime
            .block_on(client.close())
            .expect("close RPC TCP pipeline requester");
    }
    runtime
        .block_on(server.shutdown())
        .expect("shutdown RPC TCP pipeline server");
}

fn measure_ws_pipeline(
    ctx: &mut StressContext,
    client_count: usize,
    inflight_per_client: usize,
    measurement: &'static str,
) {
    let layer = if client_count == 1 {
        LayerKind::WebSocket
    } else {
        LayerKind::WebSocketMultiClient
    };
    tag_pipeline(ctx, layer, client_count, inflight_per_client);
    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .expect("start RPC WS pipeline server");
    let subscribe = build_rpc_subscribe_with_max_concurrent(SERVICE_ROUTE, WORKER_MAX_CONCURRENT);
    let mut worker = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .expect("connect RPC WS pipeline worker");
    runtime
        .block_on(worker.request(&subscribe, PIPELINE_TIMEOUT_MS))
        .expect("register RPC WS pipeline worker");
    let mut clients = (0..client_count)
        .map(|_| {
            runtime
                .block_on(TestWebSocketClient::connect(&format!(
                    "ws://{}",
                    server.ws_addr
                )))
                .expect("connect RPC WS pipeline requester")
        })
        .collect::<Vec<_>>();
    let payload = vec![0xD7; PIPELINE_PAYLOAD_SIZE];
    let request_batches = (0..client_count)
        .map(|_| {
            build_network_request_frame_ring(
                SERVICE_ROUTE,
                &payload,
                RouteFamily::new(1),
                inflight_per_client,
            )
        })
        .collect::<Vec<_>>();
    let responses = prebuilt_response_frames(&request_batches);
    let operations = client_count * inflight_per_client;
    let logical_operations = u64::try_from(operations).expect("RPC operation count fits u64");

    measure_operations(ctx, measurement, logical_operations, |latencies| {
        let per_client = runtime
            .block_on(async {
                let service = service_pipelined_requests_ws(
                    &mut worker,
                    &responses,
                    operations,
                    PIPELINE_TIMEOUT_MS,
                );
                let requests = try_join_all(clients.iter_mut().zip(&request_batches).map(
                    |(client, batch)| {
                        complete_pipelined_requests_ws_with_latencies(
                            client,
                            batch,
                            PIPELINE_TIMEOUT_MS,
                        )
                    },
                ));
                let (_, observations) = tokio::try_join!(service, requests)?;
                Ok::<_, String>(observations)
            })
            .expect("complete RPC WS pipeline");
        latencies.extend(per_client.into_iter().flatten());
    });

    runtime
        .block_on(worker.close())
        .expect("close RPC WS pipeline worker");
    for mut client in clients {
        runtime
            .block_on(client.close())
            .expect("close RPC WS pipeline requester");
    }
    runtime
        .block_on(server.shutdown())
        .expect("shutdown RPC WS pipeline server");
}

#[stress(tier = 4)]
fn should_characterize_tcp_32_inflight_pipeline(ctx: &mut StressContext) {
    measure_tcp_pipeline(ctx, 1, 32, "tcp_32_inflight_pipeline");
}

#[stress(tier = 4)]
fn should_characterize_ws_32_inflight_pipeline(ctx: &mut StressContext) {
    measure_ws_pipeline(ctx, 1, 32, "ws_32_inflight_pipeline");
}

#[stress(tier = 4)]
fn should_measure_tcp_8_client_pipeline(ctx: &mut StressContext) {
    measure_tcp_pipeline(ctx, 8, 16, "tcp_8_client_pipeline");
}

#[stress(tier = 4)]
fn should_measure_ws_8_client_pipeline(ctx: &mut StressContext) {
    measure_ws_pipeline(ctx, 8, 16, "ws_8_client_pipeline");
}

stress_main!();
