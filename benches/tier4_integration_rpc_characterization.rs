#[path = "characterization_support.rs"]
mod characterization_support;

use characterization_support::{
    compute_stats, delta_per_unit, detect_cliff, measure_idle_ws_connection_cost, parse_bench_args,
    parse_counts, stable_working_set_bytes, write_report, ClientRun, DomainReport,
    ProductionReport, ScalingPoint,
};
use fitz::benchkit::{
    build_rpc_request, build_rpc_response_frame, build_rpc_subscribe, shared_bench_runtime,
};
use fitz::domains::rpc::protocol::{RpcMessage, RpcRequest, RpcResponse};
use fitz::protocol::frame::ChannelId;
use fitz::protocol::frame_context::FrameContext;
use fitz::protocol::rpc_codec::parse_request;
use fitz::protocol::tlv::MessageType;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::transport::TlvFrameParser;
use fitz::testkit::{TestServer, TestWebSocketClient};
use futures::future::join_all;
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const RESPONSE_TIMEOUT_MS: u64 = 2_000;
const RPC_PENDING_TIMEOUT_SECS: u64 = 60;
const RPC_SCALING_WORKER_COUNT: usize = 4;
const UNIQUE_FRAME_RING: usize = 512;

#[derive(Debug, Clone)]
struct NetworkRequestFrame {
    frame: Vec<u8>,
    correlation_id: Uuid,
    body: Vec<u8>,
}

fn try_parse_rpc_request_frame(frame: &[u8], family: RouteFamily) -> Option<RpcRequest> {
    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser.next_field_ref()?;
    if msg_type != 302 {
        return None;
    }

    let frame_ctx = FrameContext::new(
        1,
        ChannelId::Rpc,
        MessageType::new(msg_type),
        bytes::Bytes::new(),
        family,
    );

    match parse_request(&frame_ctx, payload, family) {
        Ok(RpcMessage::Request(request)) => Some(request),
        _ => None,
    }
}

fn try_parse_rpc_worker_response_frame(frame: &[u8], family: RouteFamily) -> Option<RpcResponse> {
    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser.next_field_ref()?;
    if msg_type != 303 {
        return None;
    }

    let frame_ctx = FrameContext::new(
        1,
        ChannelId::Rpc,
        MessageType::new(msg_type),
        bytes::Bytes::new(),
        family,
    );

    match parse_request(&frame_ctx, payload, family) {
        Ok(RpcMessage::Response(response)) => Some(response),
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
        body: request.body.to_vec(),
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
    response: &RpcResponse,
    expected_correlation_id: Uuid,
    expected_body: &[u8],
) -> Result<(), String> {
    if response.correlation_id != expected_correlation_id {
        return Err("unexpected rpc correlation id".to_string());
    }
    if response.body.as_ref() != expected_body {
        return Err("unexpected rpc response body".to_string());
    }
    if !response.stream_end {
        return Err("expected single-response stream_end".to_string());
    }
    Ok(())
}

async fn request_until_worker_response_ws(
    client: &mut TestWebSocketClient,
    request_frame: &NetworkRequestFrame,
    family: RouteFamily,
) -> Result<(), String> {
    client
        .send_frame(&request_frame.frame)
        .await
        .map_err(|error| error.to_string())?;

    for _ in 0..4 {
        let frame = client
            .recv_frame(RESPONSE_TIMEOUT_MS)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(response) = try_parse_rpc_worker_response_frame(&frame, family) {
            return assert_rpc_worker_response(
                &response,
                request_frame.correlation_id,
                &request_frame.body,
            );
        }
    }

    Err("expected worker rpc response frame".to_string())
}

async fn drain_rpc_request_without_response(
    requester: &mut TestWebSocketClient,
    request_frame: &NetworkRequestFrame,
) -> Result<(), String> {
    requester
        .send_frame(&request_frame.frame)
        .await
        .map_err(|error| error.to_string())?;
    let _ = requester.recv_frame(200).await;
    Ok(())
}

fn spawn_rpc_ws_workers(
    worker_clients: Vec<TestWebSocketClient>,
    family: RouteFamily,
    respond: bool,
) -> Vec<tokio::task::JoinHandle<()>> {
    worker_clients
        .into_iter()
        .map(|mut worker_client| {
            let runtime = shared_bench_runtime();
            runtime.spawn(async move {
                loop {
                    let frame = match worker_client.recv_frame(RESPONSE_TIMEOUT_MS).await {
                        Ok(frame) => frame,
                        Err(_) => continue,
                    };

                    if let Some(request) = try_parse_rpc_request_frame(&frame, family) {
                        if respond {
                            let response_frame = build_rpc_response_frame(
                                request.correlation_id,
                                request.body.as_ref(),
                            );
                            let _ = worker_client.send_frame(&response_frame).await;
                        }
                    }
                }
            })
        })
        .collect()
}

fn measure_rpc(
    single_duration: Duration,
    scaling_duration: Duration,
    client_counts: &[usize],
    resource_samples: usize,
    idle_connection_cost: i64,
) -> Result<DomainReport, String> {
    let runtime = shared_bench_runtime();
    let family = RouteFamily::new(1);
    let service_route = "rpc://characterization/service";
    let subscribe_frame = build_rpc_subscribe(service_route);

    let server = runtime
        .block_on(TestServer::start())
        .map_err(|error| error.to_string())?;
    let mut worker = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .map_err(|error| error.to_string())?;
    runtime
        .block_on(worker.send_frame(&subscribe_frame))
        .map_err(|error| error.to_string())?;
    let _ = runtime.block_on(worker.recv_frame(RESPONSE_TIMEOUT_MS));
    let worker_handles = spawn_rpc_ws_workers(vec![worker], family, true);
    let mut requester = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .map_err(|error| error.to_string())?;
    let request_ring =
        build_network_request_frame_ring(service_route, b"ping", family, UNIQUE_FRAME_RING);

    let started = Instant::now();
    let deadline = started + single_duration;
    let mut single_latencies = Vec::new();
    let mut single_errors = 0usize;
    let mut next_index = 0usize;
    while Instant::now() < deadline {
        let request_frame = &request_ring[next_index];
        next_index = (next_index + 1) % request_ring.len();
        let op_start = Instant::now();
        match runtime.block_on(request_until_worker_response_ws(
            &mut requester,
            request_frame,
            family,
        )) {
            Ok(()) => single_latencies.push(op_start.elapsed().as_micros() as u64),
            Err(_) => single_errors += 1,
        }
    }
    let single_client_ws = compute_stats(
        "request_response",
        started.elapsed(),
        single_latencies,
        1,
        single_errors,
    );
    let _ = runtime.block_on(requester.close());
    for handle in worker_handles {
        handle.abort();
    }
    drop(server);

    let mut scaling_curve_ws = Vec::new();
    for &count in client_counts {
        let server = runtime
            .block_on(TestServer::start())
            .map_err(|error| error.to_string())?;
        let worker_clients: Vec<TestWebSocketClient> = (0..RPC_SCALING_WORKER_COUNT)
            .map(|_| {
                let mut worker = runtime
                    .block_on(TestWebSocketClient::connect(&format!(
                        "ws://{}",
                        server.ws_addr
                    )))
                    .map_err(|error| error.to_string())?;
                runtime
                    .block_on(worker.send_frame(&subscribe_frame))
                    .map_err(|error| error.to_string())?;
                let _ = runtime.block_on(worker.recv_frame(RESPONSE_TIMEOUT_MS));
                Ok(worker)
            })
            .collect::<Result<_, String>>()?;
        let worker_handles = spawn_rpc_ws_workers(worker_clients, family, true);

        let mut requesters = Vec::with_capacity(count);
        let mut rings = Vec::with_capacity(count);
        for _ in 0..count {
            requesters.push(
                runtime
                    .block_on(TestWebSocketClient::connect(&format!(
                        "ws://{}",
                        server.ws_addr
                    )))
                    .map_err(|error| error.to_string())?,
            );
            rings.push(build_network_request_frame_ring(
                service_route,
                b"ping",
                family,
                UNIQUE_FRAME_RING,
            ));
        }

        let started = Instant::now();
        let deadline = started + scaling_duration;
        let results =
            runtime.block_on(join_all(requesters.into_iter().zip(rings.into_iter()).map(
                |(mut requester, request_ring)| async move {
                    let mut latencies = Vec::new();
                    let mut errors = 0usize;
                    let mut next_index = 0usize;
                    while Instant::now() < deadline {
                        let request_frame = &request_ring[next_index];
                        next_index = (next_index + 1) % request_ring.len();
                        let op_start = Instant::now();
                        match request_until_worker_response_ws(
                            &mut requester,
                            request_frame,
                            family,
                        )
                        .await
                        {
                            Ok(()) => latencies.push(op_start.elapsed().as_micros() as u64),
                            Err(_) => errors += 1,
                        }
                    }
                    let _ = requester.close().await;
                    ClientRun {
                        latencies_us: latencies,
                        errors,
                    }
                },
            )));
        for handle in worker_handles {
            handle.abort();
        }
        drop(server);

        let mut latencies = Vec::new();
        let mut errors = 0usize;
        for result in results {
            latencies.extend(result.latencies_us);
            errors += result.errors;
        }
        scaling_curve_ws.push(ScalingPoint {
            dimension: "requesters".to_string(),
            count,
            stats: compute_stats("request_response", started.elapsed(), latencies, 1, errors),
        });
    }

    let server = runtime
        .block_on(TestServer::start_with_rpc_timeout(Duration::from_secs(
            RPC_PENDING_TIMEOUT_SECS,
        )))
        .map_err(|error| error.to_string())?;
    let mut worker = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .map_err(|error| error.to_string())?;
    runtime
        .block_on(worker.send_frame(&subscribe_frame))
        .map_err(|error| error.to_string())?;
    let _ = runtime.block_on(worker.recv_frame(RESPONSE_TIMEOUT_MS));
    let worker_handles = spawn_rpc_ws_workers(vec![worker], family, false);
    let mut requester = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .map_err(|error| error.to_string())?;
    let request_ring =
        build_network_request_frame_ring(service_route, b"pending", family, resource_samples);
    thread::sleep(Duration::from_millis(100));
    let before = stable_working_set_bytes()?;
    for request in &request_ring {
        runtime.block_on(drain_rpc_request_without_response(&mut requester, request))?;
    }
    thread::sleep(Duration::from_millis(250));
    let after = stable_working_set_bytes()?;
    let _ = runtime.block_on(requester.close());
    for handle in worker_handles {
        handle.abort();
    }
    drop(server);
    let mut resource_memory = delta_per_unit(before, after, resource_samples);
    resource_memory.resource = "inflight_rpc_request".to_string();

    Ok(DomainReport {
        domain: "rpc".to_string(),
        single_client_ws,
        suspected_cliff_at: detect_cliff(&scaling_curve_ws),
        scaling_curve_ws,
        additional_scenarios: Vec::new(),
        resource_memory,
        idle_connection_bytes_per_client: idle_connection_cost,
        notes: vec![
            format!("rpc scaling curve uses {RPC_SCALING_WORKER_COUNT} workers"),
            "pending-request memory run uses a subscribed worker that reads requests but never responds".to_string(),
        ],
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    characterization_support::configure_characterization_env();

    let args = parse_bench_args();
    let client_counts = parse_counts(&args.client_counts)?;
    let single_duration = Duration::from_millis(args.single_duration_ms);
    let scaling_duration = Duration::from_millis(args.scaling_duration_ms);
    let runtime = shared_bench_runtime();
    let idle_connection_cost = measure_idle_ws_connection_cost(runtime, args.connection_samples)?;

    let domain_report = measure_rpc(
        single_duration,
        scaling_duration,
        &client_counts,
        args.resource_samples.min(128),
        idle_connection_cost,
    )?;

    let report = ProductionReport {
        generated_at: chrono::Utc::now().to_rfc3339(),
        transport: "websocket e2e via TestServer/TestWebSocketClient".to_string(),
        single_duration_ms: args.single_duration_ms,
        scaling_duration_ms: args.scaling_duration_ms,
        idle_connection_samples: args.connection_samples,
        resource_samples: args.resource_samples,
        idle_ws_connection_bytes_per_client: idle_connection_cost,
        domains: vec![domain_report],
    };

    write_report(&args.output_dir, &report, "rpc")
}
