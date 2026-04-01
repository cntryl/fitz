#[path = "characterization_support.rs"]
mod characterization_support;

use characterization_support::{
    compute_stats, detect_cliff, delta_per_unit, measure_idle_ws_connection_cost,
    parse_bench_args, parse_counts, stable_working_set_bytes, write_report, ClientRun,
    DomainReport, ProductionReport, ScalingPoint,
};
use fitz::benchkit::{
    build_stream_append, build_stream_begin, parse_stream_response, parse_stream_session_id,
    shared_bench_runtime,
};
use fitz::testkit::{TestServer, TestWebSocketClient};
use futures::future::join_all;
use std::thread;
use std::time::{Duration, Instant};

const RESPONSE_TIMEOUT_MS: u64 = 2_000;

async fn open_stream_session(client: &mut TestWebSocketClient, route: &str) -> Result<u64, String> {
    let begin_frame = build_stream_begin(route, 0);
    let response = client
        .request(&begin_frame, RESPONSE_TIMEOUT_MS)
        .await
        .map_err(|error| error.to_string())?;
    let (_, _, data) = parse_stream_response(&response);
    parse_stream_session_id(&data)
}

fn measure_stream(
    single_duration: Duration,
    scaling_duration: Duration,
    client_counts: &[usize],
    resource_samples: usize,
    idle_connection_cost: i64,
) -> Result<DomainReport, String> {
    let runtime = shared_bench_runtime();
    let route = "stream://characterization/stream/single/append";
    let server = runtime
        .block_on(TestServer::start())
        .map_err(|error| error.to_string())?;
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr)))
        .map_err(|error| error.to_string())?;
    let session_id = runtime.block_on(open_stream_session(&mut client, route))?;
    let append_frame = build_stream_append(session_id, b"event");

    let started = Instant::now();
    let deadline = started + single_duration;
    let mut single_latencies = Vec::new();
    let mut single_errors = 0usize;
    while Instant::now() < deadline {
        let op_start = Instant::now();
        match runtime.block_on(client.request(&append_frame, RESPONSE_TIMEOUT_MS)) {
            Ok(_) => single_latencies.push(op_start.elapsed().as_micros() as u64),
            Err(_) => single_errors += 1,
        }
    }
    let single_client_ws = compute_stats("append", started.elapsed(), single_latencies, 1, single_errors);
    let _ = runtime.block_on(client.close());
    drop(server);

    let mut scaling_curve_ws = Vec::new();
    for &count in client_counts {
        let server = runtime
            .block_on(TestServer::start())
            .map_err(|error| error.to_string())?;
        let mut clients = Vec::with_capacity(count);
        let mut append_frames = Vec::with_capacity(count);
        for index in 0..count {
            let route = format!("stream://characterization/stream/{index}/append");
            let mut ws_client = runtime
                .block_on(TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr)))
                .map_err(|error| error.to_string())?;
            let session_id = runtime.block_on(open_stream_session(&mut ws_client, &route))?;
            append_frames.push(build_stream_append(session_id, b"event"));
            clients.push(ws_client);
        }

        let start = Instant::now();
        let deadline = start + scaling_duration;
        let results = runtime.block_on(join_all(
            clients
                .into_iter()
                .zip(append_frames.into_iter())
                .map(|(mut ws_client, append_frame)| async move {
                    let mut latencies = Vec::new();
                    let mut errors = 0usize;
                    while Instant::now() < deadline {
                        let op_start = Instant::now();
                        match ws_client.request(&append_frame, RESPONSE_TIMEOUT_MS).await {
                            Ok(_) => latencies.push(op_start.elapsed().as_micros() as u64),
                            Err(_) => errors += 1,
                        }
                    }
                    let _ = ws_client.close().await;
                    ClientRun {
                        latencies_us: latencies,
                        errors,
                    }
                }),
        ));

        let elapsed = start.elapsed();
        let mut latencies = Vec::new();
        let mut errors = 0usize;
        for result in results {
            latencies.extend(result.latencies_us);
            errors += result.errors;
        }
        drop(server);
        scaling_curve_ws.push(ScalingPoint {
            dimension: "clients".to_string(),
            count,
            stats: compute_stats("append", elapsed, latencies, 1, errors),
        });
    }

    let server = runtime
        .block_on(TestServer::start())
        .map_err(|error| error.to_string())?;
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr)))
        .map_err(|error| error.to_string())?;
    thread::sleep(Duration::from_millis(100));
    let before = stable_working_set_bytes()?;
    for index in 0..resource_samples {
        let route = format!("stream://characterization/stream/memory/{index}/append");
        let _ = runtime.block_on(open_stream_session(&mut client, &route))?;
    }
    thread::sleep(Duration::from_millis(150));
    let after = stable_working_set_bytes()?;
    let _ = runtime.block_on(client.close());
    drop(server);
    let mut resource_memory = delta_per_unit(before, after, resource_samples);
    resource_memory.resource = "active_stream_session".to_string();

    let same_route_limit_note = {
        let server = runtime
            .block_on(TestServer::start())
            .map_err(|error| error.to_string())?;
        let mut first = runtime
            .block_on(TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr)))
            .map_err(|error| error.to_string())?;
        let mut second = runtime
            .block_on(TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr)))
            .map_err(|error| error.to_string())?;
        let route = "stream://characterization/stream/hot/append";
        let _ = runtime.block_on(open_stream_session(&mut first, route))?;
        let second_response = runtime
            .block_on(second.request(&build_stream_begin(route, 0), RESPONSE_TIMEOUT_MS))
            .map_err(|error| error.to_string())?;
        let (_, status, _) = parse_stream_response(&second_response);
        let _ = runtime.block_on(first.close());
        let _ = runtime.block_on(second.close());
        format!("second writer on same route returned status {} in verification run", status)
    };

    Ok(DomainReport {
        domain: "stream".to_string(),
        single_client_ws,
        suspected_cliff_at: detect_cliff(&scaling_curve_ws),
        scaling_curve_ws,
        resource_memory,
        idle_connection_bytes_per_client: idle_connection_cost,
        notes: vec![
            "scaling curve uses distinct stream routes per client because one active writer per resource is enforced".to_string(),
            same_route_limit_note,
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

    let domain_report = measure_stream(
        single_duration,
        scaling_duration,
        &client_counts,
        args.resource_samples,
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

    write_report(&args.output_dir, &report, "stream")
}