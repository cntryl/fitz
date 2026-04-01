#[path = "characterization_support.rs"]
mod characterization_support;

use characterization_support::{
    compute_stats, detect_cliff, delta_per_unit, measure_idle_ws_connection_cost,
    parse_bench_args, parse_counts, stable_working_set_bytes, write_report, ClientRun,
    DomainReport, ProductionReport, ScalingPoint,
};
use fitz::benchkit::{build_queue_enqueue, parse_queue_response, shared_bench_runtime};
use fitz::testkit::{TestServer, TestWebSocketClient};
use futures::future::join_all;
use std::thread;
use std::time::{Duration, Instant};

const RESPONSE_TIMEOUT_MS: u64 = 2_000;

fn measure_queue(
    single_duration: Duration,
    scaling_duration: Duration,
    client_counts: &[usize],
    resource_samples: usize,
    idle_connection_cost: i64,
) -> Result<DomainReport, String> {
    let runtime = shared_bench_runtime();
    let route = "queue://characterization/queue/main/enqueue";
    let enqueue_frame = build_queue_enqueue(route, b"msg");

    let server = runtime
        .block_on(TestServer::start())
        .map_err(|error| error.to_string())?;
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr)))
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    let deadline = started + single_duration;
    let mut single_latencies = Vec::new();
    let mut single_errors = 0usize;
    while Instant::now() < deadline {
        let op_start = Instant::now();
        match runtime.block_on(client.request(&enqueue_frame, RESPONSE_TIMEOUT_MS)) {
            Ok(response) => {
                let _ = parse_queue_response(&response);
                single_latencies.push(op_start.elapsed().as_micros() as u64);
            }
            Err(_) => single_errors += 1,
        }
    }
    let single_client_ws = compute_stats("enqueue", started.elapsed(), single_latencies, 1, single_errors);
    let _ = runtime.block_on(client.close());
    drop(server);

    let mut scaling_curve_ws = Vec::new();
    for &count in client_counts {
        let server = runtime
            .block_on(TestServer::start())
            .map_err(|error| error.to_string())?;
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(
                runtime
                    .block_on(TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr)))
                    .map_err(|error| error.to_string())?,
            );
        }

        let start = Instant::now();
        let deadline = start + scaling_duration;
        let results = runtime.block_on(join_all(clients.into_iter().map(|mut ws_client| {
            let enqueue_frame = enqueue_frame.clone();
            async move {
                let mut latencies = Vec::new();
                let mut errors = 0usize;
                while Instant::now() < deadline {
                    let op_start = Instant::now();
                    match ws_client.request(&enqueue_frame, RESPONSE_TIMEOUT_MS).await {
                        Ok(response) => {
                            let _ = parse_queue_response(&response);
                            latencies.push(op_start.elapsed().as_micros() as u64);
                        }
                        Err(_) => errors += 1,
                    }
                }
                let _ = ws_client.close().await;
                ClientRun {
                    latencies_us: latencies,
                    errors,
                }
            }
        })));

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
            stats: compute_stats("enqueue", elapsed, latencies, 1, errors),
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
    for _ in 0..resource_samples {
        let response = runtime
            .block_on(client.request(&enqueue_frame, RESPONSE_TIMEOUT_MS))
            .map_err(|error| error.to_string())?;
        let _ = parse_queue_response(&response);
    }
    thread::sleep(Duration::from_millis(150));
    let after = stable_working_set_bytes()?;
    let _ = runtime.block_on(client.close());
    drop(server);
    let mut resource_memory = delta_per_unit(before, after, resource_samples);
    resource_memory.resource = "queued_message".to_string();

    Ok(DomainReport {
        domain: "queue".to_string(),
        single_client_ws,
        suspected_cliff_at: detect_cliff(&scaling_curve_ws),
        scaling_curve_ws,
        resource_memory,
        idle_connection_bytes_per_client: idle_connection_cost,
        notes: vec!["queue scaling curve uses many producers against one queue route".to_string()],
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

    let domain_report = measure_queue(
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

    write_report(&args.output_dir, &report, "queue")
}