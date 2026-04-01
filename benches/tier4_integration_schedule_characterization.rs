#[path = "characterization_support.rs"]
mod characterization_support;

use characterization_support::{
    compute_stats, detect_cliff, delta_per_unit, measure_idle_ws_connection_cost,
    parse_bench_args, parse_counts, stable_working_set_bytes, write_report, ClientRun,
    DomainReport, ProductionReport, ScalingPoint, ScenarioReport,
};
use fitz::benchkit::{
    build_schedule_create, build_schedule_create_batch, ensure_schedule_ok, shared_bench_runtime,
};
use fitz::testkit::{TestServer, TestWebSocketClient};
use futures::future::join_all;
use std::thread;
use std::time::{Duration, Instant};

const RESPONSE_TIMEOUT_MS: u64 = 2_000;
const UNIQUE_FRAME_RING: usize = 512;
const CREATE_BATCH_WIDTH: usize = 32;

fn measure_ws_scenario<F>(
    unit: &str,
    meaningful_ops_per_sample: u64,
    single_duration: Duration,
    scaling_duration: Duration,
    client_counts: &[usize],
    build_frame: F,
) -> Result<(characterization_support::LatencyStats, Vec<ScalingPoint>), String>
where
    F: Fn(usize, usize) -> Vec<u8>,
{
    let runtime = shared_bench_runtime();
    let frame_ring: Vec<Vec<u8>> = (0..UNIQUE_FRAME_RING)
        .map(|index| build_frame(0, index))
        .collect();

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
    let mut next_index = 0usize;
    while Instant::now() < deadline {
        let frame = &frame_ring[next_index];
        next_index = (next_index + 1) % frame_ring.len();
        let op_start = Instant::now();
        match runtime.block_on(client.request(frame, RESPONSE_TIMEOUT_MS)) {
            Ok(response) => {
                if ensure_schedule_ok(&response).is_ok() {
                    single_latencies.push(op_start.elapsed().as_micros() as u64);
                } else {
                    single_errors += 1;
                }
            }
            Err(_) => single_errors += 1,
        }
    }
    let single_client_ws = compute_stats(
        unit,
        started.elapsed(),
        single_latencies,
        meaningful_ops_per_sample,
        single_errors,
    );
    let _ = runtime.block_on(client.close());
    drop(server);

    let mut scaling_curve_ws = Vec::new();
    for &count in client_counts {
        let server = runtime
            .block_on(TestServer::start())
            .map_err(|error| error.to_string())?;
        let mut clients = Vec::with_capacity(count);
        let mut rings = Vec::with_capacity(count);
        for client_index in 0..count {
            clients.push(
                runtime
                    .block_on(TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr)))
                    .map_err(|error| error.to_string())?,
            );
            rings.push(
                (0..UNIQUE_FRAME_RING)
                    .map(|frame_index| build_frame(client_index, frame_index))
                    .collect::<Vec<_>>(),
            );
        }

        let started = Instant::now();
        let deadline = started + scaling_duration;
        let results = runtime.block_on(join_all(
            clients
                .into_iter()
                .zip(rings.into_iter())
                .map(|(mut client, ring)| async move {
                    let mut next_index = 0usize;
                    let mut latencies = Vec::new();
                    let mut errors = 0usize;
                    while Instant::now() < deadline {
                        let frame = &ring[next_index];
                        next_index = (next_index + 1) % ring.len();
                        let op_start = Instant::now();
                        match client.request(frame, RESPONSE_TIMEOUT_MS).await {
                            Ok(response) => {
                                if ensure_schedule_ok(&response).is_ok() {
                                    latencies.push(op_start.elapsed().as_micros() as u64);
                                } else {
                                    errors += 1;
                                }
                            }
                            Err(_) => errors += 1,
                        }
                    }
                    let _ = client.close().await;
                    ClientRun {
                        latencies_us: latencies,
                        errors,
                    }
                }),
        ));

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
            stats: compute_stats(
                unit,
                started.elapsed(),
                latencies,
                meaningful_ops_per_sample,
                errors,
            ),
        });
    }

    Ok((single_client_ws, scaling_curve_ws))
}

fn measure_schedule(
    single_duration: Duration,
    scaling_duration: Duration,
    client_counts: &[usize],
    resource_samples: usize,
    idle_connection_cost: i64,
) -> Result<DomainReport, String> {
    let (single_client_ws, scaling_curve_ws) = measure_ws_scenario(
        "create",
        1,
        single_duration,
        scaling_duration,
        client_counts,
        |client_index, frame_index| {
            build_schedule_create(
                &format!(
                    "schedule://characterization/jobs/client-{client_index}/run-{frame_index}"
                ),
                "0 * * * *",
                b"payload",
            )
        },
    )?;

    let (batch_single_client_ws, batch_scaling_curve_ws) = measure_ws_scenario(
        "batch_create_32",
        CREATE_BATCH_WIDTH as u64,
        single_duration,
        scaling_duration,
        client_counts,
        |client_index, frame_index| {
            let base_index = frame_index * CREATE_BATCH_WIDTH;
            let routes: Vec<String> = (0..CREATE_BATCH_WIDTH)
                .map(|entry_index| {
                    format!(
                        "schedule://characterization/batch/client-{client_index}/resource-{}/run",
                        base_index + entry_index
                    )
                })
                .collect();
            let entries: Vec<_> = routes
                .iter()
                .map(|route| (route.as_str(), "0 * * * *", b"payload".as_slice()))
                .collect();
            build_schedule_create_batch(&entries)
        },
    )?;

    let runtime = shared_bench_runtime();
    let server = runtime
        .block_on(TestServer::start())
        .map_err(|error| error.to_string())?;
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr)))
        .map_err(|error| error.to_string())?;
    let resource_ring: Vec<Vec<u8>> = (0..resource_samples)
        .map(|index| {
            build_schedule_create(
                &format!("schedule://characterization/memory/resource-{index}/run"),
                "0 * * * *",
                b"payload",
            )
        })
        .collect();
    thread::sleep(Duration::from_millis(100));
    let before = stable_working_set_bytes()?;
    for frame in &resource_ring {
        let response = runtime
            .block_on(client.request(frame, RESPONSE_TIMEOUT_MS))
            .map_err(|error| error.to_string())?;
        ensure_schedule_ok(&response)?;
    }
    thread::sleep(Duration::from_millis(150));
    let after = stable_working_set_bytes()?;
    let _ = runtime.block_on(client.close());
    drop(server);
    let mut resource_memory = delta_per_unit(before, after, resource_samples);
    resource_memory.resource = "active_schedule".to_string();

    Ok(DomainReport {
        domain: "schedule".to_string(),
        single_client_ws,
        suspected_cliff_at: detect_cliff(&scaling_curve_ws),
        scaling_curve_ws,
        additional_scenarios: vec![ScenarioReport {
            name: "Batch Create (32 schedules/request)".to_string(),
            single_client_ws: batch_single_client_ws,
            suspected_cliff_at: detect_cliff(&batch_scaling_curve_ws),
            scaling_curve_ws: batch_scaling_curve_ws,
            notes: vec![
                "ops_per_s counts schedules created, while latency values are for each batch request".to_string(),
                "batch create runs use 32 unique concrete schedule routes per request".to_string(),
            ],
        }],
        resource_memory,
        idle_connection_bytes_per_client: idle_connection_cost,
        notes: vec![
            "schedule create runs use unique routes so the active schedule set grows during measurement".to_string(),
            "resource-memory sampling still measures per-schedule state; batching only changes admission shape".to_string(),
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

    let domain_report = measure_schedule(
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

    write_report(&args.output_dir, &report, "schedule")
}