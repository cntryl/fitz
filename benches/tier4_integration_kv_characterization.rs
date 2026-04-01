#[path = "characterization_support.rs"]
mod characterization_support;

use characterization_support::{
    compute_stats, detect_cliff, delta_per_unit, measure_idle_ws_connection_cost,
    parse_bench_args, parse_counts, stable_working_set_bytes, write_report, ClientRun,
    DomainReport, ProductionReport, ScalingPoint,
};
use fitz::benchkit::{
    build_kv_begin, build_kv_put, build_kv_rollback, parse_kv_response, parse_kv_tx_id,
    shared_bench_runtime,
};
use fitz::testkit::{TestServer, TestWebSocketClient};
use futures::future::join_all;
use std::thread;
use std::time::{Duration, Instant};

const RESPONSE_TIMEOUT_MS: u64 = 2_000;

async fn kv_sequence(client: &mut TestWebSocketClient, route: &str) -> Result<(), String> {
    let begin_frame = build_kv_begin(route, 1, 0);
    let response = client
        .request(&begin_frame, RESPONSE_TIMEOUT_MS)
        .await
        .map_err(|error| error.to_string())?;
    let (_, _, data) = parse_kv_response(&response);
    let tx_id = parse_kv_tx_id(&data)?;

    let put_frame = build_kv_put(tx_id, route, b"key", b"value");
    client
        .request(&put_frame, RESPONSE_TIMEOUT_MS)
        .await
        .map_err(|error| error.to_string())?;

    let rollback_frame = build_kv_rollback(tx_id, route);
    client
        .request(&rollback_frame, RESPONSE_TIMEOUT_MS)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn measure_kv(
    single_duration: Duration,
    scaling_duration: Duration,
    client_counts: &[usize],
    resource_samples: usize,
    idle_connection_cost: i64,
) -> Result<DomainReport, String> {
    let runtime = shared_bench_runtime();
    let route = "kv://characterization/kv/hot";
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
        match runtime.block_on(kv_sequence(&mut client, route)) {
            Ok(()) => single_latencies.push(op_start.elapsed().as_micros() as u64),
            Err(_) => single_errors += 1,
        }
    }
    let single_client_ws = compute_stats(
        "begin_put_rollback_sequence",
        started.elapsed(),
        single_latencies,
        3,
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
        for _ in 0..count {
            clients.push(
                runtime
                    .block_on(TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr)))
                    .map_err(|error| error.to_string())?,
            );
        }

        let start = Instant::now();
        let deadline = start + scaling_duration;
        let results = runtime.block_on(join_all(clients.into_iter().map(|mut ws_client| async move {
            let mut latencies = Vec::new();
            let mut errors = 0usize;
            while Instant::now() < deadline {
                let op_start = Instant::now();
                match kv_sequence(&mut ws_client, route).await {
                    Ok(()) => latencies.push(op_start.elapsed().as_micros() as u64),
                    Err(_) => errors += 1,
                }
            }
            let _ = ws_client.close().await;
            ClientRun {
                latencies_us: latencies,
                errors,
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
            stats: compute_stats("begin_put_rollback_sequence", elapsed, latencies, 3, errors),
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
        let begin_frame = build_kv_begin(route, 1, 0);
        let response = runtime
            .block_on(client.request(&begin_frame, RESPONSE_TIMEOUT_MS))
            .map_err(|error| error.to_string())?;
        let _ = parse_kv_tx_id(&parse_kv_response(&response).2)?;
    }
    thread::sleep(Duration::from_millis(150));
    let after = stable_working_set_bytes()?;
    let _ = runtime.block_on(client.close());
    drop(server);
    let mut resource_memory = delta_per_unit(before, after, resource_samples);
    resource_memory.resource = "active_transaction".to_string();

    Ok(DomainReport {
        domain: "kv".to_string(),
        single_client_ws,
        suspected_cliff_at: detect_cliff(&scaling_curve_ws),
        scaling_curve_ws,
        additional_scenarios: Vec::new(),
        resource_memory,
        idle_connection_bytes_per_client: idle_connection_cost,
        notes: vec![
            "single and scaling runs hit the same route to expose hot-route contention".to_string(),
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

    let domain_report = measure_kv(
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

    write_report(&args.output_dir, &report, "kv")
}