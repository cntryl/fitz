#[path = "characterization_support.rs"]
mod characterization_support;

use characterization_support::{
    compute_stats, delta_per_unit, detect_cliff, measure_idle_ws_connection_cost, parse_bench_args,
    parse_counts, stable_working_set_bytes, write_report, DomainReport, ProductionReport,
    ScalingPoint,
};
use fitz::benchkit::{
    build_notice_publish, build_notice_subscribe, parse_notice_response, shared_bench_runtime,
};
use fitz::testkit::{TestServer, TestWebSocketClient};
use futures::future::join_all;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const RESPONSE_TIMEOUT_MS: u64 = 2_000;

fn measure_notice(
    single_duration: Duration,
    scaling_duration: Duration,
    client_counts: &[usize],
    resource_samples: usize,
    idle_connection_cost: i64,
) -> Result<DomainReport, String> {
    let runtime = shared_bench_runtime();
    let subscribe_frame = build_notice_subscribe("notice://characterization/notice/events");
    let publish_frame = build_notice_publish("notice://characterization/notice/events", b"event");

    let server = runtime
        .block_on(TestServer::start())
        .map_err(|error| error.to_string())?;
    let mut subscriber = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .map_err(|error| error.to_string())?;
    let mut publisher = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .map_err(|error| error.to_string())?;
    runtime
        .block_on(subscriber.request(&subscribe_frame, RESPONSE_TIMEOUT_MS))
        .map_err(|error| error.to_string())?;

    let started = Instant::now();
    let deadline = started + single_duration;
    let mut single_latencies = Vec::new();
    let mut single_errors = 0usize;
    while Instant::now() < deadline {
        let op_start = Instant::now();
        let publish = runtime.block_on(publisher.request(&publish_frame, RESPONSE_TIMEOUT_MS));
        let notification = runtime.block_on(subscriber.recv_frame(RESPONSE_TIMEOUT_MS));
        match (publish, notification) {
            (Ok(ack), Ok(notification)) => {
                let _ = parse_notice_response(&ack);
                let _ = parse_notice_response(&notification);
                single_latencies.push(op_start.elapsed().as_micros() as u64);
            }
            _ => single_errors += 1,
        }
    }
    let single_client_ws = compute_stats(
        "publish_to_1_subscriber",
        started.elapsed(),
        single_latencies,
        1,
        single_errors,
    );
    let _ = runtime.block_on(publisher.close());
    let _ = runtime.block_on(subscriber.close());
    drop(server);

    let mut scaling_curve_ws = Vec::new();
    for &count in client_counts {
        let server = runtime
            .block_on(TestServer::start())
            .map_err(|error| error.to_string())?;
        let subscribers: Vec<Arc<Mutex<TestWebSocketClient>>> = (0..count)
            .map(|_| {
                let client = runtime
                    .block_on(TestWebSocketClient::connect(&format!(
                        "ws://{}",
                        server.ws_addr
                    )))
                    .map_err(|error| error.to_string())?;
                Ok(Arc::new(Mutex::new(client)))
            })
            .collect::<Result<_, String>>()?;
        let mut publisher = runtime
            .block_on(TestWebSocketClient::connect(&format!(
                "ws://{}",
                server.ws_addr
            )))
            .map_err(|error| error.to_string())?;
        runtime.block_on(join_all(subscribers.iter().map(|client| {
            let client = client.clone();
            let subscribe_frame = subscribe_frame.clone();
            async move {
                let mut subscriber = client.lock().await;
                let _ = subscriber
                    .request(&subscribe_frame, RESPONSE_TIMEOUT_MS)
                    .await;
            }
        })));

        let started = Instant::now();
        let deadline = started + scaling_duration;
        let mut latencies = Vec::new();
        let mut errors = 0usize;
        while Instant::now() < deadline {
            let op_start = Instant::now();
            let publish = runtime.block_on(publisher.request(&publish_frame, RESPONSE_TIMEOUT_MS));
            let notifications = runtime.block_on(join_all(subscribers.iter().map(|client| {
                let client = client.clone();
                async move {
                    let mut subscriber = client.lock().await;
                    subscriber.recv_frame(RESPONSE_TIMEOUT_MS).await
                }
            })));

            let delivered = notifications.iter().all(|result| result.is_ok());
            let publish_ok = publish.is_ok();
            if let Ok(ref ack) = publish {
                let _ = parse_notice_response(ack);
            }
            if publish_ok && delivered {
                for notification in notifications.into_iter().flatten() {
                    let _ = parse_notice_response(&notification);
                }
                latencies.push(op_start.elapsed().as_micros() as u64);
            } else {
                errors += 1;
            }
        }

        let _ = runtime.block_on(publisher.close());
        runtime.block_on(join_all(subscribers.iter().map(|client| {
            let client = client.clone();
            async move {
                let mut subscriber = client.lock().await;
                let _ = subscriber.close().await;
            }
        })));
        drop(server);

        scaling_curve_ws.push(ScalingPoint {
            dimension: "subscribers".to_string(),
            count,
            stats: compute_stats(
                &format!("publish_to_{count}_subscribers"),
                started.elapsed(),
                latencies,
                count as u64,
                errors,
            ),
        });
    }

    let server = runtime
        .block_on(TestServer::start())
        .map_err(|error| error.to_string())?;
    let mut client = runtime
        .block_on(TestWebSocketClient::connect(&format!(
            "ws://{}",
            server.ws_addr
        )))
        .map_err(|error| error.to_string())?;
    let subscribe_ring: Vec<Vec<u8>> = (0..resource_samples)
        .map(|index| {
            build_notice_subscribe(&format!("notice://characterization/notice/sub/{index}"))
        })
        .collect();
    thread::sleep(Duration::from_millis(100));
    let before = stable_working_set_bytes()?;
    for frame in &subscribe_ring {
        runtime
            .block_on(client.request(frame, RESPONSE_TIMEOUT_MS))
            .map_err(|error| error.to_string())?;
    }
    thread::sleep(Duration::from_millis(150));
    let after = stable_working_set_bytes()?;
    let _ = runtime.block_on(client.close());
    drop(server);
    let mut resource_memory = delta_per_unit(before, after, resource_samples);
    resource_memory.resource = "active_subscription".to_string();

    Ok(DomainReport {
        domain: "notice".to_string(),
        single_client_ws,
        suspected_cliff_at: detect_cliff(&scaling_curve_ws),
        scaling_curve_ws,
        additional_scenarios: Vec::new(),
        resource_memory,
        idle_connection_bytes_per_client: idle_connection_cost,
        notes: vec![
            "notice scaling varies subscriber count while keeping one publisher".to_string(),
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

    let domain_report = measure_notice(
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

    write_report(&args.output_dir, &report, "notice")
}
