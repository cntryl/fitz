//! External driver: the broker must be capped independently of this process.
mod fixtures;
#[path = "constrained_runtime/s3.rs"]
#[cfg(feature = "recovery-qualification")]
mod s3;

use fixtures::transport::*;
use std::net::SocketAddr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

const QUEUE: &str = "queue://test/resources/burst";
const KV: &str = "kv://test/resources/state";

fn inspect_container(name: &str) -> serde_json::Value {
    let output = std::process::Command::new("docker")
        .args(["inspect", name])
        .output()
        .expect("inspect capped broker");
    assert!(output.status.success(), "docker inspect failed");
    let containers: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout).unwrap();
    let container = containers.into_iter().next().expect("broker container");
    assert_eq!(container["HostConfig"]["NanoCpus"], 250_000_000);
    assert_eq!(container["HostConfig"]["Memory"], 536_870_912);
    assert_eq!(container["HostConfig"]["MemorySwap"], 536_870_912);
    assert_eq!(container["State"]["Running"], true);
    assert_eq!(container["State"]["OOMKilled"], false);
    assert_eq!(container["RestartCount"], 0);
    container
}

async fn monitor_health(endpoint: String, finished: Arc<AtomicBool>) -> (u64, u64) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let mut samples = 0;
    let mut maximum_millis = 0;
    while !finished.load(Ordering::Acquire) {
        let started = std::time::Instant::now();
        let health = client
            .get(format!("{endpoint}/healthz"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<serde_json::Value>()
            .await
            .unwrap();
        assert_eq!(
            health["status"], "ready",
            "broker must remain ready under load"
        );
        samples += 1;
        maximum_millis = maximum_millis.max(u64::try_from(started.elapsed().as_millis()).unwrap());
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    (samples, maximum_millis)
}

async fn connect(address: SocketAddr) -> TestClient {
    TestClient::new(address)
        .await
        .expect("external broker connection")
}

fn field(response: &[u8]) -> (u16, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);
    let result = parser.next_field().expect("response field");
    assert!(parser.next_field().is_none(), "one response field");
    result
}

async fn request(client: &mut TestClient, frame: &[u8], expected: u16) -> Vec<u8> {
    client.send_frame(frame).await.unwrap();
    let (kind, payload) = field(&client.recv_frame(60_000).await.unwrap());
    assert_eq!(kind, expected, "response type");
    assert_eq!(
        payload.first(),
        Some(&0),
        "operation {expected} failed: {payload:?}"
    );
    payload
}

async fn smoke(address: SocketAddr, suffix: &str) {
    let mut client = connect(address).await;
    let mut receiver = connect(address).await;
    let body = suffix.as_bytes();

    let route = format!("notice://test/resources/{suffix}");
    request(&mut receiver, &build_notice_subscribe(&route), 501).await;
    client
        .send_frame(&build_notice_publish(&route, "test", body))
        .await
        .unwrap();
    let delivery = parse_notice_delivery(&receiver.recv_frame(30_000).await.unwrap()).unwrap();
    assert_eq!(delivery.body, body);

    let route = format!("rpc://test/resources/{suffix}");
    request(&mut receiver, &build_rpc_subscribe(&route), 300).await;
    client
        .send_frame(&build_rpc_request(&route, "", body))
        .await
        .unwrap();
    let delivery = parse_rpc_request_delivery(&receiver.recv_frame(30_000).await.unwrap()).unwrap();
    assert_eq!(delivery.body, body);
    receiver
        .send_frame(&build_rpc_response_delivery(
            delivery.correlation_id,
            0,
            true,
            body,
        ))
        .await
        .unwrap();
    let reply = parse_rpc_response_delivery(&client.recv_frame(30_000).await.unwrap()).unwrap();
    assert_eq!(reply.body, body);
    assert!(reply.stream_end);

    let route = format!("lease://test/resources/{suffix}");
    let acquired = request(
        &mut client,
        &build_lease_acquire_immediate(&route, "driver", 30),
        400,
    )
    .await;
    let token = parse_lease_token_response(&acquired).unwrap();
    request(
        &mut client,
        &build_lease_release(&route, "driver", token),
        402,
    )
    .await;

    let route = format!("schedule://test/resources/{suffix}/run");
    request(
        &mut client,
        &build_schedule_create(&route, "0 0 1 1 *", body),
        700,
    )
    .await;
    request(&mut client, &build_schedule_cancel(&route), 701).await;

    let route = format!("stream://test/resources/{suffix}");
    let started = request(&mut client, &build_stream_begin(&route), 600).await;
    let id = parse_stream_session_id(&started).unwrap();
    request(&mut client, &build_stream_append(id, 0, body), 601).await;
    request(&mut client, &build_stream_commit(id), 602).await;
    let readback = request(&mut client, &build_stream_read(&route, 0), 604).await;
    assert!(readback.windows(body.len()).any(|bytes| bytes == body));

    let started = request(&mut client, &build_kv_begin(KV, 1), 100).await;
    let id = parse_kv_tx_id(&started).unwrap();
    request(&mut client, &build_kv_put(id, KV, body, body), 104).await;
    request(&mut client, &build_kv_commit(id, KV, 1), 101).await;
    let started = request(&mut client, &build_kv_begin(KV, 0), 100).await;
    let id = parse_kv_tx_id(&started).unwrap();
    let readback = request(&mut client, &build_kv_get(id, KV, body), 103).await;
    assert_eq!(extract_kv_value(&readback).unwrap(), body);
    request(&mut client, &build_kv_rollback(id, KV), 102).await;

    let route = format!("queue://test/resources/{suffix}");
    request(&mut client, &build_queue_enqueue(&route, body), 200).await;
    let reserved = request(&mut client, &build_queue_dequeue(&route), 202).await;
    assert_eq!(
        extract_queue_messages(&reserved).unwrap(),
        vec![body.to_vec()]
    );
    ack(&mut client, &route, &reserved).await;
}

async fn ack(client: &mut TestClient, route: &str, reserved: &[u8]) {
    let count = u32::from_be_bytes(reserved[1..5].try_into().unwrap());
    assert_eq!(count, 1);
    let mut payload = Vec::new();
    payload.extend_from_slice(&u32::try_from(route.len()).unwrap().to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&reserved[5..21]); // message ID and reservation token
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(204, &payload);
    request(client, &builder.build(), 204).await;
}

async fn campaign(address: SocketAddr) -> usize {
    let run = uuid::Uuid::new_v4();
    smoke(address, &format!("before-{run}")).await;
    let mut producers = tokio::task::JoinSet::new();
    for worker in 0_u32..64 {
        producers.spawn(async move {
            let mut client = connect(address).await;
            let mut accepted = Vec::new();
            for sequence in 0_u32..256 {
                let mut body = vec![0x5a; 4096];
                body[..4].copy_from_slice(&worker.to_be_bytes());
                body[4..8].copy_from_slice(&sequence.to_be_bytes());
                client
                    .send_frame(&build_queue_enqueue(QUEUE, &body))
                    .await
                    .unwrap();
                let (kind, payload) = field(&client.recv_frame(60_000).await.unwrap());
                assert_eq!(kind, 200);
                if payload.first() == Some(&0) {
                    let id = u64::from_be_bytes(payload[1..9].try_into().unwrap());
                    accepted.push((id, body));
                } else {
                    // A returned rejection is observed, never blindly retried.
                    panic!("Queue rejected bounded resource campaign: {payload:?}");
                }
            }
            accepted
        });
    }
    let mut accepted = std::collections::HashMap::new();
    while let Some(result) = producers.join_next().await {
        for (id, body) in result.unwrap() {
            assert!(accepted.insert(id, body).is_none(), "unique accepted ID");
        }
    }
    let count = accepted.len();
    println!("accepted={count}; draining externally capped broker");
    let mut consumer = connect(address).await;
    while !accepted.is_empty() {
        let reserved = request(&mut consumer, &build_queue_dequeue(QUEUE), 202).await;
        let id = u64::from_be_bytes(reserved[5..13].try_into().unwrap());
        let body = extract_queue_messages(&reserved).unwrap();
        assert_eq!(body.len(), 1);
        assert_eq!(
            accepted.remove(&id).expect("accepted and unique delivery"),
            body[0]
        );
        ack(&mut consumer, QUEUE, &reserved).await;
        if accepted.len() % 1024 == 0 {
            println!("remaining={}", accepted.len());
        }
    }
    let empty = request(&mut consumer, &build_queue_dequeue(QUEUE), 202).await;
    assert_eq!(
        extract_queue_messages(&empty).unwrap(),
        Vec::<Vec<u8>>::new()
    );
    smoke(address, &format!("after-{run}")).await;
    count
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a separately resource-capped broker; see constrained-runtime.md"]
async fn should_remain_usable_after_bounded_storage_pressure() {
    // Arrange
    let address = std::env::var("FITZ_EXTERNAL_TCP_ADDRESS")
        .expect("FITZ_EXTERNAL_TCP_ADDRESS required")
        .parse::<SocketAddr>()
        .unwrap();
    let container_name =
        std::env::var("FITZ_EXTERNAL_CONTAINER").expect("FITZ_EXTERNAL_CONTAINER required");
    let endpoint =
        std::env::var("FITZ_EXTERNAL_HTTP_ENDPOINT").expect("FITZ_EXTERNAL_HTTP_ENDPOINT required");
    let before = inspect_container(&container_name);
    let finished = Arc::new(AtomicBool::new(false));
    let monitor = tokio::spawn(monitor_health(endpoint, finished.clone()));
    let started = std::time::Instant::now();

    // Act
    let accepted = tokio::time::timeout(Duration::from_secs(600), campaign(address))
        .await
        .expect("resource campaign exceeded 600 seconds");
    finished.store(true, Ordering::Release);
    let (health_samples, maximum_health_millis) = monitor.await.unwrap();
    let after = inspect_container(&container_name);
    let report = serde_json::json!({
        "accepted_and_verified_acked": accepted,
        "remaining": 0,
        "all_seven_domains_before_and_after": true,
        "seconds": started.elapsed().as_secs_f64(),
        "health_samples": health_samples,
        "maximum_health_millis": maximum_health_millis,
        "before": before,
        "after": after,
        "source_sha": std::env::var("FITZ_RESOURCE_SOURCE_SHA").ok(),
    });
    let report_path =
        std::env::var("FITZ_RESOURCE_REPORT_PATH").expect("FITZ_RESOURCE_REPORT_PATH required");
    let report_path = std::path::Path::new(&report_path);
    std::fs::create_dir_all(report_path.parent().unwrap()).unwrap();
    std::fs::write(report_path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();

    // Assert
    assert_eq!(accepted, 16_384);
    assert!(health_samples > 0);
}
