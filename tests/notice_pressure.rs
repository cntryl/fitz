//! Opt-in live fanout pressure: an independently drained receiver and a stalled receiver.
mod fixtures;
use fitz::testkit::{TestClient, TestServer};
use fixtures::transport::{
    build_notice_publish, build_notice_subscribe, parse_notice_delivery,
    parse_notice_subscription_id,
};
use serde::Serialize;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const ROUTE: &str = "notice://pressure/events/live";
fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn body(sequence: u64) -> Vec<u8> {
    let mut payload = vec![0xA5; 1024];
    payload[..8].copy_from_slice(&sequence.to_be_bytes());
    payload
}

#[derive(Default, Serialize)]
struct Stage {
    offered: usize,
    attempted: usize,
    sent: usize,
    indeterminate_writes: usize,
    fast_observed: usize,
    slow_observed: usize,
    fast_window_misses: usize,
    slow_window_misses: usize,
    fast_disconnect: Option<String>,
    slow_disconnect: Option<String>,
    elapsed_ns: u128,
    failure: Option<String>,
}

#[derive(Serialize)]
struct Report {
    schema: &'static str,
    source_sha: Option<String>,
    source_dirty: Option<bool>,
    scope: &'static str,
    transport: &'static str,
    storage_mode: &'static str,
    status: &'static str,
    planned_bursts: Vec<usize>,
    send_deadline_seconds: u64,
    observation_window_seconds: u64,
    startup_shutdown_deadline_seconds: u64,
    payload_bytes: usize,
    slow_read_delay_ms: u64,
    stages: Vec<Stage>,
    baseline_passed: bool,
    recovery_passed: bool,
    cleanup_passed: bool,
    failure: Option<String>,
    cleanup_failure: Option<String>,
    artifact_failure: Option<String>,
    #[serde(skip)]
    path: PathBuf,
}

impl Report {
    fn new() -> Result<Self, String> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(error)?
            .as_nanos();
        let directory = PathBuf::from("target/fitz-stress/notice-pressure");
        std::fs::create_dir_all(&directory).map_err(error)?;
        Ok(Self {
            schema: "fitz.notice-pressure.v1",
            source_sha: git(&["rev-parse", "HEAD"]),
            source_dirty: git(&["status", "--porcelain"]).map(|s| !s.is_empty()),
            scope: "live_delivery_observations_no_replay_or_durability",
            transport: "tcp",
            storage_mode: "memory",
            status: "running",
            planned_bursts: Vec::new(),
            send_deadline_seconds: 30,
            observation_window_seconds: 6,
            startup_shutdown_deadline_seconds: 60,
            payload_bytes: 1024,
            slow_read_delay_ms: 5000,
            stages: Vec::new(),
            baseline_passed: false,
            recovery_passed: false,
            cleanup_passed: false,
            failure: None,
            cleanup_failure: None,
            artifact_failure: None,
            path: directory.join(format!("{stamp}.json")),
        })
    }
    fn save(&self) -> Result<(), String> {
        let bytes = serde_json::to_vec_pretty(self).map_err(error)?;
        let temporary = self.path.with_extension("tmp");
        std::fs::write(&temporary, bytes).map_err(error)?;
        std::fs::rename(temporary, &self.path).map_err(error)
    }
}

fn git(args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

async fn subscribe(server: &TestServer) -> Result<(TestClient, u64), String> {
    bounded(10, async {
        let mut client = TestClient::new(server.tcp_addr).await.map_err(error)?;
        client
            .send_frame(&build_notice_subscribe(ROUTE))
            .await
            .map_err(error)?;
        let response = client.recv_frame(2000).await.map_err(error)?;
        let mut parser = fitz::testkit::transport::TlvFrameParser::new(&response);
        let (kind, data) = parser.next_field().ok_or("missing subscription reply")?;
        if kind != 501 || data.first() != Some(&0) || parser.next_field().is_some() {
            return Err("subscription rejected".into());
        }
        Ok((
            client,
            parse_notice_subscription_id(&data[1..])?.ok_or("missing subscription identity")?,
        ))
    })
    .await
}

async fn bounded<T>(
    seconds: u64,
    future: impl std::future::Future<Output = Result<T, String>>,
) -> Result<T, String> {
    tokio::time::timeout(Duration::from_secs(seconds), future)
        .await
        .map_err(|_| {
            format!("absolute {seconds}s operation deadline exceeded; outcome may be unknown")
        })?
}

async fn finish_receiver(
    mut task: tokio::task::JoinHandle<Result<(), String>>,
) -> Result<(), String> {
    if let Ok(result) = tokio::time::timeout(Duration::from_secs(2), &mut task).await {
        result.map_err(error).and_then(|r| r)
    } else {
        task.abort();
        let _ = tokio::time::timeout(Duration::from_secs(1), &mut task).await;
        Err("receiver join exceeded deadline; task aborted".into())
    }
}

fn validate(
    frame: &[u8],
    subscription_id: u64,
    offered: usize,
    seen: &mut HashSet<u64>,
) -> Result<(), String> {
    let mut parser = fitz::testkit::transport::TlvFrameParser::new(frame);
    parser.next_field().ok_or("missing delivery")?;
    if parser.next_field().is_some() {
        return Err("multiple fields in Notice delivery".into());
    }
    let delivery = parse_notice_delivery(frame)?;
    if delivery.subscription_id != subscription_id
        || delivery.route != ROUTE
        || delivery.body.len() != 1024
    {
        return Err("invalid subscription, route, or payload length".into());
    }
    let sequence = u64::from_be_bytes(delivery.body[..8].try_into().map_err(error)?);
    if sequence >= u64::try_from(offered).map_err(error)?
        || delivery.body != body(sequence)
        || !seen.insert(sequence)
    {
        return Err("unknown, corrupt, or duplicate live delivery".into());
    }
    Ok(())
}

async fn receiver(
    mut client: TestClient,
    subscription: u64,
    offered: usize,
    slow: bool,
    counters: Arc<Mutex<Stage>>,
    mut stop: tokio::sync::oneshot::Receiver<()>,
) -> Result<(), String> {
    let mut seen = HashSet::new();
    if slow {
        tokio::time::sleep(Duration::from_millis(5000)).await;
    }
    loop {
        // A canceled partial read only occurs when this connection is discarded.
        let frame = tokio::select! {
            _ = &mut stop => return Ok(()),
            frame = client.recv_frame_bytes_without_timeout() => frame,
        };
        match frame {
            Ok(frame) => {
                validate(&frame, subscription, offered, &mut seen)?;
                let mut current = counters.lock().unwrap();
                if slow {
                    current.slow_observed += 1;
                } else {
                    current.fast_observed += 1;
                }
            }
            Err(e) => {
                let mut current = counters.lock().unwrap();
                if slow {
                    current.slow_disconnect = Some(error(e));
                } else {
                    current.fast_disconnect = Some(error(e));
                }
                return Ok(());
            }
        }
    }
}

async fn stage(server: &TestServer, offered: usize, report: &mut Report) -> Result<(), String> {
    let (fast, fast_id) = subscribe(server).await?;
    let (slow, slow_id) = subscribe(server).await?;
    let mut publisher = TestClient::new(server.tcp_addr).await.map_err(error)?;
    let counters = Arc::new(Mutex::new(Stage {
        offered,
        ..Stage::default()
    }));
    let (fast_stop, fast_rx) = tokio::sync::oneshot::channel();
    let (slow_stop, slow_rx) = tokio::sync::oneshot::channel();
    let fast_task = tokio::spawn(receiver(
        fast,
        fast_id,
        offered,
        false,
        counters.clone(),
        fast_rx,
    ));
    let slow_task = tokio::spawn(receiver(
        slow,
        slow_id,
        offered,
        true,
        counters.clone(),
        slow_rx,
    ));
    let started = Instant::now();
    let send_result = tokio::time::timeout(Duration::from_secs(30), async {
        for sequence in 0..offered {
            counters.lock().unwrap().attempted += 1;
            publisher
                .send_frame(&build_notice_publish(
                    ROUTE,
                    "test-realm",
                    &body(u64::try_from(sequence).map_err(error)?),
                ))
                .await
                .map_err(error)?;
            counters.lock().unwrap().sent += 1;
        }
        Ok::<_, String>(())
    })
    .await
    .map_err(|_| "publisher write deadline exceeded".to_owned())
    .and_then(|r| r);
    // A fixed observation window makes late/unobserved live deliveries explicit.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let _ = fast_stop.send(());
    let _ = slow_stop.send(());
    let fast_result = finish_receiver(fast_task).await;
    let slow_result = finish_receiver(slow_task).await;
    let mut current = std::mem::take(&mut *counters.lock().unwrap());
    current.elapsed_ns = started.elapsed().as_nanos();
    current.indeterminate_writes = current.attempted - current.sent;
    current.fast_window_misses = current.sent.saturating_sub(current.fast_observed);
    current.slow_window_misses = current.sent.saturating_sub(current.slow_observed);
    current.failure = send_result
        .err()
        .or_else(|| fast_result.err())
        .or_else(|| slow_result.err());
    let result = current.failure.clone().map_or(Ok(()), Err);
    report.stages.push(current);
    report.save()?;
    result
}

async fn probe(server: &TestServer) -> Result<(), String> {
    let (mut subscriber, subscription) = subscribe(server).await?;
    let mut publisher = TestClient::new(server.tcp_addr).await.map_err(error)?;
    publisher
        .send_frame(&build_notice_publish(ROUTE, "test-realm", &body(0)))
        .await
        .map_err(error)?;
    let delivery = subscriber.recv_frame(2000).await.map_err(error)?;
    validate(&delivery, subscription, 1, &mut HashSet::new())
}

#[tokio::test]
#[ignore = "bounded fanout pressure diagnostic; run explicitly and serialize shared-host campaigns"]
async fn should_push_notice_slow_receiver_limits() {
    // Arrange
    let mut report = Report::new().expect("report");
    report.save().expect("initial artifact");
    let mut server = None;
    // Act
    let result = async {
        let configured = std::env::var("FITZ_NOTICE_PRESSURE_BURSTS")
            .unwrap_or_else(|_| "100,1000,10000,100000".into());
        let bursts = configured
            .split(',')
            .map(str::parse::<usize>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(error)?;
        if bursts.is_empty() || bursts.len() > 8 || bursts.iter().any(|&n| n == 0 || n > 100_000) {
            return Err("bursts require 1..8 values each in 1..100000".into());
        }
        report.planned_bursts.clone_from(&bursts);
        report.save()?;
        server = Some(bounded(60, async { TestServer::start().await.map_err(error) }).await?);
        let active = server.as_ref().expect("startup completed");
        bounded(10, probe(active)).await?;
        report.baseline_passed = true;
        for offered in bursts {
            stage(active, offered, &mut report).await?;
            bounded(10, probe(active)).await?;
        }
        report.recovery_passed = true;
        Ok::<_, String>(())
    }
    .await;
    report.failure = result.err();
    if let Err(e) = report.save() {
        report.artifact_failure = Some(e);
    }
    if let Some(active) = server {
        let shutdown = bounded(60, async { active.shutdown().await.map_err(error) }).await;
        report.cleanup_passed = shutdown.is_ok();
        if let Err(e) = shutdown {
            report.cleanup_failure = Some(e.clone());
            report.failure.get_or_insert(e);
        }
    }
    if report.artifact_failure.is_some() {
        report
            .failure
            .get_or_insert_with(|| "partial artifact persistence failed".into());
    }
    report.status = if report.failure.is_none() {
        "passed"
    } else {
        "failed"
    };
    report.save().expect("final artifact");
    // Assert
    assert!(
        report.failure.is_none(),
        "{}: {:?}",
        report.path.display(),
        report.failure
    );
    assert!(report.baseline_passed && report.recovery_passed && report.cleanup_passed);
    eprintln!("Notice pressure artifact: {}", report.path.display());
}

#[test]
fn should_detect_changed_notice_payload_bytes() {
    // Arrange
    let mut changed = body(0);
    changed[1023] = 0;
    let mut payload = fitz::protocol::payload_codec::PayloadEncoder::new();
    payload.put_u64(1);
    payload.put_string(ROUTE);
    payload.put_bytes(&changed);
    let mut frame = fitz::testkit::transport::TlvFrameBuilder::new();
    frame.encode_field(504, &payload.finish());
    // Act
    let result = validate(&frame.build(), 1, 1, &mut HashSet::new());
    // Assert
    assert!(result.is_err());
}

fn delivery_frame(sequence: u64, subscription: u64) -> Vec<u8> {
    let mut payload = fitz::protocol::payload_codec::PayloadEncoder::new();
    payload.put_u64(subscription);
    payload.put_string(ROUTE);
    payload.put_bytes(&body(sequence));
    let mut frame = fitz::testkit::transport::TlvFrameBuilder::new();
    frame.encode_field(504, &payload.finish());
    frame.build()
}

#[test]
fn should_detect_duplicate_notice_delivery() {
    // Arrange
    let frame = delivery_frame(0, 1);
    let mut seen = HashSet::new();
    // Act
    let first = validate(&frame, 1, 1, &mut seen);
    let duplicate = validate(&frame, 1, 1, &mut seen);
    // Assert
    assert_eq!(first, Ok(()));
    assert!(duplicate.is_err());
}

#[test]
fn should_detect_unknown_notice_sequence() {
    // Arrange
    let frame = delivery_frame(1, 1);
    // Act
    let result = validate(&frame, 1, 1, &mut HashSet::new());
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_detect_wrong_notice_subscription_identity() {
    // Arrange
    let frame = delivery_frame(0, 2);
    // Act
    let result = validate(&frame, 1, 1, &mut HashSet::new());
    // Assert
    assert!(result.is_err());
}
