//! Opt-in bounded real TCP admission and cancellation diagnostic.
mod fixtures;
#[path = "rpc_pressure_support/report.rs"]
mod report;
#[path = "rpc_pressure_support/wire.rs"]
mod wire;

use fitz::testkit::{TestClient, TestServer};
use fixtures::transport::{
    build_rpc_response_delivery, parse_rpc_request_delivery, parse_rpc_response_delivery,
};
use futures_util::future::join_all;
use report::{Report, Stage};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use uuid::Uuid;

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

async fn connect_worker(server: &TestServer) -> Result<TestClient, String> {
    let mut worker = TestClient::new(server.tcp_addr).await.map_err(error)?;
    worker.send_frame(&wire::subscribe()).await.map_err(error)?;
    wire::registration(&worker.recv_frame(2000).await.map_err(error)?)?;
    Ok(worker)
}

async fn producer(
    server: &TestServer,
    start: usize,
    count: usize,
    counters: Arc<Mutex<Stage>>,
) -> Result<(), String> {
    let mut caller = TestClient::new(server.tcp_addr).await.map_err(error)?;
    let mut pending = HashMap::with_capacity(count);
    for sequence in start..start + count {
        let sequence = u64::try_from(sequence).map_err(error)?;
        let id = Uuid::new_v4();
        caller
            .send_frame(&wire::request(id, sequence, 30_000))
            .await
            .map_err(error)?;
        pending.insert(id, sequence);
        counters.lock().unwrap().sent += 1;
    }
    while !pending.is_empty() {
        let frame = caller.recv_frame(60000).await.map_err(error)?;
        if wire::terminal(&frame, &mut pending)? {
            counters.lock().unwrap().completed += 1;
        } else {
            counters.lock().unwrap().backpressure += 1;
        }
    }
    Ok(())
}

async fn stage(server: &TestServer, offered: usize, report: &mut Report) -> Result<(), String> {
    let mut worker = connect_worker(server).await?;
    let counters = Arc::new(Mutex::new(Stage {
        offered,
        ..Stage::default()
    }));
    let counts = counters.clone();
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();
    let worker_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let mut dispatched = std::collections::HashSet::new();
        loop {
            let frame = tokio::select! {
                _ = &mut stop_rx => return Ok::<_, String>(()),
                frame = worker.recv_frame_bytes_without_timeout() => frame.map_err(error)?,
            };
            let request = parse_rpc_request_delivery(&frame)?;
            if request.route != wire::ROUTE
                || request.body.len() != 1024
                || request.remaining_budget_ms.is_none()
                || !dispatched.insert(request.correlation_id)
            {
                return Err("invalid or duplicate worker dispatch".into());
            }
            let sequence = u64::from_be_bytes(request.body[..8].try_into().map_err(error)?);
            if request.body != wire::body(sequence) {
                return Err("corrupt worker payload".into());
            }
            counts.lock().unwrap().worker_dispatches += 1;
            tokio::time::sleep(Duration::from_millis(1)).await;
            worker
                .send_frame(&build_rpc_response_delivery(
                    request.correlation_id,
                    0,
                    true,
                    &request.body,
                ))
                .await
                .map_err(error)?;
        }
    });
    let started = Instant::now();
    let producers = (0..offered)
        .step_by(128)
        .map(|start| producer(server, start, (offered - start).min(128), counters.clone()));
    let result = tokio::time::timeout(Duration::from_secs(60), join_all(producers)).await;
    let _ = stop_tx.send(());
    let worker_result = worker_task.await.map_err(error).and_then(|r| r);
    let failure = match result {
        Ok(results) => results.into_iter().find_map(Result::err),
        Err(_) => Some("absolute stage deadline exceeded; outcomes unresolved".into()),
    }
    .or_else(|| worker_result.err());
    let mut current = std::mem::take(&mut *counters.lock().unwrap());
    current.elapsed_ns = started.elapsed().as_nanos();
    current.unresolved = current.sent - current.completed - current.backpressure;
    current.failure = failure;
    if current.failure.is_none()
        && (current.sent != offered
            || current.unresolved != 0
            || current.worker_dispatches != current.completed)
    {
        current.failure = Some("stage reconciliation failed".into());
    }
    let result = current.failure.clone().map_or(Ok(()), Err);
    report.stages.push(current);
    report.save()?;
    result
}

async fn lifecycle(server: &TestServer, mode: u8) -> Result<(), String> {
    let mut worker = connect_worker(server).await?;
    let mut caller = TestClient::new(server.tcp_addr).await.map_err(error)?;
    let id = Uuid::new_v4();
    caller
        .send_frame(&wire::request(
            id,
            99_999,
            if mode == 4 { 250 } else { 5000 },
        ))
        .await
        .map_err(error)?;
    let delivery = parse_rpc_request_delivery(&worker.recv_frame(2000).await.map_err(error)?)?;
    match mode {
        1 => {
            caller
                .send_frame(&wire::control(1, id, Some(1)))
                .await
                .map_err(error)?;
            wire::lifecycle(&caller.recv_frame(2000).await.map_err(error)?, 4, id, 2)?;
        }
        3 => {
            drop(caller);
        }
        4 => {
            let response =
                parse_rpc_response_delivery(&caller.recv_frame(2000).await.map_err(error)?)?;
            let (code, _) = fitz::protocol::error_codes::decode_error_body(&response.body)?;
            if response.correlation_id != id
                || response.seq != 0
                || !response.stream_end
                || code != fitz::protocol::error_codes::rpc::ERR_RPC_TIMEOUT
            {
                return Err("invalid timeout terminal".into());
            }
        }
        _ => return Err("unknown lifecycle scenario".into()),
    }
    wire::lifecycle(
        &worker.recv_frame(2000).await.map_err(error)?,
        2,
        delivery.correlation_id,
        mode,
    )?;
    worker
        .send_frame(&wire::control(3, delivery.correlation_id, None))
        .await
        .map_err(error)?;
    // The same registration must regain its single credit after acknowledged cleanup.
    let mut probe = TestClient::new(server.tcp_addr).await.map_err(error)?;
    let probe_id = Uuid::new_v4();
    probe
        .send_frame(&wire::request(probe_id, 100_000, 5000))
        .await
        .map_err(error)?;
    let delivery = parse_rpc_request_delivery(&worker.recv_frame(2000).await.map_err(error)?)?;
    if delivery.body != wire::body(100_000) {
        return Err("cleanup credit probe payload mismatch".into());
    }
    worker
        .send_frame(&build_rpc_response_delivery(
            delivery.correlation_id,
            0,
            true,
            &delivery.body,
        ))
        .await
        .map_err(error)?;
    let response = parse_rpc_response_delivery(&probe.recv_frame(2000).await.map_err(error)?)?;
    if response.correlation_id != probe_id
        || response.seq != 0
        || !response.stream_end
        || response.body != wire::body(100_000)
    {
        return Err("cleanup credit probe failed".into());
    }
    Ok(())
}

async fn wait_for_cleanup(server: &TestServer) -> Result<(), String> {
    server.wait_for_session_count(0).await.map_err(error)?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if server.runtime.rpc_list_workers(None).is_empty()
                && server.runtime.rpc_list_pending(None).is_empty()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "RPC worker/pending cleanup did not settle".into())
}

#[tokio::test]
#[ignore = "bounded pressure diagnostic; run explicitly and serialize shared-host campaigns"]
async fn should_push_rpc_admission_and_cleanup_limits() {
    // Arrange
    let mut report = Report::new().expect("report");
    let server = TestServer::start().await.expect("server");
    report.save().expect("initial artifact");
    // Act
    let result = async {
        let stages = std::env::var("FITZ_RPC_PRESSURE_BURSTS")
            .unwrap_or_else(|_| "1,32,256,1024,8192".into());
        let bursts = stages
            .split(',')
            .map(str::parse::<usize>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(error)?;
        if bursts.is_empty() || bursts.len() > 8 || bursts.iter().any(|&n| n == 0 || n > 8192) {
            return Err("bursts must contain 1..8 values each in 1..8192".into());
        }
        for offered in bursts {
            stage(&server, offered, &mut report).await?;
            wait_for_cleanup(&server).await?;
        }
        for mode in [1, 3, 4] {
            lifecycle(&server, mode).await?;
            wait_for_cleanup(&server).await?;
        }
        report.lifecycle_probes_passed = true;
        stage(&server, 1, &mut report).await?;
        wait_for_cleanup(&server).await?;
        report.recovery_passed = true;
        Ok::<_, String>(())
    }
    .await;
    report.failure = result.err();
    let shutdown = server.shutdown().await.map_err(error);
    report.cleanup_passed = shutdown.is_ok();
    if let Err(e) = shutdown {
        report.failure.get_or_insert(e);
    }
    report.save().expect("final artifact");
    // Assert
    assert!(
        report.failure.is_none(),
        "{}: {:?}",
        report.path.display(),
        report.failure
    );
    assert!(report.recovery_passed && report.lifecycle_probes_passed && report.cleanup_passed);
    eprintln!("RPC pressure artifact: {}", report.path.display());
}

#[test]
fn should_reject_wrong_cancellation_correlation() {
    // Arrange
    let id = Uuid::new_v4();
    let mut payload = vec![2];
    payload.extend_from_slice(id.as_bytes());
    payload.push(1);
    let frame = wire::frame(305, &payload);
    // Act
    let result = wire::lifecycle(&frame, 2, Uuid::new_v4(), 1);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_duplicate_rpc_terminal() {
    // Arrange
    let id = Uuid::new_v4();
    let frame = build_rpc_response_delivery(id, 0, true, &wire::body(0));
    let mut pending = HashMap::from([(id, 0)]);
    // Act
    let first = wire::terminal(&frame, &mut pending);
    let duplicate = wire::terminal(&frame, &mut pending);
    // Assert
    assert_eq!(first, Ok(true));
    assert!(duplicate.is_err());
}

#[test]
fn should_reject_changed_rpc_echo_payload() {
    // Arrange
    let id = Uuid::new_v4();
    let mut payload = wire::body(0);
    payload[1023] = 0;
    let frame = build_rpc_response_delivery(id, 0, true, &payload);
    // Act
    let result = wire::terminal(&frame, &mut HashMap::from([(id, 0)]));
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_unknown_rpc_terminal_identity() {
    // Arrange
    let frame = build_rpc_response_delivery(Uuid::new_v4(), 0, true, &wire::body(0));
    // Act
    let result = wire::terminal(&frame, &mut HashMap::from([(Uuid::new_v4(), 0)]));
    // Assert
    assert!(result.is_err());
}
