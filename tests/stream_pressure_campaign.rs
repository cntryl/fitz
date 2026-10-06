#![cfg(feature = "benchkit")]

#[path = "stream_pressure_support/admission.rs"]
mod admission;
#[path = "stream_pressure_support/io.rs"]
mod io;
#[path = "stream_pressure_support/readback.rs"]
mod readback;
#[path = "stream_pressure_support/report.rs"]
mod report;

use fitz::testkit::{TestClient, TestServer};
use report::{Config, Report, Stage};
use std::fmt::Display;
use std::future::Future;
use std::time::{Duration, Instant};

async fn bounded<T, E: Display>(
    future: impl Future<Output = Result<T, E>>,
    operation: &str,
) -> Result<T, String> {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .map_err(|_| format!("{operation} exceeded 60 seconds; outcome may be indeterminate"))?
        .map_err(|error| error.to_string())
}

struct Storage(Option<tempfile::TempDir>);
impl Drop for Storage {
    fn drop(&mut self) {
        if let Some(directory) = self.0.take() {
            let _ = directory.keep();
        }
    }
}

async fn probe(client: &mut TestClient, report: &mut Report) -> Result<(), String> {
    let session = io::begin(client).await?;
    io::append(
        client,
        session,
        report.committed_events,
        report.config.payload_bytes,
    )
    .await?;
    io::commit(client, session).await?;
    report.committed_events += 1;
    report.probe_events += 1;
    io::replay(client, report.committed_events, report.config.payload_bytes).await?;
    Ok(())
}

async fn grow(
    client: &mut TestClient,
    report: &mut Report,
    stage: &mut Stage,
) -> Result<(), String> {
    while report.committed_events < stage.target_events {
        let session = io::begin(client).await?;
        let count = report
            .config
            .batch_events
            .min(stage.target_events - report.committed_events);
        for offset in report.committed_events..report.committed_events + count {
            stage.attempted_appends += 1;
            match io::append(client, session, offset, report.config.payload_bytes).await {
                Ok(()) => {
                    stage.accepted_appends += 1;
                    stage.pending_batch_events += 1;
                }
                Err(error) if admission::batch_limit(&error) => {
                    stage.rejection = Some(error);
                    io::rollback(client, session).await?;
                    stage.pending_batch_events = 0;
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        stage.commit_requests += 1;
        io::commit(client, session).await?;
        report.committed_events += count;
        stage.committed_events += count;
        stage.committed_batches += 1;
        stage.pending_batch_events = 0;
        report.current = Some(stage.clone());
        report.save()?;
    }
    Ok(())
}

async fn campaign(server: &TestServer, report: &mut Report) -> Result<(), String> {
    let mut client = bounded(TestClient::new(server.tcp_addr), "client connect").await?;
    let result = async {
        probe(&mut client, report).await?;
        for target in report.config.targets.clone() {
            let mut stage = Stage {
                target_events: target,
                ..Stage::default()
            };
            let started = Instant::now();
            report.current = Some(stage.clone());
            report.save()?;
            let result = tokio::time::timeout(
                Duration::from_secs(report.config.stage_seconds),
                grow(&mut client, report, &mut stage),
            )
            .await
            .map_err(|_| {
                "Stream stage deadline exceeded; pending batch outcome may be indeterminate"
                    .to_owned()
            })
            .and_then(std::convert::identity);
            stage.active_elapsed_ns = started.elapsed().as_nanos();
            let result = if result.is_ok() {
                let replay = Instant::now();
                let verified = io::replay(
                    &mut client,
                    report.committed_events,
                    report.config.payload_bytes,
                )
                .await;
                stage.replay_elapsed_ns = replay.elapsed().as_nanos();
                verified.map(|events| stage.replayed_events = events)
            } else {
                result
            };
            if let Err(error) = &result {
                stage.failure = Some(error.clone());
            }
            let boundary = stage.rejection.is_some();
            report.stages.push(stage);
            report.current = None;
            report.save()?;
            result?;
            if boundary {
                break;
            }
        }
        probe(&mut client, report).await
    }
    .await;
    let close = bounded(client.close(), "client close").await;
    result.and(close)
}

async fn shutdown(server: TestServer) -> Result<(), String> {
    tokio::time::timeout(Duration::from_secs(60), server.shutdown())
        .await
        .map_err(|_| "broker shutdown exceeded 60 seconds".to_owned())?
        .map_err(|e| e.to_string())
}

async fn verify_reopened(server: &TestServer, report: &mut Report) -> Result<(), String> {
    let mut client = bounded(TestClient::new(server.tcp_addr), "readback client connect").await?;
    let verified = io::replay(
        &mut client,
        report.committed_events,
        report.config.payload_bytes,
    )
    .await;
    let close = bounded(client.close(), "readback client close").await;
    let verified = verified?;
    close?;
    report.clean_restart_verified_events = verified;
    let mut client = bounded(TestClient::new(server.tcp_addr), "recovery client connect").await?;
    let probe = probe(&mut client, report).await;
    let close = bounded(client.close(), "recovery client close").await;
    probe.and(close)?;
    report.recovery_probe_passed = true;
    Ok(())
}

async fn restart(path: &std::path::Path, report: &mut Report) -> Result<(), String> {
    let reopened = bounded(
        TestServer::start_with_local_storage(path.to_string_lossy()),
        "broker reopen",
    )
    .await;
    let server = match reopened {
        Ok(server) => server,
        Err(error) => {
            report.cleanup_status = "not_confirmed";
            return Err(error);
        }
    };
    let readback = verify_reopened(&server, report).await;
    if let Err(error) = &readback {
        report.failure.get_or_insert_with(|| error.clone());
    }
    let saved = report.save();
    let stopped = shutdown(server).await;
    report.cleanup_status = if stopped.is_ok() {
        "completed"
    } else {
        "failed"
    };
    if let Err(error) = &stopped {
        report.cleanup_failure = Some(error.clone());
    }
    readback.and(saved).and(stopped)
}

async fn execute() -> Result<(), String> {
    let config = Config::from_env()?;
    let mut storage = Storage(Some(
        tempfile::Builder::new()
            .prefix("fitz-stream-pressure-")
            .tempdir()
            .map_err(|e| e.to_string())?,
    ));
    let path = storage
        .0
        .as_ref()
        .ok_or("missing fresh store")?
        .path()
        .to_path_buf();
    let mut report = Report::new(config, path.clone())?;
    report.save()?;
    eprintln!("Stream pressure artifact: {}", report.path.display());
    let server = bounded(
        TestServer::start_with_local_storage(path.to_string_lossy()),
        "broker startup",
    )
    .await;
    let mut result = match server {
        Ok(server) => {
            let result = campaign(&server, &mut report).await;
            if let Err(error) = &result {
                report.failure = Some(error.clone());
            }
            // Artifact I/O cannot prevent broker shutdown.
            let saved = report.save();
            let stopped = shutdown(server).await;
            report.cleanup_status = if stopped.is_ok() {
                "completed"
            } else {
                "failed"
            };
            if let Err(error) = &stopped {
                report.cleanup_failure = Some(error.clone());
            }
            result.and(saved).and(stopped)
        }
        Err(error) => {
            report.cleanup_status = "not_confirmed";
            Err(error)
        }
    };
    if result.is_ok() {
        result = restart(&path, &mut report).await;
    }
    if result.is_ok() {
        if let Some(directory) = storage.0.take() {
            if let Err(error) = directory.close() {
                report.cleanup_status = "failed";
                report.cleanup_failure = Some(error.to_string());
                result = Err(error.to_string());
            }
        }
    }
    if let Err(error) = &result {
        report.failure.get_or_insert_with(|| error.clone());
    }
    report.status = if result.is_ok() { "passed" } else { "failed" };
    report.save()?;
    eprintln!(
        "Stream pressure {}: {} committed events; {} verified after clean restart",
        report.status, report.committed_events, report.clean_restart_verified_events
    );
    result
}

#[test]
#[ignore = "opt-in bounded local-disk history-growth and clean-restart pressure diagnostic"]
fn should_preserve_committed_history_under_stream_pressure() {
    // Arrange
    let runtime = fitz::benchkit::shared_bench_runtime();
    // Act
    let result = runtime.block_on(execute());
    // Assert
    result.unwrap_or_else(|error| panic!("{error}"));
}
