mod report;

use super::{io, ledger::Ledger};
use crate::stress_support::fixture::{BrokerFixture, StorageDirectory, StorageProfile};
use crate::stress_support::types::{BenchFailure, FailureKind};
use cntryl_stress::{LogicalUnit, OperationOutcome, StressContext, StressError, StressResult};
use fitz::benchkit::shared_bench_runtime;
use fitz::testkit::TestClient;
use report::{CurrentReservation, PairTiming, Report};
use std::time::{Duration, Instant};

#[path = "latency_oracle.rs"]
mod oracle;

const WINDOW: Duration = Duration::from_secs(90);
const PAUSE: Duration = Duration::from_millis(5);

pub(crate) fn run(ctx: &mut StressContext) -> StressResult {
    execute(ctx).map_err(|error| StressError::new(error.to_string()))
}

fn execute(ctx: &mut StressContext) -> Result<(), BenchFailure> {
    let raw = match std::env::var("FITZ_QUEUE_DRAIN_PAIRS") {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => "1000".into(),
        Err(error) => return Err(BenchFailure::validation(error.to_string())),
    };
    let pairs = oracle::parse_pairs(&raw).map_err(BenchFailure::validation)?;
    let mut report = Report::new(pairs)?;
    ctx.parameter("workload", "queue_separate_reserve_ack_pause_latency");
    ctx.parameter("pairs", pairs);
    ctx.parameter("workload_deadline_seconds", WINDOW.as_secs());
    ctx.parameter("consumer_delay_ms", PAUSE.as_millis());
    ctx.parameter("storage_profile", "local_disk");
    ctx.metadata("durability_scope", "running_process_fast_local_disk");
    ctx.metadata("target_class", "correctness_timing_diagnostic");
    ctx.metadata(
        "elapsed_scope",
        "startup, seed, drain and empty verification; excludes cleanup",
    );
    let directory = StorageDirectory::new(StorageProfile::LocalDisk)?;
    report.local_storage_path = directory.path().map(std::path::Path::to_path_buf);
    report.save()?;
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + WINDOW;
    let fixture = shared_bench_runtime().block_on(until(
        deadline,
        BrokerFixture::start(StorageProfile::LocalDisk, directory),
    ));
    let fixture = match fixture {
        Ok(fixture) => fixture,
        Err(error) => {
            report.fail(&error);
            let saved = report.save();
            return Err::<(), _>(error).and(saved);
        }
    };
    let mut result = shared_bench_runtime().block_on(until(
        deadline,
        measure(ctx, &mut report, &fixture, started, pairs),
    ));
    report.workload_elapsed_ns = started.elapsed().as_nanos();
    (report.process_rss_bytes, report.process_peak_rss_bytes) =
        crate::stress_support::artifacts::memory_sample();
    report.capture_metrics_after();
    if let Err(error) = &result {
        report.fail(error);
    }
    // Retain the primary failure before shutdown. Artifact failures still allow cleanup.
    if let Err(error) = report.save() {
        if result.is_ok() {
            report.fail(&error);
            result = Err(error);
        }
    }
    let (server, directory) = fixture.into_parts();
    let cleanup = shared_bench_runtime().block_on(io::bounded(async {
        server.shutdown().await.map_err(BenchFailure::transport)
    }));
    match cleanup {
        Ok(()) => {
            report.cleanup_status = "completed";
            if result.is_ok() {
                if let Err(error) = directory.release() {
                    report.cleanup_status = "failed";
                    report.cleanup_failure = Some(error.to_string());
                    report.fail(&error);
                    result = Err(error);
                }
            } else {
                drop(directory);
            }
        }
        Err(error) => {
            report.cleanup_status = "failed";
            report.cleanup_failure = Some(error.to_string());
            if result.is_ok() {
                report.fail(&error);
                result = Err(error);
            }
            drop(directory);
        }
    }
    report.status = if result.is_ok() { "passed" } else { "failed" };
    let saved = report.save();
    let failures = u64::from(result.is_err() || saved.is_err());
    let completed = report.accounting.acknowledged;
    ctx.record_external_outcome(
        "validated_reserve_ack_pairs",
        Duration::from_nanos(u64::try_from(report.workload_elapsed_ns).unwrap_or(u64::MAX)),
        LogicalUnit::new("payload_and_message_id_verified_ack"),
        OperationOutcome::new(completed + failures, completed).failures(failures),
    );
    result.and(saved)
}

async fn measure(
    ctx: &mut StressContext,
    report: &mut Report,
    fixture: &BrokerFixture,
    started: Instant,
    pairs: usize,
) -> Result<(), BenchFailure> {
    let mut client = TestClient::new(fixture.tcp_addr())
        .await
        .map_err(BenchFailure::transport)?;
    let mut ledger = Ledger::default();
    seed(ctx, report, &mut client, &mut ledger, pairs).await?;
    report.setup_elapsed_ns = started.elapsed().as_nanos();
    report.capture_metrics_before();
    drain(ctx, report, &mut client, &mut ledger, pairs).await
}

async fn seed(
    ctx: &StressContext,
    report: &mut Report,
    client: &mut TestClient,
    ledger: &mut Ledger,
    pairs: usize,
) -> Result<(), BenchFailure> {
    report.phase = "seed";
    for _ in 0..pairs {
        let sequence = ledger.dispatch();
        report.accounting = ledger.counts;
        let accepted = io::enqueue(client, sequence).await?;
        let Some(id) = accepted else {
            ledger
                .rejected(sequence)
                .map_err(BenchFailure::verification)?;
            report.accounting = ledger.counts;
            return Err(BenchFailure::domain_error(
                "Queue rejected diagnostic seed message",
            ));
        };
        ledger
            .accepted(sequence, id)
            .map_err(BenchFailure::verification)?;
        report.accepted_messages.push((sequence, id));
        report.accounting = ledger.counts;
        ctx.progress_handle().advance();
        if ledger.counts.accepted.is_multiple_of(100) {
            report.save()?;
        }
    }
    Ok(())
}

async fn drain(
    ctx: &StressContext,
    report: &mut Report,
    client: &mut TestClient,
    ledger: &mut Ledger,
    pairs: usize,
) -> Result<(), BenchFailure> {
    report.phase = "reserve";
    let drain_started = Instant::now();
    for _ in 0..pairs {
        let cycle = Instant::now();
        report.phase = "reserve";
        let reserve = Instant::now();
        let reservation = io::reserve(client).await?;
        let reserve_ns = reserve.elapsed().as_nanos();
        let io::Reservation::Message {
            sequence,
            id,
            token,
        } = reservation
        else {
            return Err(BenchFailure::verification(
                "diagnostic backlog unexpectedly empty or reserve rejected",
            ));
        };
        report.current_reserved = Some(CurrentReservation {
            sequence,
            message_id: id,
            token,
            ack_state: None,
            ack_application_effect: "not_dispatched",
            ledger_acknowledged: false,
            pause_completed: false,
            reserve_ns,
            ack_ns: None,
        });
        if report.accepted_messages.get(sequence) != Some(&(sequence, id))
            || report
                .samples
                .iter()
                .any(|sample| sample.sequence == sequence)
        {
            return Err(BenchFailure::verification(
                "reserved diagnostic message identity changed or was already acknowledged",
            ));
        }
        report.phase = "ack";
        let ack_ns = acknowledge(report, client, id, token).await?;
        ledger
            .acknowledged(sequence, id)
            .map_err(BenchFailure::verification)?;
        report.accounting = ledger.counts;
        if let Some(current) = report.current_reserved.as_mut() {
            current.ledger_acknowledged = true;
            current.ack_ns = Some(ack_ns);
        }
        ctx.progress_handle().advance();
        report.phase = "acknowledged_pause_pending_sample";
        let pause = Instant::now();
        tokio::time::sleep(PAUSE).await;
        if let Some(current) = report.current_reserved.as_mut() {
            current.pause_completed = true;
        }
        report.samples.push(PairTiming {
            sequence,
            message_id: id,
            reserve_ns,
            ack_ns,
            pause_ns: pause.elapsed().as_nanos(),
            cycle_ns: cycle.elapsed().as_nanos(),
        });
        report.current_reserved = None;
        report.drain_elapsed_ns = drain_started.elapsed().as_nanos();
        if report.samples.len().is_multiple_of(100) {
            report.save()?;
        }
    }
    ledger
        .verify_drained()
        .map_err(BenchFailure::verification)?;
    report.phase = "empty_verification";
    if !matches!(io::reserve(client).await?, io::Reservation::Empty) {
        return Err(BenchFailure::verification(
            "Queue not empty after diagnostic ACKs",
        ));
    }
    report.empty_verified = true;
    report.phase = "complete";
    Ok(())
}

async fn acknowledge(
    report: &mut Report,
    client: &mut TestClient,
    id: u64,
    token: u64,
) -> Result<u128, BenchFailure> {
    let ack = Instant::now();
    io::acknowledge_observed(client, id, token, |state| report.observe_ack(state)).await?;
    Ok(ack.elapsed().as_nanos())
}

async fn until<T>(
    deadline: tokio::time::Instant,
    future: impl std::future::Future<Output = Result<T, BenchFailure>>,
) -> Result<T, BenchFailure> {
    tokio::time::timeout_at(deadline, future).await.map_err(|_| BenchFailure {
        kind: FailureKind::Timeout,
        detail: "Queue diagnostic exceeded 90-second workload deadline; consult current_reserved ACK state and accounting for known outcomes or an unresolved dispatched ACK".into(),
    })?
}
