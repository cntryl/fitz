use crate::stress_support::artifacts::{memory_sample, Artifacts, CleanupFailure, Phase};
use crate::stress_support::durable::DurableDriver;
use crate::stress_support::ephemeral::EphemeralDriver;
use crate::stress_support::fixture::{BrokerFixture, StorageDirectory, StorageProfile};
use crate::stress_support::types::{BenchFailure, Domain, FailureKind, StepOutcome};
use cntryl_stress::{
    LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome, ProgressHandle,
    StressContext, StressError, StressResult,
};
use fitz::benchkit::shared_bench_runtime;
use futures_util::future::join_all;
use std::future::Future;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

const LOADS: [usize; 7] = [1, 2, 4, 8, 16, 32, 64];
const OPERATION_TIMEOUT: Duration = Duration::from_secs(10);
const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(5);
const VERIFY_INTERVAL: Duration = Duration::from_secs(30);

enum Driver {
    Durable(DurableDriver),
    Ephemeral(EphemeralDriver),
}

impl Driver {
    async fn connect(
        domain: Domain,
        address: SocketAddr,
        lane: usize,
    ) -> Result<Self, BenchFailure> {
        match domain {
            Domain::Queue | Domain::Kv | Domain::Stream | Domain::Schedule => {
                DurableDriver::connect(domain, address, lane)
                    .await
                    .map(Self::Durable)
            }
            Domain::Rpc | Domain::Notice | Domain::Lease => {
                EphemeralDriver::connect(domain, address, lane)
                    .await
                    .map(Self::Ephemeral)
            }
        }
    }

    async fn step(&mut self, sequence: u64) -> Result<StepOutcome, BenchFailure> {
        match self {
            Self::Durable(driver) => driver.step(sequence).await,
            Self::Ephemeral(driver) => driver.step(sequence).await,
        }
    }

    async fn verify(&mut self) -> Result<u64, BenchFailure> {
        match self {
            Self::Durable(driver) => driver.verify().await,
            Self::Ephemeral(driver) => driver.verify().await,
        }
    }

    async fn close(self) -> Result<(), BenchFailure> {
        match self {
            Self::Durable(driver) => driver.close().await,
            Self::Ephemeral(driver) => driver.close().await,
        }
    }
}

async fn bounded<T>(
    operation: impl Future<Output = Result<T, BenchFailure>>,
) -> Result<T, BenchFailure> {
    tokio::time::timeout(OPERATION_TIMEOUT, operation)
        .await
        .map_err(|_| BenchFailure {
            kind: FailureKind::Timeout,
            detail: "complete TCP operation exceeded 10 seconds; outcome may be indeterminate"
                .to_owned(),
        })?
}

fn configured_duration(tier: u8) -> Result<Duration, BenchFailure> {
    let key = format!("FITZ_TIER{tier}_DURATION_SECS");
    let configured = match environment_value(&key)? {
        Some(value) => Some(value),
        None => environment_value("FITZ_STRESS_DURATION_SECS")?,
    };
    let seconds = configured
        .map_or(Ok(3_600), |value| value.parse::<u64>())
        .map_err(|_| BenchFailure::validation("duration must be integer seconds"))?;
    if !(1..=86_400).contains(&seconds) {
        return Err(BenchFailure::validation(
            "duration must be between 1 and 86400 seconds",
        ));
    }
    Ok(Duration::from_secs(seconds))
}

fn environment_value(key: &str) -> Result<Option<String>, BenchFailure> {
    match std::env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(BenchFailure::validation(format!(
            "{key} must contain Unicode text"
        ))),
    }
}

fn configured_progress_timeout() -> Result<Duration, BenchFailure> {
    let seconds = environment_value("STRESS_NO_PROGRESS_TIMEOUT_SECS")?
        .map_or(Ok(60), |value| value.parse::<u64>())
        .map_err(|_| BenchFailure::validation("progress timeout must be integer seconds"))?;
    if seconds == 0 {
        return Err(BenchFailure::validation(
            "STRESS_NO_PROGRESS_TIMEOUT_SECS must be greater than zero",
        ));
    }
    Ok(Duration::from_secs(seconds))
}

fn record_failure(phase: &mut Phase, error: &BenchFailure) {
    phase.failures = phase.failures.saturating_add(1);
    match error.kind {
        FailureKind::Timeout => phase.timeouts = phase.timeouts.saturating_add(1),
        FailureKind::InvalidResponse | FailureKind::Verification => {
            phase.validation_errors = phase.validation_errors.saturating_add(1);
        }
        FailureKind::Transport | FailureKind::DomainError => {}
    }
    phase.failure.get_or_insert_with(|| error.to_string());
    phase
        .failure_kind
        .get_or_insert_with(|| format!("{:?}", error.kind));
}

fn classify(
    phase: &mut Phase,
    lane: usize,
    result: Result<StepOutcome, BenchFailure>,
    elapsed: Duration,
) -> Option<BenchFailure> {
    phase.attempted = phase.attempted.saturating_add(1);
    match result {
        Ok(StepOutcome::Completed) => {
            phase.completed = phase.completed.saturating_add(1);
            phase.lane_completions[lane] = phase.lane_completions[lane].saturating_add(1);
            phase.latencies.record(elapsed);
        }
        Ok(StepOutcome::CapacityRejected(code)) => {
            let count = phase.capacity_rejections.entry(code).or_default();
            *count = count.saturating_add(1);
            phase.lane_capacity_rejections[lane] =
                phase.lane_capacity_rejections[lane].saturating_add(1);
        }
        Ok(StepOutcome::Contended) => phase.contentions = phase.contentions.saturating_add(1),
        Ok(StepOutcome::DeliveryWindowMiss) => {
            phase.delivery_window_misses = phase.delivery_window_misses.saturating_add(1);
        }
        Err(error) => {
            record_failure(phase, &error);
            return Some(error);
        }
    }
    None
}

struct Batch {
    elapsed: Duration,
    outcomes: Vec<StepObservation>,
}

struct StepObservation {
    result: Result<StepOutcome, BenchFailure>,
    elapsed: Duration,
    completed_at: Duration,
}

async fn bounded_step(
    driver: &mut Driver,
    sequence: u64,
    remaining: Duration,
    lane: usize,
) -> Result<StepOutcome, BenchFailure> {
    let window = remaining.min(OPERATION_TIMEOUT);
    if window.is_zero() {
        return Err(BenchFailure {
            kind: FailureKind::Timeout,
            detail: format!("lane {lane} exceeded its completed-cycle progress deadline"),
        });
    }
    tokio::time::timeout(window, driver.step(sequence))
        .await
        .map_err(|_| BenchFailure {
            kind: FailureKind::Timeout,
            detail: if window < OPERATION_TIMEOUT {
                format!("lane {lane} exceeded its completed-cycle progress deadline; outcome may be indeterminate")
            } else {
                "complete TCP operation exceeded 10 seconds; outcome may be indeterminate".to_owned()
            },
        })?
}

async fn batch(
    drivers: &mut [Driver],
    sequences: &mut [u64],
    remaining: &[Duration],
    progress: &ProgressHandle,
) -> Batch {
    let batch_started = Instant::now();
    let outcomes = join_all(
        drivers
            .iter_mut()
            .zip(sequences.iter_mut())
            .enumerate()
            .map(|(lane, (driver, sequence))| async move {
                let started = Instant::now();
                let result = bounded_step(driver, *sequence, remaining[lane], lane).await;
                let completed_at = batch_started.elapsed();
                if matches!(&result, Ok(StepOutcome::Completed)) {
                    progress.advance();
                }
                *sequence = sequence.saturating_add(1);
                StepObservation {
                    result,
                    elapsed: started.elapsed(),
                    completed_at,
                }
            }),
    )
    .await;
    Batch {
        elapsed: batch_started.elapsed(),
        outcomes,
    }
}

async fn verify(drivers: &mut [Driver], phase: &mut Phase) -> Result<(), BenchFailure> {
    let results = join_all(drivers.iter_mut().map(|driver| bounded(driver.verify()))).await;
    let mut failure = None;
    for result in results {
        let result = match result {
            Ok(0) => Err(BenchFailure::verification(
                "driver supplied no actual verification checks",
            )),
            result => result,
        };
        match result {
            Ok(observed) => {
                phase.verification_checks = phase.verification_checks.saturating_add(observed);
            }
            Err(error) => {
                record_failure(phase, &error);
                failure.get_or_insert(error);
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

fn record_phase(ctx: &mut StressContext, phase: &Phase) {
    let outcome = OperationOutcome::new(
        phase.completed.saturating_add(phase.failures),
        phase.completed,
    )
    .failures(phase.failures)
    .timeouts(phase.timeouts)
    .validation_errors(phase.validation_errors);
    ctx.record_external_outcome(
        &phase.name,
        Duration::from_nanos(u64::try_from(phase.elapsed_ns).unwrap_or(u64::MAX)),
        LogicalUnit::new("accepted_domain_cycle"),
        outcome,
    );
    // Context parameters also update the last row, so set this after its creation.
    ctx.parameter("client_count", phase.clients);
    ctx.parameter("framework_attempt_scope", "accepted_or_unexpected_failure");
    for (name, value, unit) in [
        ("raw_attempts", phase.attempted, ObservationUnit::Count),
        (
            "capacity_rejections",
            phase.capacity_rejections.values().copied().sum(),
            ObservationUnit::Count,
        ),
        ("contentions", phase.contentions, ObservationUnit::Count),
        (
            "delivery_window_misses",
            phase.delivery_window_misses,
            ObservationUnit::Count,
        ),
        (
            "verification_checks",
            phase.verification_checks,
            ObservationUnit::Count,
        ),
        (
            "latency_p50_upper_ns",
            phase.latencies.percentile_upper_ns(50),
            ObservationUnit::Nanoseconds,
        ),
        (
            "latency_p95_upper_ns",
            phase.latencies.percentile_upper_ns(95),
            ObservationUnit::Nanoseconds,
        ),
        (
            "latency_p99_upper_ns",
            phase.latencies.percentile_upper_ns(99),
            ObservationUnit::Nanoseconds,
        ),
        (
            "latency_max_ns",
            phase.latencies.max_ns,
            ObservationUnit::Nanoseconds,
        ),
    ] {
        #[allow(clippy::cast_precision_loss)]
        ctx.record_observation(
            name,
            value as f64,
            unit,
            ObservationDirection::Informational,
        );
    }
}

struct PhaseSettings {
    name: String,
    duration: Duration,
    progress_timeout: Duration,
    allow_capacity_boundary: bool,
}

impl PhaseSettings {
    fn new(
        name: impl Into<String>,
        duration: Duration,
        progress_timeout: Duration,
        allow_capacity_boundary: bool,
    ) -> Self {
        Self {
            name: name.into(),
            duration,
            progress_timeout,
            allow_capacity_boundary,
        }
    }
}

fn elapsed_duration(nanos: u128) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
}

fn remaining_progress(report: &Phase, limit: Duration) -> Vec<Duration> {
    report
        .lane_last_completion_ns
        .iter()
        .map(|last| limit.saturating_sub(elapsed_duration(report.elapsed_ns.saturating_sub(*last))))
        .collect()
}

fn apply_batch(report: &mut Phase, batch: Batch) -> Result<(), BenchFailure> {
    let began_at = report.elapsed_ns;
    report.elapsed_ns = report.elapsed_ns.saturating_add(batch.elapsed.as_nanos());
    let mut failure = None;
    for (lane, observed) in batch.outcomes.into_iter().enumerate() {
        if matches!(&observed.result, Ok(StepOutcome::Completed)) {
            report.lane_last_completion_ns[lane] =
                began_at.saturating_add(observed.completed_at.as_nanos());
        }
        if let Some(error) = classify(report, lane, observed.result, observed.elapsed) {
            failure.get_or_insert(error);
        }
    }
    failure.map_or(Ok(()), Err)
}

fn check_lane_progress(report: &mut Phase, limit: Duration) -> Result<(), BenchFailure> {
    if let Some(lane) = remaining_progress(report, limit)
        .iter()
        .position(Duration::is_zero)
    {
        let error = BenchFailure {
            kind: FailureKind::Timeout,
            detail: format!(
                "lane {lane} completed no useful cycle within {} active seconds",
                limit.as_secs()
            ),
        };
        record_failure(report, &error);
        return Err(error);
    }
    Ok(())
}

fn verify_phase(drivers: &mut [Driver], report: &mut Phase) -> Result<(), BenchFailure> {
    let started = Instant::now();
    let result = shared_bench_runtime().block_on(verify(drivers, report));
    report.verification_elapsed_ns = report
        .verification_elapsed_ns
        .saturating_add(started.elapsed().as_nanos());
    result
}

fn run_phase_loop(
    artifacts: &mut Artifacts,
    drivers: &mut [Driver],
    sequences: &mut [u64],
    progress: &ProgressHandle,
    settings: &PhaseSettings,
    report: &mut Phase,
    started: Instant,
) -> Result<(), BenchFailure> {
    let runtime = shared_bench_runtime();
    let mut checkpoint = started;
    let mut last_verification_ns = 0;
    loop {
        let remaining = remaining_progress(report, settings.progress_timeout);
        let observed = runtime.block_on(batch(drivers, sequences, &remaining, progress));
        apply_batch(report, observed)?;
        if settings.allow_capacity_boundary && !report.capacity_rejections.is_empty() {
            report.capacity_boundary = true;
            break;
        }
        check_lane_progress(report, settings.progress_timeout)?;
        if report.elapsed_ns.saturating_sub(last_verification_ns) >= VERIFY_INTERVAL.as_nanos() {
            verify_phase(drivers, report)?;
            last_verification_ns = report.elapsed_ns;
        }
        if checkpoint.elapsed() >= CHECKPOINT_INTERVAL {
            (report.process_rss_bytes, report.process_peak_rss_bytes) = memory_sample();
            report.wall_elapsed_ns = started.elapsed().as_nanos();
            artifacts.current = Some(report.clone());
            artifacts.save()?;
            checkpoint = Instant::now();
        }
        if report.elapsed_ns >= settings.duration.as_nanos() {
            break;
        }
    }
    Ok(())
}

fn finish_phase_verification(
    drivers: &mut [Driver],
    report: &mut Phase,
) -> Result<(), BenchFailure> {
    if !report.capacity_boundary && report.lane_completions.contains(&0) {
        let error =
            BenchFailure::verification("at least one lane completed no validated domain cycles");
        record_failure(report, &error);
        return Err(error);
    }
    verify_phase(drivers, report)
}

fn phase(
    ctx: &mut StressContext,
    artifacts: &mut Artifacts,
    drivers: &mut [Driver],
    sequences: &mut [u64],
    progress: &ProgressHandle,
    settings: &PhaseSettings,
) -> Result<(), BenchFailure> {
    let mut report = Phase {
        name: settings.name.clone(),
        clients: drivers.len(),
        configured_ns: settings.duration.as_nanos(),
        no_progress_timeout_ns: settings.progress_timeout.as_nanos(),
        termination: "running",
        lane_completions: vec![0; drivers.len()],
        lane_capacity_rejections: vec![0; drivers.len()],
        lane_last_completion_ns: vec![0; drivers.len()],
        ..Phase::default()
    };
    let started = Instant::now();
    artifacts.current = Some(report.clone());
    artifacts.save()?;
    let mut result = run_phase_loop(
        artifacts,
        drivers,
        sequences,
        progress,
        settings,
        &mut report,
        started,
    );
    if result.is_ok() {
        result = finish_phase_verification(drivers, &mut report);
    }
    if let Err(error) = &result {
        if report.failure.is_none() {
            record_failure(&mut report, error);
        }
    }
    report.termination = if result.is_err() {
        "failed"
    } else if report.capacity_boundary {
        "capacity_boundary"
    } else if settings.duration.is_zero() {
        "probe_complete"
    } else {
        "duration_complete"
    };
    report.wall_elapsed_ns = started.elapsed().as_nanos();
    (report.process_rss_bytes, report.process_peak_rss_bytes) = memory_sample();
    record_phase(ctx, &report);
    artifacts.current = None;
    artifacts.phases.push(report);
    let saved = artifacts.save();
    result.and(saved)
}

fn configure_context(
    ctx: &mut StressContext,
    domain: Domain,
    tier: u8,
    duration: Duration,
    storage_profile: StorageProfile,
) {
    ctx.parameter("domain", domain.label());
    ctx.parameter("storage_profile", storage_profile.label());
    ctx.parameter("transport", "tcp");
    ctx.parameter("inflight_per_lane", 1);
    ctx.parameter("configured_seconds", duration.as_secs());
    ctx.parameter(
        "workload",
        if tier == 5 {
            "closed_loop_concurrency_sweep"
        } else {
            "bounded_state_endurance"
        },
    );
    ctx.metadata("target_class", "stress_characterization");
    ctx.metadata(
        "latency_estimator",
        "fixed_power_of_two_nanosecond_histogram",
    );
    ctx.metadata("durability_scope", storage_profile.durability_scope());
}

fn prepare_drivers(
    domain: Domain,
    address: SocketAddr,
    maximum: usize,
    drivers: &mut Vec<Driver>,
) -> Result<(), BenchFailure> {
    let preparations = shared_bench_runtime().block_on(join_all(
        (0..maximum).map(|lane| bounded(Driver::connect(domain, address, lane))),
    ));
    let mut failure = None;
    for preparation in preparations {
        match preparation {
            Ok(driver) => drivers.push(driver),
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    failure.map_or(Ok(()), Err)
}

fn run_phases(
    ctx: &mut StressContext,
    artifacts: &mut Artifacts,
    drivers: &mut [Driver],
    duration: Duration,
    progress_timeout: Duration,
    tier: u8,
) -> Result<(), BenchFailure> {
    let mut sequences = vec![1; drivers.len()];
    let progress = ctx.progress_handle();
    phase(
        ctx,
        artifacts,
        &mut drivers[..1],
        &mut sequences[..1],
        &progress,
        &PhaseSettings::new(
            "baseline_one_client",
            Duration::ZERO,
            progress_timeout,
            false,
        ),
    )?;
    let loads: &[usize] = if tier == 5 { &LOADS } else { &[8] };
    let count = u32::try_from(loads.len()).map_err(BenchFailure::transport)?;
    let per_stage = duration / count;
    for (index, clients) in loads.iter().copied().enumerate() {
        let budget = if index + 1 == loads.len() {
            duration.saturating_sub(per_stage * (count - 1))
        } else {
            per_stage
        };
        phase(
            ctx,
            artifacts,
            &mut drivers[..clients],
            &mut sequences[..clients],
            &progress,
            &PhaseSettings::new(
                format!("active_{clients}_clients"),
                budget,
                progress_timeout,
                tier == 5,
            ),
        )?;
    }
    phase(
        ctx,
        artifacts,
        &mut drivers[..1],
        &mut sequences[..1],
        &progress,
        &PhaseSettings::new(
            "recovery_one_client",
            Duration::ZERO,
            progress_timeout,
            false,
        ),
    )
}

fn remember_failure(artifacts: &mut Artifacts, result: &Result<(), BenchFailure>) {
    if let Err(error) = result {
        artifacts.status = "failed";
        artifacts.failure = Some(error.to_string());
        artifacts.failure_kind = Some(format!("{:?}", error.kind));
    }
}

fn append_failure(result: &mut Result<(), BenchFailure>, error: BenchFailure, context: &str) {
    match result {
        Ok(()) => {
            *result = Err(BenchFailure {
                kind: error.kind,
                detail: format!("{context}: {}", error.detail),
            });
        }
        Err(original) => {
            original.detail = format!("{}; {context}: {error}", original.detail);
        }
    }
}

fn save_cleanup_progress(artifacts: &mut Artifacts, result: &mut Result<(), BenchFailure>) {
    remember_failure(artifacts, result);
    if let Err(error) = artifacts.save() {
        append_failure(result, error, "saving cleanup evidence failed");
        remember_failure(artifacts, result);
    }
}

fn record_cleanup_failure(
    artifacts: &mut Artifacts,
    result: &mut Result<(), BenchFailure>,
    stage: &'static str,
    driver_index: Option<usize>,
    error: BenchFailure,
) {
    artifacts.cleanup_failures.push(CleanupFailure {
        stage,
        driver_index,
        failure_kind: format!("{:?}", error.kind),
        failure: error.to_string(),
    });
    let context = driver_index.map_or_else(
        || format!("{stage} cleanup failed"),
        |index| format!("{stage} cleanup at prepared index {index} failed"),
    );
    append_failure(result, error, &context);
    save_cleanup_progress(artifacts, result);
}

fn close_server_and_directory(
    fixture: BrokerFixture,
    artifacts: &mut Artifacts,
    result: &mut Result<(), BenchFailure>,
) {
    let (server, storage_directory) = fixture.into_parts();
    // Keep the directory outside the timed future: a canceled shutdown must not
    // remove files still owned by a live storage engine.
    let shutdown = shared_bench_runtime().block_on(bounded(async {
        server.shutdown().await.map_err(BenchFailure::transport)
    }));
    match shutdown {
        Ok(()) => {
            if let Err(error) = storage_directory.release() {
                record_cleanup_failure(artifacts, result, "directory", None, error);
            }
        }
        Err(mut error) => {
            if let Some(path) = storage_directory.path() {
                error.detail = format!(
                    "{}; benchmark storage retained at {}",
                    error.detail,
                    path.display()
                );
            }
            drop(storage_directory);
            record_cleanup_failure(artifacts, result, "server", None, error);
        }
    }
}

fn close_fixture(
    drivers: Vec<Driver>,
    fixture: BrokerFixture,
    mut result: Result<(), BenchFailure>,
    artifacts: &mut Artifacts,
) -> Result<(), BenchFailure> {
    // Persist the workload failure while cleanup is still not_started, then
    // persist running before any close or synchronous actor join can block.
    save_cleanup_progress(artifacts, &mut result);
    artifacts.cleanup_status = "running";
    save_cleanup_progress(artifacts, &mut result);
    let closed = shared_bench_runtime().block_on(join_all(
        drivers.into_iter().map(|driver| bounded(driver.close())),
    ));
    for (index, outcome) in closed.into_iter().enumerate() {
        if let Err(error) = outcome {
            record_cleanup_failure(artifacts, &mut result, "driver", Some(index), error);
        }
    }
    close_server_and_directory(fixture, artifacts, &mut result);
    artifacts.cleanup_status = if artifacts.cleanup_failures.is_empty() {
        "completed"
    } else {
        "failed"
    };
    save_cleanup_progress(artifacts, &mut result);
    result
}

fn finalize(artifacts: &mut Artifacts, result: Result<(), BenchFailure>) -> StressResult {
    artifacts.status = if result.is_ok() {
        "completed"
    } else {
        "failed"
    };
    remember_failure(artifacts, &result);
    if let Err(save_error) = artifacts.save() {
        let detail = result.as_ref().err().map_or_else(
            || format!("saving final workload evidence failed: {save_error}"),
            |error| format!("{error}; saving final workload evidence also failed: {save_error}"),
        );
        return Err(StressError::new(detail));
    }
    result.map_err(|error| StressError::new(error.to_string()))
}

pub(crate) fn run(ctx: &mut StressContext, domain: Domain, tier: u8) -> StressResult {
    let storage_profile =
        StorageProfile::from_env().map_err(|error| StressError::new(error.to_string()))?;
    let duration =
        configured_duration(tier).map_err(|error| StressError::new(error.to_string()))?;
    let progress_timeout =
        configured_progress_timeout().map_err(|error| StressError::new(error.to_string()))?;
    let mut artifacts = Artifacts::new(tier, domain.label(), duration, storage_profile)
        .map_err(|error| StressError::new(error.to_string()))?;
    let storage_directory = match StorageDirectory::new(storage_profile) {
        Ok(directory) => directory,
        Err(error) => return finalize(&mut artifacts, Err(error)),
    };
    artifacts.local_storage_path = storage_directory.path().map(std::path::Path::to_path_buf);
    artifacts
        .save()
        .map_err(|error| StressError::new(error.to_string()))?;
    configure_context(ctx, domain, tier, duration, storage_profile);
    let fixture = match shared_bench_runtime().block_on(bounded(BrokerFixture::start(
        storage_profile,
        storage_directory,
    ))) {
        Ok(fixture) => fixture,
        Err(error) => return finalize(&mut artifacts, Err(error)),
    };
    let maximum = if tier == 5 { 64 } else { 8 };
    let mut drivers = Vec::with_capacity(maximum);
    let result =
        prepare_drivers(domain, fixture.tcp_addr(), maximum, &mut drivers).and_then(|()| {
            run_phases(
                ctx,
                &mut artifacts,
                &mut drivers,
                duration,
                progress_timeout,
                tier,
            )
        });
    let result = close_fixture(drivers, fixture, result, &mut artifacts);
    finalize(&mut artifacts, result)
}
