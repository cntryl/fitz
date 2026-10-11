mod attribution;
mod campaign;
mod config;
mod envelope;
mod io;
mod latency;
mod ledger;
mod pacing;
mod report;
mod timing;

use super::fixture::{BrokerFixture, StorageDirectory, StorageProfile};
use super::types::BenchFailure;
use cntryl_stress::{
    LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome, StressContext,
    StressError, StressResult,
};
use fitz::benchkit::shared_bench_runtime;
use report::Report;
use std::time::Duration;

pub(crate) use envelope::{Backlog, Envelope};
pub(crate) use latency::run as run_latency;

pub(crate) fn run(ctx: &mut StressContext) -> StressResult {
    config::Config::from_env()
        .and_then(|config| execute(ctx, config, None))
        .map_err(|error| StressError::new(error.to_string()))
}

pub(crate) fn run_envelope(ctx: &mut StressContext, envelope: Envelope) -> StressResult {
    execute(ctx, config::Config::from_envelope(envelope), Some(envelope))
        .map_err(|error| StressError::new(error.to_string()))
}

fn configure(ctx: &mut StressContext, report: &Report, envelope: Option<Envelope>) {
    ctx.parameter("workload", "queue_independent_offered_rate");
    ctx.parameter("producer_connections", report.config.producer_connections);
    ctx.parameter("consumer_connections", 1);
    ctx.parameter("consumer_delay_ms", report.config.consumer_delay_ms);
    ctx.parameter("stage_seconds", report.config.stage_seconds);
    ctx.parameter("max_backlog", report.config.max_backlog);
    ctx.parameter(
        "max_attempts_per_stage",
        report.config.max_attempts_per_stage,
    );
    ctx.parameter("drain_seconds", report.config.drain_seconds);
    ctx.parameter("storage_profile", "local_disk");
    ctx.metadata("durability_scope", report.durability_scope);
    ctx.metadata("target_class", "stress_characterization");
    ctx.metadata("pacing_implementation", pacing::IMPLEMENTATION);
    ctx.metadata("comparison_label", "corrected_pacing");
    if let Some(envelope) = envelope {
        ctx.parameter("drain_envelope", envelope.scope());
        ctx.metadata("acceptance_scope", envelope.scope());
    }
}

fn execute(
    ctx: &mut StressContext,
    config: config::Config,
    envelope: Option<Envelope>,
) -> Result<(), BenchFailure> {
    let mut report = Report::new(config)?;
    configure(ctx, &report, envelope);
    let storage = StorageDirectory::new(StorageProfile::LocalDisk)?;
    report.local_storage_path = storage.path().map(std::path::Path::to_path_buf);
    report.save()?;
    let fixture = match shared_bench_runtime().block_on(io::bounded(BrokerFixture::start(
        StorageProfile::LocalDisk,
        storage,
    ))) {
        Ok(fixture) => fixture,
        Err(error) => {
            report.status = "failed";
            report.failure = Some(error.to_string());
            report.save()?;
            return Err(error);
        }
    };
    let mut result: Result<(), BenchFailure> = shared_bench_runtime().block_on(async {
        campaign::recovery(fixture.tcp_addr()).await?;
        for rate in report.config.rates.clone() {
            campaign::run_stage(ctx, &mut report, &fixture, rate).await?;
            let stage = report
                .stages
                .last()
                .ok_or_else(|| BenchFailure::validation("missing pressure stage"))?;
            if let Some(envelope) = envelope {
                envelope
                    .verify(&observed(stage))
                    .map_err(BenchFailure::verification)?;
            }
            if stage.termination != "configured_window" {
                break;
            }
        }
        campaign::recovery(fixture.tcp_addr()).await?;
        report.recovery_probe_passed = true;
        Ok(())
    });
    if let Err(error) = &result {
        report.failure = Some(error.to_string());
        report.status = "failed";
    }
    report.cleanup_status = "running";
    if let Err(error) = report.save() {
        if result.is_ok() {
            result = Err(error);
        }
    }
    let (server, directory) = fixture.into_parts();
    let shutdown = shared_bench_runtime().block_on(io::bounded(async {
        server.shutdown().await.map_err(BenchFailure::transport)
    }));
    let cleanup = shutdown.and_then(|()| {
        if result.is_ok() {
            directory.release()
        } else {
            drop(directory);
            Ok(())
        }
    });
    match cleanup {
        Ok(()) => report.cleanup_status = "completed",
        Err(error) => {
            report.cleanup_status = "failed";
            report.cleanup_failure = Some(error.to_string());
            if result.is_ok() {
                report.failure = Some(error.to_string());
                result = Err(error);
            }
        }
    }
    report.status = if result.is_ok() { "passed" } else { "failed" };
    report.save()?;
    record_framework_outcomes(ctx, &report, envelope);
    result
}

fn record_framework_outcomes(ctx: &mut StressContext, report: &Report, envelope: Option<Envelope>) {
    for stage in &report.stages {
        let completed = stage.accounting.acknowledged;
        let failures = u64::from(
            !stage.drained
                || !stage.empty_verified
                || envelope.is_some_and(|envelope| envelope.verify(&observed(stage)).is_err()),
        );
        ctx.record_external_outcome(
            format!("offered_{}_per_second", stage.rate_per_second),
            Duration::from_nanos(
                u64::try_from(
                    stage.active_elapsed_ns
                        + stage.producer_settle_elapsed_ns
                        + stage.drain_elapsed_ns,
                )
                .unwrap_or(u64::MAX),
            ),
            LogicalUnit::new("accepted_message_through_ack"),
            OperationOutcome::new(completed + failures, completed).failures(failures),
        );
        ctx.metadata(
            "framework_attempt_scope",
            "validated_acknowledgements_plus_failures",
        );
        if envelope.is_some() {
            record_envelope_observations(ctx, stage);
        }
    }
}

fn observed(stage: &report::Stage) -> envelope::ObservedStage {
    let timing = &stage.consumer_timing;
    envelope::ObservedStage {
        termination: stage.termination.as_str().into(),
        configured_window_completed: stage.configured_window_completed,
        active_elapsed_ns: stage.active_elapsed_ns,
        offered: stage.offered,
        harness_missed: stage.harness_missed,
        sent: stage.accounting.sent,
        accepted: stage.accounting.accepted,
        rejected: stage.accounting.rejected,
        acknowledged: stage.accounting.acknowledged,
        accepted_unacknowledged: stage.accounting.accepted_unacknowledged,
        acknowledged_timing_samples: [
            timing.ack.distribution.count,
            timing.pause.distribution.count,
            timing.worker_pause.distribution.count,
            timing.handoff.distribution.count,
            timing.cycle.distribution.count,
        ],
        minimum_pause_ns: timing.pause.min_ns,
        minimum_worker_pause_ns: timing.worker_pause.min_ns,
        drain: envelope::DrainEvidence {
            drained: stage.drained,
            empty_verified: stage.empty_verified,
            clients_closed: stage.cleanup_failure.is_none(),
        },
    }
}

fn record_envelope_observations(ctx: &mut StressContext, stage: &report::Stage) {
    for (name, value, unit, direction) in [
        (
            "accepted_messages",
            u128::from(stage.accounting.accepted),
            ObservationUnit::Count,
            ObservationDirection::HigherIsBetter,
        ),
        (
            "active_elapsed_ns",
            stage.active_elapsed_ns,
            ObservationUnit::Nanoseconds,
            ObservationDirection::Informational,
        ),
        (
            "drain_elapsed_ns",
            stage.drain_elapsed_ns,
            ObservationUnit::Nanoseconds,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "ack_p99_ns",
            u128::from(
                stage
                    .consumer_timing
                    .ack
                    .distribution
                    .percentile_upper_ns(99),
            ),
            ObservationUnit::Nanoseconds,
            ObservationDirection::LowerIsBetter,
        ),
        (
            "cycle_p99_ns",
            u128::from(
                stage
                    .consumer_timing
                    .cycle
                    .distribution
                    .percentile_upper_ns(99),
            ),
            ObservationUnit::Nanoseconds,
            ObservationDirection::LowerIsBetter,
        ),
    ] {
        #[allow(clippy::cast_precision_loss)]
        ctx.record_observation(name, value as f64, unit, direction);
    }
}
