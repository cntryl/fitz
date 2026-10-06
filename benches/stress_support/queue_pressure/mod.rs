mod campaign;
mod config;
mod io;
mod ledger;
mod report;

use super::fixture::{BrokerFixture, StorageDirectory, StorageProfile};
use super::types::BenchFailure;
use cntryl_stress::{LogicalUnit, OperationOutcome, StressContext, StressError, StressResult};
use fitz::benchkit::shared_bench_runtime;
use report::Report;
use std::time::Duration;

pub(crate) fn run(ctx: &mut StressContext) -> StressResult {
    execute(ctx).map_err(|error| StressError::new(error.to_string()))
}

fn configure(ctx: &mut StressContext, report: &Report) {
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
}

fn execute(ctx: &mut StressContext) -> Result<(), BenchFailure> {
    let config = config::Config::from_env()?;
    let mut report = Report::new(config)?;
    configure(ctx, &report);
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
            campaign::run_stage(ctx, &mut report, fixture.tcp_addr(), rate).await?;
            let stage = report
                .stages
                .last()
                .ok_or_else(|| BenchFailure::validation("missing pressure stage"))?;
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
    for stage in &report.stages {
        let completed = stage.accounting.acknowledged;
        let failures = u64::from(!stage.drained || !stage.empty_verified);
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
    }
    result
}
