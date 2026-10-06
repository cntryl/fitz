mod campaign;
mod io;
mod ledger;
mod report;

use super::{
    fixture::{StorageDirectory, StorageProfile},
    types::BenchFailure,
};
use cntryl_stress::{LogicalUnit, OperationOutcome, StressContext, StressError, StressResult};
use fitz::{benchkit::shared_bench_runtime, testkit::TestServer};
use report::Report;
use std::time::Duration;

pub(crate) fn run(ctx: &mut StressContext) -> StressResult {
    execute(ctx).map_err(|error| StressError::new(error.to_string()))
}

fn execute(ctx: &mut StressContext) -> Result<(), BenchFailure> {
    let directory = StorageDirectory::new(StorageProfile::LocalDisk)?;
    let path = directory.path().expect("local storage").to_path_buf();
    let mut report = Report::new(path)?;
    ctx.parameter("workload", "schedule_forced_due_wave");
    ctx.parameter("definitions", format!("{:?}", report.counts));
    ctx.parameter("receivers", 2);
    ctx.parameter("storage_profile", "local_disk_sync");
    ctx.parameter("fire_deadline_seconds", report.fire_deadline_seconds);
    ctx.metadata(
        "completion_scope",
        "observed_live_receipt_per_mode_no_claim_ack_proof",
    );
    ctx.metadata(
        "clock_scope",
        "one_test_hook_due_reset_then_normal_scans_not_natural_due_latency",
    );
    ctx.metadata(
        "durability_scope",
        "local_sync_intent_clean_restart_no_crash_or_downstream_ack_proof",
    );
    report.save()?;
    let mut server = None;
    let mut result: Result<(), BenchFailure> = shared_bench_runtime().block_on(async {
        server = Some(
            io::bounded(async {
                TestServer::start_with_local_storage(
                    report.local_storage_path.to_string_lossy().into_owned(),
                )
                .await
                .map_err(BenchFailure::transport)
            })
            .await?,
        );
        let mut generation = 0;
        for count in report.counts.clone() {
            for mode in [0, 1] {
                generation += 1;
                campaign::stage(ctx, &mut server, &mut report, count, mode, generation).await?;
            }
        }
        campaign::stage(ctx, &mut server, &mut report, 1, 0, generation + 1).await?;
        report.recovery_probe_passed = true;
        Ok(())
    });
    if let Err(error) = &result {
        report.failure = Some(error.to_string());
    }
    if let Err(error) = report.save() {
        if result.is_ok() {
            result = Err(error);
        }
    }
    let cleanup = match server {
        Some(server) => shared_bench_runtime().block_on(io::bounded(async {
            server.shutdown().await.map_err(BenchFailure::transport)
        })),
        None => Ok(()),
    };
    if let Err(error) = cleanup {
        report.cleanup_failure = Some(error.to_string());
        if result.is_ok() {
            result = Err(error);
        }
    }
    if result.is_ok() {
        if let Err(error) = directory.release() {
            report.cleanup_failure = Some(error.to_string());
            result = Err(error);
        }
    } else {
        drop(directory);
    }
    if let Err(error) = &result {
        if report.failure.is_none() {
            report.failure = Some(error.to_string());
        }
    }
    report.status = if result.is_ok() { "passed" } else { "failed" };
    report.save()?;
    for stage in &report.stages {
        let failures = u64::from(stage.failure.is_some());
        ctx.record_external_outcome(
            format!("due_wave_{}_mode_{}", stage.definitions, stage.mode),
            Duration::from_nanos(u64::try_from(stage.fire_elapsed_ns).unwrap_or(u64::MAX)),
            LogicalUnit::new("observed_occurrence_receipts"),
            OperationOutcome::new(
                stage.completed_occurrences + failures,
                stage.completed_occurrences,
            )
            .failures(failures),
        );
    }
    result
}
