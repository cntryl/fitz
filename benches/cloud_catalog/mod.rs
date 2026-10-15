mod fixture;
mod protocol;
mod stream_history;
pub(super) use stream_history::run as run_stream_history;

use cntryl_stress::{
    LogicalUnit, ObservationDirection, ObservationUnit, OperationOutcome, ProgressHandle,
    StressContext, StressError, StressResult,
};
use fitz::benchkit::shared_bench_runtime;
use fitz::testkit::cloud_process::{inspect_container, Broker, Campaign};
use serde_json::{json, Value};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::{Duration, Instant};

const STAGES: [u32; 3] = [1_024, 8_192, 32_768];
const WINDOW: Duration = Duration::from_mins(20);

fn stages() -> Result<Vec<u32>, StressError> {
    match std::env::var("FITZ_CATALOG_SEGMENTS") {
        Ok(raw) => {
            let segments = raw.parse::<u32>().map_err(|_| {
                StressError::new("FITZ_CATALOG_SEGMENTS must be 1024, 8192 or 32768")
            })?;
            if !STAGES.contains(&segments) {
                return Err(StressError::new(
                    "FITZ_CATALOG_SEGMENTS must be 1024, 8192 or 32768",
                ));
            }
            Ok(vec![segments])
        }
        Err(std::env::VarError::NotPresent) => Ok(STAGES.to_vec()),
        Err(error) => Err(StressError::new(error.to_string())),
    }
}

fn configure(ctx: &mut StressContext) {
    ctx.parameter("storage_profile", "s3_compatible");
    ctx.parameter("cpu_millicores", 250);
    ctx.parameter("memory_bytes", 536_870_912_u64);
    ctx.parameter("swap_bytes", 0);
    ctx.parameter("readiness_deadline_seconds", 180);
    ctx.parameter("campaign_deadline_seconds", WINDOW.as_secs());
    ctx.parameter("incident_catalog_bytes", fixture::INCIDENT_CATALOG_BYTES);
    ctx.metadata("target_class", "stress_characterization");
    ctx.metadata("durability_scope", "cloud_strict_kv_fresh_cache_recovery");
    ctx.metadata(
        "fixture_scope",
        "offline_supported_format_many_small_segments",
    );
    ctx.metadata(
        "elapsed_scope",
        "startup, exact reads, strict commit, crash, cold recovery; excludes fixture upload",
    );
}

pub(super) fn run(ctx: &mut StressContext) -> StressResult {
    let stages = stages()?;
    configure(ctx);
    ctx.parameter("logical_keys", fixture::KEYS);
    ctx.parameter("stage_selection", format!("{stages:?}"));
    let started = Instant::now();
    for segments in stages {
        let campaign = Campaign::new(&format!("catalog-{segments}"));
        let mut report = json!({
            "status": "preparing", "segments": segments,
            "incident_catalog_bytes": fixture::INCIDENT_CATALOG_BYTES,
            "driver_head": git(&["rev-parse", "HEAD"]),
            "driver_changes": git(&["status", "--porcelain=v1"]),
            "runtime_image": std::env::var("FITZ_TEST_IMAGE").ok(),
        });
        campaign.report(&report);
        let progress = ctx.progress_handle();
        progress.advance();
        let mut measured = None;
        let result = catch_unwind(AssertUnwindSafe(|| {
            let prepared = fixture::publish(&campaign, segments);
            if segments == *STAGES.last().unwrap() {
                assert!(
                    prepared.serialized_catalog_bytes >= fixture::INCIDENT_CATALOG_BYTES,
                    "largest stage must reach the incident's encoded catalog size"
                );
            }
            report["fixture"] = serde_json::to_value(&prepared).unwrap();
            report["before_boot"] = serde_json::to_value(campaign.snapshot("before-boot")).unwrap();
            report["status"] = json!("running");
            campaign.report(&report);
            progress.advance();
            let remaining = WINDOW
                .checked_sub(started.elapsed())
                .expect("fixture exceeded campaign deadline");
            measured = Some(Instant::now());
            let outcome = shared_bench_runtime().block_on(async {
                tokio::time::timeout(
                    remaining,
                    recover(&campaign, segments, &mut report, &progress),
                )
                .await
            });
            outcome.expect("catalog pressure campaign exceeded 1200 seconds");
        }));
        let failure = result.err().map(|failure| panic_message(failure.as_ref()));
        let elapsed = measured.map_or(Duration::ZERO, |started| started.elapsed());
        report["status"] = json!(if failure.is_some() {
            "failed"
        } else {
            "passed"
        });
        report["failure"] = json!(failure);
        report["campaign_elapsed_seconds"] = json!(started.elapsed().as_secs_f64());
        campaign.report(&report);
        let failed = u64::from(failure.is_some());
        ctx.record_external_outcome(
            format!("segments_{segments}"),
            elapsed,
            LogicalUnit::new("catalog_recovery_cycle"),
            OperationOutcome::new(1, 1 - failed)
                .failures(failed)
                .validation_errors(failed),
        );
        ctx.parameter("catalog_segments", segments);
        if let Some(bytes) = report["fixture"]["serialized_catalog_bytes"].as_u64() {
            #[allow(clippy::cast_precision_loss)]
            ctx.record_observation(
                "serialized_catalog_bytes",
                bytes as f64,
                ObservationUnit::Bytes,
                ObservationDirection::Informational,
            );
        }
        if let Some(failure) = failure {
            return Err(StressError::new(format!(
                "catalog stage {segments}: {failure}; evidence: {}",
                campaign.directory.display()
            )));
        }
        ctx.progress_handle().advance();
    }
    Ok(())
}

async fn recover(
    campaign: &Campaign,
    segments: u32,
    report: &mut Value,
    progress: &ProgressHandle,
) {
    let mut broker = Broker::start(campaign);
    report["first_ready_seconds"] = json!(broker.ready().await);
    progress.advance();
    report["first_container"] = inspect_container(&broker.name);
    report["verified_before_crash"] =
        json!(protocol::verify(broker.address(), segments, progress).await);
    protocol::write_marker(broker.address()).await;
    progress.advance();
    campaign.report(report);
    let crash = Instant::now();
    broker.crash();
    report["frozen"] = serde_json::to_value(campaign.snapshot("frozen")).unwrap();
    // Remove the first anonymous cache volume. The successor must recover from
    // provider authority, never from the driver's fixture files or old cache.
    broker.remove();
    broker = Broker::start(campaign);
    report["crash_to_ready_seconds"] = json!(broker.ready_since(crash).await);
    progress.advance();
    report["verified_after_cache_loss"] =
        json!(protocol::verify(broker.address(), segments, progress).await);
    protocol::verify_marker(broker.address()).await;
    report["strict_commit_recovered"] = json!(true);
    report["final_container"] = inspect_container(&broker.name);
    assert_eq!(
        report["first_container"]["Image"],
        report["final_container"]["Image"]
    );
    assert_ne!(
        report["first_container"]["Id"],
        report["final_container"]["Id"]
    );
    assert_ne!(
        cache_source(&report["first_container"]),
        cache_source(&report["final_container"]),
        "successor must use a fresh anonymous cache volume"
    );
    report["after_recovery"] = serde_json::to_value(campaign.snapshot("after-recovery")).unwrap();
    campaign.report(report);
    broker.checkpoint();
    broker.remove();
}

fn cache_source(container: &Value) -> &Value {
    &container["Mounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|mount| mount["Destination"] == "/data")
        .expect("broker cache volume")["Source"]
}

fn panic_message(failure: &(dyn std::any::Any + Send)) -> String {
    failure
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            failure
                .downcast_ref::<&str>()
                .map(|value| (*value).to_owned())
        })
        .unwrap_or_else(|| "campaign panicked".into())
}

fn git(args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .args(args)
        .output()
        .expect("driver source identity");
    assert!(
        output.status.success(),
        "driver source identity command failed"
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
