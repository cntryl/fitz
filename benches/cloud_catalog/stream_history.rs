#[path = "stream_io.rs"]
mod io;

use cntryl_stress::{
    LogicalUnit, OperationOutcome, ProgressHandle, StressContext, StressError, StressResult,
};
use fitz::benchkit::shared_bench_runtime;
use fitz::testkit::cloud_process::{inspect_container, Broker, Campaign, Snapshot};
use serde_json::{json, Value};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

const TOTAL_BYTES: u64 = 1024 * 1024 * 1024;
const WAL_LIMIT: u64 = 128 * 1024 * 1024;
const CATALOG_LIMIT: u64 = 1024 * 1024;

pub(crate) fn run(ctx: &mut StressContext) -> StressResult {
    super::configure(ctx);
    ctx.metadata(
        "durability_scope",
        "cloud_strict_stream_full_history_fresh_cache_recovery",
    );
    ctx.parameter("logical_unit", "verified_stream_record");
    ctx.parameter("payload_bytes", TOTAL_BYTES);
    ctx.parameter("streams", io::STREAMS);
    ctx.parameter("events_per_stream", io::EVENTS);
    ctx.parameter("event_bytes", io::BODY_BYTES);
    ctx.parameter("post_load_wal_limit_bytes", WAL_LIMIT);
    ctx.parameter("catalog_json_limit_bytes", CATALOG_LIMIT);
    ctx.metadata(
        "fixture_scope",
        "live_stream_sync_commits_automatic_retirement",
    );
    ctx.metadata("elapsed_scope", "live writes, automatic retirement, orderly restart, cold crash recovery and exact readback");
    let campaign = Campaign::new("stream-gib");
    let progress = ctx.progress_handle();
    let accepted = Arc::new(AtomicU64::new(0));
    let mut report = json!({"status": "running", "driver_head": super::git(&["rev-parse", "HEAD"]), "driver_changes": super::git(&["status", "--porcelain=v1"]), "payload_bytes": TOTAL_BYTES, "runtime_image": std::env::var("FITZ_TEST_IMAGE").ok()});
    campaign.report(&report);
    let started = Instant::now();
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        shared_bench_runtime().block_on(async {
            tokio::time::timeout(
                super::WINDOW,
                qualify(&campaign, &mut report, &progress, Arc::clone(&accepted)),
            )
            .await
            .expect("1 GiB Stream qualification exceeded 1200 seconds");
        });
    }));
    let failure = outcome
        .err()
        .map(|error| super::panic_message(error.as_ref()));
    report["status"] = json!(if failure.is_some() {
        "failed"
    } else {
        "passed"
    });
    report["failure"] = json!(failure);
    report["accepted_sync_events"] = json!(accepted.load(Ordering::Acquire));
    report["seconds"] = json!(started.elapsed().as_secs_f64());
    campaign.report(&report);
    let completed = report["verified_after_cold_restart"].as_u64().unwrap_or(0);
    let failures = u64::from(failure.is_some());
    ctx.record_external_outcome(
        "gib_stream_cold_recovery",
        started.elapsed(),
        LogicalUnit::new("verified_stream_record"),
        OperationOutcome::new(completed + failures, completed)
            .failures(failures)
            .validation_errors(failures),
    );
    failure.map_or(Ok(()), |error| {
        Err(StressError::new(format!(
            "{error}; evidence: {}",
            campaign.directory.display()
        )))
    })
}

async fn seed(
    campaign: &Campaign,
    broker: &Broker,
    report: &mut Value,
    progress: &ProgressHandle,
    accepted: Arc<AtomicU64>,
) -> Snapshot {
    io::append_batch(broker.address(), 0, 0).await;
    accepted.fetch_add(16, Ordering::AcqRel);
    progress.advance();
    let original = campaign.snapshot("stream-first-commit");
    assert!(
        !original.segment_ids.is_empty(),
        "need catalog-authorized history to prove later automatic retirement"
    );
    report["original"] = serde_json::to_value(&original).unwrap();
    report["growth_samples"] = json!([]);
    let address = broker.address();
    let endpoint = broker.endpoint();
    let seed = io::seed(address, &endpoint, progress, Arc::clone(&accepted));
    tokio::pin!(seed);
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            () = &mut seed => break,
            _ = tick.tick() => {
                let sample = campaign.snapshot(&format!("stream-growing-{}", accepted.load(Ordering::Acquire)));
                assert!(sample.serialized_catalog_bytes <= CATALOG_LIMIT, "catalog grew beyond its declared envelope: {sample:?}");
                report["growth_samples"].as_array_mut().unwrap().push(serde_json::to_value(sample).unwrap());
                report["accepted_sync_events"] = json!(accepted.load(Ordering::Acquire));
                campaign.report(report);
            }
        }
    }
    assert_eq!(
        accepted.load(Ordering::Acquire) * u64::from(io::BODY_BYTES),
        TOTAL_BYTES
    );
    original
}

async fn retire(campaign: &Campaign, original: &Snapshot, broker: &Broker) -> Snapshot {
    let deadline = Instant::now() + Duration::from_mins(2);
    loop {
        io::healthy(&broker.endpoint()).await;
        let sample = campaign.snapshot("stream-retirement");
        if original
            .segment_ids
            .iter()
            .all(|id| !sample.segment_ids.contains(id))
            && sample.remote_wal_bytes <= WAL_LIMIT
            && sample.serialized_catalog_bytes <= CATALOG_LIMIT
            && sample.sst_objects > 0
        {
            return sample;
        }
        assert!(
            Instant::now() < deadline,
            "ordinary maintenance did not retire historical WAL: {sample:?}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn qualify(
    campaign: &Campaign,
    report: &mut Value,
    progress: &ProgressHandle,
    accepted: Arc<AtomicU64>,
) {
    let mut broker = Broker::start(campaign);
    report["initial_ready_seconds"] = json!(broker.ready().await);
    let original = seed(campaign, &broker, report, progress, accepted).await;
    report["automatic_retirement"] =
        serde_json::to_value(retire(campaign, &original, &broker).await).unwrap();
    report["before_restart"] = inspect_container(&broker.name);
    campaign.report(report);
    broker.checkpoint();
    broker.restart();
    report["orderly_ready_seconds"] = json!(broker.ready().await);
    report["verified_after_orderly_restart"] = json!(io::verify(broker.address(), progress).await);
    campaign.report(report);
    let crash = Instant::now();
    broker.crash();
    report["frozen"] = serde_json::to_value(campaign.snapshot("stream-frozen")).unwrap();
    let old = inspect_stopped(&broker.name);
    broker.remove();
    broker = Broker::start(campaign);
    report["crash_to_ready_seconds"] = json!(broker.ready_since(crash).await);
    let verified = io::verify(broker.address(), progress).await;
    assert_eq!(verified, u64::from(io::STREAMS) * u64::from(io::EVENTS));
    report["after_restart"] = inspect_container(&broker.name);
    assert_eq!(old["Image"], report["after_restart"]["Image"]);
    assert_ne!(
        super::cache_source(&old),
        super::cache_source(&report["after_restart"])
    );
    report["verified_after_cold_restart"] = json!(verified);
    report["verified_payload_bytes"] = json!(verified * u64::from(io::BODY_BYTES));
    campaign.report(report);
    broker.checkpoint();
    broker.remove();
}

fn inspect_stopped(name: &str) -> Value {
    let output = std::process::Command::new("docker")
        .args(["inspect", name])
        .output()
        .unwrap();
    assert!(output.status.success());
    let containers: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    containers.into_iter().next().unwrap()
}
