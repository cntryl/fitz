//! Native S3 process qualification; provider and driver are outside the broker cap.
#[cfg(feature = "recovery-qualification")]
#[path = "s3/fixture.rs"]
mod fixture;
#[path = "s3/support.rs"]
mod support;
#[path = "s3/workload.rs"]
mod workload;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use support::{Broker, Campaign, Snapshot};

const MIB: u64 = 1024 * 1024;
const RECORDS: u32 = 4096;
const WAL_LIMIT: u64 = 128 * MIB;
const CAMPAIGN_LIMIT: Duration = Duration::from_mins(20);

async fn recovery(cold: bool) -> serde_json::Value {
    let campaign = Campaign::new(if cold { "cold" } else { "warm" });
    let mut broker = Broker::start(&campaign);
    broker.ready().await;
    let accepted = workload::enqueue(broker.address(), RECORDS, 0).await;
    // Allow ordinary background persistence to catch up before preparing the
    // persisted fixture. Recovery below verifies completion; this quiet period
    // is not an acknowledgement durability barrier or a production guarantee.
    tokio::time::sleep(Duration::from_secs(1)).await;
    broker.checkpoint();
    broker.restart();
    broker.ready().await;
    let before = super::inspect_container(&broker.name);
    let crash_started = Instant::now();
    broker.crash();
    // Queue recovery requires persisted state, not merely an enqueue response.
    // The explicit orderly checkpoint prepares that fixture before SIGKILL.
    // The separate 640 MiB KV fixture qualifies WAL recovery.
    let backlog = campaign.snapshot("acknowledged-backlog");
    assert!(
        backlog.sst_objects > 0,
        "need persisted SST state after the explicit checkpoint: {backlog:?}"
    );
    let restart_started = Instant::now();
    if cold {
        broker.remove();
        broker = Broker::start(&campaign);
    } else {
        broker.restart();
    }
    let ready_seconds = broker.ready_since(crash_started).await;
    let crash_to_ready_seconds = crash_started.elapsed().as_secs_f64();
    let restart_to_ready_seconds = restart_started.elapsed().as_secs_f64();
    workload::drain(broker.address(), accepted).await;
    super::smoke(broker.address(), &format!("recovered-{}", campaign.id)).await;
    let after = super::inspect_container(&broker.name);
    assert_eq!(
        before["Image"], after["Image"],
        "same immutable runtime image"
    );
    assert_ne!(before["State"]["StartedAt"], after["State"]["StartedAt"]);
    let cache_source = |container: &serde_json::Value| {
        container["Mounts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|mount| mount["Destination"] == "/data")
            .unwrap()["Source"]
            .clone()
    };
    if cold {
        assert_ne!(before["Id"], after["Id"]);
        assert_ne!(
            cache_source(&before),
            cache_source(&after),
            "fresh cache volume"
        );
    } else {
        assert_eq!(before["Id"], after["Id"]);
        assert_eq!(
            cache_source(&before),
            cache_source(&after),
            "original cache volume"
        );
    }
    let recovered = campaign.snapshot("recovered");
    let report = serde_json::json!({
        "cold_cache": cold, "accepted_and_verified_acked": RECORDS,
        "queue_persistence": "best_effort", "fixture_checkpoint": "orderly_shutdown_before_crash",
        "remaining": 0, "accepted_payload_bytes": u64::from(RECORDS) * 16 * 1024,
        "ready_seconds": ready_seconds, "crash_to_ready_seconds": crash_to_ready_seconds,
        "restart_to_ready_seconds": restart_to_ready_seconds,
        "restart_and_verify_seconds": restart_started.elapsed().as_secs_f64(),
        "before": before, "after": after, "backlog": backlog, "recovered": recovered,
    });
    campaign.report(&report);
    broker.remove();
    report
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, AWS CLI and pinned Sqrzl; CI runs explicitly"]
async fn should_recover_checkpointed_s3_backlog_after_abrupt_restart() {
    // Arrange
    let cold = false;

    // Act
    let report = tokio::time::timeout(CAMPAIGN_LIMIT, recovery(cold))
        .await
        .unwrap();

    // Assert
    assert_eq!(report["accepted_and_verified_acked"], RECORDS);
    assert_eq!(report["remaining"], 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, AWS CLI and pinned Sqrzl; CI runs explicitly"]
async fn should_recover_checkpointed_s3_backlog_after_complete_cache_loss() {
    // Arrange
    let cold = true;

    // Act
    let report = tokio::time::timeout(CAMPAIGN_LIMIT, recovery(cold))
        .await
        .unwrap();

    // Assert
    assert_eq!(report["accepted_and_verified_acked"], RECORDS);
    assert_eq!(report["remaining"], 0);
}

async fn retention() -> serde_json::Value {
    let campaign = Campaign::new("retention");
    let historical_wal_bytes = fixture::publish(&campaign, RECORDS);
    let historical_backlog = campaign.snapshot("historical-before-boot");
    assert!(historical_wal_bytes > 64 * MIB);
    assert!(historical_backlog.catalog_bytes >= historical_wal_bytes);
    let original = historical_backlog.segment_ids.clone();
    assert!(
        !original.is_empty(),
        "qualification must exercise historical published WAL"
    );
    let mut broker = Broker::start(&campaign);
    broker.ready().await;
    let before = super::inspect_container(&broker.name);
    let mut samples: Vec<Snapshot> = Vec::new();
    let finished = Arc::new(AtomicBool::new(false));
    let monitor = tokio::spawn(super::monitor_health(broker.endpoint(), finished.clone()));
    for round in 0..4 {
        let accepted = workload::enqueue(broker.address(), RECORDS, round * RECORDS).await;
        let snapshot = campaign.snapshot(&format!("round-{round}-enqueued"));
        samples.push(snapshot);
        workload::drain(broker.address(), accepted).await;
        samples.push(campaign.snapshot(&format!("round-{round}-acked")));
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    let final_snapshot = loop {
        let sample = campaign.snapshot("maintenance");
        if original.iter().all(|id| !sample.segment_ids.contains(id))
            && sample.catalog_bytes <= WAL_LIMIT
            && sample.remote_wal_bytes <= WAL_LIMIT
            && sample.sst_objects > 0
        {
            break sample;
        }
        assert!(
            Instant::now() < deadline,
            "historical WAL did not retire within the declared envelope: {sample:?}"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    fixture::verify(&broker, RECORDS).await;
    super::smoke(broker.address(), &format!("retained-{}", campaign.id)).await;
    finished.store(true, Ordering::Release);
    let (health_samples, maximum_health_millis) = monitor.await.unwrap();
    assert!(health_samples > 0);
    let after = super::inspect_container(&broker.name);
    let report = serde_json::json!({
        "accepted_and_verified_acked": u64::from(RECORDS) * 4,
        "payload_bytes_written": u64::from(RECORDS) * 4 * 16 * 1024,
        "remaining": 0, "wal_limit_bytes": WAL_LIMIT,
        "historical_wal_bytes": historical_wal_bytes,
        "historical_kv_values_verified": RECORDS, "historical_backlog": historical_backlog,
        "original_segments": original, "samples": samples, "final": final_snapshot,
        "health_samples": health_samples, "maximum_health_millis": maximum_health_millis,
        "before": before, "after": after,
    });
    campaign.report(&report);
    broker.remove();
    report
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, AWS CLI and pinned Sqrzl; CI runs explicitly"]
async fn should_retire_historical_s3_wal_during_best_effort_queue_churn() {
    // Arrange
    let deadline = CAMPAIGN_LIMIT;

    // Act
    let report = tokio::time::timeout(deadline, retention()).await.unwrap();

    // Assert
    assert_eq!(report["accepted_and_verified_acked"], 16_384);
    assert_eq!(report["remaining"], 0);
}
