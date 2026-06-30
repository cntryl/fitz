use super::*;
use crate::boot::domains::{
    DomainHandles, KvDomainSink, LeaseDomainSink, NoticeDomainSink, QueueDomainSink, RpcDomainSink,
    ScheduleDomainSink, StreamDomainSink,
};
use crate::domains::schedule::store::{ScheduleFireClaim, ScheduleInsert, ScheduleStore};
use crate::runtime::Router;
use bytes::Bytes;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_millis() as u64
}

fn runtime_with_preloaded_schedule_metrics() -> Arc<Runtime> {
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let runtime = Arc::new(Runtime::with_admin_read_model(
        router.clone(),
        admin_read_model.clone(),
    ));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let schedule_store = ScheduleStore::new(store.clone());
    let route = "schedule://acme/jobs/nightly/run";
    let payload = Bytes::from_static(b"nightly");
    let now_ms = current_epoch_ms();
    let claimed_fire_ms = now_ms.saturating_sub(30_000);
    let next_fire_ms = now_ms.saturating_add(30_000);
    let last_fire_ms = Some(now_ms.saturating_sub(1_000));

    schedule_store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "* * * * *",
                payload: &payload,
                next_fire_ms: claimed_fire_ms,
                previous_fire_ms: None,
                last_fire_ms,
                executions_total: 4,
            },
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("insert schedule");
    schedule_store
        .claim_due_batch(
            1,
            &[ScheduleFireClaim {
                route,
                cron: "* * * * *",
                payload: &payload,
                claimed_at_ms: claimed_fire_ms,
                next_fire_ms,
                previous_fire_ms: claimed_fire_ms,
                last_fire_ms,
                executions_total: 4,
            }],
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("claim due schedule");

    let domains = Arc::new(DomainHandles {
        kv: Arc::new(KvDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
        )),
        queue: Arc::new(QueueDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
            crate::utils::idempotency::default_dedup_store(),
        )),
        notice: Arc::new(NoticeDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        )),
        stream: Arc::new(StreamDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
        )),
        rpc: Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone())),
        lease: Arc::new(LeaseDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        )),
        schedule: Arc::new(ScheduleDomainSink::new(
            store,
            router,
            admin_read_model.clone(),
        )),
    });

    domains
        .schedule
        .preload_persisted_families()
        .expect("preload schedules");
    runtime.attach_domains(domains);
    runtime
}

fn assert_metric_exported(metrics: &str, metric_name: &str) {
    let prefix = format!("{metric_name} ");
    let line = metrics
        .lines()
        .find(|line| line.starts_with(&prefix))
        .unwrap_or_else(|| panic!("missing metric {metric_name}"));
    let value = line
        .split_whitespace()
        .nth(1)
        .unwrap_or_else(|| panic!("missing metric value for {metric_name}"));
    value
        .parse::<u64>()
        .unwrap_or_else(|err| panic!("invalid metric value for {metric_name}: {err}"));
}

fn metric_family_name(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() || trimmed.starts_with('#') {
        return None;
    }

    let name = trimmed
        .split(|character: char| character == '{' || character.is_whitespace())
        .next()?;

    Some(
        ["_bucket", "_count", "_sum"]
            .iter()
            .find_map(|suffix| name.strip_suffix(suffix))
            .unwrap_or(name)
            .to_string(),
    )
}

fn typed_metric_families(metrics: &str) -> BTreeSet<String> {
    metrics
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_string)
        .collect()
}

#[test]
fn should_export_schedule_metrics_given_preloaded_schedule_runtime() {
    // Arrange
    let runtime = runtime_with_preloaded_schedule_metrics();
    let metrics = crate::observability::metrics();
    let latency_before = metrics
        .histogram_get_buckets("fitz_schedule_latency_ms")
        .unwrap_or([0; 9]);
    metrics.histogram_observe_ms("fitz_schedule_latency_ms", 1);
    metrics.histogram_observe_ms("fitz_schedule_latency_ms", 250);

    // Act
    let metrics = generate_prometheus_metrics(&runtime);

    // Assert
    assert!(metrics.contains("fitz_schedule_active 1"));
    assert!(metrics.contains("fitz_schedule_executions_per_minute 1"));
    assert!(metrics.contains("fitz_schedule_subscriptions_active 0"));
    assert!(metrics.contains("fitz_schedule_pending_fire_claims 1"));
    assert!(metrics.contains("fitz_schedule_pending_ack_retries 0"));
    let oldest_pending_claim_age_line = metrics
        .lines()
        .find(|line| line.starts_with("fitz_schedule_oldest_pending_claim_age_seconds "))
        .expect("oldest pending claim age gauge");
    let oldest_pending_claim_age_seconds: u64 = oldest_pending_claim_age_line
        .split_whitespace()
        .nth(1)
        .expect("pending claim age value")
        .parse()
        .expect("valid pending claim age value");
    assert!(oldest_pending_claim_age_seconds >= 30);
    assert!(metrics.contains("fitz_schedule_notify_failures_total 0"));
    assert!(metrics.contains("fitz_schedule_ack_failures_total 0"));
    assert!(metrics.contains("fitz_schedule_overdue_normalizations_total 0"));
    assert!(metrics.contains("fitz_schedule_pending_claims_expired_total 0"));
    assert!(metrics.contains("fitz_schedule_pending_claim_cleanup_failure_total 0"));
    assert!(metrics.contains("fitz_schedule_latency_ms{le=\"100ms\"}"));
    assert!(metrics.contains(&format!(
        "fitz_schedule_latency_ms{{le=\"100ms\"}} {}",
        latency_before[0] + 1
    )));
    assert!(metrics.contains("fitz_schedule_latency_ms_count"));
    assert_metric_exported(&metrics, "fitz_notice_delivery_drops_total");
    assert_metric_exported(&metrics, "fitz_stream_notify_drops_total");
    assert_metric_exported(&metrics, "fitz_queue_notify_drops_total");
    assert_metric_exported(&metrics, "fitz_rpc_request_timeouts_total");
    assert_metric_exported(&metrics, "fitz_rpc_backpressure_rejects_total");
    assert_metric_exported(&metrics, "fitz_rpc_duplicate_correlation_rejects_total");
    assert_metric_exported(&metrics, "fitz_rpc_wrong_worker_rejects_total");
    assert_metric_exported(&metrics, "fitz_lease_waiter_depth");
    assert_metric_exported(&metrics, "fitz_notice_wildcard_limit_rejects_total");
}

#[test]
fn should_export_type_metadata_for_every_metric_family() {
    // Arrange
    let metrics = crate::observability::metrics();
    metrics.counter_inc("fitz_test_typed_counter_total");
    metrics.gauge_set("fitz_test_typed_gauge", 7);
    metrics.histogram_observe_ms("fitz_test_typed_latency_ms", 1);
    let runtime = Arc::new(Runtime::with_admin_read_model(
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    ));

    // Act
    let payload = generate_prometheus_metrics(&runtime);

    // Assert
    let typed = typed_metric_families(&payload);
    let untyped = payload
        .lines()
        .filter_map(metric_family_name)
        .filter(|family| !typed.contains(family))
        .collect::<BTreeSet<_>>();

    assert!(untyped.is_empty(), "untyped metric families: {untyped:?}");
}
