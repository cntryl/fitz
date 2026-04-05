//! Prometheus metrics endpoint

use crate::boot::{observability, Runtime};
use hyper::{Body, Response, StatusCode};
use std::convert::Infallible;
use std::sync::Arc;

/// Handle /metrics endpoint (Prometheus format)
pub async fn handle_metrics(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    let metrics = generate_prometheus_metrics(runtime);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(Body::from(metrics))
        .unwrap())
}

/// Generate Prometheus-format metrics
fn generate_prometheus_metrics(runtime: Arc<Runtime>) -> String {
    let mut output = String::new();

    // Broker-level metrics
    output.push_str("# HELP fitz_uptime_seconds Broker uptime in seconds\n");
    output.push_str("# TYPE fitz_uptime_seconds gauge\n");
    output.push_str(&format!(
        "fitz_uptime_seconds {}\n",
        runtime.uptime().as_secs()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_connections_total Total number of active connections\n");
    output.push_str("# TYPE fitz_connections_total gauge\n");
    output.push_str(&format!(
        "fitz_connections_total {}\n",
        runtime.connection_count()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_sessions_total Total number of active sessions\n");
    output.push_str("# TYPE fitz_sessions_total gauge\n");
    output.push_str(&format!(
        "fitz_sessions_total {}\n",
        runtime.session_count()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_messages_received_total Total messages received\n");
    output.push_str("# TYPE fitz_messages_received_total counter\n");
    output.push_str(&format!(
        "fitz_messages_received_total {}\n",
        runtime.messages_received()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_messages_sent_total Total messages sent\n");
    output.push_str("# TYPE fitz_messages_sent_total counter\n");
    output.push_str(&format!(
        "fitz_messages_sent_total {}\n",
        runtime.messages_sent()
    ));
    output.push('\n');

    // Add observability metrics (from MetricsCollector)
    add_observability_metrics(&mut output);

    // Domain-specific metrics
    add_domain_metrics(&mut output, &runtime);

    output
}

/// Add metrics from the observability MetricsCollector
fn add_observability_metrics(output: &mut String) {
    // Try to get metrics (will fail gracefully if not yet initialized)
    match std::panic::catch_unwind(|| {
        let metrics = observability::metrics();

        let mut result = String::new();

        // Export counters
        result.push_str("# Observability metrics from MetricsCollector\n");
        for (name, value) in metrics.export_counters() {
            result.push_str(&format!("{} {}\n", name, value));
        }

        result.push('\n');

        // Export gauges
        for (name, value) in metrics.export_gauges() {
            result.push_str(&format!("{} {}\n", name, value));
        }

        result.push('\n');

        // Export histograms (simplified bucket output)
        for (name, buckets) in metrics.export_histograms() {
            let bucket_bounds = ["1ms", "5ms", "10ms", "50ms", "100ms", "500ms", "1s", "5s"];
            let mut cumsum = 0u64;
            for (i, bucket_bound) in bucket_bounds.iter().enumerate() {
                cumsum += buckets[i];
                result.push_str(&format!("{}{{le=\"{}\"}} {}\n", name, bucket_bound, cumsum));
            }
            cumsum += buckets[8]; // +Inf
            result.push_str(&format!("{}{{le=\"+Inf\"}} {}\n", name, cumsum));
            result.push_str(&format!("{}_count {}\n", name, cumsum));
        }

        result
    }) {
        Ok(metrics_output) => output.push_str(&metrics_output),
        Err(_) => {
            // MetricsCollector not yet initialized; skip or log
            tracing::debug!("MetricsCollector not yet initialized in metrics endpoint");
        }
    }
}

fn encode_prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn append_stream_watermark_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str(
        "# HELP fitz_stream_realm_watermark Highest committed realm watermark per Stream route family and realm\n",
    );
    output.push_str("# TYPE fitz_stream_realm_watermark gauge\n");
    for detail in runtime.stream_list_realm_watermark_details() {
        let realm = encode_prometheus_label_value(&detail.realm);
        for watermark in detail.family_watermarks {
            output.push_str(&format!(
                "fitz_stream_realm_watermark{{realm=\"{}\",family=\"{}\"}} {}\n",
                realm, watermark.family, watermark.watermark
            ));
        }
    }
    output.push('\n');

    output.push_str(
        "# HELP fitz_stream_area_watermark Highest committed area watermark per Stream route family, realm, and area\n",
    );
    output.push_str("# TYPE fitz_stream_area_watermark gauge\n");
    for detail in runtime.stream_list_area_watermark_details() {
        let realm = encode_prometheus_label_value(&detail.realm);
        let area = encode_prometheus_label_value(&detail.area);
        for watermark in detail.family_watermarks {
            output.push_str(&format!(
                "fitz_stream_area_watermark{{realm=\"{}\",area=\"{}\",family=\"{}\"}} {}\n",
                realm, area, watermark.family, watermark.watermark
            ));
        }
    }
    output.push('\n');
}

fn add_domain_metrics(output: &mut String, runtime: &Runtime) {
    // KV domain
    output.push_str("# HELP fitz_kv_transactions_active Active KV transactions\n");
    output.push_str("# TYPE fitz_kv_transactions_active gauge\n");
    output.push_str(&format!(
        "fitz_kv_transactions_active {}\n",
        runtime.kv_transactions_active()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_kv_keys_total Total number of keys\n");
    output.push_str("# TYPE fitz_kv_keys_total gauge\n");
    output.push_str(&format!("fitz_kv_keys_total {}\n", runtime.kv_keys_total()));
    output.push('\n');

    // Notice domain
    output.push_str("# HELP fitz_notice_subscriptions_active Active subscriptions\n");
    output.push_str("# TYPE fitz_notice_subscriptions_active gauge\n");
    output.push_str(&format!(
        "fitz_notice_subscriptions_active {}\n",
        runtime.notice_subscriptions_active()
    ));
    output.push('\n');

    // Queue domain
    output.push_str("# HELP fitz_queue_messages_pending Pending queue messages\n");
    output.push_str("# TYPE fitz_queue_messages_pending gauge\n");
    output.push_str(&format!(
        "fitz_queue_messages_pending {}\n",
        runtime.queue_messages_pending()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_queue_leases_active Active queue leases\n");
    output.push_str("# TYPE fitz_queue_leases_active gauge\n");
    output.push_str(&format!(
        "fitz_queue_leases_active {}\n",
        runtime.queue_leases_active()
    ));
    output.push('\n');

    // RPC domain
    output.push_str("# HELP fitz_rpc_workers_registered Registered RPC workers\n");
    output.push_str("# TYPE fitz_rpc_workers_registered gauge\n");
    output.push_str(&format!(
        "fitz_rpc_workers_registered {}\n",
        runtime.rpc_workers_registered()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_rpc_requests_pending Pending RPC requests\n");
    output.push_str("# TYPE fitz_rpc_requests_pending gauge\n");
    output.push_str(&format!(
        "fitz_rpc_requests_pending {}\n",
        runtime.rpc_requests_pending()
    ));
    output.push('\n');

    // Lease domain
    output.push_str("# HELP fitz_lease_active Active leases\n");
    output.push_str("# TYPE fitz_lease_active gauge\n");
    output.push_str(&format!("fitz_lease_active {}\n", runtime.lease_active()));
    output.push('\n');

    // Stream domain
    output.push_str("# HELP fitz_stream_active Active streams\n");
    output.push_str("# TYPE fitz_stream_active gauge\n");
    output.push_str(&format!("fitz_stream_active {}\n", runtime.stream_active()));
    output.push('\n');

    output.push_str("# HELP fitz_stream_events_total Total committed stream events visible through the admin snapshot\n");
    output.push_str("# TYPE fitz_stream_events_total gauge\n");
    output.push_str(&format!(
        "fitz_stream_events_total {}\n",
        runtime.stream_events_total()
    ));
    output.push('\n');

    output.push_str(
        "# HELP fitz_stream_operations_per_second Lifetime-average Stream operations per second\n",
    );
    output.push_str("# TYPE fitz_stream_operations_per_second gauge\n");
    output.push_str(&format!(
        "fitz_stream_operations_per_second {}\n",
        runtime.stream_operations_per_second()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_stream_subscriptions_active Active Stream subscriptions\n");
    output.push_str("# TYPE fitz_stream_subscriptions_active gauge\n");
    output.push_str(&format!(
        "fitz_stream_subscriptions_active {}\n",
        runtime.stream_subscriptions_active()
    ));
    output.push('\n');

    append_stream_watermark_metrics(output, runtime);

    // Schedule domain
    output.push_str("# HELP fitz_schedule_active Active schedules\n");
    output.push_str("# TYPE fitz_schedule_active gauge\n");
    output.push_str(&format!(
        "fitz_schedule_active {}\n",
        runtime.schedule_active()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_executions_per_minute Successful schedule deliveries over the last minute\n");
    output.push_str("# TYPE fitz_schedule_executions_per_minute gauge\n");
    output.push_str(&format!(
        "fitz_schedule_executions_per_minute {}\n",
        runtime.schedule_executions_per_minute()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_subscriptions_active Active schedule subscriptions\n");
    output.push_str("# TYPE fitz_schedule_subscriptions_active gauge\n");
    output.push_str(&format!(
        "fitz_schedule_subscriptions_active {}\n",
        runtime.schedule_subscriptions_active()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_pending_fires Durably claimed schedule fires awaiting successful publish acknowledgement\n");
    output.push_str("# TYPE fitz_schedule_pending_fires gauge\n");
    output.push_str(&format!(
        "fitz_schedule_pending_fires {}\n",
        runtime.schedule_pending_fires()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_publish_failures_total Total schedule fires that failed to route to a subscriber\n");
    output.push_str("# TYPE fitz_schedule_publish_failures_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_publish_failures_total {}\n",
        runtime.schedule_publish_failures()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_ack_failures_total Total pending-fire acknowledgement persistence failures\n");
    output.push_str("# TYPE fitz_schedule_ack_failures_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_ack_failures_total {}\n",
        runtime.schedule_ack_failures()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_overdue_normalizations_total Total schedule definitions normalized forward on last broker start\n");
    output.push_str("# TYPE fitz_schedule_overdue_normalizations_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_overdue_normalizations_total {}\n",
        runtime.schedule_overdue_normalizations()
    ));
    output.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::boot::domains::{
        DomainHandles, KvDomainSink, LeaseDomainSink, NoticeDomainSink, QueueDomainSink,
        RpcDomainSink, ScheduleDomainSink, StreamDomainSink,
    };
    use crate::domains::schedule::store::{ScheduleFireClaim, ScheduleInsert, ScheduleStore};
    use crate::runtime::Router;
    use bytes::Bytes;
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
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
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

    #[test]
    fn should_export_schedule_metrics_given_preloaded_schedule_runtime() {
        // Arrange
        let runtime = runtime_with_preloaded_schedule_metrics();

        // Act
        let metrics = generate_prometheus_metrics(runtime);

        // Assert
        assert!(metrics.contains("fitz_schedule_active 1"));
        assert!(metrics.contains("fitz_schedule_executions_per_minute 1"));
        assert!(metrics.contains("fitz_schedule_subscriptions_active 0"));
        assert!(metrics.contains("fitz_schedule_pending_fires 1"));
        assert!(metrics.contains("fitz_schedule_publish_failures_total 0"));
        assert!(metrics.contains("fitz_schedule_ack_failures_total 0"));
        assert!(metrics.contains("fitz_schedule_overdue_normalizations_total 0"));
    }
}
