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

    output.push_str("# HELP fitz_notice_routes_active Active notice routes\n");
    output.push_str("# TYPE fitz_notice_routes_active gauge\n");
    output.push_str(&format!(
        "fitz_notice_routes_active {}\n",
        runtime.notice_routes_active()
    ));
    output.push('\n');

    output.push_str(
        "# HELP fitz_notice_max_route_subscribers Peak subscribers on a single notice route\n",
    );
    output.push_str("# TYPE fitz_notice_max_route_subscribers gauge\n");
    output.push_str(&format!(
        "fitz_notice_max_route_subscribers {}\n",
        runtime.notice_max_route_subscribers()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_notice_unsubscribes_total Total notice unsubscriptions processed by this broker process\n");
    output.push_str("# TYPE fitz_notice_unsubscribes_total counter\n");
    output.push_str(&format!(
        "fitz_notice_unsubscribes_total {}\n",
        runtime.notice_unsubscribes_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_notice_delivery_drops_total Total notice deliveries dropped by this broker process\n");
    output.push_str("# TYPE fitz_notice_delivery_drops_total counter\n");
    output.push_str(&format!(
        "fitz_notice_delivery_drops_total {}\n",
        runtime.notice_delivery_drops_total()
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

    output.push_str("# HELP fitz_queue_inflight_active Active queue inflight entries\n");
    output.push_str("# TYPE fitz_queue_inflight_active gauge\n");
    output.push_str(&format!(
        "fitz_queue_inflight_active {}\n",
        runtime.queue_inflight_active()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_queue_oldest_message_age_seconds Oldest visible queue message age in seconds\n");
    output.push_str("# TYPE fitz_queue_oldest_message_age_seconds gauge\n");
    output.push_str(&format!(
        "fitz_queue_oldest_message_age_seconds {}\n",
        runtime.queue_oldest_message_age_seconds()
    ));
    output.push('\n');

    let backlog_age_buckets = runtime.queue_backlog_age_buckets();

    output.push_str("# HELP fitz_queue_oldest_backlog_age_seconds Oldest ready-or-delayed queue backlog age in seconds\n");
    output.push_str("# TYPE fitz_queue_oldest_backlog_age_seconds gauge\n");
    output.push_str(&format!(
        "fitz_queue_oldest_backlog_age_seconds {}\n",
        runtime.queue_oldest_backlog_age_seconds()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_queue_backlog_age_bucket_under_1m Ready-or-delayed queue messages younger than 1 minute\n");
    output.push_str("# TYPE fitz_queue_backlog_age_bucket_under_1m gauge\n");
    output.push_str(&format!(
        "fitz_queue_backlog_age_bucket_under_1m {}\n",
        backlog_age_buckets.under_1m
    ));
    output.push('\n');

    output.push_str("# HELP fitz_queue_backlog_age_bucket_under_5m Ready-or-delayed queue messages between 1 and 5 minutes old\n");
    output.push_str("# TYPE fitz_queue_backlog_age_bucket_under_5m gauge\n");
    output.push_str(&format!(
        "fitz_queue_backlog_age_bucket_under_5m {}\n",
        backlog_age_buckets.under_5m
    ));
    output.push('\n');

    output.push_str("# HELP fitz_queue_backlog_age_bucket_under_15m Ready-or-delayed queue messages between 5 and 15 minutes old\n");
    output.push_str("# TYPE fitz_queue_backlog_age_bucket_under_15m gauge\n");
    output.push_str(&format!(
        "fitz_queue_backlog_age_bucket_under_15m {}\n",
        backlog_age_buckets.under_15m
    ));
    output.push('\n');

    output.push_str("# HELP fitz_queue_backlog_age_bucket_over_15m Ready-or-delayed queue messages 15 minutes old or older\n");
    output.push_str("# TYPE fitz_queue_backlog_age_bucket_over_15m gauge\n");
    output.push_str(&format!(
        "fitz_queue_backlog_age_bucket_over_15m {}\n",
        backlog_age_buckets.over_15m
    ));
    output.push('\n');

    output.push_str("# HELP fitz_queue_redeliveries_total Total queue message redeliveries recorded by this broker process\n");
    output.push_str("# TYPE fitz_queue_redeliveries_total counter\n");
    output.push_str(&format!(
        "fitz_queue_redeliveries_total {}\n",
        runtime.queue_redeliveries_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_queue_notify_drops_total Total queue notifications dropped by this broker process\n");
    output.push_str("# TYPE fitz_queue_notify_drops_total counter\n");
    output.push_str(&format!(
        "fitz_queue_notify_drops_total {}\n",
        runtime.queue_notify_drops_total()
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

    output.push_str(
        "# HELP fitz_rpc_oldest_pending_request_age_seconds Oldest pending RPC request age in seconds\n",
    );
    output.push_str("# TYPE fitz_rpc_oldest_pending_request_age_seconds gauge\n");
    output.push_str(&format!(
        "fitz_rpc_oldest_pending_request_age_seconds {}\n",
        runtime.rpc_oldest_pending_request_age_seconds()
    ));
    output.push('\n');

    output.push_str(
        "# HELP fitz_rpc_pending_routes_active Distinct RPC routes with pending requests\n",
    );
    output.push_str("# TYPE fitz_rpc_pending_routes_active gauge\n");
    output.push_str(&format!(
        "fitz_rpc_pending_routes_active {}\n",
        runtime.rpc_pending_routes_active()
    ));
    output.push('\n');

    let rpc_latency_buckets = runtime.rpc_worker_latency_buckets();

    output.push_str(
        "# HELP fitz_rpc_slowest_worker_average_latency_ms Slowest RPC worker average latency in milliseconds\n",
    );
    output.push_str("# TYPE fitz_rpc_slowest_worker_average_latency_ms gauge\n");
    output.push_str(&format!(
        "fitz_rpc_slowest_worker_average_latency_ms {:.3}\n",
        runtime.rpc_slowest_worker_average_latency_ms()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_rpc_worker_latency_bucket_under_5ms RPC workers with average latency under 5 milliseconds\n");
    output.push_str("# TYPE fitz_rpc_worker_latency_bucket_under_5ms gauge\n");
    output.push_str(&format!(
        "fitz_rpc_worker_latency_bucket_under_5ms {}\n",
        rpc_latency_buckets.under_5ms
    ));
    output.push('\n');

    output.push_str("# HELP fitz_rpc_worker_latency_bucket_under_25ms RPC workers with average latency between 5 and 25 milliseconds\n");
    output.push_str("# TYPE fitz_rpc_worker_latency_bucket_under_25ms gauge\n");
    output.push_str(&format!(
        "fitz_rpc_worker_latency_bucket_under_25ms {}\n",
        rpc_latency_buckets.under_25ms
    ));
    output.push('\n');

    output.push_str("# HELP fitz_rpc_worker_latency_bucket_under_100ms RPC workers with average latency between 25 and 100 milliseconds\n");
    output.push_str("# TYPE fitz_rpc_worker_latency_bucket_under_100ms gauge\n");
    output.push_str(&format!(
        "fitz_rpc_worker_latency_bucket_under_100ms {}\n",
        rpc_latency_buckets.under_100ms
    ));
    output.push('\n');

    output.push_str("# HELP fitz_rpc_worker_latency_bucket_over_100ms RPC workers with average latency of 100 milliseconds or more\n");
    output.push_str("# TYPE fitz_rpc_worker_latency_bucket_over_100ms gauge\n");
    output.push_str(&format!(
        "fitz_rpc_worker_latency_bucket_over_100ms {}\n",
        rpc_latency_buckets.over_100ms
    ));
    output.push('\n');

    // Lease domain
    output.push_str("# HELP fitz_lease_active Active leases\n");
    output.push_str("# TYPE fitz_lease_active gauge\n");
    output.push_str(&format!("fitz_lease_active {}\n", runtime.lease_active()));
    output.push('\n');

    output.push_str(
        "# HELP fitz_lease_oldest_lease_age_seconds Oldest active lease age in seconds\n",
    );
    output.push_str("# TYPE fitz_lease_oldest_lease_age_seconds gauge\n");
    output.push_str(&format!(
        "fitz_lease_oldest_lease_age_seconds {}\n",
        runtime.lease_oldest_lease_age_seconds()
    ));
    output.push('\n');

    output.push_str(
        "# HELP fitz_lease_ownership_churn_total Successful lease renewals and ownership churn events\n",
    );
    output.push_str("# TYPE fitz_lease_ownership_churn_total counter\n");
    output.push_str(&format!(
        "fitz_lease_ownership_churn_total {}\n",
        runtime.lease_ownership_churn_total()
    ));
    output.push('\n');

    // Stream domain
    output.push_str("# HELP fitz_stream_active Active streams\n");
    output.push_str("# TYPE fitz_stream_active gauge\n");
    output.push_str(&format!("fitz_stream_active {}\n", runtime.stream_active()));
    output.push('\n');

    output.push_str(
        "# HELP fitz_stream_append_sessions_active Live append sessions currently tracked by the broker process\n",
    );
    output.push_str("# TYPE fitz_stream_append_sessions_active gauge\n");
    output.push_str(&format!(
        "fitz_stream_append_sessions_active {}\n",
        runtime.stream_append_sessions_active()
    ));
    output.push('\n');

    let watermark_lag_buckets = runtime.stream_watermark_lag_buckets();

    output.push_str("# HELP fitz_stream_events_total Total committed stream events visible through the admin snapshot\n");
    output.push_str("# TYPE fitz_stream_events_total gauge\n");
    output.push_str(&format!(
        "fitz_stream_events_total {}\n",
        runtime.stream_events_total()
    ));
    output.push('\n');

    output.push_str(
        "# HELP fitz_stream_append_sessions_started_total Total stream append sessions started in this broker process\n",
    );
    output.push_str("# TYPE fitz_stream_append_sessions_started_total counter\n");
    output.push_str(&format!(
        "fitz_stream_append_sessions_started_total {}\n",
        runtime.stream_append_sessions_started_total()
    ));
    output.push('\n');

    output.push_str(
        "# HELP fitz_stream_append_sessions_ended_total Total stream append sessions ended in this broker process\n",
    );
    output.push_str("# TYPE fitz_stream_append_sessions_ended_total counter\n");
    output.push_str(&format!(
        "fitz_stream_append_sessions_ended_total {}\n",
        runtime.stream_append_sessions_ended_total()
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

    output.push_str("# HELP fitz_stream_notify_drops_total Total stream notifications dropped by this broker process\n");
    output.push_str("# TYPE fitz_stream_notify_drops_total counter\n");
    output.push_str(&format!(
        "fitz_stream_notify_drops_total {}\n",
        runtime.stream_notify_drops_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_stream_watermark_lag_bucket_caught_up Stream family watermarks aligned with the fastest family in their area\n");
    output.push_str("# TYPE fitz_stream_watermark_lag_bucket_caught_up gauge\n");
    output.push_str(&format!(
        "fitz_stream_watermark_lag_bucket_caught_up {}\n",
        watermark_lag_buckets.caught_up
    ));
    output.push('\n');

    output.push_str("# HELP fitz_stream_watermark_lag_bucket_under_10 Stream family watermarks trailing the fastest family by fewer than 10 events\n");
    output.push_str("# TYPE fitz_stream_watermark_lag_bucket_under_10 gauge\n");
    output.push_str(&format!(
        "fitz_stream_watermark_lag_bucket_under_10 {}\n",
        watermark_lag_buckets.under_10
    ));
    output.push('\n');

    output.push_str("# HELP fitz_stream_watermark_lag_bucket_under_100 Stream family watermarks trailing the fastest family by fewer than 100 events\n");
    output.push_str("# TYPE fitz_stream_watermark_lag_bucket_under_100 gauge\n");
    output.push_str(&format!(
        "fitz_stream_watermark_lag_bucket_under_100 {}\n",
        watermark_lag_buckets.under_100
    ));
    output.push('\n');

    output.push_str("# HELP fitz_stream_watermark_lag_bucket_over_100 Stream family watermarks trailing the fastest family by 100 events or more\n");
    output.push_str("# TYPE fitz_stream_watermark_lag_bucket_over_100 gauge\n");
    output.push_str(&format!(
        "fitz_stream_watermark_lag_bucket_over_100 {}\n",
        watermark_lag_buckets.over_100
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

    output.push_str("# HELP fitz_schedule_executions_per_minute Acknowledged schedule handoffs over the last minute\n");
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

    output.push_str("# HELP fitz_schedule_pending_fire_claims Durably claimed schedule occurrences awaiting acknowledged live handoff\n");
    output.push_str("# TYPE fitz_schedule_pending_fire_claims gauge\n");
    output.push_str(&format!(
        "fitz_schedule_pending_fire_claims {}\n",
        runtime.schedule_pending_fire_claims()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_pending_ack_retries Pending schedule live handoffs waiting on durable acknowledgement retry\n");
    output.push_str("# TYPE fitz_schedule_pending_ack_retries gauge\n");
    output.push_str(&format!(
        "fitz_schedule_pending_ack_retries {}\n",
        runtime.schedule_pending_ack_retries()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_oldest_pending_claim_age_seconds Oldest pending schedule fire claim age in seconds\n");
    output.push_str("# TYPE fitz_schedule_oldest_pending_claim_age_seconds gauge\n");
    output.push_str(&format!(
        "fitz_schedule_oldest_pending_claim_age_seconds {}\n",
        runtime.schedule_oldest_pending_claim_age_seconds()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_notify_failures_total Total schedule live publish handoffs that failed to route\n");
    output.push_str("# TYPE fitz_schedule_notify_failures_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_notify_failures_total {}\n",
        runtime.schedule_notify_failures()
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

    output.push_str("# HELP fitz_schedule_pending_claims_expired_total Total stale pending schedule claims cleaned up by this broker process\n");
    output.push_str("# TYPE fitz_schedule_pending_claims_expired_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_pending_claims_expired_total {}\n",
        runtime.schedule_pending_claims_expired_total()
    ));
    output.push('\n');

    output.push_str("# HELP fitz_schedule_pending_claim_cleanup_failure_total Total failed pending schedule claim cleanup attempts\n");
    output.push_str("# TYPE fitz_schedule_pending_claim_cleanup_failure_total counter\n");
    output.push_str(&format!(
        "fitz_schedule_pending_claim_cleanup_failure_total {}\n",
        runtime.schedule_pending_claim_cleanup_failures_total()
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

    #[test]
    fn should_export_schedule_metrics_given_preloaded_schedule_runtime() {
        // Arrange
        let runtime = runtime_with_preloaded_schedule_metrics();
        let metrics = crate::boot::observability::metrics();
        let latency_before = metrics
            .histogram_get_buckets("fitz_schedule_latency_ms")
            .unwrap_or([0; 9]);
        metrics.histogram_observe_ms("fitz_schedule_latency_ms", 1);
        metrics.histogram_observe_ms("fitz_schedule_latency_ms", 250);

        // Act
        let metrics = generate_prometheus_metrics(runtime);

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
        assert!(metrics.contains("fitz_notice_delivery_drops_total 0"));
        assert!(metrics.contains("fitz_stream_notify_drops_total 0"));
        assert!(metrics.contains("fitz_queue_notify_drops_total 0"));
    }
}
