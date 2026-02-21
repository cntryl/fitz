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

    // Schedule domain
    output.push_str("# HELP fitz_schedule_active Active schedules\n");
    output.push_str("# TYPE fitz_schedule_active gauge\n");
    output.push_str(&format!(
        "fitz_schedule_active {}\n",
        runtime.schedule_active()
    ));
    output.push('\n');
}
