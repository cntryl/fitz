use crate::boot::Runtime;
use std::fmt::Write as _;

pub(super) fn append_metrics(output: &mut String, runtime: &Runtime) {
    append_gauge_metrics(output, runtime);
    append_latency_bucket_metrics(output, runtime);
    append_counter_metrics(output, runtime);
}

fn append_gauge_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str("# HELP fitz_rpc_workers_registered Registered RPC workers\n");
    output.push_str("# TYPE fitz_rpc_workers_registered gauge\n");
    let _ = writeln!(
        output,
        "fitz_rpc_workers_registered {}",
        runtime.rpc_workers_registered()
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_requests_pending Pending RPC requests\n");
    output.push_str("# TYPE fitz_rpc_requests_pending gauge\n");
    let _ = writeln!(
        output,
        "fitz_rpc_requests_pending {}",
        runtime.rpc_requests_pending()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_rpc_oldest_pending_request_age_seconds Oldest pending RPC request age in seconds\n",
    );
    output.push_str("# TYPE fitz_rpc_oldest_pending_request_age_seconds gauge\n");
    let _ = writeln!(
        output,
        "fitz_rpc_oldest_pending_request_age_seconds {}",
        runtime.rpc_oldest_pending_request_age_seconds()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_rpc_pending_routes_active Distinct RPC routes with pending requests\n",
    );
    output.push_str("# TYPE fitz_rpc_pending_routes_active gauge\n");
    let _ = writeln!(
        output,
        "fitz_rpc_pending_routes_active {}",
        runtime.rpc_pending_routes_active()
    );
    output.push('\n');

    output.push_str(
        "# HELP fitz_rpc_slowest_worker_average_latency_ms Slowest RPC worker average latency in milliseconds\n",
    );
    output.push_str("# TYPE fitz_rpc_slowest_worker_average_latency_ms gauge\n");
    let _ = writeln!(
        output,
        "fitz_rpc_slowest_worker_average_latency_ms {:.3}",
        runtime.rpc_slowest_worker_average_latency_ms()
    );
    output.push('\n');
}

fn append_latency_bucket_metrics(output: &mut String, runtime: &Runtime) {
    let rpc_latency_buckets = runtime.rpc_worker_latency_buckets();
    output.push_str("# HELP fitz_rpc_worker_latency_bucket_under_5ms RPC workers with average latency under 5 milliseconds\n");
    output.push_str("# TYPE fitz_rpc_worker_latency_bucket_under_5ms gauge\n");
    let _ = writeln!(
        output,
        "fitz_rpc_worker_latency_bucket_under_5ms {}",
        rpc_latency_buckets.under_5ms
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_worker_latency_bucket_under_25ms RPC workers with average latency between 5 and 25 milliseconds\n");
    output.push_str("# TYPE fitz_rpc_worker_latency_bucket_under_25ms gauge\n");
    let _ = writeln!(
        output,
        "fitz_rpc_worker_latency_bucket_under_25ms {}",
        rpc_latency_buckets.under_25ms
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_worker_latency_bucket_under_100ms RPC workers with average latency between 25 and 100 milliseconds\n");
    output.push_str("# TYPE fitz_rpc_worker_latency_bucket_under_100ms gauge\n");
    let _ = writeln!(
        output,
        "fitz_rpc_worker_latency_bucket_under_100ms {}",
        rpc_latency_buckets.under_100ms
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_worker_latency_bucket_over_100ms RPC workers with average latency of 100 milliseconds or more\n");
    output.push_str("# TYPE fitz_rpc_worker_latency_bucket_over_100ms gauge\n");
    let _ = writeln!(
        output,
        "fitz_rpc_worker_latency_bucket_over_100ms {}",
        rpc_latency_buckets.over_100ms
    );
    output.push('\n');
}

fn append_counter_metrics(output: &mut String, runtime: &Runtime) {
    output.push_str(
        "# HELP fitz_rpc_response_drops_total Total RPC responses dropped by this broker process\n",
    );
    output.push_str("# TYPE fitz_rpc_response_drops_total counter\n");
    let _ = writeln!(
        output,
        "fitz_rpc_response_drops_total {}",
        runtime.rpc_response_drops_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_request_timeouts_total Total RPC requests that timed out before completion\n");
    output.push_str("# TYPE fitz_rpc_request_timeouts_total counter\n");
    let _ = writeln!(
        output,
        "fitz_rpc_request_timeouts_total {}",
        runtime.rpc_request_timeouts_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_backpressure_rejects_total Total RPC requests rejected because the sink was backpressured\n");
    output.push_str("# TYPE fitz_rpc_backpressure_rejects_total counter\n");
    let _ = writeln!(
        output,
        "fitz_rpc_backpressure_rejects_total {}",
        runtime.rpc_backpressure_rejects_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_duplicate_correlation_rejects_total Total RPC requests rejected because the correlation ID was already live\n");
    output.push_str("# TYPE fitz_rpc_duplicate_correlation_rejects_total counter\n");
    let _ = writeln!(
        output,
        "fitz_rpc_duplicate_correlation_rejects_total {}",
        runtime.rpc_duplicate_correlation_rejects_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_wrong_worker_rejects_total Total RPC requests rejected for the wrong worker session\n");
    output.push_str("# TYPE fitz_rpc_wrong_worker_rejects_total counter\n");
    let _ = writeln!(
        output,
        "fitz_rpc_wrong_worker_rejects_total {}",
        runtime.rpc_wrong_worker_rejects_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_responses_dropped_closed_caller_total Total RPC responses dropped because the caller session was closed\n");
    output.push_str("# TYPE fitz_rpc_responses_dropped_closed_caller_total counter\n");
    let _ = writeln!(
        output,
        "fitz_rpc_responses_dropped_closed_caller_total {}",
        runtime.rpc_responses_dropped_closed_caller_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_responses_missing_pending_total Total RPC responses that arrived without a pending caller request\n");
    output.push_str("# TYPE fitz_rpc_responses_missing_pending_total counter\n");
    let _ = writeln!(
        output,
        "fitz_rpc_responses_missing_pending_total {}",
        runtime.rpc_responses_missing_pending_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_invalid_sequence_responses_total Total RPC responses rejected because their sequence was invalid\n");
    output.push_str("# TYPE fitz_rpc_invalid_sequence_responses_total counter\n");
    let _ = writeln!(
        output,
        "fitz_rpc_invalid_sequence_responses_total {}",
        runtime.rpc_invalid_sequence_responses_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_invalid_sequence_errors_forwarded_total Total RPC invalid-sequence errors forwarded to callers\n");
    output.push_str("# TYPE fitz_rpc_invalid_sequence_errors_forwarded_total counter\n");
    let _ = writeln!(
        output,
        "fitz_rpc_invalid_sequence_errors_forwarded_total {}",
        runtime.rpc_invalid_sequence_errors_forwarded_total()
    );
    output.push('\n');

    output.push_str("# HELP fitz_rpc_invalid_sequence_errors_dropped_total Total RPC invalid-sequence errors dropped because no caller could receive them\n");
    output.push_str("# TYPE fitz_rpc_invalid_sequence_errors_dropped_total counter\n");
    let _ = writeln!(
        output,
        "fitz_rpc_invalid_sequence_errors_dropped_total {}",
        runtime.rpc_invalid_sequence_errors_dropped_total()
    );
    output.push('\n');
}
