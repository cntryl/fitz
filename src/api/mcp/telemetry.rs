//! Fixed-cardinality process aggregates for the MCP surface.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

#[derive(Default)]
struct Metrics {
    requests: AtomicU64,
    denials: AtomicU64,
    errors: AtomicU64,
    cancellations: AtomicU64,
    overload_rejections: AtomicU64,
    authentication_failures: AtomicU64,
    in_flight: AtomicU64,
    duration_ms_sum: AtomicU64,
    duration_ms_count: AtomicU64,
    duration_ms_max: AtomicU64,
    audit_records_dropped: AtomicU64,
}

static METRICS: OnceLock<Metrics> = OnceLock::new();

fn metrics() -> &'static Metrics {
    METRICS.get_or_init(Metrics::default)
}

pub(crate) struct RequestTimer {
    started: Instant,
    finished: bool,
}

pub(crate) fn begin_request() -> RequestTimer {
    let metrics = metrics();
    metrics.requests.fetch_add(1, Ordering::Relaxed);
    metrics.in_flight.fetch_add(1, Ordering::Relaxed);
    RequestTimer {
        started: Instant::now(),
        finished: false,
    }
}

impl RequestTimer {
    pub(crate) fn finish(mut self, failed: bool) {
        if failed {
            metrics().errors.fetch_add(1, Ordering::Relaxed);
        }
        self.finish_tracking();
    }

    fn finish_tracking(&mut self) {
        let elapsed = elapsed_millis(self.started);
        let metrics = metrics();
        metrics.in_flight.fetch_sub(1, Ordering::Relaxed);
        metrics
            .duration_ms_sum
            .fetch_add(elapsed, Ordering::Relaxed);
        metrics.duration_ms_count.fetch_add(1, Ordering::Relaxed);
        metrics
            .duration_ms_max
            .fetch_max(elapsed, Ordering::Relaxed);
        self.finished = true;
    }
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        if !self.finished {
            metrics().cancellations.fetch_add(1, Ordering::Relaxed);
            self.finish_tracking();
        }
    }
}

pub(crate) fn record_denial() {
    metrics().denials.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_cancellation() {
    metrics().cancellations.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_overload() {
    metrics()
        .overload_rejections
        .fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_authentication_failure() {
    let metrics = metrics();
    metrics
        .authentication_failures
        .fetch_add(1, Ordering::Relaxed);
    metrics.denials.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_audit_eviction() {
    metrics()
        .audit_records_dropped
        .fetch_add(1, Ordering::Relaxed);
}

#[allow(clippy::too_many_lines)] // Keep the fixed-cardinality metric exposition in one auditable list.
pub(crate) fn append_prometheus_metrics(output: &mut String) {
    let metrics = metrics();
    let _ = writeln!(
        output,
        "# HELP fitz_mcp_requests_total MCP tool calls received."
    );
    let _ = writeln!(output, "# TYPE fitz_mcp_requests_total counter");
    let _ = writeln!(
        output,
        "fitz_mcp_requests_total {}",
        metrics.requests.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        output,
        "# HELP fitz_mcp_denials_total MCP authorization and policy denials."
    );
    let _ = writeln!(output, "# TYPE fitz_mcp_denials_total counter");
    let _ = writeln!(
        output,
        "fitz_mcp_denials_total {}",
        metrics.denials.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        output,
        "# HELP fitz_mcp_errors_total MCP tool calls that returned errors."
    );
    let _ = writeln!(output, "# TYPE fitz_mcp_errors_total counter");
    let _ = writeln!(
        output,
        "fitz_mcp_errors_total {}",
        metrics.errors.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        output,
        "# HELP fitz_mcp_cancellations_total MCP calls cancelled or abandoned by the client."
    );
    let _ = writeln!(output, "# TYPE fitz_mcp_cancellations_total counter");
    let _ = writeln!(
        output,
        "fitz_mcp_cancellations_total {}",
        metrics.cancellations.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        output,
        "# HELP fitz_mcp_overload_rejections_total MCP calls rejected at an admission limit."
    );
    let _ = writeln!(output, "# TYPE fitz_mcp_overload_rejections_total counter");
    let _ = writeln!(
        output,
        "fitz_mcp_overload_rejections_total {}",
        metrics.overload_rejections.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        output,
        "# HELP fitz_mcp_authentication_failures_total MCP bearer authentication failures."
    );
    let _ = writeln!(
        output,
        "# TYPE fitz_mcp_authentication_failures_total counter"
    );
    let _ = writeln!(
        output,
        "fitz_mcp_authentication_failures_total {}",
        metrics.authentication_failures.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        output,
        "# HELP fitz_mcp_requests_in_flight MCP tool calls currently executing."
    );
    let _ = writeln!(output, "# TYPE fitz_mcp_requests_in_flight gauge");
    let _ = writeln!(
        output,
        "fitz_mcp_requests_in_flight {}",
        metrics.in_flight.load(Ordering::Relaxed)
    );
    let _ = writeln!(output, "# HELP fitz_mcp_request_duration_milliseconds MCP tool call duration aggregates in milliseconds.");
    let _ = writeln!(
        output,
        "# TYPE fitz_mcp_request_duration_milliseconds summary"
    );
    let _ = writeln!(
        output,
        "fitz_mcp_request_duration_milliseconds_sum {}",
        metrics.duration_ms_sum.load(Ordering::Relaxed)
    );
    let _ = writeln!(
        output,
        "fitz_mcp_request_duration_milliseconds_count {}",
        metrics.duration_ms_count.load(Ordering::Relaxed)
    );
    let _ = writeln!(output, "# HELP fitz_mcp_request_duration_milliseconds_max Maximum observed MCP tool call duration in milliseconds.");
    let _ = writeln!(
        output,
        "# TYPE fitz_mcp_request_duration_milliseconds_max gauge"
    );
    let _ = writeln!(
        output,
        "fitz_mcp_request_duration_milliseconds_max {}",
        metrics.duration_ms_max.load(Ordering::Relaxed)
    );
    let _ = writeln!(output, "# HELP fitz_mcp_audit_records_dropped_total MCP process-local audit records evicted from bounded retention.");
    let _ = writeln!(
        output,
        "# TYPE fitz_mcp_audit_records_dropped_total counter"
    );
    let _ = writeln!(
        output,
        "fitz_mcp_audit_records_dropped_total {}",
        metrics.audit_records_dropped.load(Ordering::Relaxed)
    );
}

fn elapsed_millis(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}
