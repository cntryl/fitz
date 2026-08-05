use crate::observability::metrics::{DomainMetricSet, MetricsCollector};
use std::time::Instant;

pub const METRIC_REQUESTS_TOTAL: &str = "fitz_queue_requests_total";
pub const METRIC_SUCCESS_TOTAL: &str = "fitz_queue_success_total";
pub const METRIC_FAILURE_TOTAL: &str = "fitz_queue_failure_total";
pub const METRIC_LATENCY_MS: &str = "fitz_queue_latency_ms";
pub const METRIC_READY_GAUGE: &str = "fitz_queue_ready_gauge";
pub const METRIC_DELAYED_GAUGE: &str = "fitz_queue_delayed_gauge";
pub const METRIC_INFLIGHT_GAUGE: &str = "fitz_queue_inflight_gauge";
pub const METRIC_NOTIFY_DROPS_TOTAL: &str = "fitz_queue_notify_drops_total";

// Operation-specific counters
pub const METRIC_ENQUEUE_TOTAL: &str = "fitz_queue_enqueue_total";
pub const METRIC_RESERVE_TOTAL: &str = "fitz_queue_reserve_total";
pub const METRIC_COMPLETE_TOTAL: &str = "fitz_queue_complete_total";
pub const METRIC_RELEASE_TOTAL: &str = "fitz_queue_release_total";
pub const METRIC_EXTEND_TOTAL: &str = "fitz_queue_extend_total";
pub const METRIC_ENQUEUE_LATENCY_MS: &str = "fitz_queue_enqueue_latency_ms";
pub const METRIC_RESERVE_LATENCY_MS: &str = "fitz_queue_reserve_latency_ms";

#[derive(Clone)]
pub struct QueueMetrics {
    metrics: DomainMetricSet,
}

impl QueueMetrics {
    #[must_use]
    pub fn new(collector: MetricsCollector) -> Self {
        Self {
            metrics: DomainMetricSet::new(
                collector,
                METRIC_REQUESTS_TOTAL,
                METRIC_SUCCESS_TOTAL,
                METRIC_FAILURE_TOTAL,
                METRIC_LATENCY_MS,
            ),
        }
    }

    #[must_use]
    pub fn record_request_start(&self) -> Instant {
        self.metrics.record_request_start()
    }

    pub fn record_success(&self, started_at: Instant) {
        self.metrics.record_success(started_at);
    }

    pub fn record_failure(&self, started_at: Instant) {
        self.metrics.record_failure(started_at);
    }

    pub fn counter_inc(&self, name: &str) {
        self.metrics.counter_inc(name);
    }

    pub fn histogram_observe_us(&self, name: &str, value_us: u64) {
        self.metrics.histogram_observe_us(name, value_us);
    }

    pub fn set_ready_messages(&self, count: usize) {
        self.metrics
            .gauge_set(METRIC_READY_GAUGE, Self::usize_to_u64(count));
    }

    pub fn set_delayed_messages(&self, count: usize) {
        self.metrics
            .gauge_set(METRIC_DELAYED_GAUGE, Self::usize_to_u64(count));
    }

    pub fn set_inflight_messages(&self, count: usize) {
        self.metrics
            .gauge_set(METRIC_INFLIGHT_GAUGE, Self::usize_to_u64(count));
    }

    pub fn record_enqueue(&self, started_at: Instant) {
        self.metrics.counter_inc(METRIC_ENQUEUE_TOTAL);
        let elapsed_ms = Self::elapsed_ms_since(started_at);
        self.metrics
            .histogram_observe_ms(METRIC_ENQUEUE_LATENCY_MS, elapsed_ms);
    }

    pub fn record_reserve(&self, started_at: Instant) {
        self.metrics.counter_inc(METRIC_RESERVE_TOTAL);
        let elapsed_ms = Self::elapsed_ms_since(started_at);
        self.metrics
            .histogram_observe_ms(METRIC_RESERVE_LATENCY_MS, elapsed_ms);
    }

    pub fn record_complete(&self) {
        self.metrics.counter_inc(METRIC_COMPLETE_TOTAL);
    }

    pub fn record_release(&self) {
        self.metrics.counter_inc(METRIC_RELEASE_TOTAL);
    }

    pub fn record_extend(&self) {
        self.metrics.counter_inc(METRIC_EXTEND_TOTAL);
    }

    fn elapsed_ms_since(started_at: Instant) -> u64 {
        u64::try_from(started_at.elapsed().as_micros())
            .unwrap_or(u64::MAX)
            .saturating_div(1_000)
    }

    fn usize_to_u64(value: usize) -> u64 {
        u64::try_from(value).unwrap_or(u64::MAX)
    }
}
