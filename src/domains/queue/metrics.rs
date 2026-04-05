use crate::observability::metrics::{DomainMetricSet, MetricsCollector};
use std::time::Instant;

pub const METRIC_REQUESTS_TOTAL: &str = "fitz_queue_requests_total";
pub const METRIC_SUCCESS_TOTAL: &str = "fitz_queue_success_total";
pub const METRIC_FAILURE_TOTAL: &str = "fitz_queue_failure_total";
pub const METRIC_LATENCY_MS: &str = "fitz_queue_latency_ms";
pub const METRIC_READY_GAUGE: &str = "fitz_queue_ready_gauge";
pub const METRIC_DELAYED_GAUGE: &str = "fitz_queue_delayed_gauge";
pub const METRIC_INFLIGHT_GAUGE: &str = "fitz_queue_inflight_gauge";

#[derive(Clone)]
pub struct QueueMetrics {
    metrics: DomainMetricSet,
}

impl QueueMetrics {
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
        self.metrics.gauge_set(METRIC_READY_GAUGE, count as u64);
    }

    pub fn set_delayed_messages(&self, count: usize) {
        self.metrics.gauge_set(METRIC_DELAYED_GAUGE, count as u64);
    }

    pub fn set_inflight_messages(&self, count: usize) {
        self.metrics.gauge_set(METRIC_INFLIGHT_GAUGE, count as u64);
    }
}
