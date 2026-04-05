use crate::observability::metrics::{DomainMetricSet, MetricsCollector};
use std::time::Instant;

pub const METRIC_REQUESTS_TOTAL: &str = "fitz_schedule_requests_total";
pub const METRIC_SUCCESS_TOTAL: &str = "fitz_schedule_success_total";
pub const METRIC_FAILURE_TOTAL: &str = "fitz_schedule_failure_total";
pub const METRIC_LATENCY_MS: &str = "fitz_schedule_latency_ms";
pub const METRIC_ACTIVE_GAUGE: &str = "fitz_schedule_active_gauge";
pub const METRIC_PENDING_GAUGE: &str = "fitz_schedule_pending_gauge";

#[derive(Clone)]
pub struct ScheduleMetrics {
    metrics: DomainMetricSet,
}

impl ScheduleMetrics {
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

    pub fn set_schedule_count(&self, count: usize) {
        self.metrics.gauge_set(METRIC_ACTIVE_GAUGE, count as u64);
    }

    pub fn set_pending_fire_count(&self, count: usize) {
        self.metrics.gauge_set(METRIC_PENDING_GAUGE, count as u64);
    }
}