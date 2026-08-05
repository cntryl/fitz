use crate::observability::metrics::{DomainMetricSet, MetricsCollector};
use std::time::Instant;

pub const METRIC_REQUESTS_TOTAL: &str = "fitz_lease_requests_total";
pub const METRIC_SUCCESS_TOTAL: &str = "fitz_lease_success_total";
pub const METRIC_FAILURE_TOTAL: &str = "fitz_lease_failure_total";
pub const METRIC_LATENCY_MS: &str = "fitz_lease_latency_ms";
pub const METRIC_ACTIVE_GAUGE: &str = "fitz_lease_active_gauge";
pub const METRIC_WAITERS_GAUGE: &str = "fitz_lease_waiters_gauge";
pub const METRIC_RESPONSE_DROPS_TOTAL: &str = "fitz_lease_response_drops_total";
pub const METRIC_NOTIFY_DROPS_TOTAL: &str = "fitz_lease_notify_drops_total";

#[derive(Clone)]
pub struct LeaseMetrics {
    metrics: DomainMetricSet,
}

impl LeaseMetrics {
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

    pub fn record_response_drop(&self) {
        self.metrics.counter_inc(METRIC_RESPONSE_DROPS_TOTAL);
    }

    pub fn record_notify_drop(&self) {
        self.metrics.counter_inc(METRIC_NOTIFY_DROPS_TOTAL);
    }

    pub fn set_active_leases(&self, count: usize) {
        self.metrics.gauge_set(METRIC_ACTIVE_GAUGE, count as u64);
    }

    pub fn set_waiter_depth(&self, count: usize) {
        self.metrics.gauge_set(METRIC_WAITERS_GAUGE, count as u64);
    }

    pub fn increment_waiter_depth(&self) {
        self.metrics.gauge_inc(METRIC_WAITERS_GAUGE);
    }

    pub fn decrement_waiter_depth(&self) {
        self.metrics.gauge_dec(METRIC_WAITERS_GAUGE);
    }
}
