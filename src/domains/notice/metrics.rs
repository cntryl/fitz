use crate::observability::metrics::{DomainMetricSet, MetricsCollector};
use std::time::Instant;

pub const METRIC_REQUESTS_TOTAL: &str = "fitz_notice_requests_total";
pub const METRIC_SUCCESS_TOTAL: &str = "fitz_notice_success_total";
pub const METRIC_FAILURE_TOTAL: &str = "fitz_notice_failure_total";
pub const METRIC_LATENCY_MS: &str = "fitz_notice_latency_ms";
pub const METRIC_SUBSCRIPTIONS_GAUGE: &str = "fitz_notice_subscriptions_gauge";
pub const METRIC_RESPONSE_DROPS_TOTAL: &str = "fitz_notice_response_drops_total";
// Wire name kept as `delivery` rather than `notify` for dashboard compatibility.
pub const METRIC_DELIVERY_DROPS_TOTAL: &str = "fitz_notice_delivery_drops_total";
/// Accepted deliveries whose envelope failed inside the actor. The client was
/// already told the publish was accepted, so there is nobody to report to -
/// but the failure must still be countable.
pub const METRIC_ACCEPTED_DELIVERY_FAILURES_TOTAL: &str =
    "fitz_notice_accepted_delivery_failures_total";

#[derive(Clone)]
pub struct NoticeMetrics {
    metrics: DomainMetricSet,
}

impl NoticeMetrics {
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

    pub fn counter_add(&self, name: &str, amount: u64) {
        self.metrics.counter_add(name, amount);
    }

    pub fn record_response_drop(&self) {
        self.metrics.counter_inc(METRIC_RESPONSE_DROPS_TOTAL);
    }

    pub fn set_subscription_count(&self, count: usize) {
        self.metrics
            .gauge_set(METRIC_SUBSCRIPTIONS_GAUGE, count as u64);
    }
}
