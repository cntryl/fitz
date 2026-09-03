use crate::observability::metrics::{DomainMetricSet, MetricsCollector};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

pub const METRIC_REQUESTS_TOTAL: &str = "fitz_stream_requests_total";
pub const METRIC_SUCCESS_TOTAL: &str = "fitz_stream_success_total";
pub const METRIC_FAILURE_TOTAL: &str = "fitz_stream_failure_total";
pub const METRIC_LATENCY_MS: &str = "fitz_stream_latency_ms";
pub const METRIC_ACTIVE_GAUGE: &str = "fitz_stream_active_gauge";
pub const METRIC_SUBSCRIPTIONS_GAUGE: &str = "fitz_stream_subscriptions_gauge";
pub const METRIC_APPEND_SESSIONS_GAUGE: &str = "fitz_stream_append_sessions_active";
pub const METRIC_RESPONSE_DROPS_TOTAL: &str = "fitz_stream_response_drops_total";
pub const METRIC_NOTIFY_DROPS_TOTAL: &str = "fitz_stream_notify_drops_total";
/// Incremented once per route family whose handler panics and fails closed.
/// Non-fatal and scoped to that family only (see
/// `KeyedFamilyExecutor::is_family_running`) — this is the only
/// operator-visible signal for a permanently degraded realm, since a
/// per-family failure deliberately does not flip domain-wide health/liveness.
pub const METRIC_FAMILY_FAILED_CLOSED_TOTAL: &str = "fitz_stream_family_failed_closed_total";
pub const METRIC_WATERMARK_COORDINATION_DROPS_TOTAL: &str =
    "fitz_stream_watermark_coordination_drops_total";
pub const METRIC_MAINTENANCE_ATTEMPTS_TOTAL: &str = "fitz_stream_maintenance_attempts_total";
pub const METRIC_MAINTENANCE_FAILURES_TOTAL: &str = "fitz_stream_maintenance_failures_total";
pub const METRIC_MAINTENANCE_RETRIES_TOTAL: &str = "fitz_stream_maintenance_retries_total";
pub const METRIC_MAINTENANCE_BUCKETS_COMPACTED_TOTAL: &str =
    "fitz_stream_maintenance_buckets_compacted_total";
pub const METRIC_ADMIN_PROJECTION_FAILURES_TOTAL: &str =
    "fitz_stream_admin_projection_failures_total";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamRealmWatermarkMetric {
    pub(crate) family: u64,
    pub(crate) realm: String,
    pub(crate) watermark: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StreamAreaWatermarkMetric {
    pub(crate) family: u64,
    pub(crate) realm: String,
    pub(crate) area: String,
    pub(crate) watermark: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StreamDurableMetricsSnapshot {
    pub(crate) events_total: usize,
    pub(crate) realm_watermarks: Vec<StreamRealmWatermarkMetric>,
    pub(crate) area_watermarks: Vec<StreamAreaWatermarkMetric>,
}

impl StreamDurableMetricsSnapshot {
    pub(crate) fn watermark_lag_buckets(&self) -> crate::control::admin::StreamLagBuckets {
        let mut watermarks_by_area: BTreeMap<(&str, &str), Vec<u64>> = BTreeMap::new();
        for metric in &self.area_watermarks {
            watermarks_by_area
                .entry((&metric.realm, &metric.area))
                .or_default()
                .push(metric.watermark);
        }

        watermarks_by_area.values().fold(
            crate::control::admin::StreamLagBuckets::default(),
            |mut buckets, watermarks| {
                let max_watermark = watermarks.iter().copied().max().unwrap_or(0);
                for watermark in watermarks {
                    buckets.record_lag_events(max_watermark.saturating_sub(*watermark));
                }
                buckets
            },
        )
    }
}

#[derive(Default)]
pub(crate) struct StreamDurableMetrics {
    events_total: AtomicUsize,
    realm_watermarks: RwLock<BTreeMap<(u64, String), u64>>,
    area_watermarks: RwLock<BTreeMap<(u64, String, String), u64>>,
}

impl StreamDurableMetrics {
    pub(crate) fn observe_snapshot(
        &self,
        events_total: usize,
        realm_details: &[crate::control::admin::StreamRealmWatermarkDetail],
        area_details: &[crate::control::admin::StreamAreaWatermarkDetail],
    ) {
        self.events_total.fetch_max(events_total, Ordering::Relaxed);
        let mut realm_watermarks = self.realm_watermarks.write();
        for detail in realm_details {
            for watermark in &detail.family_watermarks {
                realm_watermarks
                    .entry((watermark.family, detail.realm.clone()))
                    .and_modify(|current| *current = (*current).max(watermark.watermark))
                    .or_insert(watermark.watermark);
            }
        }
        drop(realm_watermarks);

        let mut area_watermarks = self.area_watermarks.write();
        for detail in area_details {
            for watermark in &detail.family_watermarks {
                area_watermarks
                    .entry((watermark.family, detail.realm.clone(), detail.area.clone()))
                    .and_modify(|current| *current = (*current).max(watermark.watermark))
                    .or_insert(watermark.watermark);
            }
        }
    }

    pub(crate) fn record_events(&self, count: usize) {
        let _ = self
            .events_total
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_add(count))
            });
    }

    pub(crate) fn set_realm_watermark(&self, family: u64, realm: &str, watermark: u64) {
        self.realm_watermarks
            .write()
            .insert((family, realm.to_string()), watermark);
    }

    pub(crate) fn set_area_watermark(&self, family: u64, realm: &str, area: &str, watermark: u64) {
        self.area_watermarks
            .write()
            .insert((family, realm.to_string(), area.to_string()), watermark);
    }

    pub(crate) fn snapshot(&self) -> StreamDurableMetricsSnapshot {
        let realm_watermarks = self
            .realm_watermarks
            .read()
            .iter()
            .map(|((family, realm), watermark)| StreamRealmWatermarkMetric {
                family: *family,
                realm: realm.clone(),
                watermark: *watermark,
            })
            .collect();
        let area_watermarks = self
            .area_watermarks
            .read()
            .iter()
            .map(
                |((family, realm, area), watermark)| StreamAreaWatermarkMetric {
                    family: *family,
                    realm: realm.clone(),
                    area: area.clone(),
                    watermark: *watermark,
                },
            )
            .collect();
        StreamDurableMetricsSnapshot {
            events_total: self.events_total.load(Ordering::Relaxed),
            realm_watermarks,
            area_watermarks,
        }
    }
}

#[derive(Clone)]
pub struct StreamMetrics {
    metrics: DomainMetricSet,
}

impl StreamMetrics {
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

    pub fn set_stream_count(&self, count: usize) {
        self.metrics.gauge_set(METRIC_ACTIVE_GAUGE, count as u64);
    }

    pub fn set_subscription_count(&self, count: usize) {
        self.metrics
            .gauge_set(METRIC_SUBSCRIPTIONS_GAUGE, count as u64);
    }

    pub fn set_append_session_count(&self, count: usize) {
        self.metrics
            .gauge_set(METRIC_APPEND_SESSIONS_GAUGE, count as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_not_regress_durable_metrics_given_an_older_admin_snapshot() {
        // Arrange
        let metrics = StreamDurableMetrics::default();
        metrics.record_events(5);
        metrics.set_realm_watermark(1, "prod", 4);
        metrics.set_area_watermark(1, "prod", "audit", 4);
        let realm = crate::control::admin::StreamRealmWatermarkDetail::snapshot(
            "prod",
            1,
            1,
            vec![crate::control::admin::StreamRealmWatermark::snapshot(1, 2)],
        );
        let area = crate::control::admin::StreamAreaWatermarkDetail::snapshot(
            "prod",
            "audit",
            1,
            vec![crate::control::admin::StreamAreaWatermark::snapshot(1, 2)],
        );

        // Act
        metrics.observe_snapshot(3, &[realm], &[area]);
        let snapshot = metrics.snapshot();

        // Assert
        assert_eq!(snapshot.events_total, 5);
        assert_eq!(snapshot.realm_watermarks[0].watermark, 4);
        assert_eq!(snapshot.area_watermarks[0].watermark, 4);
    }
}
