use serde::{Deserialize, Serialize};

fn saturating_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

fn histogram_tail_ratio(slow_tail_count: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        usize_to_f64(slow_tail_count) / usize_to_f64(total)
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
pub struct ScheduleLatencyBuckets {
    pub under_1ms: usize,
    pub under_5ms: usize,
    pub under_10ms: usize,
    pub under_50ms: usize,
    pub under_100ms: usize,
    pub under_500ms: usize,
    pub under_1s: usize,
    pub under_5s: usize,
    pub over_5s: usize,
}

impl ScheduleLatencyBuckets {
    #[must_use]
    pub fn from_histogram(buckets: [u64; 9]) -> Self {
        Self {
            under_1ms: saturating_usize(buckets[0]),
            under_5ms: saturating_usize(buckets[1]),
            under_10ms: saturating_usize(buckets[2]),
            under_50ms: saturating_usize(buckets[3]),
            under_100ms: saturating_usize(buckets[4]),
            under_500ms: saturating_usize(buckets[5]),
            under_1s: saturating_usize(buckets[6]),
            under_5s: saturating_usize(buckets[7]),
            over_5s: saturating_usize(buckets[8]),
        }
    }

    #[must_use]
    pub fn total(&self) -> usize {
        self.under_1ms
            + self.under_5ms
            + self.under_10ms
            + self.under_50ms
            + self.under_100ms
            + self.under_500ms
            + self.under_1s
            + self.under_5s
            + self.over_5s
    }

    #[must_use]
    pub fn slow_tail_count(&self) -> usize {
        self.under_500ms + self.under_1s + self.under_5s + self.over_5s
    }

    #[must_use]
    pub fn slow_tail_ratio(&self) -> f64 {
        histogram_tail_ratio(self.slow_tail_count(), self.total())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleInfo {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
    pub cron: String,
    pub delivery_mode: crate::domain::ScheduleDeliveryMode,
    pub next_run: String,
    pub last_run: Option<String>,
    pub executions_total: u64,
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulePendingClaimInfo {
    pub route_family: u64,
    pub route: String,
    pub delivery_mode: crate::domain::ScheduleDeliveryMode,
    pub fire_ms: u64,
    pub claimed_at_ms: u64,
}

impl ScheduleInfo {
    #[must_use]
    pub fn enabled_snapshot(
        route_family: u64,
        realm: String,
        area: String,
        resource: String,
        operation: String,
        cron: String,
        next_run: &str,
    ) -> Self {
        Self {
            route_family,
            realm,
            area,
            resource,
            operation,
            cron,
            delivery_mode: crate::domain::ScheduleDeliveryMode::Broadcast,
            next_run: next_run.to_string(),
            last_run: None,
            executions_total: 0,
            enabled: true,
        }
    }
}
