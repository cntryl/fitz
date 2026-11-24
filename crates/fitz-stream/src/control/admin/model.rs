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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub committed_event_count: u64,
    pub offset: u64,
    pub watermark: u64,
    pub size_bytes: u64,
    pub sessions_active: usize,
    pub subscriptions_active: usize,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
pub struct StreamLagBuckets {
    pub caught_up: usize,
    pub under_10: usize,
    pub under_100: usize,
    pub over_100: usize,
}

impl StreamLagBuckets {
    pub fn record_lag_events(&mut self, lag_events: u64) {
        if lag_events == 0 {
            self.caught_up += 1;
        } else if lag_events < 10 {
            self.under_10 += 1;
        } else if lag_events < 100 {
            self.under_100 += 1;
        } else {
            self.over_100 += 1;
        }
    }

    pub fn merge(&mut self, other: StreamLagBuckets) {
        self.caught_up += other.caught_up;
        self.under_10 += other.under_10;
        self.under_100 += other.under_100;
        self.over_100 += other.over_100;
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
pub struct StreamLatencyBuckets {
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

impl StreamLatencyBuckets {
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
pub struct StreamRealmWatermarkDetail {
    pub realm: String,
    pub area_count: usize,
    pub resource_count: usize,
    pub family_watermarks: Vec<StreamRealmWatermark>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRealmWatermark {
    pub family: u64,
    pub watermark: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAreaWatermarkDetail {
    pub realm: String,
    pub area: String,
    pub resource_count: usize,
    pub family_watermarks: Vec<StreamAreaWatermark>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAreaWatermark {
    pub family: u64,
    pub watermark: u64,
}

#[derive(Clone, Copy)]
pub struct StreamInfoSnapshot<'a> {
    pub route_family: u64,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub committed_event_count: u64,
    pub offset: u64,
    pub watermark: u64,
    pub size_bytes: u64,
    pub sessions_active: usize,
}

impl StreamInfo {
    #[must_use]
    pub fn snapshot(snapshot: StreamInfoSnapshot<'_>) -> Self {
        Self {
            route_family: snapshot.route_family,
            realm: snapshot.realm.to_string(),
            area: snapshot.area.to_string(),
            resource: snapshot.resource.to_string(),
            committed_event_count: snapshot.committed_event_count,
            offset: snapshot.offset,
            watermark: snapshot.watermark,
            size_bytes: snapshot.size_bytes,
            sessions_active: snapshot.sessions_active,
            subscriptions_active: 0,
        }
    }
}

impl StreamRealmWatermarkDetail {
    #[must_use]
    pub fn snapshot(
        realm: &str,
        area_count: usize,
        resource_count: usize,
        family_watermarks: Vec<StreamRealmWatermark>,
    ) -> Self {
        Self {
            realm: realm.to_string(),
            area_count,
            resource_count,
            family_watermarks,
        }
    }
}

impl StreamRealmWatermark {
    #[must_use]
    pub fn snapshot(family: u64, watermark: u64) -> Self {
        Self { family, watermark }
    }
}

impl StreamAreaWatermarkDetail {
    #[must_use]
    pub fn snapshot(
        realm: &str,
        area: &str,
        resource_count: usize,
        family_watermarks: Vec<StreamAreaWatermark>,
    ) -> Self {
        Self {
            realm: realm.to_string(),
            area: area.to_string(),
            resource_count,
            family_watermarks,
        }
    }
}

impl StreamAreaWatermark {
    #[must_use]
    pub fn snapshot(family: u64, watermark: u64) -> Self {
        Self { family, watermark }
    }
}

use fitz_core::projection::{FamilyRow, ResourceIdentity};

macro_rules! resource_row {
    ($field:ident; $($ty:ty),+) => { $(impl FamilyRow for $ty {
        fn family(&self) -> u64 { self.$field }
        fn inventory_identity(&self) -> Option<ResourceIdentity> { Some((self.$field, self.realm.clone(), self.area.clone(), self.resource.clone())) }
    })+ };
}
resource_row!(route_family; StreamInfo);
