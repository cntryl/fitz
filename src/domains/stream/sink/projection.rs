//! Family-owned Stream admin snapshots combined for global admin reads.
//!
//! A failed family retains its last published observation; another family can
//! refresh without replacing that family's rows or watermarks.

use crate::control::admin::read_model::AdminReadModel;
use crate::control::admin::{StreamAreaWatermarkDetail, StreamInfo, StreamRealmWatermarkDetail};
use crate::domains::stream::metrics::StreamDurableMetrics;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(super) struct StreamFamilyAdminSnapshot {
    pub(super) streams: Vec<StreamInfo>,
    pub(super) realm_watermarks: Vec<StreamRealmWatermarkDetail>,
    pub(super) area_watermarks: Vec<StreamAreaWatermarkDetail>,
    pub(super) committed_events_total: usize,
}

pub(super) struct StreamAdminProjection {
    read_model: Arc<AdminReadModel>,
    durable_metrics: Arc<StreamDurableMetrics>,
    families: Mutex<BTreeMap<u64, StreamFamilyAdminSnapshot>>,
}

impl StreamAdminProjection {
    pub(super) fn new(
        read_model: Arc<AdminReadModel>,
        durable_metrics: Arc<StreamDurableMetrics>,
    ) -> Self {
        Self {
            read_model,
            durable_metrics,
            families: Mutex::new(BTreeMap::new()),
        }
    }

    pub(super) fn publish(&self, family: u64, snapshot: StreamFamilyAdminSnapshot) {
        let mut families = self.families.lock();
        families.insert(family, snapshot);
        self.publish_merged(&families);
    }

    pub(super) fn clear_failed_family_live_sessions(&self, family: u64) {
        let mut families = self.families.lock();
        let Some(snapshot) = families.get_mut(&family) else {
            return;
        };
        let mut changed = false;
        for stream in &mut snapshot.streams {
            if stream.route_family == family && stream.sessions_active != 0 {
                stream.sessions_active = 0;
                changed = true;
            }
        }
        if changed {
            self.publish_merged(&families);
        }
    }

    fn publish_merged(&self, families: &BTreeMap<u64, StreamFamilyAdminSnapshot>) {
        let mut streams = Vec::new();
        let mut realm_watermarks: BTreeMap<String, StreamRealmWatermarkDetail> = BTreeMap::new();
        let mut area_watermarks: BTreeMap<(String, String), StreamAreaWatermarkDetail> =
            BTreeMap::new();
        let mut realm_areas: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut committed_events_total = 0usize;

        for snapshot in families.values() {
            streams.extend(snapshot.streams.iter().cloned());
            committed_events_total =
                committed_events_total.saturating_add(snapshot.committed_events_total);

            for detail in &snapshot.realm_watermarks {
                let aggregate = realm_watermarks
                    .entry(detail.realm.clone())
                    .or_insert_with(|| {
                        StreamRealmWatermarkDetail::snapshot(&detail.realm, 0, 0, Vec::new())
                    });
                aggregate.resource_count = aggregate
                    .resource_count
                    .saturating_add(detail.resource_count);
                aggregate
                    .family_watermarks
                    .extend(detail.family_watermarks.iter().cloned());
            }

            for detail in &snapshot.area_watermarks {
                realm_areas
                    .entry(detail.realm.clone())
                    .or_default()
                    .insert(detail.area.clone());
                let aggregate = area_watermarks
                    .entry((detail.realm.clone(), detail.area.clone()))
                    .or_insert_with(|| {
                        StreamAreaWatermarkDetail::snapshot(
                            &detail.realm,
                            &detail.area,
                            0,
                            Vec::new(),
                        )
                    });
                aggregate.resource_count = aggregate
                    .resource_count
                    .saturating_add(detail.resource_count);
                aggregate
                    .family_watermarks
                    .extend(detail.family_watermarks.iter().cloned());
            }
        }

        for (realm, detail) in &mut realm_watermarks {
            detail.area_count = realm_areas.get(realm).map_or(0, BTreeSet::len);
        }

        let realm_watermarks = realm_watermarks.into_values().collect::<Vec<_>>();
        let area_watermarks = area_watermarks.into_values().collect::<Vec<_>>();
        self.durable_metrics.observe_snapshot(
            committed_events_total,
            &realm_watermarks,
            &area_watermarks,
        );
        self.read_model.replace_streams(streams);
        self.read_model
            .replace_stream_realm_watermarks(realm_watermarks);
        self.read_model
            .replace_stream_area_watermarks(area_watermarks);
        self.read_model
            .replace_stream_events_total(committed_events_total);
    }
}
