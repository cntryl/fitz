//! Admin read-model projection and metrics glue: when and how live Stream
//! state is mirrored into the admin snapshot and metric gauges.
//!
//! Projection failure must never affect domain correctness.

use super::model::{
    u64_to_usize_saturating, AdminStreamReadRequest, StreamAreaSnapshot, StreamFamilyState,
    StreamLiveCounts, StreamRealmSnapshot,
};
use crate::domains::stream::{StreamClientResponseBody, StreamReadItem};
use std::collections::BTreeMap;

type StreamAdminSnapshotMap =
    BTreeMap<(u64, String, String, String), crate::control::admin::StreamInfo>;
type StreamRealmSnapshotMap = BTreeMap<String, StreamRealmSnapshot>;
type StreamAreaSnapshotMap = BTreeMap<(String, String), StreamAreaSnapshot>;

impl StreamFamilyState {
    pub(in crate::domains::stream::sink) fn mark_admin_snapshot_dirty(&mut self) {
        self.admin_snapshot.mark_dirty();
        self.refresh_metrics_gauges();
    }

    pub(in crate::domains::stream::sink) fn refresh_metrics_gauges(&mut self) {
        let counts = self.live_counts();

        if let Some(metrics) = &mut self.metrics {
            metrics.set_stream_count(counts.streams);
            metrics.set_subscription_count(counts.subscriptions);
            metrics.set_append_session_count(counts.append_sessions);
        } else {
            crate::observability::gauge_set("fitz_stream_active_gauge", counts.streams as u64);
            crate::observability::gauge_set(
                "fitz_stream_subscriptions_gauge",
                counts.subscriptions as u64,
            );
            crate::observability::gauge_set(
                "fitz_stream_append_sessions_active",
                counts.append_sessions as u64,
            );
        }
    }

    pub(in crate::domains::stream::sink) fn counter_inc(&mut self, name: &str) {
        if let Some(metrics) = &mut self.metrics {
            metrics.counter_inc(name);
        } else {
            crate::observability::counter_inc(name);
        }
    }

    pub(in crate::domains::stream::sink) fn counter_add(&mut self, name: &str, amount: u64) {
        if let Some(metrics) = &mut self.metrics {
            metrics.counter_add(name, amount);
        } else {
            crate::observability::counter_add(name, amount);
        }
    }

    pub(in crate::domains::stream::sink) fn stream_response_is_failure(
        response: &StreamClientResponseBody,
    ) -> bool {
        matches!(
            response,
            StreamClientResponseBody::Error(_) | StreamClientResponseBody::SubscriptionError(_)
        )
    }

    pub(in crate::domains::stream::sink) fn refresh_admin_snapshot_if_dirty(&mut self) {
        if self.admin_snapshot.take_dirty() {
            self.sync_admin_snapshot();
        }
    }

    /// # Errors
    ///
    /// Returns an error if the requested route cannot be read or if the stream
    /// store rejects the read parameters.
    pub(in crate::domains::stream::sink) fn admin_read_resource_records(
        &mut self,
        request: AdminStreamReadRequest<'_>,
    ) -> Result<
        (
            Vec<StreamReadItem>,
            crate::domains::stream::protocol::ReadCursor,
        ),
        String,
    > {
        let filter =
            request
                .discriminator
                .map(|value| crate::domains::stream::protocol::StreamFilterSet {
                    clauses: vec![
                        crate::domains::stream::protocol::StreamFilterClause::Equals(value),
                    ],
                });
        let params = crate::domains::stream::store::ReadResourceParams {
            family: request.family.as_u64(),
            realm: request.realm,
            area: request.area,
            resource: request.resource,
            from_offset: request.from_offset,
            limit: request.limit,
            max_bytes: None,
        };

        self.stream_store
            .read_resource_with_filter(&params, filter.as_ref())
    }

    pub(in crate::domains::stream::sink) fn sync_admin_snapshot(&mut self) {
        if let Err(error) = self.try_sync_admin_snapshot() {
            self.admin_snapshot.mark_dirty();
            self.counter_inc(
                crate::domains::stream::metrics::METRIC_ADMIN_PROJECTION_FAILURES_TOTAL,
            );
            tracing::warn!(
                domain = "stream",
                error,
                "Stream admin projection refresh failed; retaining prior snapshot"
            );
        }
    }

    fn try_sync_admin_snapshot(&mut self) -> Result<(), String> {
        let (mut streams, realm_snapshots, area_snapshots, committed_events_total) =
            self.collect_committed_stream_snapshots()?;
        let stream_realm_watermarks = self.collect_stream_realm_watermarks(realm_snapshots)?;
        let stream_area_watermarks = self.collect_stream_area_watermarks(area_snapshots)?;
        self.overlay_live_actor_snapshots_from(&mut streams);
        self.publish_admin_snapshot(
            streams,
            stream_realm_watermarks,
            stream_area_watermarks,
            committed_events_total,
        );
        Ok(())
    }

    fn collect_committed_stream_snapshots(
        &mut self,
    ) -> Result<
        (
            StreamAdminSnapshotMap,
            StreamRealmSnapshotMap,
            StreamAreaSnapshotMap,
            usize,
        ),
        String,
    > {
        let mut streams: StreamAdminSnapshotMap = BTreeMap::new();
        let mut realm_snapshots: StreamRealmSnapshotMap = BTreeMap::new();
        let mut area_snapshots: StreamAreaSnapshotMap = BTreeMap::new();
        let mut committed_events_total = 0usize;

        let families = self.stream_store.column_family_ids()?;
        for family_id in families {
            let records = self.stream_store.list_resource_metadata(family_id)?;
            for crate::domains::stream::store::StreamAdminRecord {
                realm,
                area,
                resource,
                next_offset,
                committed_size_bytes,
            } in records
            {
                committed_events_total =
                    committed_events_total.saturating_add(u64_to_usize_saturating(next_offset));
                let last_offset = next_offset.saturating_sub(1);
                streams.insert(
                    (family_id, realm.clone(), area.clone(), resource.clone()),
                    crate::control::admin::StreamInfo::snapshot(
                        crate::control::admin::StreamInfoSnapshot {
                            route_family: family_id,
                            realm: &realm,
                            area: &area,
                            resource: &resource,
                            offset: last_offset,
                            watermark: last_offset,
                            size_bytes: committed_size_bytes,
                            sessions_active: 0,
                        },
                    ),
                );

                let realm_snapshot = realm_snapshots.entry(realm.clone()).or_default();
                realm_snapshot.areas.insert(area.clone());
                realm_snapshot.resource_count = realm_snapshot.resource_count.saturating_add(1);
                realm_snapshot.families.insert(family_id);

                let area_snapshot = area_snapshots
                    .entry((realm.clone(), area.clone()))
                    .or_default();
                area_snapshot.resource_count = area_snapshot.resource_count.saturating_add(1);
                area_snapshot.families.insert(family_id);
            }
        }

        Ok((
            streams,
            realm_snapshots,
            area_snapshots,
            committed_events_total,
        ))
    }

    fn collect_stream_realm_watermarks(
        &mut self,
        realm_snapshots: StreamRealmSnapshotMap,
    ) -> Result<Vec<crate::control::admin::StreamRealmWatermarkDetail>, String> {
        realm_snapshots
            .into_iter()
            .map(|(realm, snapshot)| {
                let family_watermarks = snapshot
                    .families
                    .into_iter()
                    .map(|family_id| {
                        self.stream_store
                            .get_realm_watermark(family_id, &realm)
                            .map(|watermark| {
                                crate::control::admin::StreamRealmWatermark::snapshot(
                                    family_id, watermark,
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(crate::control::admin::StreamRealmWatermarkDetail::snapshot(
                    &realm,
                    snapshot.areas.len(),
                    snapshot.resource_count,
                    family_watermarks,
                ))
            })
            .collect()
    }

    fn collect_stream_area_watermarks(
        &mut self,
        area_snapshots: StreamAreaSnapshotMap,
    ) -> Result<Vec<crate::control::admin::StreamAreaWatermarkDetail>, String> {
        area_snapshots
            .into_iter()
            .map(|((realm, area), snapshot)| {
                let family_watermarks = snapshot
                    .families
                    .into_iter()
                    .map(|family_id| {
                        self.stream_store
                            .get_watermark(family_id, &realm, &area)
                            .map(|watermark| {
                                crate::control::admin::StreamAreaWatermark::snapshot(
                                    family_id, watermark,
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(crate::control::admin::StreamAreaWatermarkDetail::snapshot(
                    &realm,
                    &area,
                    snapshot.resource_count,
                    family_watermarks,
                ))
            })
            .collect()
    }

    fn overlay_live_actor_snapshots_from(&mut self, streams: &mut StreamAdminSnapshotMap) {
        for (key, actor) in &mut self.actors {
            let last_offset = actor
                .metadata()
                .ok()
                .and_then(|response| response.metadata.last_resource_offset);
            let sessions_active = usize::from(actor.has_active_session());
            let stream_key = (
                key.family.as_u64(),
                key.realm.clone(),
                key.area.clone(),
                key.resource.clone(),
            );
            let committed_snapshot = streams.get(&stream_key);
            if committed_snapshot.is_none() && last_offset.is_none() {
                continue;
            }
            let committed_size_bytes = committed_snapshot.map_or(0, |item| item.size_bytes);
            let committed_offset = committed_snapshot.map(|item| item.offset);
            let visible_offset = last_offset.or(committed_offset).unwrap_or(0);

            streams.insert(
                stream_key,
                crate::control::admin::StreamInfo::snapshot(
                    crate::control::admin::StreamInfoSnapshot {
                        route_family: key.family.as_u64(),
                        realm: &key.realm,
                        area: &key.area,
                        resource: &key.resource,
                        offset: visible_offset,
                        watermark: visible_offset,
                        size_bytes: committed_size_bytes,
                        sessions_active,
                    },
                ),
            );
        }
    }

    fn publish_admin_snapshot(
        &mut self,
        streams: StreamAdminSnapshotMap,
        stream_realm_watermarks: Vec<crate::control::admin::StreamRealmWatermarkDetail>,
        stream_area_watermarks: Vec<crate::control::admin::StreamAreaWatermarkDetail>,
        committed_events_total: usize,
    ) {
        self.durable_metrics.observe_snapshot(
            committed_events_total,
            &stream_realm_watermarks,
            &stream_area_watermarks,
        );
        self.admin_snapshot
            .read_model
            .replace_streams(streams.into_values().collect());
        self.admin_snapshot
            .read_model
            .replace_stream_realm_watermarks(stream_realm_watermarks);
        self.admin_snapshot
            .read_model
            .replace_stream_area_watermarks(stream_area_watermarks);
        self.admin_snapshot
            .read_model
            .replace_stream_events_total(committed_events_total);
    }

    pub(in crate::domains::stream::sink) fn live_counts(&mut self) -> StreamLiveCounts {
        let subscriptions = self
            .subscriptions
            .families
            .values()
            .map(crate::domains::subscription_state::RoutedSubscriptionSet::subscription_count)
            .sum();

        StreamLiveCounts {
            streams: self.actors.len(),
            append_sessions: self.session_owners.len(),
            subscriptions,
        }
    }
}
