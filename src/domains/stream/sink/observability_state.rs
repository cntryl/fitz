//! Family-owned Stream admin projection and metric publishers.

use super::live_gauges::StreamLiveGaugeCoordinator;
use super::model::StreamLiveCounts;
use super::projection::{StreamAdminProjection, StreamFamilyAdminSnapshot};
use crate::domains::stream::metrics::StreamDurableMetrics;
use crate::runtime::routing::RouteFamily;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

pub(super) struct StreamObservability {
    projection: Arc<StreamAdminProjection>,
    dirty: Arc<AtomicBool>,
    live_gauges: Arc<StreamLiveGaugeCoordinator>,
    durable_metrics: Arc<StreamDurableMetrics>,
    committed_snapshot: Mutex<Option<super::projection::StreamCommittedAdminSnapshot>>,
    observed_drop_revision: AtomicU64,
    drop_watermarks_dirty: AtomicBool,
}

impl StreamObservability {
    pub(super) fn new(
        projection: Arc<StreamAdminProjection>,
        dirty: Arc<AtomicBool>,
        live_gauges: Arc<StreamLiveGaugeCoordinator>,
        durable_metrics: Arc<StreamDurableMetrics>,
    ) -> Self {
        Self {
            projection,
            dirty,
            live_gauges,
            durable_metrics,
            committed_snapshot: Mutex::new(None),
            observed_drop_revision: AtomicU64::new(0),
            drop_watermarks_dirty: AtomicBool::new(false),
        }
    }

    pub(super) fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    pub(super) fn mark_committed_dirty(&self) {
        *self.committed_snapshot.lock() = None;
        self.mark_dirty();
    }

    pub(super) fn committed_snapshot(
        &self,
    ) -> Option<super::projection::StreamCommittedAdminSnapshot> {
        self.committed_snapshot.lock().clone()
    }

    pub(super) fn publish_committed_snapshot(
        &self,
        snapshot: super::projection::StreamCommittedAdminSnapshot,
    ) {
        *self.committed_snapshot.lock() = Some(snapshot);
    }

    pub(super) fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Acquire)
    }

    pub(super) fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Release);
    }

    pub(super) fn publish_family(&self, family: RouteFamily, counts: StreamLiveCounts) {
        self.live_gauges.publish_family(family, counts);
    }

    pub(super) fn publish_admin(&self, family: u64, snapshot: StreamFamilyAdminSnapshot) {
        self.projection.publish(family, snapshot);
    }

    pub(super) fn record_events(&self, count: usize) {
        self.durable_metrics.record_events(count);
    }

    pub(super) fn durable_metrics(&self) -> Arc<StreamDurableMetrics> {
        self.durable_metrics.clone()
    }

    pub(super) fn watermark_generation(&self) -> u64 {
        self.durable_metrics.watermark_generation()
    }

    pub(super) fn observe_drop_revision(&self, revision: u64) -> bool {
        if self
            .observed_drop_revision
            .swap(revision, Ordering::Relaxed)
            == revision
        {
            return false;
        }
        self.drop_watermarks_dirty.store(true, Ordering::Relaxed);
        true
    }

    pub(super) fn drop_watermarks_dirty(&self) -> bool {
        self.drop_watermarks_dirty.load(Ordering::Relaxed)
    }

    pub(super) fn forget_drop_watermarks(&self, scope: &crate::domains::stream::StreamDropScope) {
        self.projection.forget_drop_watermarks(scope);
    }

    pub(super) fn publish_reconciled_watermarks(
        &self,
        family: u64,
        realms: Vec<crate::domains::stream::metrics::StreamRealmWatermarkMetric>,
        areas: Vec<crate::domains::stream::metrics::StreamAreaWatermarkMetric>,
    ) {
        self.projection
            .replace_family_watermarks(family, realms, areas);
        self.drop_watermarks_dirty.store(false, Ordering::Relaxed);
    }
}
