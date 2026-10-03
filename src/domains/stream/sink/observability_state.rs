//! Family-owned Stream admin projection and metric publishers.

use super::live_gauges::StreamLiveGaugeCoordinator;
use super::model::StreamLiveCounts;
use super::projection::{StreamAdminProjection, StreamFamilyAdminSnapshot};
use crate::domains::stream::metrics::StreamDurableMetrics;
use crate::runtime::routing::RouteFamily;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(super) struct StreamObservability {
    projection: Arc<StreamAdminProjection>,
    dirty: Arc<AtomicBool>,
    live_gauges: Arc<StreamLiveGaugeCoordinator>,
    durable_metrics: Arc<StreamDurableMetrics>,
    committed_snapshot: Mutex<Option<super::projection::StreamCommittedAdminSnapshot>>,
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
}
