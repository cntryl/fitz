//! Admin snapshot and metrics upkeep for the queue domain core.

use super::model::{QueueFamilyState, QueueLiveCounts};
use crate::domains::queue::projection::{QueueProjectionEntry, QueueProjectionState};

impl QueueFamilyState {
    pub(super) fn mark_admin_snapshot_dirty(&mut self) {
        self.projection.mark_dirty(self.route_family);
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&mut self) {
        let counts = self.live_counts();
        if let Some(metrics) = &mut self.metrics {
            metrics.set_ready_messages(counts.ready);
            metrics.set_delayed_messages(counts.delayed);
            metrics.set_inflight_messages(counts.inflight);
        }
    }

    pub(super) fn observe_histogram_us(&mut self, name: &str, value_us: u64) {
        if let Some(metrics) = &mut self.metrics {
            metrics.histogram_observe_us(name, value_us);
        } else {
            crate::observability::histogram_observe_us(name, value_us);
        }
    }

    pub(super) fn refresh_admin_snapshot_if_dirty(&mut self) {
        self.sweep_idle_actors();
        let projection = self.projection.clone();
        projection.refresh_if_dirty(self.route_family, || self.collect_projection_state());
    }

    pub(super) fn collect_projection_state(&mut self) -> QueueProjectionState {
        let actors = &self.actors;
        let families = &self.families;
        let entries = actors
            .iter()
            .map(|(key, warm_actor)| {
                let ready_route = Self::queue_ready_route(key);
                let subscriptions_active = families.get(&key.family.as_u64()).map_or(0, |state| {
                    state.for_each_matching_route(key.family, ready_route.as_str(), |_| {})
                });
                QueueProjectionEntry {
                    key: key.clone(),
                    snapshot: warm_actor.actor.admin_snapshot(),
                    subscriptions_active,
                    inflight: warm_actor.actor.admin_inflight(),
                    dead_letters: warm_actor.actor.admin_dead_letters(),
                }
            })
            .collect();

        QueueProjectionState::from_entries(entries)
    }

    pub(super) fn live_counts(&mut self) -> QueueLiveCounts {
        let actors = &self.actors;
        let mut counts = QueueLiveCounts::default();

        for warm_actor in actors.values() {
            let actor_counts = warm_actor.actor.live_counts();
            counts.ready = counts.ready.saturating_add(actor_counts.ready);
            counts.delayed = counts.delayed.saturating_add(actor_counts.delayed);
            counts.inflight = counts.inflight.saturating_add(actor_counts.inflight);
            counts.dead_letters = counts
                .dead_letters
                .saturating_add(actor_counts.dead_letters);
        }
        counts.pending = counts.ready.saturating_add(counts.delayed);
        counts
    }
}
