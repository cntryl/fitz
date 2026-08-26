//! Admin read-model projection and metrics glue: when and how live Schedule
//! state is mirrored into the admin snapshot and metric gauges.
//!
//! Projection failure must never affect domain correctness.

use super::model::{now_epoch_ms, schedule_admin_snapshot_due, Ordering, ScheduleDomainRuntime, ScheduleLiveCounts, EXECUTIONS_WINDOW_MS};

impl ScheduleDomainRuntime<'_> {
    pub(super) fn subscription_count(&self) -> usize {
        let families = self.core.sub_families.lock();
        families
            .values()
            .map(super::model::ScheduleSubscriptionSet::subscription_count)
            .sum()
    }

    pub(super) fn schedule_count(&self) -> usize {
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(crate::domains::schedule::ScheduleActor::schedule_count)
            .sum()
    }

    pub(super) fn pending_fire_count(&self) -> usize {
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(crate::domains::schedule::ScheduleActor::pending_fire_count)
            .sum()
    }

    /// Legacy metric name: counts acknowledged live handoffs over the last minute.
    pub(super) fn executions_per_minute(&self) -> f64 {
        let now_ms = now_epoch_ms();
        let cutoff = now_ms.saturating_sub(EXECUTIONS_WINDOW_MS);
        let mut deque = self.core.recent_acknowledgement_ms.lock();
        while deque.front().copied().is_some_and(|t| t < cutoff) {
            deque.pop_front();
        }
        f64::from(u32::try_from(deque.len()).unwrap_or(u32::MAX))
    }

    pub(super) fn notify_failure_count(&self) -> u64 {
        self.core.live_publish_failures.load(Ordering::Relaxed)
    }

    pub(super) fn ack_failure_count(&self) -> u64 {
        self.core.ack_failures.load(Ordering::Relaxed)
    }

    pub(super) fn pending_ack_retry_count(&self) -> usize {
        let pending_ack_retries = self.core.pending_ack_retries.lock();
        pending_ack_retries
            .values()
            .map(std::collections::HashMap::len)
            .sum()
    }

    pub(super) fn admin_pending_claims(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Vec<crate::control::admin::SchedulePendingClaimInfo> {
        let actors = self.core.actors.lock();
        actors
            .get(&route_family)
            .map(crate::domains::schedule::ScheduleActor::admin_pending_claims)
            .unwrap_or_default()
    }

    pub(super) fn oldest_pending_claim_age_seconds(&self) -> u64 {
        let now_ms = now_epoch_ms();
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(|actor| actor.oldest_pending_claim_age_seconds(now_ms))
            .max()
            .unwrap_or(0)
    }

    pub(super) fn overdue_normalization_count(&self) -> u64 {
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(crate::domains::schedule::ScheduleActor::overdue_normalization_count)
            .sum()
    }

    pub(super) fn live_counts(&self) -> ScheduleLiveCounts {
        ScheduleLiveCounts {
            subscriptions: self.subscription_count(),
            schedules: self.schedule_count(),
            pending_fires: self.pending_fire_count(),
            executions_per_minute: self.executions_per_minute(),
            notify_failures: self.notify_failure_count(),
            ack_failures: self.ack_failure_count(),
            pending_ack_retries: self.pending_ack_retry_count(),
            oldest_pending_claim_age_seconds: self.oldest_pending_claim_age_seconds(),
            overdue_normalizations: self.overdue_normalization_count(),
        }
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(super) fn sync_admin_snapshot(&self) {
        let snapshot = {
            let actors = self.core.actors.lock();
            let mut schedules = Vec::new();
            for actor in actors.values() {
                schedules.extend(actor.admin_snapshot());
            }
            schedules
        };

        self.core.admin_read_model.replace_schedules(snapshot);
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.core.metrics {
            metrics.set_schedule_count(self.schedule_count());
            metrics.set_pending_fire_count(self.pending_fire_count());
        }
    }

    pub(super) fn schedule_response_is_failure(
        response: &crate::domains::schedule::ScheduleResponse,
    ) -> bool {
        matches!(
            response,
            crate::domains::schedule::ScheduleResponse::Error(_)
        )
    }

    pub(super) fn schedule_admin_snapshot(&self, force: bool) {
        self.core.snapshot_dirty.store(true, Ordering::Relaxed);
        self.maybe_sync_admin_snapshot(force);
    }

    pub(super) fn maybe_sync_admin_snapshot(&self, force: bool) {
        #[cfg(feature = "bench-no-snapshot")]
        if !force {
            return;
        }

        let now_elapsed_us =
            u64::try_from(self.core.snapshot_epoch.elapsed().as_micros()).unwrap_or(u64::MAX);
        let last_snapshot_elapsed_us = self.core.last_snapshot_elapsed_us.load(Ordering::Relaxed);
        let snapshot_dirty = self.core.snapshot_dirty.load(Ordering::Relaxed);

        if !schedule_admin_snapshot_due(
            snapshot_dirty,
            force,
            now_elapsed_us,
            last_snapshot_elapsed_us,
        ) {
            return;
        }

        if self
            .core
            .snapshot_syncing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        if !self.core.snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.core.snapshot_syncing.store(false, Ordering::Release);
            return;
        }

        self.sync_admin_snapshot();
        self.core.last_snapshot_elapsed_us.store(
            u64::try_from(self.core.snapshot_epoch.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.core.snapshot_syncing.store(false, Ordering::Release);
    }

    pub(super) fn refresh_admin_snapshot_if_dirty(&self) {
        self.maybe_sync_admin_snapshot(true);
    }

    #[doc(hidden)]
    pub(super) fn bench_publish_event(&self, event: &crate::runtime::DomainPublishEvent) {
        self.handle_domain_publish(event);
    }
}
