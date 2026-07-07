use super::super::model::{
    now_epoch_ms, schedule_admin_snapshot_due, HashSet, Ordering, ScheduleDomainRuntime,
    ScheduleLiveCounts, SCHEDULE_PENDING_CLAIM_CLEANUP_INTERVAL_MS,
};

impl ScheduleDomainRuntime<'_> {
    pub fn subscription_count(&self) -> usize {
        let families = self.core.sub_families.lock();
        families
            .values()
            .map(super::super::model::ScheduleSubscriptionSet::subscription_count)
            .sum()
    }

    pub fn schedule_count(&self) -> usize {
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(crate::domains::schedule::ScheduleActor::schedule_count)
            .sum()
    }

    pub fn pending_fire_count(&self) -> usize {
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(crate::domains::schedule::ScheduleActor::pending_fire_count)
            .sum()
    }

    /// Legacy metric name: counts acknowledged live handoffs over the last minute.
    pub fn executions_per_minute(&self) -> f64 {
        let now_ms = now_epoch_ms();
        let cutoff = now_ms.saturating_sub(60_000);
        let mut deque = self.core.recent_acknowledgement_ms.lock();
        while deque.front().copied().is_some_and(|t| t < cutoff) {
            deque.pop_front();
        }
        f64::from(u32::try_from(deque.len()).unwrap_or(u32::MAX))
    }

    pub fn notify_failure_count(&self) -> u64 {
        self.core.live_publish_failures.load(Ordering::Relaxed)
    }

    pub fn ack_failure_count(&self) -> u64 {
        self.core.ack_failures.load(Ordering::Relaxed)
    }

    pub fn pending_ack_retry_count(&self) -> usize {
        let pending_ack_retries = self.core.pending_ack_retries.lock();
        pending_ack_retries.values().map(HashSet::len).sum()
    }

    pub fn admin_pending_claims(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Vec<crate::control::admin::SchedulePendingClaimInfo> {
        let actors = self.core.actors.lock();
        actors
            .get(&route_family)
            .map(crate::domains::schedule::ScheduleActor::admin_pending_claims)
            .unwrap_or_default()
    }

    pub fn oldest_pending_claim_age_seconds(&self) -> u64 {
        let now_ms = now_epoch_ms();
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(|actor| actor.oldest_pending_claim_age_seconds(now_ms))
            .max()
            .unwrap_or(0)
    }

    pub fn overdue_normalization_count(&self) -> u64 {
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(crate::domains::schedule::ScheduleActor::overdue_normalization_count)
            .sum()
    }

    pub(in crate::domains::schedule::sink) fn live_counts(&self) -> ScheduleLiveCounts {
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

    pub(super) fn pending_claim_cleanup_due(&self) -> bool {
        let now_elapsed_ms =
            u64::try_from(self.core.snapshot_epoch.elapsed().as_millis()).unwrap_or(u64::MAX);
        let mut last_elapsed_ms = self
            .core
            .last_pending_claim_cleanup_elapsed_ms
            .load(Ordering::Relaxed);

        loop {
            if last_elapsed_ms == 0 {
                let first_elapsed_ms = now_elapsed_ms.max(1);
                match self
                    .core
                    .last_pending_claim_cleanup_elapsed_ms
                    .compare_exchange(0, first_elapsed_ms, Ordering::AcqRel, Ordering::Relaxed)
                {
                    Ok(_) => return true,
                    Err(observed) => {
                        last_elapsed_ms = observed;
                        continue;
                    }
                }
            }

            if now_elapsed_ms.saturating_sub(last_elapsed_ms)
                < SCHEDULE_PENDING_CLAIM_CLEANUP_INTERVAL_MS
            {
                return false;
            }

            match self
                .core
                .last_pending_claim_cleanup_elapsed_ms
                .compare_exchange(
                    last_elapsed_ms,
                    now_elapsed_ms,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                Ok(_) => return true,
                Err(observed) => last_elapsed_ms = observed,
            }
        }
    }

    pub(in crate::domains::schedule::sink) fn schedule_response_is_failure(
        response: &crate::domains::schedule::ScheduleResponse,
    ) -> bool {
        matches!(
            response,
            crate::domains::schedule::ScheduleResponse::Error(_)
        )
    }

    pub(in crate::domains::schedule::sink) fn schedule_admin_snapshot(&self, force: bool) {
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

    pub(crate) fn refresh_admin_snapshot_if_dirty(&self) {
        self.maybe_sync_admin_snapshot(true);
    }

    #[doc(hidden)]
    pub fn bench_publish_event(&self, event: &crate::runtime::DomainPublishEvent) {
        self.handle_domain_publish(event);
    }
}
