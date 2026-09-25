//! Per-queue warm actor lifecycle: lookup, idle sweep, fast flush, dead-letter ops.

use super::model::{QueueFamilyState, QUEUE_ACTOR_IDLE_TTL, QUEUE_IDLE_SWEEP_BATCH_SIZE};
use std::collections::HashSet;
use std::time::Instant;

impl QueueFamilyState {
    pub(super) fn queue_key_for_route(
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
    ) -> Result<crate::domains::queue::QueueKey, crate::domains::queue::QueueResponse> {
        crate::domains::queue::QueueKey::from_route(family_id, route).ok_or_else(|| {
            crate::domains::queue::QueueResponse::BadRequest {
                reason: format!("invalid queue route: {}", route.as_str()),
            }
        })
    }

    #[cfg(test)]
    pub(super) fn session_inbox_address(
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
    ) -> crate::runtime::routing::RouteAddress {
        crate::runtime::routing::RouteAddress::new(
            family_id,
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        )
    }

    pub(super) fn matching_queue_keys(
        &mut self,
        family: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
    ) -> Vec<crate::domains::queue::QueueKey> {
        self.reservation_book.matching_keys(family, pattern)
    }

    pub(super) fn matching_queue_key_count(
        &mut self,
        family: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
    ) -> usize {
        self.reservation_book.matching_key_count(family, pattern)
    }

    pub(super) fn inventory_existing_queue_keys(
        store: &crate::domains::queue::actor::recovery_store::QueueStore,
    ) -> Result<HashSet<crate::domains::queue::QueueKey>, String> {
        let families = store
            .family_ids()
            .map_err(|error| format!("list queue inventory families failed: {error:?}"))?;
        let mut known_queue_keys = HashSet::new();

        for family in families {
            if family == 0 {
                continue;
            }
            let route_family = crate::runtime::routing::RouteFamily::new(family);
            let txn = store
                .begin(
                    family,
                    crate::domains::queue::actor::recovery_store::QueueTransactionMode::ReadOnly,
                )
                .map_err(|error| {
                    format!("queue inventory transaction failed: family={family} error={error:?}")
                })?;
            let rows = txn.scan_all().map_err(|error| {
                format!("queue inventory scan failed: family={family} error={error:?}")
            })?;

            for (key, value) in rows {
                drop(value);
                if let Some(queue_key) =
                    crate::domains::queue::QueueActor::queue_key_from_authoritative_storage_key(
                        route_family,
                        &key,
                    )
                {
                    known_queue_keys.insert(queue_key);
                }
            }
        }

        Ok(known_queue_keys)
    }

    pub(super) fn record_ready_state(
        &mut self,
        key: &crate::domains::queue::QueueKey,
        counts: crate::domains::queue::QueueActorLiveCounts,
    ) -> Option<super::model::QueueReadyNotification> {
        let is_ready = counts.ready > 0;
        let ready_states = &mut self.ready_states;
        let was_ready = ready_states.get(key).copied().unwrap_or(false);

        if counts.total() == 0 {
            ready_states.remove(key);
        } else {
            ready_states.insert(key.clone(), is_ready);
        }

        if !was_ready && is_ready {
            Some(super::model::QueueReadyNotification {
                family_id: key.family,
                counts,
            })
        } else {
            None
        }
    }

    pub(super) fn sweep_runtime_state_at(&mut self, now: Instant) {
        #[cfg(test)]
        if self
            .panic_next_runtime_sweep
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            panic!("test Queue runtime sweep panic");
        }
        self.expire_pending_reserves_at(now);
        self.sweep_idle_actors_at(now);
        self.maybe_cleanup_dedup_at(now);
        self.maybe_flush_dirty_fast_families_at(now);
    }

    pub(super) fn fast_flush_enabled(&mut self) -> bool {
        self.queue_write_policy == crate::domains::WritePolicy::BestEffort
            && self.maintenance_clock.fast_flush_enabled()
    }

    pub(super) fn mark_fast_flush_dirty(
        &mut self,
        family_id: crate::runtime::routing::RouteFamily,
    ) {
        if self.fast_flush_enabled() {
            self.dirty_fast_flush_families.insert(family_id.id());
        }
    }

    pub(super) fn maybe_flush_dirty_fast_families_at(&mut self, now: Instant) {
        if self.queue_write_policy != crate::domains::WritePolicy::BestEffort {
            return;
        }
        if self.maintenance_clock.fast_flush_due(now) {
            self.flush_dirty_fast_families();
        }
    }

    pub(super) fn flush_dirty_fast_families(&mut self) {
        let dirty_family_ids = {
            let dirty = &mut self.dirty_fast_flush_families;
            dirty.drain().collect::<Vec<_>>()
        };
        if dirty_family_ids.is_empty() {
            return;
        }

        let mut retry_family_ids = Vec::new();
        for family_id in dirty_family_ids {
            match self.store.flush_family(family_id) {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!(
                        domain = "queue",
                        family = family_id,
                        "Queue fast flush skipped missing column family"
                    );
                    retry_family_ids.push(family_id);
                }
                Err(error) => {
                    tracing::warn!(
                        domain = "queue",
                        family = family_id,
                        error = ?error,
                        "Queue fast flush failed"
                    );
                    retry_family_ids.push(family_id);
                }
            }
        }

        if !retry_family_ids.is_empty() {
            self.dirty_fast_flush_families.extend(retry_family_ids);
        }
    }

    pub(super) fn maybe_cleanup_dedup_at(&mut self, now: Instant) {
        if self.maintenance_clock.dedup_sweep_due(now) {
            self.dedup_store.cleanup();
        }
    }

    pub(super) fn with_actor<R, F>(
        &mut self,
        key: &crate::domains::queue::QueueKey,
        operation: F,
    ) -> Result<(R, bool), String>
    where
        F: FnOnce(&mut crate::domains::queue::QueueActor) -> R,
    {
        self.actor_registry.with_actor(
            key,
            &self.store,
            &self.dedup_store,
            self.queue_write_policy,
            operation,
        )
    }

    pub(super) fn maybe_sweep_idle_actors(&mut self) {
        let now = Instant::now();

        if !self.maintenance_clock.idle_sweep_due(now) {
            return;
        }

        self.sweep_idle_actors_at(now);
    }

    pub(super) fn sweep_idle_actors_at(&mut self, now: Instant) {
        let mut changed = false;
        let mut notifications = Vec::new();
        let mut removed_keys = Vec::new();
        let mut empty_removed_keys = Vec::new();
        let mut dirty_families = HashSet::new();
        let sweep_keys = self
            .actor_registry
            .take_idle_sweep_batch(QUEUE_IDLE_SWEEP_BATCH_SIZE);

        for key in sweep_keys {
            let (last_used, counts, due_work_changed) = {
                let Some(warm_actor) = self.actor_registry.get_mut(&key) else {
                    continue;
                };
                let last_used = warm_actor.last_used;
                let due_work_changed = warm_actor.actor.process_due_work();
                let counts = warm_actor.actor.live_counts();
                (last_used, counts, due_work_changed)
            };
            if due_work_changed {
                changed = true;
                dirty_families.insert(key.family);
            }

            if let Some(notification) = self.record_ready_state(&key, counts) {
                notifications.push((key.clone(), notification));
            }

            let idle_for = now.saturating_duration_since(last_used);
            let should_keep =
                idle_for < QUEUE_ACTOR_IDLE_TTL || counts.delayed > 0 || counts.inflight > 0;
            if should_keep {
                self.actor_registry.requeue_idle_key(key);
                continue;
            }

            let removed = self.actor_registry.remove(&key).is_some();
            if removed {
                changed = true;
                removed_keys.push(key.clone());
                if counts.total() == 0 {
                    empty_removed_keys.push(key);
                }
            } else {
                self.actor_registry.requeue_idle_key(key);
            }
        }

        if !removed_keys.is_empty() {
            let ready_states = &mut self.ready_states;
            for key in removed_keys {
                ready_states.remove(&key);
            }
        }
        if !empty_removed_keys.is_empty() {
            for key in empty_removed_keys {
                self.reservation_book.remove_key(&key);
            }
        }
        for family in dirty_families {
            self.mark_fast_flush_dirty(family);
        }
        if changed {
            self.mark_admin_snapshot_dirty();
        }
        for (key, notification) in notifications {
            self.notify_queue_ready_and_wake_reserves(&key, Some(notification), now);
        }
    }

    /// Replays a dead-lettered message back into its queue.
    ///
    /// # Errors
    ///
    /// Returns an error when the warm queue actor cannot be recovered or the replay fails.
    pub(super) fn replay_dead_letter(
        &mut self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        let ((result, counts), created_actor) = self.with_actor(key, |actor| {
            let result = actor.replay_dead_letter(id);
            (result, actor.live_counts())
        })?;

        if matches!(result, Ok(true)) {
            self.mark_fast_flush_dirty(key.family);
            let notification = self.record_ready_state(key, counts);
            self.mark_admin_snapshot_dirty();
            self.notify_queue_ready_and_wake_reserves(key, notification, Instant::now());
        }

        if created_actor && counts.total() == 0 {
            self.actor_registry.remove(key);
            self.ready_states.remove(key);
            self.reservation_book.remove_key(key);
            self.mark_admin_snapshot_dirty();
        }

        result
    }

    /// Permanently removes a dead-lettered message from its queue.
    ///
    /// # Errors
    ///
    /// Returns an error when the warm queue actor cannot be recovered or the purge fails.
    pub(super) fn purge_dead_letter(
        &mut self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        let ((result, counts), created_actor) = self.with_actor(key, |actor| {
            let result = actor.purge_dead_letter(id);
            (result, actor.live_counts())
        })?;

        if matches!(result, Ok(true)) {
            self.mark_fast_flush_dirty(key.family);
            self.mark_admin_snapshot_dirty();
        }

        if created_actor && counts.total() == 0 {
            self.actor_registry.remove(key);
            self.ready_states.remove(key);
            self.reservation_book.remove_key(key);
            self.mark_admin_snapshot_dirty();
        }

        result
    }
}
