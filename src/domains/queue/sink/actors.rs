//! Per-queue warm actor lifecycle: lookup, idle sweep, fast flush, dead-letter ops.

use super::model::{
    QueueFamilyState, WarmQueueActor, QUEUE_ACTOR_IDLE_TTL, QUEUE_DEDUP_SWEEP_INTERVAL,
    QUEUE_IDLE_SWEEP_BATCH_SIZE, QUEUE_IDLE_SWEEP_INTERVAL,
};
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
        let mut keys = self
            .known_queue_keys
            .iter()
            .filter(|key| key.family == family)
            .filter(|key| pattern.matches(&Self::queue_ready_route(key)))
            .cloned()
            .collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            (&left.realm, &left.area, &left.resource).cmp(&(
                &right.realm,
                &right.area,
                &right.resource,
            ))
        });
        keys
    }

    pub(super) fn matching_queue_key_count(
        &mut self,
        family: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
    ) -> usize {
        self.known_queue_keys
            .iter()
            .filter(|key| key.family == family)
            .filter(|key| pattern.matches(&Self::queue_ready_route(key)))
            .count()
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
            && self.fast_flush_interval.is_some()
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
        let Some(interval) = self.fast_flush_interval else {
            return;
        };
        if self.queue_write_policy != crate::domains::WritePolicy::BestEffort {
            return;
        }

        let should_flush = {
            let next_fast_flush_at = &mut self.next_fast_flush_at;
            if now < *next_fast_flush_at {
                false
            } else {
                *next_fast_flush_at = now + interval;
                true
            }
        };

        if should_flush {
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
        let should_cleanup = {
            let next_dedup_sweep_at = &mut self.next_dedup_sweep_at;
            if now < *next_dedup_sweep_at {
                false
            } else {
                *next_dedup_sweep_at = now + QUEUE_DEDUP_SWEEP_INTERVAL;
                true
            }
        };

        if should_cleanup {
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
        use std::collections::hash_map::Entry;

        let now = Instant::now();
        match self.actors.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().last_used = now;
                Ok((operation(&mut entry.get_mut().actor), false))
            }
            Entry::Vacant(entry) => {
                let actor = crate::domains::queue::QueueActor::try_new_with_write_policy(
                    key.family,
                    key.clone(),
                    self.store.clone(),
                    None,
                    self.dedup_store.clone(),
                    self.queue_write_policy,
                )?;
                let warm_actor = entry.insert(WarmQueueActor {
                    actor,
                    last_used: now,
                });
                self.idle_sweep_keys.push_back(key.clone());
                Ok((operation(&mut warm_actor.actor), true))
            }
        }
    }

    pub(super) fn sweep_idle_actors(&mut self) {
        self.sweep_idle_actors_at(Instant::now());
    }

    pub(super) fn maybe_sweep_idle_actors(&mut self) {
        let now = Instant::now();

        {
            let next_idle_sweep_at = &mut self.next_idle_sweep_at;
            if now < *next_idle_sweep_at {
                return;
            }
            *next_idle_sweep_at = now + QUEUE_IDLE_SWEEP_INTERVAL;
        }

        self.sweep_idle_actors_at(now);
    }

    pub(super) fn sweep_idle_actors_at(&mut self, now: Instant) {
        let mut changed = false;
        let mut notifications = Vec::new();
        let mut removed_keys = Vec::new();
        let mut empty_removed_keys = Vec::new();
        let mut dirty_families = HashSet::new();
        let sweep_keys = {
            let idle_sweep_keys = &mut self.idle_sweep_keys;
            let count = idle_sweep_keys.len().min(QUEUE_IDLE_SWEEP_BATCH_SIZE);
            idle_sweep_keys.drain(..count).collect::<Vec<_>>()
        };

        for key in sweep_keys {
            let (last_used, counts, due_work_changed) = {
                let Some(warm_actor) = self.actors.get_mut(&key) else {
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
                self.idle_sweep_keys.push_back(key);
                continue;
            }

            let removed = self.actors.remove(&key).is_some();
            if removed {
                changed = true;
                removed_keys.push(key.clone());
                if counts.total() == 0 {
                    empty_removed_keys.push(key);
                }
            } else {
                self.idle_sweep_keys.push_back(key);
            }
        }

        if !removed_keys.is_empty() {
            let ready_states = &mut self.ready_states;
            for key in removed_keys {
                ready_states.remove(&key);
            }
        }
        if !empty_removed_keys.is_empty() {
            let known_queue_keys = &mut self.known_queue_keys;
            for key in empty_removed_keys {
                known_queue_keys.remove(&key);
            }
        }
        for family in dirty_families {
            self.mark_fast_flush_dirty(family);
        }
        if changed {
            self.mark_admin_snapshot_dirty();
        }
        for (key, notification) in notifications {
            self.route_queue_ready_notification(&key, notification);
            let route = Self::queue_ready_route(&key);
            self.wake_pending_reserves_for_route(key.family, &route, now);
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
            if let Some(notification) = notification {
                self.route_queue_ready_notification(key, notification);
            }
            let route = Self::queue_ready_route(key);
            self.wake_pending_reserves_for_route(key.family, &route, Instant::now());
        }

        if created_actor && counts.total() == 0 {
            self.actors.remove(key);
            self.ready_states.remove(key);
            self.known_queue_keys.remove(key);
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
            self.actors.remove(key);
            self.ready_states.remove(key);
            self.known_queue_keys.remove(key);
            self.mark_admin_snapshot_dirty();
        }

        result
    }
}
