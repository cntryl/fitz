//! Per-queue warm actor lifecycle: lookup, idle sweep, fast flush, dead-letter ops.

use super::model::{
    Arc, HashSet, Instant, Mutex, QueueDomainCore, WarmQueueActor, QUEUE_ACTOR_IDLE_TTL,
    QUEUE_DEDUP_SWEEP_INTERVAL, QUEUE_IDLE_SWEEP_BATCH_SIZE, QUEUE_IDLE_SWEEP_INTERVAL,
};

impl QueueDomainCore {
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
        &self,
        family: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
    ) -> Vec<crate::domains::queue::QueueKey> {
        let mut keys = self
            .known_queue_keys
            .lock()
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
        &self,
        family: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
    ) -> usize {
        self.known_queue_keys
            .lock()
            .iter()
            .filter(|key| key.family == family)
            .filter(|key| pattern.matches(&Self::queue_ready_route(key)))
            .count()
    }

    pub(super) fn inventory_existing_queue_keys(
        store: &crate::storage::FitzStorageEngine,
    ) -> Result<HashSet<crate::domains::queue::QueueKey>, String> {
        let families = store
            .list_column_families()
            .map_err(|error| format!("list queue inventory families failed: {error:?}"))?;
        let mut known_queue_keys = HashSet::new();

        for family in families {
            if family.id() == 0 {
                continue;
            }
            let route_family = crate::runtime::routing::RouteFamily::new(family.id());
            let txn = store
                .begin_tx(family.id(), cntryl_midge::TransactionMode::ReadOnly)
                .map_err(|error| {
                    format!(
                        "queue inventory transaction failed: family={} error={error:?}",
                        family.id()
                    )
                })?;
            let rows = txn.scan(&cntryl_midge::Query::new()).map_err(|error| {
                format!(
                    "queue inventory scan failed: family={} error={error:?}",
                    family.id()
                )
            })?;

            for row in rows {
                let (key, value) = row.map_err(|error| {
                    format!(
                        "queue inventory scan failed: family={} error={error:?}",
                        family.id()
                    )
                })?;
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
        &self,
        key: &crate::domains::queue::QueueKey,
        counts: crate::domains::queue::QueueActorLiveCounts,
    ) -> Option<super::model::QueueReadyNotification> {
        let is_ready = counts.ready > 0;
        let mut ready_states = self.ready_states.lock();
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

    pub(super) fn sweep_runtime_state_at(&self, now: Instant) {
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

    pub(super) fn fast_flush_enabled(&self) -> bool {
        self.queue_write_options.is_best_effort() && self.fast_flush_interval.is_some()
    }

    pub(super) fn mark_fast_flush_dirty(&self, family_id: crate::runtime::routing::RouteFamily) {
        if self.fast_flush_enabled() {
            self.dirty_fast_flush_families.lock().insert(family_id.id());
        }
    }

    pub(super) fn maybe_flush_dirty_fast_families_at(&self, now: Instant) {
        let Some(interval) = self.fast_flush_interval else {
            return;
        };
        if !self.queue_write_options.is_best_effort() {
            return;
        }

        let should_flush = {
            let mut next_fast_flush_at = self.next_fast_flush_at.lock();
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

    pub(super) fn flush_dirty_fast_families(&self) {
        let dirty_family_ids = {
            let mut dirty = self.dirty_fast_flush_families.lock();
            dirty.drain().collect::<Vec<_>>()
        };
        if dirty_family_ids.is_empty() {
            return;
        }

        let families = match self.store.list_column_families() {
            Ok(families) => families,
            Err(error) => {
                tracing::warn!(
                    domain = "queue",
                    error = ?error,
                    "Failed to list queue column families for fast flush"
                );
                self.dirty_fast_flush_families
                    .lock()
                    .extend(dirty_family_ids);
                return;
            }
        };

        let mut retry_family_ids = Vec::new();
        for family_id in dirty_family_ids {
            let Some(cf) = families.iter().find(|cf| cf.id() == family_id) else {
                tracing::warn!(
                    domain = "queue",
                    family = family_id,
                    "Queue fast flush skipped missing column family"
                );
                retry_family_ids.push(family_id);
                continue;
            };

            if let Err(error) = self.store.flush_cf(cf) {
                tracing::warn!(
                    domain = "queue",
                    family = family_id,
                    error = ?error,
                    "Queue fast flush failed"
                );
                retry_family_ids.push(family_id);
            }
        }

        if !retry_family_ids.is_empty() {
            self.dirty_fast_flush_families
                .lock()
                .extend(retry_family_ids);
        }
    }

    pub(super) fn maybe_cleanup_dedup_at(&self, now: Instant) {
        let should_cleanup = {
            let mut next_dedup_sweep_at = self.next_dedup_sweep_at.lock();
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

    pub(super) fn get_or_create_actor(
        &self,
        key: &crate::domains::queue::QueueKey,
    ) -> Result<(Arc<Mutex<crate::domains::queue::QueueActor>>, bool), String> {
        use std::collections::hash_map::Entry;

        let now = Instant::now();
        match self.actors.lock().entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().last_used = now;
                Ok((entry.get().actor.clone(), false))
            }
            Entry::Vacant(entry) => {
                let actor = Arc::new(Mutex::new(
                    crate::domains::queue::QueueActor::try_new_with_write_options(
                        key.family,
                        key.clone(),
                        self.store.clone_inner(),
                        None,
                        self.dedup_store.clone(),
                        self.queue_write_options,
                    )?,
                ));
                entry.insert(WarmQueueActor {
                    actor: actor.clone(),
                    last_used: now,
                });
                self.idle_sweep_keys.lock().push_back(key.clone());
                Ok((actor, true))
            }
        }
    }

    pub(super) fn sweep_idle_actors(&self) {
        self.sweep_idle_actors_at(Instant::now());
    }

    pub(super) fn maybe_sweep_idle_actors(&self) {
        let now = Instant::now();

        {
            let mut next_idle_sweep_at = self.next_idle_sweep_at.lock();
            if now < *next_idle_sweep_at {
                return;
            }
            *next_idle_sweep_at = now + QUEUE_IDLE_SWEEP_INTERVAL;
        }

        self.sweep_idle_actors_at(now);
    }

    pub(super) fn sweep_idle_actors_at(&self, now: Instant) {
        let mut changed = false;
        let mut notifications = Vec::new();
        let mut removed_keys = Vec::new();
        let mut empty_removed_keys = Vec::new();
        let mut dirty_families = HashSet::new();
        let sweep_keys = {
            let mut idle_sweep_keys = self.idle_sweep_keys.lock();
            let count = idle_sweep_keys.len().min(QUEUE_IDLE_SWEEP_BATCH_SIZE);
            idle_sweep_keys.drain(..count).collect::<Vec<_>>()
        };

        for key in sweep_keys {
            let Some((actor_ref, last_used)) = self
                .actors
                .lock()
                .get(&key)
                .map(|warm_actor| (warm_actor.actor.clone(), warm_actor.last_used))
            else {
                continue;
            };
            let mut actor = actor_ref.lock();
            if actor.process_due_work() {
                changed = true;
                dirty_families.insert(key.family);
            }
            let counts = actor.live_counts();

            if let Some(notification) = self.record_ready_state(&key, counts) {
                notifications.push((key.clone(), notification));
            }

            let idle_for = now.saturating_duration_since(last_used);
            let should_keep =
                idle_for < QUEUE_ACTOR_IDLE_TTL || counts.delayed > 0 || counts.inflight > 0;
            drop(actor);

            if should_keep {
                self.idle_sweep_keys.lock().push_back(key);
                continue;
            }

            let removed = {
                let mut actors = self.actors.lock();
                let unchanged = actors.get(&key).is_some_and(|warm_actor| {
                    warm_actor.last_used == last_used && Arc::ptr_eq(&warm_actor.actor, &actor_ref)
                });
                unchanged && actors.remove(&key).is_some()
            };
            if removed {
                changed = true;
                removed_keys.push(key.clone());
                if counts.total() == 0 {
                    empty_removed_keys.push(key);
                }
            } else {
                self.idle_sweep_keys.lock().push_back(key);
            }
        }

        if !removed_keys.is_empty() {
            let mut ready_states = self.ready_states.lock();
            for key in removed_keys {
                ready_states.remove(&key);
            }
        }
        if !empty_removed_keys.is_empty() {
            let mut known_queue_keys = self.known_queue_keys.lock();
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
        &self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        let (actor_handle, created_actor) = self.get_or_create_actor(key)?;
        let result = {
            let mut actor = actor_handle.lock();
            actor.replay_dead_letter(id)
        };

        if matches!(result, Ok(true)) {
            self.mark_fast_flush_dirty(key.family);
            let counts = actor_handle.lock().live_counts();
            let notification = self.record_ready_state(key, counts);
            self.mark_admin_snapshot_dirty();
            if let Some(notification) = notification {
                self.route_queue_ready_notification(key, notification);
            }
        }

        if created_actor {
            let should_remove = {
                let actor = actor_handle.lock();
                actor.live_counts().total() == 0
            };
            if should_remove {
                self.actors.lock().remove(key);
                self.ready_states.lock().remove(key);
                self.known_queue_keys.lock().remove(key);
                self.mark_admin_snapshot_dirty();
            }
        }

        result
    }

    /// Permanently removes a dead-lettered message from its queue.
    ///
    /// # Errors
    ///
    /// Returns an error when the warm queue actor cannot be recovered or the purge fails.
    pub(super) fn purge_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        let (actor_handle, created_actor) = self.get_or_create_actor(key)?;
        let result = {
            let mut actor = actor_handle.lock();
            actor.purge_dead_letter(id)
        };

        if matches!(result, Ok(true)) {
            self.mark_fast_flush_dirty(key.family);
            self.mark_admin_snapshot_dirty();
        }

        if created_actor {
            let should_remove = {
                let actor = actor_handle.lock();
                actor.live_counts().total() == 0
            };
            if should_remove {
                self.actors.lock().remove(key);
                self.ready_states.lock().remove(key);
                self.known_queue_keys.lock().remove(key);
                self.mark_admin_snapshot_dirty();
            }
        }

        result
    }
}
