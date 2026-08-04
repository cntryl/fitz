#[cfg(test)]
use super::FrameContext;
use super::{
    Arc, Envelope, HashSet, Instant, Mutex, QueueDomainCore, QueueLiveCounts, QueueNotification,
    QueueProjectionEntry, QueueProjectionState, QueueReadyNotification, WarmQueueActor,
    QUEUE_ACTOR_IDLE_TTL, QUEUE_DEDUP_SWEEP_INTERVAL, QUEUE_IDLE_SWEEP_INTERVAL,
};

impl QueueDomainCore {
    pub(in crate::domains::queue::sink) fn queue_key_for_route(
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
    pub(in crate::domains::queue::sink) fn session_inbox_address(
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
    ) -> crate::runtime::routing::RouteAddress {
        crate::runtime::routing::RouteAddress::new(
            family_id,
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        )
    }

    pub(in crate::domains::queue::sink) fn route_queue_response(
        &self,
        request_envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::queue::QueueResponse,
    ) {
        #[cfg(test)]
        {
            let response_bytes = crate::dispatch::protocol::queue_codec::encode_response(response);
            let response_ctx = FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::dispatch::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            );
            if let Some(response_envelope) = request_envelope.try_reply_to(response_ctx) {
                if let Err(error) = self.router.route(response_envelope) {
                    tracing::warn!(
                        domain = "queue",
                        session = meta.session_id,
                        error = ?error,
                        "Failed to route queue response"
                    );
                }
            }
        }

        #[cfg(not(test))]
        {
            let response = crate::domains::queue::QueueClientResponse::new(meta, response.clone());
            if let Some(response_envelope) = request_envelope.try_reply_to(response) {
                if let Err(error) = self.router.route(response_envelope) {
                    tracing::warn!(
                        domain = "queue",
                        session = meta.session_id,
                        error = ?error,
                        "Failed to route queue response"
                    );
                }
            }
        }
    }

    pub(in crate::domains::queue::sink) fn route_queue_recovery_error(
        &self,
        request_envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<Instant>,
        message: String,
    ) {
        tracing::error!(
            domain = "queue",
            family = meta.route_family.as_u64(),
            error = %message,
            "Queue actor recovery failed"
        );
        let response = crate::domains::queue::QueueResponse::Error { message };
        self.route_queue_response(request_envelope, meta, &response);
        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            metrics.record_failure(started_at);
        }
    }

    pub(in crate::domains::queue::sink) fn queue_ready_route(
        key: &crate::domains::queue::QueueKey,
    ) -> crate::runtime::routing::Route {
        crate::runtime::routing::Route::new(format!(
            "queue://{}/{}/{}",
            key.realm, key.area, key.resource
        ))
    }

    pub(in crate::domains::queue::sink) fn matching_queue_keys(
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

    pub(in crate::domains::queue::sink) fn inventory_existing_queue_keys(
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

    pub(in crate::domains::queue::sink) fn record_ready_state(
        &self,
        key: &crate::domains::queue::QueueKey,
        counts: crate::domains::queue::QueueActorLiveCounts,
    ) -> Option<QueueReadyNotification> {
        let is_ready = counts.ready > 0;
        let mut ready_states = self.ready_states.lock();
        let was_ready = ready_states.get(key).copied().unwrap_or(false);

        if counts.total() == 0 {
            ready_states.remove(key);
        } else {
            ready_states.insert(key.clone(), is_ready);
        }

        if !was_ready && is_ready {
            Some(QueueReadyNotification {
                family_id: key.family,
                counts,
            })
        } else {
            None
        }
    }

    pub(in crate::domains::queue::sink) fn route_queue_notify_to_subscription(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &crate::runtime::routing::Route,
        counts: crate::domains::queue::QueueActorLiveCounts,
    ) {
        #[cfg(test)]
        {
            let payload = crate::dispatch::protocol::queue_codec::encode_notify(
                subscription_id,
                route,
                QueueNotification {
                    ready_messages: counts.ready as u64,
                    delayed_messages: counts.delayed as u64,
                    inflight_messages: counts.inflight as u64,
                },
            );
            let notify_ctx = FrameContext::new(
                session_id,
                crate::dispatch::protocol::frame::ChannelId::Sub,
                crate::dispatch::protocol::tlv::MessageType::new(
                    crate::dispatch::protocol::queue_codec::msg_type::NOTIFY,
                ),
                bytes::Bytes::from(payload),
                *subscriber.family(),
            );
            let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);
            if self.router.route(notify_envelope).is_err() {
                crate::observability::counter_inc("fitz_queue_notify_drops_total");
            }
        }

        #[cfg(not(test))]
        {
            let notification = crate::domains::queue::QueueClientNotification::new(
                session_id,
                *subscriber.family(),
                subscription_id,
                route.clone(),
                QueueNotification {
                    ready_messages: counts.ready as u64,
                    delayed_messages: counts.delayed as u64,
                    inflight_messages: counts.inflight as u64,
                },
            );
            let notify_envelope = Envelope::new(subscriber.clone(), notification);
            if self.router.route(notify_envelope).is_err() {
                crate::observability::counter_inc("fitz_queue_notify_drops_total");
            }
        }
    }

    pub(in crate::domains::queue::sink) fn route_queue_ready_notification(
        &self,
        key: &crate::domains::queue::QueueKey,
        notification: QueueReadyNotification,
    ) {
        let route = Self::queue_ready_route(key);
        let targets = {
            let families = self.families.lock();
            let mut targets = Vec::new();
            if let Some(state) = families.get(&notification.family_id.as_u64()) {
                state.for_each_matching_route(
                    notification.family_id,
                    route.as_str(),
                    |subscription| {
                        targets.push((
                            subscription.session_id,
                            subscription.subscription_id,
                            subscription.subscriber.clone(),
                        ));
                    },
                );
            }
            targets
        };

        for (session_id, subscription_id, subscriber) in targets {
            self.route_queue_notify_to_subscription(
                session_id,
                subscription_id,
                &subscriber,
                &route,
                notification.counts,
            );
        }
    }

    pub(in crate::domains::queue::sink) fn emit_current_ready_notifications_for_watch(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) {
        let actors = self.actors.lock();
        let ready_snapshots: Vec<_> = actors
            .iter()
            .filter(|(key, _)| key.family == family_id)
            .filter_map(|(key, warm_actor)| {
                let counts = warm_actor.actor.lock().live_counts();
                let route = Self::queue_ready_route(key);
                (counts.ready > 0 && pattern.matches(&route)).then_some((route, counts))
            })
            .collect();
        drop(actors);

        for (route, counts) in ready_snapshots {
            self.route_queue_notify_to_subscription(
                session_id,
                subscription_id,
                subscriber,
                &route,
                counts,
            );
        }
    }

    pub(in crate::domains::queue::sink) fn sweep_runtime_state_at(&self, now: Instant) {
        self.sweep_idle_actors_at(now);
        self.maybe_cleanup_dedup_at(now);
        self.maybe_flush_dirty_fast_families_at(now);
    }

    pub(in crate::domains::queue::sink) fn fast_flush_enabled(&self) -> bool {
        self.queue_write_options.is_best_effort() && self.fast_flush_interval.is_some()
    }

    pub(in crate::domains::queue::sink) fn mark_fast_flush_dirty(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
    ) {
        if self.fast_flush_enabled() {
            self.dirty_fast_flush_families.lock().insert(family_id.id());
        }
    }

    pub(in crate::domains::queue::sink) fn maybe_flush_dirty_fast_families_at(&self, now: Instant) {
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

    pub(in crate::domains::queue::sink) fn flush_dirty_fast_families(&self) {
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

    pub(in crate::domains::queue::sink) fn maybe_cleanup_dedup_at(&self, now: Instant) {
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

    pub(in crate::domains::queue::sink) fn get_or_create_actor(
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
                Ok((actor, true))
            }
        }
    }

    pub(in crate::domains::queue::sink) fn mark_admin_snapshot_dirty(&self) {
        self.projection.mark_dirty();
        self.refresh_metrics_gauges();
    }

    pub(in crate::domains::queue::sink) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            let counts = self.live_counts();
            metrics.set_ready_messages(counts.ready);
            metrics.set_delayed_messages(counts.delayed);
            metrics.set_inflight_messages(counts.inflight);
        }
    }

    pub(in crate::domains::queue::sink) fn observe_histogram_us(&self, name: &str, value_us: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.histogram_observe_us(name, value_us);
        } else {
            crate::observability::histogram_observe_us(name, value_us);
        }
    }

    pub(in crate::domains::queue::sink) fn queue_response_is_failure(
        response: &crate::domains::queue::QueueResponse,
    ) -> bool {
        matches!(
            response,
            crate::domains::queue::QueueResponse::InvalidToken
                | crate::domains::queue::QueueResponse::InflightExpired
                | crate::domains::queue::QueueResponse::NotFound
                | crate::domains::queue::QueueResponse::QueueNotFound
                | crate::domains::queue::QueueResponse::BadRequest { .. }
                | crate::domains::queue::QueueResponse::Error { .. }
        )
    }

    pub(in crate::domains::queue::sink) fn refresh_admin_snapshot_if_dirty(&self) {
        self.sweep_idle_actors();
        self.projection
            .refresh_if_dirty(|| self.collect_projection_state());
    }

    pub(in crate::domains::queue::sink) fn collect_projection_state(&self) -> QueueProjectionState {
        let actors = self.actors.lock();
        let families = self.families.lock();
        let entries = actors
            .iter()
            .map(|(key, warm_actor)| {
                let actor = warm_actor.actor.lock();
                let ready_route = Self::queue_ready_route(key);
                let subscriptions_active = families.get(&key.family.as_u64()).map_or(0, |state| {
                    state.for_each_matching_route(key.family, ready_route.as_str(), |_| {})
                });
                QueueProjectionEntry {
                    key: key.clone(),
                    snapshot: actor.admin_snapshot(),
                    subscriptions_active,
                    inflight: actor.admin_inflight(),
                    dead_letters: actor.admin_dead_letters(),
                }
            })
            .collect();

        QueueProjectionState::from_entries(entries)
    }

    pub(in crate::domains::queue::sink) fn sweep_idle_actors(&self) {
        self.sweep_idle_actors_at(Instant::now());
    }

    pub(in crate::domains::queue::sink) fn maybe_sweep_idle_actors(&self) {
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

    pub(in crate::domains::queue::sink) fn sweep_idle_actors_at(&self, now: Instant) {
        let mut changed = false;
        let mut notifications = Vec::new();
        let mut removed_keys = Vec::new();
        let mut empty_removed_keys = Vec::new();
        let mut dirty_families = HashSet::new();
        let mut actors = self.actors.lock();

        actors.retain(|key, warm_actor| {
            let mut actor = warm_actor.actor.lock();
            if actor.process_due_work() {
                changed = true;
                dirty_families.insert(key.family);
            }
            let counts = actor.live_counts();

            if let Some(notification) = self.record_ready_state(key, counts) {
                notifications.push((key.clone(), notification));
            }

            let idle_for = now.saturating_duration_since(warm_actor.last_used);
            let should_keep =
                idle_for < QUEUE_ACTOR_IDLE_TTL || counts.delayed > 0 || counts.inflight > 0;
            if !should_keep {
                changed = true;
                removed_keys.push(key.clone());
                if counts.total() == 0 {
                    empty_removed_keys.push(key.clone());
                }
            }
            should_keep
        });

        drop(actors);
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
        }
    }

    /// Drop all live queue inflight entries owned by the disconnected session and return
    /// those accepted messages to the ready queue. Inflight ownership is
    /// broker-local runtime state only.
    pub(in crate::domains::queue::sink) fn cleanup_session(&self, session_id: u64) {
        let mut released_any = false;
        let mut notifications = Vec::new();
        let mut actors = self.actors.lock();
        for (key, warm_actor) in actors.iter_mut() {
            let mut actor = warm_actor.actor.lock();
            if actor.cleanup_session_inflight(session_id) > 0 {
                released_any = true;
                if let Some(notification) = self.record_ready_state(key, actor.live_counts()) {
                    notifications.push((key.clone(), notification));
                }
            }
        }
        drop(actors);

        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                crate::runtime::routing::RouteFamily::try_from(*family_id)
                    .expect("queue family IDs originate from RouteFamily"),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        drop(families);

        if released_any {
            self.mark_admin_snapshot_dirty();
        }

        for (key, notification) in notifications {
            self.route_queue_ready_notification(&key, notification);
        }

        tracing::debug!(
            domain = "queue",
            session = session_id,
            "Queue session cleanup completed"
        );
    }

    pub(in crate::domains::queue::sink) fn live_counts(&self) -> QueueLiveCounts {
        let actors = self.actors.lock();
        let mut counts = QueueLiveCounts::default();

        for warm_actor in actors.values() {
            let actor_counts = warm_actor.actor.lock().live_counts();
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

    /// Replays a dead-lettered message back into its queue.
    ///
    /// # Errors
    ///
    /// Returns an error when the warm queue actor cannot be recovered or the replay fails.
    pub(in crate::domains::queue::sink) fn replay_dead_letter(
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
    pub(in crate::domains::queue::sink) fn purge_dead_letter(
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

#[cfg(test)]
fn test_protocol_channel_from_client(
    channel: crate::runtime::ClientChannel,
) -> crate::dispatch::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => {
            crate::dispatch::protocol::frame::ChannelId::Control
        }
        crate::runtime::ClientChannel::Pub => crate::dispatch::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::dispatch::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::dispatch::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => {
            crate::dispatch::protocol::frame::ChannelId::Internal
        }
    }
}
