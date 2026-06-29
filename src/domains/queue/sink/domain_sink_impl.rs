use super::model::*;

impl QueueDomainSink {
    pub fn try_new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_options: cntryl_midge::WriteOptions,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Result<Self, String> {
        Self::try_new_with_storage(
            crate::storage::FitzStorageEngine::new(store),
            router,
            admin_read_model,
            queue_write_options,
            dedup_store,
        )
    }

    pub(crate) fn try_new_with_storage(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_options: cntryl_midge::WriteOptions,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Result<Self, String> {
        crate::domains::queue::QueueActor::validate_persisted_state_for_existing_families(
            store.inner(),
        )?;
        Ok(Self::new_with_storage(
            store,
            router,
            admin_read_model,
            queue_write_options,
            dedup_store,
        ))
    }

    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_options: cntryl_midge::WriteOptions,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        Self::new_with_storage(
            crate::storage::FitzStorageEngine::new(store),
            router,
            admin_read_model,
            queue_write_options,
            dedup_store,
        )
    }

    pub(crate) fn new_with_storage(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_options: cntryl_midge::WriteOptions,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        Self {
            store,
            queue_write_options,
            dedup_store,
            actors: Mutex::new(HashMap::new()),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            ready_states: Mutex::new(HashMap::new()),
            router,
            projection: QueueAdminProjection::new(admin_read_model),
            metrics: None,
            active: AtomicBool::new(true),
            next_idle_sweep_at: Mutex::new(Instant::now()),
            next_dedup_sweep_at: Mutex::new(Instant::now()),
            dirty_fast_flush_families: Mutex::new(HashSet::new()),
            fast_flush_interval: None,
            next_fast_flush_at: Mutex::new(Instant::now()),
        }
    }

    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.metrics = Some(QueueMetrics::new(collector));
        self.refresh_metrics_gauges();
        self
    }

    pub fn with_fast_flush_interval(mut self, interval: Option<Duration>) -> Self {
        self.fast_flush_interval = interval;
        if let Some(interval) = interval {
            *self.next_fast_flush_at.lock() = Instant::now() + interval;
        }
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

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

    pub(super) fn route_queue_response(
        &self,
        request_envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::queue::QueueResponse,
    ) {
        #[cfg(test)]
        {
            let response_bytes = crate::protocol::queue_codec::encode_response(response);
            let response_ctx = FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::protocol::tlv::MessageType::new(meta.message_type),
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

    pub(super) fn route_queue_recovery_error(
        &self,
        request_envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<Instant>,
        message: String,
    ) -> Result<(), DeliveryError> {
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
        Ok(())
    }

    pub(super) fn queue_ready_route(
        key: &crate::domains::queue::QueueKey,
    ) -> crate::runtime::routing::Route {
        crate::runtime::routing::Route::new(format!(
            "queue://{}/{}/{}/ready",
            key.realm, key.area, key.resource
        ))
    }

    pub(super) fn record_ready_state(
        &self,
        key: &crate::domains::queue::QueueKey,
        snapshot: QueueAdminSnapshot,
    ) -> Option<QueueReadyNotification> {
        let is_ready = snapshot.messages_ready > 0;
        let mut ready_states = self.ready_states.lock();
        let was_ready = ready_states.get(key).copied().unwrap_or(false);

        if snapshot.messages_total == 0 && snapshot.messages_inflight == 0 {
            ready_states.remove(key);
        } else {
            ready_states.insert(key.clone(), is_ready);
        }

        if !was_ready && is_ready {
            Some(QueueReadyNotification {
                family_id: key.family,
                snapshot,
            })
        } else {
            None
        }
    }

    pub(super) fn route_queue_notify_to_subscription(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &crate::runtime::routing::Route,
        snapshot: QueueAdminSnapshot,
    ) {
        #[cfg(test)]
        {
            let payload = crate::protocol::queue_codec::encode_notify(
                subscription_id,
                route,
                QueueNotification {
                    ready_messages: snapshot.messages_ready as u64,
                    delayed_messages: snapshot.messages_delayed as u64,
                    inflight_messages: snapshot.messages_inflight as u64,
                },
            );
            let notify_ctx = FrameContext::new(
                session_id,
                crate::protocol::frame::ChannelId::Sub,
                crate::protocol::tlv::MessageType::new(
                    crate::protocol::queue_codec::msg_type::NOTIFY,
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
                    ready_messages: snapshot.messages_ready as u64,
                    delayed_messages: snapshot.messages_delayed as u64,
                    inflight_messages: snapshot.messages_inflight as u64,
                },
            );
            let notify_envelope = Envelope::new(subscriber.clone(), notification);
            if self.router.route(notify_envelope).is_err() {
                crate::observability::counter_inc("fitz_queue_notify_drops_total");
            }
        }
    }

    pub(super) fn route_queue_ready_notification(
        &self,
        key: &crate::domains::queue::QueueKey,
        notification: QueueReadyNotification,
    ) {
        let route = Self::queue_ready_route(key);
        let families = self.families.lock();
        if let Some(state) = families.get(&notification.family_id.as_u64()) {
            state.for_each_matching_route(notification.family_id, route.as_str(), |subscription| {
                self.route_queue_notify_to_subscription(
                    subscription.session_id,
                    subscription.subscription_id,
                    &subscription.subscriber,
                    &route,
                    notification.snapshot,
                );
            });
        }
    }

    pub(super) fn emit_current_ready_notifications_for_watch(
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
                let snapshot = warm_actor.actor.lock().admin_snapshot();
                let route = Self::queue_ready_route(key);
                (snapshot.messages_ready > 0 && pattern.matches(&route))
                    .then_some((route, snapshot))
            })
            .collect();
        drop(actors);

        for (route, snapshot) in ready_snapshots {
            self.route_queue_notify_to_subscription(
                session_id,
                subscription_id,
                subscriber,
                &route,
                snapshot,
            );
        }
    }

    pub(crate) fn sweep_runtime_state(&self) {
        self.sweep_runtime_state_at(Instant::now());
    }

    pub(super) fn sweep_runtime_state_at(&self, now: Instant) {
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
        key: crate::domains::queue::QueueKey,
    ) -> Result<(Arc<Mutex<crate::domains::queue::QueueActor>>, bool), String> {
        use std::collections::hash_map::Entry;

        let now = Instant::now();
        let mut actors = self.actors.lock();
        match actors.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().last_used = now;
                Ok((entry.get().actor.clone(), false))
            }
            Entry::Vacant(entry) => {
                let actor = Arc::new(Mutex::new(
                    crate::domains::queue::QueueActor::try_new_with_write_options(
                        key.family,
                        key,
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

    pub(super) fn mark_admin_snapshot_dirty(&self) {
        self.projection.mark_dirty();
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.set_ready_messages(self.ready_message_count());
            metrics.set_delayed_messages(self.delayed_message_count());
            metrics.set_inflight_messages(self.active_inflight_count());
        }
    }

    pub(super) fn observe_histogram_us(&self, name: &str, value_us: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.histogram_observe_us(name, value_us);
        } else {
            crate::observability::histogram_observe_us(name, value_us);
        }
    }

    pub(super) fn queue_response_is_failure(
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

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        self.sweep_idle_actors();
        self.projection
            .refresh_if_dirty(|| self.collect_projection_state());
    }

    pub(super) fn collect_projection_state(&self) -> QueueProjectionState {
        let actors = self.actors.lock();
        let families = self.families.lock();
        let entries = actors
            .iter()
            .map(|(key, warm_actor)| {
                let actor = warm_actor.actor.lock();
                let ready_route = Self::queue_ready_route(key);
                let subscriptions_active = families
                    .get(&key.family.as_u64())
                    .map(|state| {
                        state.for_each_matching_route(key.family, ready_route.as_str(), |_| {})
                    })
                    .unwrap_or(0);
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
        let mut actors = self.actors.lock();

        actors.retain(|key, warm_actor| {
            let mut actor = warm_actor.actor.lock();
            let ready_before = actor.ready_len();
            let inflight_before = actor.inflight.len();
            actor.process_due_work();
            let ready_after = actor.ready_len();
            let inflight_after = actor.inflight.len();
            let snapshot = actor.admin_snapshot();
            if ready_before != ready_after || inflight_before != inflight_after {
                changed = true;
            }

            if let Some(notification) = self.record_ready_state(key, snapshot) {
                notifications.push((key.clone(), notification));
            }

            let idle_for = now.saturating_duration_since(warm_actor.last_used);
            let should_keep = idle_for < QUEUE_ACTOR_IDLE_TTL
                || snapshot.messages_delayed > 0
                || snapshot.messages_inflight > 0;
            if !should_keep {
                changed = true;
                removed_keys.push(key.clone());
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
    pub fn cleanup_session(&self, session_id: u64) {
        let mut released_any = false;
        let mut notifications = Vec::new();
        let mut actors = self.actors.lock();
        for (key, warm_actor) in actors.iter_mut() {
            let mut actor = warm_actor.actor.lock();
            if actor.cleanup_session_inflight(session_id) > 0 {
                released_any = true;
                if let Some(notification) = self.record_ready_state(key, actor.admin_snapshot()) {
                    notifications.push((key.clone(), notification));
                }
            }
        }
        drop(actors);

        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                crate::runtime::routing::RouteFamily::new(*family_id),
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

    pub fn pending_message_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|warm_actor| {
                let snapshot = warm_actor.actor.lock().admin_snapshot();
                snapshot.messages_ready + snapshot.messages_delayed
            })
            .sum()
    }

    pub fn ready_message_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|warm_actor| warm_actor.actor.lock().admin_snapshot().messages_ready)
            .sum()
    }

    pub fn delayed_message_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|warm_actor| warm_actor.actor.lock().admin_snapshot().messages_delayed)
            .sum()
    }

    pub fn active_inflight_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|warm_actor| warm_actor.actor.lock().inflight.len())
            .sum()
    }

    pub fn dead_letter_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|warm_actor| {
                warm_actor
                    .actor
                    .lock()
                    .admin_snapshot()
                    .messages_dead_lettered
            })
            .sum()
    }

    pub fn replay_dead_letter(
        &self,
        key: crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        let (actor_handle, created_actor) = self.get_or_create_actor(key.clone())?;
        let result = {
            let mut actor = actor_handle.lock();
            actor.replay_dead_letter(id)
        };

        if matches!(result, Ok(true)) {
            self.mark_fast_flush_dirty(key.family);
            let snapshot = actor_handle.lock().admin_snapshot();
            let notification = self.record_ready_state(&key, snapshot);
            self.mark_admin_snapshot_dirty();
            if let Some(notification) = notification {
                self.route_queue_ready_notification(&key, notification);
            }
        }

        if created_actor {
            let should_remove = {
                let actor = actor_handle.lock();
                actor.admin_snapshot().messages_total == 0 && actor.inflight.is_empty()
            };
            if should_remove {
                self.actors.lock().remove(&key);
                self.ready_states.lock().remove(&key);
                self.mark_admin_snapshot_dirty();
            }
        }

        result
    }

    pub fn purge_dead_letter(
        &self,
        key: crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        let (actor_handle, created_actor) = self.get_or_create_actor(key.clone())?;
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
                actor.admin_snapshot().messages_total == 0 && actor.inflight.is_empty()
            };
            if should_remove {
                self.actors.lock().remove(&key);
                self.ready_states.lock().remove(&key);
                self.mark_admin_snapshot_dirty();
            }
        }

        result
    }
}

#[cfg(test)]
fn test_protocol_channel_from_client(
    channel: crate::runtime::ClientChannel,
) -> crate::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => crate::protocol::frame::ChannelId::Control,
        crate::runtime::ClientChannel::Pub => crate::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => crate::protocol::frame::ChannelId::Internal,
    }
}
