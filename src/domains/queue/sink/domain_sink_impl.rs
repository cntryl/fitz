use super::model::{
    AtomicBool, AtomicU64, Duration, Envelope, HashMap, HashSet, Instant, Mutex, Ordering,
    QueueAdminProjection, QueueDomainActor, QueueDomainCommand, QueueDomainCore,
    QueueDomainRuntime, QueueDomainSink, QueueLiveCounts, QueueMetrics, QueueNotification,
    QueueProjectionEntry, QueueProjectionState, QueueReadyNotification, Router, WarmQueueActor,
    QUEUE_ACTOR_IDLE_TTL, QUEUE_DEDUP_SWEEP_INTERVAL, QUEUE_IDLE_SWEEP_INTERVAL,
};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;
use std::sync::Arc;

impl QueueDomainActor {
    pub(super) fn new(core: Arc<QueueDomainCore>) -> Self {
        Self { core }
    }

    pub(super) fn route_address() -> crate::runtime::routing::RouteAddress {
        crate::runtime::routing::RouteAddress::new(
            crate::runtime::routing::RouteFamily::new(0),
            crate::runtime::routing::Route::new("internal://domain/queue"),
        )
    }

    pub(super) fn runtime(&self) -> QueueDomainRuntime<'_> {
        QueueDomainRuntime { core: &self.core }
    }
}

impl QueueDomainSink {
    /// Constructs a queue sink over a raw Midge engine after validating persisted queue state.
    ///
    /// # Errors
    ///
    /// Returns an error when the persisted queue state is invalid for the current runtime.
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
        let core = Arc::new(QueueDomainCore {
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
        });
        let actor = Self::spawn_actor(core.clone());
        Self { core, actor }
    }

    fn spawn_actor(core: Arc<QueueDomainCore>) -> crate::runtime::ManagedActor<QueueDomainCommand> {
        let router = core.router.clone();
        crate::runtime::ManagedActor::spawn_supervised(
            router,
            QueueDomainActor::route_address(),
            move || QueueDomainActor::new(core.clone()),
            1024,
        )
    }

    fn rebuild_actor(&mut self) {
        self.actor.stop();
        self.actor = Self::spawn_actor(self.core.clone());
    }

    fn core_for_builder(&mut self) -> &mut QueueDomainCore {
        Arc::get_mut(&mut self.core).expect("Queue sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.actor.stop();
        self.core_for_builder().metrics = Some(QueueMetrics::new(collector));
        self.core.refresh_metrics_gauges();
        self.rebuild_actor();
        self
    }

    #[must_use]
    pub fn with_fast_flush_interval(mut self, interval: Option<Duration>) -> Self {
        self.actor.stop();
        self.core_for_builder().fast_flush_interval = interval;
        if let Some(interval) = interval {
            *self.core.next_fast_flush_at.lock() = Instant::now() + interval;
        }
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.core.active.store(false, Ordering::Relaxed);
        self.actor.stop();
    }

    pub(crate) fn is_active(&self) -> bool {
        self.core.active.load(Ordering::Relaxed)
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.actor.health_snapshot()
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.actor.is_running()
    }

    #[cfg(test)]
    pub(crate) fn panic_actor_for_tests(&self) {
        let _ = self
            .actor
            .try_send_high_priority(QueueDomainCommand::PanicForTests);
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.actor.stop();
    }

    #[cfg(test)]
    pub(super) fn actor_count_for_tests(&self) -> usize {
        self.core.actors.lock().len()
    }

    #[cfg(test)]
    pub(super) fn actors_are_empty_for_tests(&self) -> bool {
        self.core.actors.lock().is_empty()
    }

    #[cfg(test)]
    pub(super) fn queue_snapshot_for_tests(
        &self,
        family: crate::runtime::routing::RouteFamily,
        queue_route: &str,
    ) -> crate::domains::queue::QueueAdminSnapshot {
        let key = crate::domains::queue::QueueKey::from_route(
            family,
            &crate::runtime::routing::Route::new(queue_route),
        )
        .expect("queue key");
        self.core
            .actors
            .lock()
            .get(&key)
            .expect("warm queue actor")
            .actor
            .lock()
            .admin_snapshot()
    }

    #[cfg(test)]
    pub(super) fn force_actor_idle_for_tests(
        &self,
        family: crate::runtime::routing::RouteFamily,
        queue_route: &str,
    ) {
        let key = crate::domains::queue::QueueKey::from_route(
            family,
            &crate::runtime::routing::Route::new(queue_route),
        )
        .expect("queue key");
        let mut actors = self.core.actors.lock();
        let warm_actor = actors.get_mut(&key).expect("warm queue actor");
        warm_actor.last_used = Instant::now()
            .checked_sub(QUEUE_ACTOR_IDLE_TTL + Duration::from_secs(1))
            .expect("idle deadline should remain representable");
    }

    #[cfg(test)]
    pub(super) fn dirty_fast_flush_contains_family_for_tests(&self, family_id: u32) -> bool {
        self.core
            .dirty_fast_flush_families
            .lock()
            .contains(&family_id)
    }

    #[cfg(test)]
    pub(super) fn dirty_fast_flush_is_empty_for_tests(&self) -> bool {
        self.core.dirty_fast_flush_families.lock().is_empty()
    }

    #[cfg(test)]
    pub(super) fn insert_dirty_fast_flush_family_for_tests(&self, family_id: u32) {
        self.core.dirty_fast_flush_families.lock().insert(family_id);
    }

    #[cfg(test)]
    pub(super) fn watch_families_are_empty_for_tests(&self) -> bool {
        self.core.families.lock().is_empty()
    }

    #[cfg(test)]
    pub(super) fn set_next_dedup_sweep_at_for_tests(&self, now: Instant) {
        *self.core.next_dedup_sweep_at.lock() = now;
    }

    fn send_unit_actor_command(
        &self,
        operation: &'static str,
        build_command: impl FnOnce(crossbeam_channel::Sender<()>) -> QueueDomainCommand,
    ) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.actor.try_send_high_priority(build_command(reply_tx)) {
            tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command enqueue failed");
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(Duration::from_secs(1)) {
            tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command reply failed");
        }
    }

    fn send_bool_actor_command(
        &self,
        operation: &'static str,
        build_command: impl FnOnce(
            crossbeam_channel::Sender<Result<bool, String>>,
        ) -> QueueDomainCommand,
    ) -> Result<bool, String> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.actor.try_send_high_priority(build_command(reply_tx)) {
            tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command enqueue failed");
            return Err(format!(
                "Queue actor command enqueue failed for {operation}: {error}"
            ));
        }

        reply_rx.recv_timeout(Duration::from_secs(1)).map_err(|error| {
            tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command reply failed");
            format!("Queue actor command reply failed for {operation}: {error}")
        })?
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        self.send_unit_actor_command(
            "refresh_admin_snapshot_if_dirty",
            QueueDomainCommand::RefreshAdminSnapshotIfDirty,
        );
    }

    fn live_counts(&self) -> QueueLiveCounts {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(QueueDomainCommand::ReadLiveCounts(reply_tx))
        {
            tracing::warn!(domain = "queue", error = %error, "Queue live-count query enqueue failed");
            return QueueLiveCounts::default();
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or_default()
    }

    pub fn pending_message_count(&self) -> usize {
        self.live_counts().pending
    }

    pub fn ready_message_count(&self) -> usize {
        self.live_counts().ready
    }

    pub fn delayed_message_count(&self) -> usize {
        self.live_counts().delayed
    }

    pub fn active_inflight_count(&self) -> usize {
        self.live_counts().inflight
    }

    pub fn dead_letter_count(&self) -> usize {
        self.live_counts().dead_letters
    }

    pub fn cleanup_session(&self, session_id: u64) {
        self.send_unit_actor_command("cleanup_session", |reply| {
            QueueDomainCommand::CleanupSession(session_id, reply)
        });
    }

    pub(crate) fn sweep_runtime_state(&self) {
        self.sweep_runtime_state_at(Instant::now());
    }

    pub(super) fn sweep_runtime_state_at(&self, now: Instant) {
        self.send_unit_actor_command("sweep_runtime_state", |reply| {
            QueueDomainCommand::SweepRuntimeStateAt(now, reply)
        });
    }

    /// Replays a dead-lettered message back into its queue.
    ///
    /// # Errors
    ///
    /// Returns an error when the queue domain actor cannot process the command or
    /// the replay fails.
    pub fn replay_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        self.send_bool_actor_command("replay_dead_letter", |reply| {
            QueueDomainCommand::ReplayDeadLetter(key.clone(), id, reply)
        })
    }

    /// Permanently removes a dead-lettered message from its queue.
    ///
    /// # Errors
    ///
    /// Returns an error when the queue domain actor cannot process the command or
    /// the purge fails.
    pub fn purge_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        self.send_bool_actor_command("purge_dead_letter", |reply| {
            QueueDomainCommand::PurgeDeadLetter(key.clone(), id, reply)
        })
    }
}

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

    pub(super) fn route_queue_notify_to_subscription(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &crate::runtime::routing::Route,
        counts: crate::domains::queue::QueueActorLiveCounts,
    ) {
        #[cfg(test)]
        {
            let payload = crate::protocol::queue_codec::encode_notify(
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
                    notification.counts,
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
            let counts = self.live_counts();
            metrics.set_ready_messages(counts.ready);
            metrics.set_delayed_messages(counts.delayed);
            metrics.set_inflight_messages(counts.inflight);
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

    pub(super) fn refresh_admin_snapshot_if_dirty(&self) {
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
            let counts = actor.live_counts();
            if ready_before != ready_after || inflight_before != inflight_after {
                changed = true;
            }

            if let Some(notification) = self.record_ready_state(key, counts) {
                notifications.push((key.clone(), notification));
            }

            let idle_for = now.saturating_duration_since(warm_actor.last_used);
            let should_keep =
                idle_for < QUEUE_ACTOR_IDLE_TTL || counts.delayed > 0 || counts.inflight > 0;
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
    pub(super) fn cleanup_session(&self, session_id: u64) {
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

    pub(super) fn live_counts(&self) -> QueueLiveCounts {
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
    pub(super) fn replay_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        let (actor_handle, created_actor) = self.get_or_create_actor(key.clone())?;
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
                actor.live_counts().total() == 0
            };
            if should_remove {
                self.actors.lock().remove(key);
                self.ready_states.lock().remove(key);
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
