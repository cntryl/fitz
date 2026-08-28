use super::model::{
    AtomicBool, AtomicU64, Duration, HashMap, HashSet, Instant, Mutex, Ordering,
    QueueAdminProjection, QueueDomainActor, QueueDomainCommand, QueueDomainCore,
    QueueDomainRuntime, QueueDomainSink, QueueLiveCounts, QueueMetrics, Router,
    QUEUE_ACTOR_REPLY_TIMEOUT,
};
#[cfg(test)]
use super::model::{WarmQueueActor, QUEUE_ACTOR_IDLE_TTL};
use std::{collections::VecDeque, sync::Arc};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueCounts {
    pub pending: usize,
    pub ready: usize,
    pub delayed: usize,
    pub inflight: usize,
    pub dead_letters: usize,
}

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
    /// Constructs a queue sink over a raw Midge engine after preparing persisted queue state.
    ///
    /// The recovery write policy is explicit because startup reconciliation can write before the
    /// sink starts handling queue traffic. Cloud-backed engines must receive a cloud-compatible
    /// recovery policy such as [`cntryl_midge::WriteOptions::cloud_async`] or
    /// [`cntryl_midge::WriteOptions::cloud_strict`].
    ///
    /// # Errors
    ///
    /// Returns an error when persisted queue state is invalid or cannot be reconciled.
    pub fn try_new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_options: cntryl_midge::WriteOptions,
        recovery_write_options: cntryl_midge::WriteOptions,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Result<Self, String> {
        Self::try_new_with_storage(
            crate::storage::FitzStorageEngine::new(store),
            router,
            admin_read_model,
            queue_write_options,
            recovery_write_options,
            dedup_store,
        )
    }

    pub(crate) fn try_new_with_storage(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_options: cntryl_midge::WriteOptions,
        recovery_write_options: cntryl_midge::WriteOptions,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Result<Self, String> {
        crate::domains::queue::QueueActor::prepare_persisted_state_for_existing_families(
            store.inner(),
            queue_write_options,
            recovery_write_options,
        )?;
        let known_queue_keys = QueueDomainCore::inventory_existing_queue_keys(&store)?;
        Ok(Self::new_with_storage_and_inventory(
            store,
            router,
            admin_read_model,
            queue_write_options,
            dedup_store,
            known_queue_keys,
            None,
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
        let (known_queue_keys, inventory_error) =
            match QueueDomainCore::inventory_existing_queue_keys(&store) {
                Ok(keys) => (keys, None),
                Err(error) => {
                    tracing::warn!(
                        domain = "queue",
                        error = %error,
                        "Queue inventory unavailable during infallible sink construction"
                    );
                    (HashSet::new(), Some(error))
                }
            };
        Self::new_with_storage_and_inventory(
            store,
            router,
            admin_read_model,
            queue_write_options,
            dedup_store,
            known_queue_keys,
            inventory_error,
        )
    }

    fn new_with_storage_and_inventory(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_options: cntryl_midge::WriteOptions,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        known_queue_keys: HashSet<crate::domains::queue::QueueKey>,
        inventory_error: Option<String>,
    ) -> Self {
        let core = Arc::new(QueueDomainCore {
            delivery_service_us: Arc::new(std::sync::atomic::AtomicU64::new(
                super::model::assumed_service_us(),
            )),
            store,
            queue_write_options,
            dedup_store,
            actors: Mutex::new(HashMap::new()),
            idle_sweep_keys: Mutex::new(VecDeque::new()),
            known_queue_keys: Mutex::new(known_queue_keys),
            inventory_error: Mutex::new(inventory_error),
            wildcard_reserve_sequence: AtomicU64::new(0),
            families: Mutex::new(HashMap::new()),
            cleaned_up_sessions: Mutex::new(crate::runtime::CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            )),
            next_sub_id: AtomicU64::new(1),
            ready_states: Mutex::new(HashMap::new()),
            pending_reserves: Mutex::new(VecDeque::default()),
            router,
            projection: QueueAdminProjection::new(admin_read_model),
            metrics: None,
            active: AtomicBool::new(true),
            runtime_sweep_pending: AtomicBool::new(false),
            #[cfg(test)]
            panic_next_runtime_sweep: AtomicBool::new(false),
            next_idle_sweep_at: Mutex::new(Instant::now()),
            next_dedup_sweep_at: Mutex::new(Instant::now()),
            dirty_fast_flush_families: Mutex::new(HashSet::new()),
            fast_flush_interval: None,
            next_fast_flush_at: Mutex::new(Instant::now()),
        });
        let actor = Self::spawn_actor(core.clone());
        Self {
            core,
            actor,
            inflight_client_deliveries: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn spawn_actor(core: Arc<QueueDomainCore>) -> crate::runtime::ManagedActor<QueueDomainCommand> {
        let router = core.router.clone();
        crate::runtime::ManagedActor::spawn_fail_closed(
            router,
            QueueDomainActor::route_address(),
            move || QueueDomainActor::new(core.clone()),
            crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
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
    pub(super) fn set_inventory_error_for_tests(&self, error: impl Into<String>) {
        *self.core.inventory_error.lock() = Some(error.into());
    }

    pub(crate) fn panic_actor_for_failpoint(&self) {
        let _ = self
            .actor
            .try_send_high_priority(QueueDomainCommand::PanicForFailpoint);
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
    pub(super) fn known_queue_count_for_tests(&self) -> usize {
        self.core.known_queue_keys.lock().len()
    }

    #[cfg(test)]
    pub(super) fn known_queue_contains_for_tests(
        &self,
        key: &crate::domains::queue::QueueKey,
    ) -> bool {
        self.core.known_queue_keys.lock().contains(key)
    }

    #[cfg(test)]
    pub(super) fn install_actor_for_tests(
        &self,
        key: crate::domains::queue::QueueKey,
        actor: crate::domains::queue::QueueActor,
    ) {
        self.core.known_queue_keys.lock().insert(key.clone());
        self.core.actors.lock().insert(
            key.clone(),
            WarmQueueActor {
                actor: Arc::new(Mutex::new(actor)),
                last_used: Instant::now(),
            },
        );
        self.core.idle_sweep_keys.lock().push_back(key);
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
        let actors = self.core.actors.lock();
        let actor = actors.get(&key).expect("warm queue actor").actor.lock();
        actor.admin_snapshot()
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
    pub(super) fn clear_dirty_fast_flush_for_tests(&self) {
        self.core.dirty_fast_flush_families.lock().clear();
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

    #[cfg(test)]
    pub(super) fn panic_next_runtime_sweep_for_tests(&self) {
        self.core
            .panic_next_runtime_sweep
            .store(true, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn runtime_sweep_pending_for_tests(&self) -> bool {
        self.core.runtime_sweep_pending.load(Ordering::Acquire)
    }

    fn send_unit_actor_command(
        &self,
        operation: &'static str,
        build_command: impl FnOnce(crossbeam_channel::Sender<()>) -> QueueDomainCommand,
    ) -> Result<(), crate::runtime::DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.actor.try_send_high_priority(build_command(reply_tx)) {
            tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command enqueue failed");
            return Err(error);
        }

        // Returning the outcome rather than swallowing it: callers previously
        // could not tell a completed command from one that timed out, so a
        // silently dropped session cleanup looked identical to a successful
        // one.
        reply_rx
            .recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT)
            .map_err(|error| {
                tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command reply failed");
                crate::runtime::reply_wait::map_reply_wait_error(error)
            })
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

        reply_rx.recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT).map_err(|error| {
            tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command reply failed");
            format!("Queue actor command reply failed for {operation}: {error}")
        })?
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        // Best effort: the snapshot refreshes again on the next tick, so a
        // missed one is not worth surfacing.
        let _ = self.send_unit_actor_command(
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
            .recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_else(|error| {
                tracing::warn!(domain = "queue", error = %error, "Queue live-count query reply failed");
                QueueLiveCounts::default()
            })
    }

    pub fn counts(&self) -> QueueCounts {
        let counts = self.live_counts();
        QueueCounts {
            pending: counts.pending,
            ready: counts.ready,
            delayed: counts.delayed,
            inflight: counts.inflight,
            dead_letters: counts.dead_letters,
        }
    }

    /// Run session cleanup on the actor, reporting whether it completed.
    ///
    /// The outcome must reach the caller: swallowing it made a cleanup that
    /// never ran indistinguishable from one that succeeded, so the ingress
    /// retry-ticket machinery never saw a queue cleanup failure at all.
    ///
    /// # Errors
    ///
    /// Returns the delivery failure when the command could not be enqueued, or
    /// when the actor did not reply before its deadline.
    #[must_use = "a dropped cleanup failure is indistinguishable from a cleanup that succeeded"]
    pub fn cleanup_session(&self, session_id: u64) -> Result<(), crate::runtime::DeliveryError> {
        self.send_unit_actor_command("cleanup_session", |reply| {
            QueueDomainCommand::CleanupSession(session_id, reply)
        })
    }

    pub(crate) fn sweep_runtime_state(&self) {
        self.request_runtime_sweep_at(Instant::now());
    }

    #[cfg(test)]
    pub(super) fn sweep_runtime_state_at(&self, now: Instant) {
        let _ = self.send_unit_actor_command("sweep_runtime_state", |reply| {
            QueueDomainCommand::SweepRuntimeStateAt(now, Some(reply))
        });
    }

    pub(super) fn request_runtime_sweep_at(&self, now: Instant) -> bool {
        if self.core.runtime_sweep_pending.swap(true, Ordering::AcqRel) {
            return false;
        }

        if let Err(error) = self
            .actor
            .try_send_high_priority(QueueDomainCommand::SweepRuntimeStateAt(now, None))
        {
            self.core
                .runtime_sweep_pending
                .store(false, Ordering::Release);
            tracing::warn!(domain = "queue", operation = "sweep_runtime_state", error = %error, "Queue actor command enqueue failed");
            return false;
        }

        true
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
