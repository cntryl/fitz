use super::model::{
    AtomicBool, AtomicU64, Duration, Envelope, HashMap, HashSet, Instant, Mutex, Ordering,
    QueueAdminProjection, QueueDomainActor, QueueDomainCommand, QueueDomainCore,
    QueueDomainRuntime, QueueDomainSink, QueueLiveCounts, QueueMetrics, QueueNotification,
    QueueProjectionEntry, QueueProjectionState, QueueReadyNotification, Router, WarmQueueActor,
    QUEUE_ACTOR_IDLE_TTL, QUEUE_DEDUP_SWEEP_INTERVAL, QUEUE_IDLE_SWEEP_INTERVAL,
};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use std::sync::Arc;

mod domain_core_impl;

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
    /// # Errors
    ///
    /// Returns an error when persisted queue state is invalid or cannot be reconciled.
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
            cntryl_midge::WriteOptions::sync(),
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
