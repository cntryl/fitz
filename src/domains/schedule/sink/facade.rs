//! Public `ScheduleDomainSink` API and actor lifecycle management.

use super::model::{
    duration_millis, Arc, AtomicBool, AtomicU64, HashMap, Instant, Mutex, Ordering, Router,
    ScheduleDomainActor, ScheduleDomainCommand, ScheduleDomainCore, ScheduleDomainRuntime,
    ScheduleDomainSink, ScheduleDomainState, ScheduleLiveCounts, ScheduleMetrics, VecDeque,
};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

pub(crate) const DEFAULT_SCHEDULE_PRELOAD_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(120);

impl ScheduleDomainState {
    fn new_with_storage(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            core: ScheduleDomainCore {
                store,
                actors: Mutex::new(HashMap::new()),
                sub_families: Mutex::new(HashMap::new()),
                cleaned_up_sessions: Mutex::new(super::cleanup::CleanedUpSessions::new(
                    crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
                )),
                next_sub_id: AtomicU64::new(1),
                router,
                admin_read_model,
                snapshot_dirty: AtomicBool::new(false),
                snapshot_syncing: AtomicBool::new(false),
                last_snapshot_elapsed_us: AtomicU64::new(0),
                snapshot_epoch: Instant::now(),
                live_publish_failures: AtomicU64::new(0),
                ack_failures: AtomicU64::new(0),
                pending_ack_retries: Mutex::new(HashMap::new()),
                recent_acknowledgement_ms: Mutex::new(VecDeque::new()),
                write_options: cntryl_midge::WriteOptions::buffered(),
                metrics: None,
            },
            active: AtomicBool::new(true),
        }
    }

    pub(super) fn runtime(&self) -> ScheduleDomainRuntime<'_> {
        ScheduleDomainRuntime {
            core: &self.core,
            active: &self.active,
        }
    }
}

impl ScheduleDomainActor {
    pub(super) fn new(state: Arc<ScheduleDomainState>) -> Self {
        Self { state }
    }

    pub(super) fn route_address() -> RouteAddress {
        RouteAddress::new(
            RouteFamily::new(0),
            Route::new("internal://domain/schedule"),
        )
    }
}

impl ScheduleDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self::new_with_storage(
            crate::storage::FitzStorageEngine::new(store),
            router,
            admin_read_model,
        )
    }

    pub(crate) fn new_with_storage(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        let state = Arc::new(ScheduleDomainState::new_with_storage(
            store,
            router,
            admin_read_model,
        ));
        let actor = Self::spawn_actor(state.clone());
        Self { state, actor }
    }

    fn spawn_actor(
        state: Arc<ScheduleDomainState>,
    ) -> crate::runtime::ManagedActor<ScheduleDomainCommand> {
        let router = state.core.router.clone();
        crate::runtime::ManagedActor::spawn_fail_closed(
            router,
            ScheduleDomainActor::route_address(),
            move || ScheduleDomainActor::new(state.clone()),
            crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
        )
    }

    fn rebuild_actor(&mut self) {
        self.actor.stop();
        self.actor = Self::spawn_actor(self.state.clone());
    }

    fn state_for_builder(&mut self) -> &mut ScheduleDomainState {
        Arc::get_mut(&mut self.state)
            .expect("Schedule sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_write_options(mut self, write_options: cntryl_midge::WriteOptions) -> Self {
        self.actor.stop();
        self.state_for_builder().core.write_options = write_options;
        self.rebuild_actor();
        self
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.actor.stop();
        let state = self.state_for_builder();
        state.core.metrics = Some(ScheduleMetrics::new(collector));
        state.runtime().refresh_metrics_gauges();
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.state.active.store(false, Ordering::Relaxed);
        self.actor.stop();
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.actor.is_running()
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.actor.health_snapshot()
    }

    #[cfg(test)]
    pub(crate) fn panic_actor_for_tests(&self) {
        let _ = self
            .actor
            .try_send_high_priority(ScheduleDomainCommand::PanicForTests);
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.actor.stop();
    }

    #[cfg(test)]
    pub(super) fn block_actor_for_tests(
        &self,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    ) {
        self.actor
            .try_send_high_priority(ScheduleDomainCommand::BlockForTests(entered, release))
            .expect("enqueue Schedule actor test block");
    }

    /// # Errors
    ///
    /// Returns an error when listing column families or preloading a persisted
    /// schedule actor fails.
    pub fn preload_persisted_families(&self) -> Result<(), String> {
        self.preload_persisted_families_with_timeout(DEFAULT_SCHEDULE_PRELOAD_TIMEOUT)
    }

    /// # Errors
    ///
    /// Returns an error when the actor cannot be reached, preload fails, or the
    /// actor does not reply before `timeout`.
    pub(crate) fn preload_persisted_families_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        let started_at = std::time::Instant::now();
        let timeout_ms = duration_millis(timeout);
        tracing::info!(domain = "schedule", timeout_ms, "Schedule preload started");
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(ScheduleDomainCommand::PreloadPersistedFamilies(reply_tx))
        {
            return Err(format!("schedule preload enqueue failed: {error}"));
        }

        match reply_rx.recv_timeout(timeout) {
            Ok(result) => {
                result?;
                tracing::info!(
                    domain = "schedule",
                    elapsed_ms = duration_millis(started_at.elapsed()),
                    "Schedule preload completed"
                );
                Ok(())
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                tracing::error!(
                    domain = "schedule",
                    timeout_ms,
                    elapsed_ms = duration_millis(started_at.elapsed()),
                    "Schedule preload timed out"
                );
                Err(format!(
                    "schedule preload reply timed out after {timeout_ms}ms"
                ))
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                Err("schedule preload reply failed: actor reply channel disconnected".to_string())
            }
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.state.active.load(Ordering::Relaxed)
    }

    pub(crate) fn scan_due_schedules(&self) {
        if let Err(error) = self
            .actor
            .try_send_high_priority(ScheduleDomainCommand::ScanDueSchedules)
        {
            tracing::warn!(domain = "schedule", error = %error, "Schedule due scan enqueue failed");
        }
    }

    pub(crate) fn force_due_scan_for_tests(&self, ready_count: usize) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) =
            self.actor
                .try_send_high_priority(ScheduleDomainCommand::ForceDueScanForTests(
                    ready_count,
                    reply_tx,
                ))
        {
            tracing::warn!(domain = "schedule", error = %error, "Schedule forced due scan enqueue failed");
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(std::time::Duration::from_secs(1)) {
            tracing::warn!(domain = "schedule", error = %error, "Schedule forced due scan reply failed");
        }
    }

    pub fn admin_pending_claims(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Vec<crate::control::admin::SchedulePendingClaimInfo> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) =
            self.actor
                .try_send_high_priority(ScheduleDomainCommand::ReadPendingClaims(
                    route_family,
                    reply_tx,
                ))
        {
            tracing::warn!(domain = "schedule", error = %error, "Schedule pending claim read enqueue failed");
            return Vec::new();
        }

        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap_or_default()
    }

    fn live_counts(&self) -> ScheduleLiveCounts {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(ScheduleDomainCommand::ReadLiveCounts(reply_tx))
        {
            tracing::warn!(domain = "schedule", error = %error, "Schedule live-count query enqueue failed");
            return ScheduleLiveCounts::default();
        }

        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap_or_default()
    }

    pub(crate) fn refresh_admin_snapshot_if_dirty(&self) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(ScheduleDomainCommand::RefreshAdminSnapshotIfDirty(reply_tx))
        {
            tracing::warn!(domain = "schedule", error = %error, "Schedule admin snapshot refresh enqueue failed");
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(std::time::Duration::from_secs(1)) {
            tracing::warn!(domain = "schedule", error = %error, "Schedule admin snapshot refresh reply failed");
        }
    }

    #[doc(hidden)]
    pub fn bench_publish_event(&self, event: &crate::runtime::DomainPublishEvent) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) =
            self.actor
                .try_send_high_priority(ScheduleDomainCommand::BenchPublishEvent(
                    event.clone(),
                    reply_tx,
                ))
        {
            tracing::warn!(domain = "schedule", error = %error, "Schedule bench publish enqueue failed");
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(std::time::Duration::from_secs(1)) {
            tracing::warn!(domain = "schedule", error = %error, "Schedule bench publish reply failed");
        }
    }
}

/// Narrow read-only surface used by metrics and administration code.
impl ScheduleDomainSink {
    pub fn subscription_count(&self) -> usize {
        self.live_counts().subscriptions
    }

    pub fn schedule_count(&self) -> usize {
        self.live_counts().schedules
    }

    pub fn pending_fire_count(&self) -> usize {
        self.live_counts().pending_fires
    }

    pub fn executions_per_minute(&self) -> f64 {
        self.live_counts().executions_per_minute
    }

    pub fn notify_failure_count(&self) -> u64 {
        self.live_counts().notify_failures
    }

    pub fn ack_failure_count(&self) -> u64 {
        self.live_counts().ack_failures
    }

    pub fn pending_ack_retry_count(&self) -> usize {
        self.live_counts().pending_ack_retries
    }

    pub fn oldest_pending_claim_age_seconds(&self) -> u64 {
        self.live_counts().oldest_pending_claim_age_seconds
    }

    pub fn overdue_normalization_count(&self) -> u64 {
        self.live_counts().overdue_normalizations
    }
}
