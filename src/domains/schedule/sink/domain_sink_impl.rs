use super::delivery_strategy::DeliveryStrategy;
use super::model::{
    now_epoch_ms, Arc, AtomicBool, AtomicU64, Entry, Envelope, HashMap, HashSet, Instant, Mutex,
    Ordering, PendingFireKey, PendingFireState, PendingFireStates, Router, ScheduleDomainActor,
    ScheduleDomainCommand, ScheduleDomainCore, ScheduleDomainRuntime, ScheduleDomainSink,
    ScheduleDomainState, ScheduleLiveCounts, ScheduleMetrics, VecDeque, EXECUTIONS_WINDOW_MS,
};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

type PendingAckRetryMap = HashMap<crate::runtime::routing::RouteFamily, Vec<PendingFireKey>>;
pub(crate) const DEFAULT_SCHEDULE_PRELOAD_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(120);
type LivePublishCandidate = (
    crate::runtime::routing::RouteFamily,
    u64,
    String,
    crate::domains::schedule::ScheduleDeliveryMode,
    bytes::Bytes,
);

mod admin_runtime;

struct DueScanPlan {
    live_publish_candidates: Vec<LivePublishCandidate>,
    ack_retry_candidates: PendingAckRetryMap,
    snapshot_dirty: bool,
}

/// Narrow read-only surface used by metrics and administration code.
pub trait ScheduleObservability {
    fn subscription_count(&self) -> usize;
    fn schedule_count(&self) -> usize;
    fn pending_fire_count(&self) -> usize;
    fn executions_per_minute(&self) -> f64;
    fn notify_failure_count(&self) -> u64;
    fn ack_failure_count(&self) -> u64;
    fn pending_ack_retry_count(&self) -> usize;
    fn oldest_pending_claim_age_seconds(&self) -> u64;
    fn overdue_normalization_count(&self) -> u64;
}

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
        let timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX);
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
                    elapsed_ms =
                        u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "Schedule preload completed"
                );
                Ok(())
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                tracing::error!(
                    domain = "schedule",
                    timeout_ms,
                    elapsed_ms =
                        u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
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

    pub fn unsubscribe_all(&self, session_id: u64) {
        if let Err(error) = self
            .actor
            .try_send_high_priority(ScheduleDomainCommand::CleanupSession(session_id))
        {
            tracing::warn!(domain = "schedule", error = %error, "Schedule cleanup enqueue failed");
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

impl ScheduleObservability for ScheduleDomainSink {
    fn subscription_count(&self) -> usize {
        self.live_counts().subscriptions
    }

    fn schedule_count(&self) -> usize {
        self.live_counts().schedules
    }

    fn pending_fire_count(&self) -> usize {
        self.live_counts().pending_fires
    }

    fn executions_per_minute(&self) -> f64 {
        self.live_counts().executions_per_minute
    }

    fn notify_failure_count(&self) -> u64 {
        self.live_counts().notify_failures
    }

    fn ack_failure_count(&self) -> u64 {
        self.live_counts().ack_failures
    }

    fn pending_ack_retry_count(&self) -> usize {
        self.live_counts().pending_ack_retries
    }

    fn oldest_pending_claim_age_seconds(&self) -> u64 {
        self.live_counts().oldest_pending_claim_age_seconds
    }

    fn overdue_normalization_count(&self) -> u64 {
        self.live_counts().overdue_normalizations
    }
}

impl ScheduleDomainSink {
    pub fn subscription_count(&self) -> usize {
        ScheduleObservability::subscription_count(self)
    }

    pub fn schedule_count(&self) -> usize {
        ScheduleObservability::schedule_count(self)
    }

    pub fn pending_fire_count(&self) -> usize {
        ScheduleObservability::pending_fire_count(self)
    }

    pub fn executions_per_minute(&self) -> f64 {
        ScheduleObservability::executions_per_minute(self)
    }

    pub fn notify_failure_count(&self) -> u64 {
        ScheduleObservability::notify_failure_count(self)
    }

    pub fn ack_failure_count(&self) -> u64 {
        ScheduleObservability::ack_failure_count(self)
    }

    pub fn pending_ack_retry_count(&self) -> usize {
        ScheduleObservability::pending_ack_retry_count(self)
    }

    pub fn oldest_pending_claim_age_seconds(&self) -> u64 {
        ScheduleObservability::oldest_pending_claim_age_seconds(self)
    }

    pub fn overdue_normalization_count(&self) -> u64 {
        ScheduleObservability::overdue_normalization_count(self)
    }
}

impl ScheduleDomainRuntime<'_> {
    /// # Errors
    ///
    /// Returns an error when listing column families or preloading a persisted
    /// schedule actor fails.
    pub(super) fn preload_persisted_families(&self) -> Result<(), String> {
        let started_at = std::time::Instant::now();
        let column_families = self
            .core
            .store
            .list_column_families()
            .map_err(|e| format!("list schedule column families failed: {e}"))?;
        let persisted_family_count = column_families
            .iter()
            .filter(|column_family| column_family.id() != 0)
            .count();
        tracing::info!(
            domain = "schedule",
            persisted_family_count,
            "Schedule preload discovered persisted families"
        );

        let mut actors = self.core.actors.lock();
        let mut preloaded_family_count = 0_usize;
        for column_family in column_families {
            if column_family.id() == 0 {
                continue;
            }

            let family = crate::runtime::routing::RouteFamily::new(column_family.id());
            if actors.contains_key(&family) {
                continue;
            }

            let actor = crate::domains::schedule::ScheduleActor::try_new_with_storage(
                family,
                self.core.store.clone(),
                self.core.write_options,
            )?;
            actors.insert(family, actor);
            preloaded_family_count = preloaded_family_count.saturating_add(1);
            tracing::debug!(
                domain = "schedule",
                route_family = family.id(),
                preloaded_family_count,
                persisted_family_count,
                "Schedule persisted family preloaded"
            );
        }

        // Seed the rolling-window acknowledgement counter from persisted
        // last_fire_ms values so executions-per-minute survives restarts for
        // occurrences already acknowledged within the last 60 seconds.
        let now_ms = now_epoch_ms();
        let cutoff_ms = now_ms.saturating_sub(EXECUTIONS_WINDOW_MS);
        let mut deque = self.core.recent_acknowledgement_ms.lock();
        for actor in actors.values() {
            for ts in actor.last_fire_timestamps_since(cutoff_ms) {
                deque.push_back(ts);
            }
        }
        deque.make_contiguous().sort_unstable();
        drop(deque);

        drop(actors);

        self.schedule_admin_snapshot(true);
        tracing::info!(
            domain = "schedule",
            preloaded_family_count,
            persisted_family_count,
            elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
            "Schedule actor projection preload completed"
        );
        Ok(())
    }

    pub(super) fn scan_due_schedules(&self) {
        let DueScanPlan {
            live_publish_candidates,
            mut ack_retry_candidates,
            snapshot_dirty,
        } = self.claim_due();
        let had_live_handoffs =
            self.deliver_claims(live_publish_candidates, &mut ack_retry_candidates);
        let acknowledged_handoffs = self.acknowledge_delivered(ack_retry_candidates);

        if snapshot_dirty || had_live_handoffs || acknowledged_handoffs {
            self.schedule_admin_snapshot(false);
        }

        self.refresh_metrics_gauges();
    }

    pub(crate) fn force_due_scan_for_tests(&self, ready_count: usize) {
        {
            let mut actors = self.core.actors.lock();
            for actor in actors.values_mut() {
                actor.bench_prepare_scan(ready_count);
            }
        }

        self.scan_due_schedules();
        self.schedule_admin_snapshot(true);
    }

    fn claim_due(&self) -> DueScanPlan {
        let mut live_publish_candidates = Vec::new();
        let mut ack_retry_candidates = PendingAckRetryMap::new();
        let mut snapshot_dirty = false;
        let mut actors = self.core.actors.lock();
        let mut pending_ack_retries = self.core.pending_ack_retries.lock();

        for (family, actor) in actors.iter_mut() {
            if !actor.claim_due_fires().is_empty() {
                snapshot_dirty = true;
            }

            Self::collect_family_pending_fires(
                *family,
                actor,
                &mut pending_ack_retries,
                &mut live_publish_candidates,
                &mut ack_retry_candidates,
            );
        }

        DueScanPlan {
            live_publish_candidates,
            ack_retry_candidates,
            snapshot_dirty,
        }
    }

    fn collect_family_pending_fires(
        family: crate::runtime::routing::RouteFamily,
        actor: &crate::domains::schedule::ScheduleActor,
        pending_ack_retries: &mut HashMap<u64, PendingFireStates>,
        live_publish_candidates: &mut Vec<LivePublishCandidate>,
        ack_retry_candidates: &mut PendingAckRetryMap,
    ) {
        let family_id = family.as_u64();
        let pending_fires = actor.pending_claimed_occurrences_for_publish();
        let mut pending_keys = HashSet::with_capacity(pending_fires.len());
        let remove_retry_entry = {
            let tracked_retries = pending_ack_retries.entry(family_id).or_default();
            for pending_fire in pending_fires {
                let pending_key = (pending_fire.fire_ms, pending_fire.route.clone());
                pending_keys.insert(pending_key.clone());

                match tracked_retries
                    .entry(pending_key.clone())
                    .or_insert(PendingFireState::Claimed)
                {
                    PendingFireState::HandedOff => {
                        ack_retry_candidates
                            .entry(family)
                            .or_default()
                            .push(pending_key);
                        continue;
                    }
                    PendingFireState::Acknowledged => continue,
                    PendingFireState::Claimed => {}
                }

                live_publish_candidates.push((
                    family,
                    pending_fire.fire_ms,
                    pending_fire.route,
                    pending_fire.delivery_mode,
                    pending_fire.payload,
                ));
            }

            tracked_retries.retain(|pending_key, _| pending_keys.contains(pending_key));
            tracked_retries.is_empty()
        };

        if remove_retry_entry {
            pending_ack_retries.remove(&family_id);
        }
    }

    fn deliver_claims(
        &self,
        live_publish_candidates: Vec<LivePublishCandidate>,
        ack_retry_candidates: &mut PendingAckRetryMap,
    ) -> bool {
        let mut had_live_handoffs = false;

        for (family, fire_ms, route, delivery_mode, payload) in live_publish_candidates {
            let accepted = self.handle_schedule_publish(family, &route, delivery_mode, &payload);
            had_live_handoffs |= accepted;
            if !accepted {
                self.core
                    .live_publish_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
            ack_retry_candidates
                .entry(family)
                .or_default()
                .push((fire_ms, route));
        }

        had_live_handoffs
    }

    fn acknowledge_delivered(&self, ack_retry_candidates: PendingAckRetryMap) -> bool {
        let mut acknowledged_handoffs = false;

        if ack_retry_candidates.is_empty() {
            return false;
        }

        let mut actors = self.core.actors.lock();
        let mut pending_ack_retries = self.core.pending_ack_retries.lock();
        for (family, ack_candidates) in ack_retry_candidates {
            if let Some(actor) = actors.get_mut(&family) {
                acknowledged_handoffs |= self.acknowledge_family_pending_fire_claims(
                    family,
                    actor,
                    &ack_candidates,
                    &mut pending_ack_retries,
                );
            }
        }

        acknowledged_handoffs
    }

    fn acknowledge_family_pending_fire_claims(
        &self,
        family: crate::runtime::routing::RouteFamily,
        actor: &mut crate::domains::schedule::ScheduleActor,
        ack_candidates: &[PendingFireKey],
        pending_ack_retries: &mut HashMap<u64, PendingFireStates>,
    ) -> bool {
        let family_id = family.as_u64();
        let tracked = pending_ack_retries.entry(family_id).or_default();
        for pending_key in ack_candidates {
            tracked.insert(pending_key.clone(), PendingFireState::HandedOff);
        }
        match actor.ack_pending_fire_claims(ack_candidates) {
            Ok((acked, acknowledged_at_ms)) if acked > 0 => {
                for pending_key in ack_candidates {
                    tracked.insert(pending_key.clone(), PendingFireState::Acknowledged);
                }
                Self::clear_ack_retry_candidates(family_id, ack_candidates, pending_ack_retries);
                self.record_recent_acknowledgements(acked, acknowledged_at_ms);
                true
            }
            Ok(_) => {
                Self::clear_ack_retry_candidates(family_id, ack_candidates, pending_ack_retries);
                false
            }
            Err(error) => {
                self.core.ack_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    route_family = family.as_u64(),
                    error = %error,
                    "Failed to acknowledge pending schedule fires"
                );
                false
            }
        }
    }

    fn clear_ack_retry_candidates(
        family_id: u64,
        ack_candidates: &[PendingFireKey],
        pending_ack_retries: &mut HashMap<u64, PendingFireStates>,
    ) {
        let remove_retry_entry =
            if let Some(tracked_retries) = pending_ack_retries.get_mut(&family_id) {
                for pending_key in ack_candidates {
                    tracked_retries.remove(pending_key);
                }
                tracked_retries.is_empty()
            } else {
                false
            };
        if remove_retry_entry {
            pending_ack_retries.remove(&family_id);
        }
    }

    fn record_recent_acknowledgements(&self, acked: usize, acknowledged_at_ms: u64) {
        let mut deque = self.core.recent_acknowledgement_ms.lock();
        let cutoff = acknowledged_at_ms.saturating_sub(EXECUTIONS_WINDOW_MS);
        while deque.front().copied().is_some_and(|t| t < cutoff) {
            deque.pop_front();
        }
        for _ in 0..acked {
            deque.push_back(acknowledged_at_ms);
        }
    }

    pub(super) fn get_or_create_actor<'a>(
        &'a self,
        actors: &'a mut HashMap<
            crate::runtime::routing::RouteFamily,
            crate::domains::schedule::ScheduleActor,
        >,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Result<&'a mut crate::domains::schedule::ScheduleActor, String> {
        match actors.entry(route_family) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let actor = crate::domains::schedule::ScheduleActor::try_new_with_storage(
                    route_family,
                    self.core.store.clone(),
                    self.core.write_options,
                )?;
                Ok(entry.insert(actor))
            }
        }
    }

    pub(super) fn route_live_notify(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &str,
        payload: &bytes::Bytes,
    ) -> bool {
        #[cfg(test)]
        let notify_payload = crate::dispatch::protocol::schedule_codec::encode_notify(
            subscription_id,
            route,
            payload.as_ref(),
        );

        #[cfg(test)]
        let notify_ctx = FrameContext::new(
            session_id,
            crate::dispatch::protocol::frame::ChannelId::Sub,
            crate::dispatch::protocol::tlv::MessageType::new(705),
            bytes::Bytes::from(notify_payload),
            *subscriber.family(),
        );

        #[cfg(test)]
        let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);

        #[cfg(not(test))]
        let notify_envelope = Envelope::new(
            subscriber.clone(),
            crate::domains::schedule::ScheduleClientNotification::new(
                session_id,
                *subscriber.family(),
                subscription_id,
                route.to_string(),
                payload.clone(),
            ),
        );

        // Subscriber notify routing is best-effort and must not redefine the
        // schedule domain's durable acknowledgement boundary.
        self.core.router.route(notify_envelope).is_ok()
    }

    fn handle_schedule_publish(
        &self,
        family: crate::runtime::routing::RouteFamily,
        route: &str,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
        payload: &bytes::Bytes,
    ) -> bool {
        let mut families = self.core.sub_families.lock();
        let Some(state) = families.get_mut(&family.as_u64()) else {
            return false;
        };
        let mut subscription_ids = state.matching_ids(family, route);
        subscription_ids.sort_unstable();
        if subscription_ids.is_empty() {
            return false;
        }

        let cursor = state.round_robin_cursors.get(route).copied().unwrap_or(0);
        let strategy =
            DeliveryStrategy::select_recipients(delivery_mode, &subscription_ids, cursor);
        let mut any_accepted = false;
        for subscription_id in strategy.recipients() {
            let Some(subscription) = state.subscriptions.get(*subscription_id) else {
                continue;
            };
            let accepted = self.route_live_notify(
                subscription.session_id,
                subscription.subscription_id,
                &subscription.subscriber,
                route,
                payload,
            );
            any_accepted |= accepted;
            if accepted && strategy.stops_after_success() {
                let index = subscription_ids
                    .iter()
                    .position(|candidate| candidate == subscription_id)
                    .unwrap_or(cursor);
                state
                    .round_robin_cursors
                    .insert(route.to_string(), (index + 1) % subscription_ids.len());
                return true;
            }
        }
        if strategy.stops_after_success() {
            state
                .round_robin_cursors
                .insert(route.to_string(), (cursor + 1) % subscription_ids.len());
        }
        any_accepted
    }

    pub(super) fn handle_domain_publish(&self, event: &crate::runtime::DomainPublishEvent) {
        self.handle_schedule_publish(
            event.family_id,
            event.route.as_str(),
            crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            &event.payload,
        );
    }

    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.core.sub_families.lock();
        for (family, state) in families.iter_mut() {
            state.remove_session(
                crate::runtime::routing::RouteFamily::new(
                    u32::try_from(*family).unwrap_or(u32::MAX),
                ),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        tracing::debug!(
            domain = "schedule",
            session = session_id,
            "All schedule subscriptions removed for session"
        );
    }
}
