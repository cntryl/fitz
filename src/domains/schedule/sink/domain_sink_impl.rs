use super::model::{
    now_epoch_ms, schedule_admin_snapshot_due, Arc, AtomicBool, AtomicU64, Entry, Envelope,
    HashMap, HashSet, Instant, Mutex, Ordering, PendingFireKey, Router, ScheduleDomainActor,
    ScheduleDomainCommand, ScheduleDomainCore, ScheduleDomainRuntime, ScheduleDomainSink,
    ScheduleDomainState, ScheduleLiveCounts, ScheduleMetrics, ScheduleSubscription, VecDeque,
    SCHEDULE_PENDING_CLAIM_CLEANUP_INTERVAL_MS, SCHEDULE_PENDING_CLAIM_TTL_MS,
};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

type PendingAckRetryMap = HashMap<crate::runtime::routing::RouteFamily, Vec<PendingFireKey>>;
type LivePublishCandidate = (
    crate::runtime::routing::RouteFamily,
    u64,
    String,
    bytes::Bytes,
);

struct DueScanPlan {
    live_publish_candidates: Vec<LivePublishCandidate>,
    ack_retry_candidates: PendingAckRetryMap,
    snapshot_dirty: bool,
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
                pending_claim_ttl_ms: AtomicU64::new(SCHEDULE_PENDING_CLAIM_TTL_MS),
                last_pending_claim_cleanup_elapsed_ms: AtomicU64::new(0),
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
        crate::runtime::ManagedActor::spawn(
            state.core.router.clone(),
            ScheduleDomainActor::route_address(),
            ScheduleDomainActor::new(state),
            1024,
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

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.actor.stop();
    }

    /// # Errors
    ///
    /// Returns an error when listing column families or preloading a persisted
    /// schedule actor fails.
    pub fn preload_persisted_families(&self) -> Result<(), String> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(ScheduleDomainCommand::PreloadPersistedFamilies(reply_tx))
        {
            return Err(format!("schedule preload enqueue failed: {error}"));
        }

        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .map_err(|error| format!("schedule preload reply failed: {error}"))?
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

    pub fn oldest_pending_claim_age_seconds(&self) -> u64 {
        self.live_counts().oldest_pending_claim_age_seconds
    }

    pub fn overdue_normalization_count(&self) -> u64 {
        self.live_counts().overdue_normalizations
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
        self.state.runtime().bench_publish_event(event);
    }
}

impl ScheduleDomainRuntime<'_> {
    /// # Errors
    ///
    /// Returns an error when listing column families or preloading a persisted
    /// schedule actor fails.
    pub(super) fn preload_persisted_families(&self) -> Result<(), String> {
        let column_families = self
            .core
            .store
            .list_column_families()
            .map_err(|e| format!("list schedule column families failed: {e}"))?;

        let mut actors = self.core.actors.lock();
        for column_family in column_families {
            if column_family.id() == 0 {
                continue;
            }

            let family = crate::runtime::routing::RouteFamily::new(column_family.id().into());
            if actors.contains_key(&family) {
                continue;
            }

            let actor = crate::domains::schedule::ScheduleActor::try_new_with_storage(
                family,
                self.core.store.clone(),
                self.core.write_options,
            )?;
            actors.insert(family, actor);
        }

        // Seed the rolling-window acknowledgement counter from persisted
        // last_fire_ms values. This preserves the legacy
        // executions-per-minute metric across broker restarts for occurrences
        // that were already acknowledged within the last 60 seconds.
        let now_ms = now_epoch_ms();
        let cutoff_ms = now_ms.saturating_sub(60_000);
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
        Ok(())
    }

    pub(super) fn scan_due_schedules(&self) {
        let DueScanPlan {
            live_publish_candidates,
            mut ack_retry_candidates,
            snapshot_dirty,
        } = self.collect_due_scan_plan();
        let had_live_handoffs =
            self.route_live_publish_candidates(live_publish_candidates, &mut ack_retry_candidates);
        let acknowledged_handoffs = self.acknowledge_pending_fire_claims(ack_retry_candidates);

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

    fn collect_due_scan_plan(&self) -> DueScanPlan {
        let mut live_publish_candidates = Vec::new();
        let mut ack_retry_candidates = PendingAckRetryMap::new();
        let mut snapshot_dirty = false;
        let cleanup_due = self.pending_claim_cleanup_due();
        let mut actors = self.core.actors.lock();
        let mut pending_ack_retries = self.core.pending_ack_retries.lock();

        for (family, actor) in actors.iter_mut() {
            if self.cleanup_stale_pending_claims_if_due(*family, actor, cleanup_due) {
                snapshot_dirty = true;
            }

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

    fn cleanup_stale_pending_claims_if_due(
        &self,
        family: crate::runtime::routing::RouteFamily,
        actor: &mut crate::domains::schedule::ScheduleActor,
        cleanup_due: bool,
    ) -> bool {
        if !cleanup_due {
            return false;
        }

        match actor
            .cleanup_stale_pending_claims(self.core.pending_claim_ttl_ms.load(Ordering::Relaxed))
        {
            Ok(expired) if expired > 0 => {
                if let Some(metrics) = &self.core.metrics {
                    metrics.record_pending_claims_expired(expired);
                }
                true
            }
            Ok(_) => false,
            Err(error) => {
                if let Some(metrics) = &self.core.metrics {
                    metrics.record_pending_claim_cleanup_failure();
                }
                tracing::warn!(
                    route_family = family.as_u64(),
                    error = %error,
                    "Failed to cleanup stale pending schedule fires"
                );
                false
            }
        }
    }

    fn collect_family_pending_fires(
        family: crate::runtime::routing::RouteFamily,
        actor: &crate::domains::schedule::ScheduleActor,
        pending_ack_retries: &mut HashMap<u64, HashSet<PendingFireKey>>,
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

                if tracked_retries.contains(&pending_key) {
                    ack_retry_candidates
                        .entry(family)
                        .or_default()
                        .push(pending_key);
                    continue;
                }

                live_publish_candidates.push((
                    family,
                    pending_fire.fire_ms,
                    pending_fire.route,
                    pending_fire.payload,
                ));
            }

            tracked_retries.retain(|pending_key| pending_keys.contains(pending_key));
            tracked_retries.is_empty()
        };

        if remove_retry_entry {
            pending_ack_retries.remove(&family_id);
        }
    }

    fn route_live_publish_candidates(
        &self,
        live_publish_candidates: Vec<LivePublishCandidate>,
        ack_retry_candidates: &mut PendingAckRetryMap,
    ) -> bool {
        let mut had_live_handoffs = false;

        for (family, fire_ms, route, payload) in live_publish_candidates {
            let route_value = crate::runtime::routing::Route::new(route.clone());
            let event =
                crate::runtime::DomainPublishEvent::new(family, route_value.clone(), payload);
            let destination = crate::runtime::routing::RouteAddress::new(family, route_value);
            if self
                .core
                .router
                .route(Envelope::new(destination, event))
                .is_ok()
            {
                had_live_handoffs = true;
                ack_retry_candidates
                    .entry(family)
                    .or_default()
                    .push((fire_ms, route));
            } else {
                self.core
                    .live_publish_failures
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        had_live_handoffs
    }

    fn acknowledge_pending_fire_claims(&self, ack_retry_candidates: PendingAckRetryMap) -> bool {
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
                    ack_candidates,
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
        ack_candidates: Vec<PendingFireKey>,
        pending_ack_retries: &mut HashMap<u64, HashSet<PendingFireKey>>,
    ) -> bool {
        let family_id = family.as_u64();
        match actor.ack_pending_fire_claims(&ack_candidates) {
            Ok((acked, acknowledged_at_ms)) if acked > 0 => {
                Self::clear_ack_retry_candidates(family_id, &ack_candidates, pending_ack_retries);
                self.record_recent_acknowledgements(acked, acknowledged_at_ms);
                true
            }
            Ok(_) => {
                Self::clear_ack_retry_candidates(family_id, &ack_candidates, pending_ack_retries);
                false
            }
            Err(error) => {
                self.core.ack_failures.fetch_add(1, Ordering::Relaxed);
                let tracked_retries = pending_ack_retries.entry(family_id).or_default();
                for pending_key in ack_candidates {
                    tracked_retries.insert(pending_key);
                }
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
        pending_ack_retries: &mut HashMap<u64, HashSet<PendingFireKey>>,
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
        let cutoff = acknowledged_at_ms.saturating_sub(60_000);
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
        subscription: &ScheduleSubscription,
        payload: &bytes::Bytes,
    ) {
        #[cfg(test)]
        let notify_payload = crate::protocol::schedule_codec::encode_notify(
            subscription.subscription_id,
            payload.as_ref(),
        );

        #[cfg(test)]
        let notify_ctx = FrameContext::new(
            subscription.session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(705),
            bytes::Bytes::from(notify_payload),
            crate::runtime::routing::RouteFamily::from_u32(subscription.subscriber.family().id()),
        );

        #[cfg(test)]
        let notify_envelope = Envelope::new(subscription.subscriber.clone(), notify_ctx);

        #[cfg(not(test))]
        let notify_envelope = Envelope::new(
            subscription.subscriber.clone(),
            crate::domains::schedule::ScheduleClientNotification::new(
                subscription.session_id,
                crate::runtime::routing::RouteFamily::from_u32(
                    subscription.subscriber.family().id(),
                ),
                subscription.subscription_id,
                payload.clone(),
            ),
        );

        // Subscriber notify routing is best-effort and must not redefine the
        // schedule domain's durable acknowledgement boundary.
        let _ = self.core.router.route(notify_envelope);
    }

    pub(super) fn handle_domain_publish(&self, event: &crate::runtime::DomainPublishEvent) {
        let family_id = event.family_id.as_u64();
        let families = self.core.sub_families.lock();
        if let Some(state) = families.get(&family_id) {
            state.for_each_route(event.route.as_str(), |subscription| {
                self.route_live_notify(subscription, &event.payload);
            });
        }
    }

    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.core.sub_families.lock();
        for state in families.values_mut() {
            state.remove_session(session_id);
        }
        families.retain(|_, state| !state.is_empty());
        tracing::debug!(
            domain = "schedule",
            session = session_id,
            "All schedule subscriptions removed for session"
        );
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.core.sub_families.lock();
        families
            .values()
            .map(super::model::ScheduleSubscriptionSet::subscription_count)
            .sum()
    }

    pub fn schedule_count(&self) -> usize {
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(crate::domains::schedule::ScheduleActor::schedule_count)
            .sum()
    }

    pub fn pending_fire_count(&self) -> usize {
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(crate::domains::schedule::ScheduleActor::pending_fire_count)
            .sum()
    }

    /// Legacy metric name: counts acknowledged live handoffs over the last minute.
    pub fn executions_per_minute(&self) -> f64 {
        let now_ms = now_epoch_ms();
        let cutoff = now_ms.saturating_sub(60_000);
        let mut deque = self.core.recent_acknowledgement_ms.lock();
        while deque.front().copied().is_some_and(|t| t < cutoff) {
            deque.pop_front();
        }
        f64::from(u32::try_from(deque.len()).unwrap_or(u32::MAX))
    }

    pub fn notify_failure_count(&self) -> u64 {
        self.core.live_publish_failures.load(Ordering::Relaxed)
    }

    pub fn ack_failure_count(&self) -> u64 {
        self.core.ack_failures.load(Ordering::Relaxed)
    }

    pub fn pending_ack_retry_count(&self) -> usize {
        let pending_ack_retries = self.core.pending_ack_retries.lock();
        pending_ack_retries.values().map(HashSet::len).sum()
    }

    pub fn admin_pending_claims(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Vec<crate::control::admin::SchedulePendingClaimInfo> {
        let actors = self.core.actors.lock();
        actors
            .get(&route_family)
            .map(crate::domains::schedule::ScheduleActor::admin_pending_claims)
            .unwrap_or_default()
    }

    pub fn oldest_pending_claim_age_seconds(&self) -> u64 {
        let now_ms = now_epoch_ms();
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(|actor| actor.oldest_pending_claim_age_seconds(now_ms))
            .max()
            .unwrap_or(0)
    }

    pub fn overdue_normalization_count(&self) -> u64 {
        let actors = self.core.actors.lock();
        actors
            .values()
            .map(crate::domains::schedule::ScheduleActor::overdue_normalization_count)
            .sum()
    }

    pub(super) fn live_counts(&self) -> ScheduleLiveCounts {
        ScheduleLiveCounts {
            subscriptions: self.subscription_count(),
            schedules: self.schedule_count(),
            pending_fires: self.pending_fire_count(),
            executions_per_minute: self.executions_per_minute(),
            notify_failures: self.notify_failure_count(),
            ack_failures: self.ack_failure_count(),
            pending_ack_retries: self.pending_ack_retry_count(),
            oldest_pending_claim_age_seconds: self.oldest_pending_claim_age_seconds(),
            overdue_normalizations: self.overdue_normalization_count(),
        }
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(super) fn sync_admin_snapshot(&self) {
        let snapshot = {
            let actors = self.core.actors.lock();
            let mut schedules = Vec::new();
            for actor in actors.values() {
                schedules.extend(actor.admin_snapshot());
            }
            schedules
        };

        self.core.admin_read_model.replace_schedules(snapshot);
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.core.metrics {
            metrics.set_schedule_count(self.schedule_count());
            metrics.set_pending_fire_count(self.pending_fire_count());
        }
    }

    pub(super) fn pending_claim_cleanup_due(&self) -> bool {
        let now_elapsed_ms =
            u64::try_from(self.core.snapshot_epoch.elapsed().as_millis()).unwrap_or(u64::MAX);
        let mut last_elapsed_ms = self
            .core
            .last_pending_claim_cleanup_elapsed_ms
            .load(Ordering::Relaxed);

        loop {
            if last_elapsed_ms == 0 {
                let first_elapsed_ms = now_elapsed_ms.max(1);
                match self
                    .core
                    .last_pending_claim_cleanup_elapsed_ms
                    .compare_exchange(0, first_elapsed_ms, Ordering::AcqRel, Ordering::Relaxed)
                {
                    Ok(_) => return true,
                    Err(observed) => {
                        last_elapsed_ms = observed;
                        continue;
                    }
                }
            }

            if now_elapsed_ms.saturating_sub(last_elapsed_ms)
                < SCHEDULE_PENDING_CLAIM_CLEANUP_INTERVAL_MS
            {
                return false;
            }

            match self
                .core
                .last_pending_claim_cleanup_elapsed_ms
                .compare_exchange(
                    last_elapsed_ms,
                    now_elapsed_ms,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                Ok(_) => return true,
                Err(observed) => last_elapsed_ms = observed,
            }
        }
    }

    pub(super) fn schedule_response_is_failure(
        response: &crate::domains::schedule::ScheduleResponse,
    ) -> bool {
        matches!(
            response,
            crate::domains::schedule::ScheduleResponse::Error(_)
        )
    }

    pub(super) fn schedule_admin_snapshot(&self, force: bool) {
        self.core.snapshot_dirty.store(true, Ordering::Relaxed);
        self.maybe_sync_admin_snapshot(force);
    }

    pub(super) fn maybe_sync_admin_snapshot(&self, force: bool) {
        #[cfg(feature = "bench-no-snapshot")]
        if !force {
            return;
        }

        let now_elapsed_us =
            u64::try_from(self.core.snapshot_epoch.elapsed().as_micros()).unwrap_or(u64::MAX);
        let last_snapshot_elapsed_us = self.core.last_snapshot_elapsed_us.load(Ordering::Relaxed);
        let snapshot_dirty = self.core.snapshot_dirty.load(Ordering::Relaxed);

        if !schedule_admin_snapshot_due(
            snapshot_dirty,
            force,
            now_elapsed_us,
            last_snapshot_elapsed_us,
        ) {
            return;
        }

        if self
            .core
            .snapshot_syncing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        if !self.core.snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.core.snapshot_syncing.store(false, Ordering::Release);
            return;
        }

        self.sync_admin_snapshot();
        self.core.last_snapshot_elapsed_us.store(
            u64::try_from(self.core.snapshot_epoch.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.core.snapshot_syncing.store(false, Ordering::Release);
    }

    pub(crate) fn refresh_admin_snapshot_if_dirty(&self) {
        self.maybe_sync_admin_snapshot(true);
    }

    #[doc(hidden)]
    pub fn bench_publish_event(&self, event: &crate::runtime::DomainPublishEvent) {
        self.handle_domain_publish(event);
    }
}
