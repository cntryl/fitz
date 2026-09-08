//! Public `RpcDomainSink` API and actor identity/lifecycle queries.

use super::state_model::{
    Duration, Instant, Ordering, RpcDomainCommand, RpcDomainRuntime, RpcDomainSink, RpcLiveCounts,
};
#[cfg(test)]
use super::state_model::{
    Route, RouteAddress, RpcPendingRequest, RpcQueuedRequest, RpcSessionCleanupResult, RpcWorker,
    RpcWorkerCleanupResult,
};
use crate::runtime::routing::RouteFamily;

impl RpcDomainSink {
    pub(super) fn runtime(&self) -> RpcDomainRuntime<'_> {
        RpcDomainRuntime {
            core: &self.core,
            active: &self.active,
        }
    }

    fn control_targets(&self) -> Vec<Option<RouteFamily>> {
        self.family_families.iter().copied().map(Some).collect()
    }

    fn primary_control_target(&self) -> Option<RouteFamily> {
        self.family_families.first().copied()
    }

    fn try_send_control(
        &self,
        family: Option<RouteFamily>,
        command: RpcDomainCommand,
    ) -> Result<(), String> {
        let family = family.ok_or_else(|| "RPC family target is missing".to_string())?;
        self.family_runtime
            .try_enqueue(family, crate::runtime::FamilyActorLane::Control, command)
            .map_err(|error| error.to_string())
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.family_runtime.stop();
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(crate) fn timeout_sweep_interval(&self) -> Duration {
        self.runtime().timeout_sweep_interval()
    }

    pub(crate) fn expire_timed_out_requests(&self) {
        for family in self.control_targets() {
            if let Err(error) = self.try_send_control(
                family,
                RpcDomainCommand::ExpireTimedOutRequestsAt(Instant::now(), None),
            ) {
                tracing::warn!(
                    domain = "rpc",
                    family = family.map(|target| target.id()),
                    error = %error,
                    "RPC timeout sweep enqueue failed"
                );
            }
        }
    }

    #[cfg(test)]
    pub(super) fn expire_timed_out_requests_at(&self, now: Instant) {
        for family in self.control_targets() {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if let Err(error) = self.try_send_control(
                family,
                RpcDomainCommand::ExpireTimedOutRequestsAt(now, Some(reply_tx)),
            ) {
                tracing::warn!(
                    domain = "rpc",
                    family = family.map(|target| target.id()),
                    error = %error,
                    "RPC timeout sweep enqueue failed"
                );
                continue;
            }
            let _ = reply_rx.recv_timeout(Duration::from_secs(1));
        }
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.family_runtime.is_running()
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.family_runtime.managed_actor_health_snapshot()
    }

    /// Panic every provisioned family's handler. Used by the opt-in failpoint and tests to
    /// drive the pool to full exhaustion; a single family's panic must never
    /// be conflated with domain-wide health, so covering every family here
    /// is required to actually observe pool-wide fail-closed behavior.
    pub(crate) fn panic_actor_for_failpoint(&self) {
        for family in self.control_targets() {
            let _ = self.try_send_control(family, RpcDomainCommand::PanicForFailpoint);
        }
    }

    pub(crate) fn panic_family_actor_for_failpoint(&self, family: RouteFamily) {
        let _ = self.try_send_control(Some(family), RpcDomainCommand::PanicForFailpoint);
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.family_runtime.stop();
    }

    #[cfg(test)]
    pub(super) fn block_family_actor_for_tests(
        &self,
        family: RouteFamily,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    ) {
        self.try_send_control(
            Some(family),
            RpcDomainCommand::BlockForTests(entered, release),
        )
        .expect("enqueue RPC family actor test block");
    }

    #[cfg(test)]
    pub(super) fn register_registration_for_tests(&self, registration: RpcWorker) {
        self.core.state.lock().register_registration(registration);
    }

    #[cfg(test)]
    pub(super) fn track_pending_request_for_tests(
        &self,
        correlation_id: uuid::Uuid,
        pending: RpcPendingRequest,
    ) {
        let mut state = self.core.state.lock();
        let previous_count = state.live_request_count();
        let current_count = state.pending.track_pending_for_family(
            pending.dispatch_info.family,
            correlation_id,
            pending,
        );
        drop(state);

        if self.core.enforce_global_pending_count && current_count > previous_count {
            self.core
                .global_pending_count
                .fetch_add(current_count - previous_count, Ordering::AcqRel);
        }
    }

    #[cfg(test)]
    pub(super) fn queue_request_for_tests(
        &self,
        correlation_id: uuid::Uuid,
        queued: RpcQueuedRequest,
    ) {
        self.core.state.lock().queue_request(correlation_id, queued);
    }

    #[cfg(test)]
    pub(super) fn live_request_count_for_tests(&self) -> usize {
        self.core.state.lock().live_request_count()
    }

    #[cfg(test)]
    pub(super) fn pending_table_len_for_tests(&self) -> usize {
        self.core.state.lock().pending.len()
    }

    #[cfg(test)]
    pub(super) fn queued_request_count_for_tests(&self) -> usize {
        self.core.state.lock().queued.len()
    }

    #[cfg(test)]
    pub(super) fn route_queued_len_for_tests(&self, route: &Route) -> usize {
        let mut state = self.core.state.lock();
        state
            .route_state(route)
            .map_or(0, |route_state| route_state.queued_len())
    }

    pub fn worker_count(&self) -> usize {
        self.live_counts().workers
    }

    pub fn pending_request_count(&self) -> usize {
        self.live_counts().pending_requests
    }

    fn live_counts(&self) -> RpcLiveCounts {
        let mut total = RpcLiveCounts::default();
        for family in self.control_targets() {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if let Err(error) =
                self.try_send_control(family, RpcDomainCommand::ReadLiveCounts(reply_tx))
            {
                tracing::warn!(
                    domain = "rpc",
                    family = family.map(|target| target.id()),
                    error = %error,
                    "RPC live-count query enqueue failed"
                );
                continue;
            }
            if let Ok(counts) = reply_rx.recv_timeout(Duration::from_secs(1)) {
                total.workers = total.workers.saturating_add(counts.workers);
                total.pending_requests = total
                    .pending_requests
                    .saturating_add(counts.pending_requests);
            }
        }
        total
    }

    #[cfg(test)]
    pub(super) fn sync_admin_snapshot(&self) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.try_send_control(
            self.primary_control_target(),
            RpcDomainCommand::SyncAdminSnapshot(Some(reply_tx)),
        ) {
            tracing::warn!(domain = "rpc", error = %error, "RPC admin snapshot enqueue failed");
            return;
        }

        let _ = reply_rx.recv_timeout(Duration::from_secs(1));
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        if let Err(error) = self.try_send_control(
            self.primary_control_target(),
            RpcDomainCommand::RefreshAdminSnapshotIfDirty(None),
        ) {
            tracing::warn!(domain = "rpc", error = %error, "RPC admin snapshot refresh enqueue failed");
        }
    }

    #[cfg(test)]
    pub(super) fn apply_session_cleanup(&self, session_id: u64) -> RpcSessionCleanupResult {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.try_send_control(
            self.primary_control_target(),
            RpcDomainCommand::ApplySessionCleanupForTests(session_id, reply_tx),
        ) {
            tracing::warn!(domain = "rpc", error = %error, "RPC session cleanup enqueue failed");
            return RpcSessionCleanupResult::default();
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(super) fn apply_worker_unsubscribe(
        &self,
        worker_addr: &RouteAddress,
        session_id: u64,
    ) -> RpcWorkerCleanupResult {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.try_send_control(
            Some(*worker_addr.family()),
            RpcDomainCommand::ApplyWorkerUnsubscribeForTests(
                worker_addr.clone(),
                session_id,
                reply_tx,
            ),
        ) {
            tracing::warn!(domain = "rpc", error = %error, "RPC worker unsubscribe enqueue failed");
            return RpcWorkerCleanupResult::default();
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or_default()
    }
}
