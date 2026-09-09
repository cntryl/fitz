//! Public `RpcDomain` API and actor identity/lifecycle queries.

use super::state_model::{RpcDomain, RpcDomainCommand, RpcLiveCounts};
#[cfg(test)]
use super::state_model::{
    RpcFamilyState, RpcPendingRequest, RpcQueuedDispatch, RpcQueuedRequest,
    RpcSessionCleanupResult, RpcState, RpcWorker, RpcWorkerCleanupResult,
};
use crate::runtime::routing::RouteFamily;
#[cfg(test)]
use crate::runtime::routing::{Route, RouteAddress};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

impl RpcDomain {
    fn control_targets(&self) -> Vec<Option<RouteFamily>> {
        self.family_families.iter().copied().map(Some).collect()
    }

    #[cfg(test)]
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
        super::state_model::rpc_timeout_sweep_interval(self.config.request_timeout)
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

    pub(crate) fn family_health_snapshot(
        &self,
    ) -> crate::runtime::family_actor_pool::FamilyActorPoolHealthSnapshot {
        self.family_runtime.health_snapshot()
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
        let family = *registration.addr.family();
        self.inspect_family_for_tests(family, move |state| {
            state.state.register_registration(registration);
        });
    }

    #[cfg(test)]
    pub(super) fn track_pending_request_for_tests(
        &self,
        correlation_id: uuid::Uuid,
        pending: RpcPendingRequest,
    ) {
        let family = pending.dispatch_info.family;
        self.inspect_family_for_tests(family, move |family_state| {
            let previous_count = family_state.state.live_request_count();
            let current_count = family_state.state.pending.track_pending_for_family(
                family,
                correlation_id,
                pending,
            );
            if current_count > previous_count {
                family_state
                    .global_pending_count
                    .fetch_add(current_count - previous_count, Ordering::AcqRel);
            }
        });
    }

    #[cfg(test)]
    pub(super) fn queue_request_for_tests(
        &self,
        correlation_id: uuid::Uuid,
        queued: RpcQueuedRequest,
    ) {
        let family = queued.request.family_id;
        self.inspect_family_for_tests(family, move |state| {
            state.state.queue_request(correlation_id, queued);
        });
    }

    #[cfg(test)]
    pub(super) fn live_request_count_for_tests(&self) -> usize {
        self.inspect_primary_state_for_tests(|state| state.live_request_count())
    }

    #[cfg(test)]
    pub(super) fn pending_table_len_for_tests(&self) -> usize {
        self.inspect_primary_state_for_tests(|state| state.pending.len())
    }

    #[cfg(test)]
    pub(super) fn queued_request_count_for_tests(&self) -> usize {
        self.inspect_primary_state_for_tests(|state| state.queued.len())
    }

    #[cfg(test)]
    pub(super) fn route_queued_len_for_tests(&self, route: &Route) -> usize {
        let route = route.clone();
        self.inspect_primary_state_for_tests(move |state| {
            state
                .route_state(&route)
                .map_or(0, |route_state| route_state.queued_len())
        })
    }

    #[cfg(test)]
    pub(super) fn inspect_primary_state_for_tests<T>(
        &self,
        inspect: impl FnOnce(&mut RpcState) -> T + Send + 'static,
    ) -> T
    where
        T: Send + 'static,
    {
        let family = self
            .family_families
            .first()
            .copied()
            .expect("RPC test family inventory");
        self.inspect_family_for_tests(family, move |state| inspect(&mut state.state))
    }

    #[cfg(test)]
    fn inspect_family_for_tests<T>(
        &self,
        family: RouteFamily,
        inspect: impl FnOnce(&mut RpcFamilyState) -> T + Send + 'static,
    ) -> T
    where
        T: Send + 'static,
    {
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);
        let (done_tx, _done_rx) = crossbeam_channel::bounded(1);
        self.try_send_control(
            Some(family),
            RpcDomainCommand::InspectForTests(
                Box::new(move |state| {
                    let _ = result_tx.send(inspect(state));
                }),
                done_tx,
            ),
        )
        .expect("enqueue RPC family-state inspection");
        result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("receive RPC family-state inspection")
    }

    #[cfg(test)]
    pub(super) fn forward_queued_dispatch_for_tests(&self, dispatch: RpcQueuedDispatch) {
        let family = dispatch.request.family_id;
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.try_send_control(
            Some(family),
            RpcDomainCommand::ForwardQueuedDispatchForTests(dispatch, reply_tx),
        )
        .expect("enqueue RPC queued dispatch");
        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("complete RPC queued dispatch");
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
        for family in self.control_targets() {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if let Err(error) =
                self.try_send_control(family, RpcDomainCommand::SyncAdminSnapshot(Some(reply_tx)))
            {
                tracing::warn!(domain = "rpc", error = %error, "RPC admin snapshot enqueue failed");
                continue;
            }
            let _ = reply_rx.recv_timeout(Duration::from_secs(1));
        }
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        for family in self.control_targets() {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if let Err(error) = self.try_send_control(
                family,
                RpcDomainCommand::RefreshAdminSnapshotIfDirty(Some(reply_tx)),
            ) {
                tracing::warn!(domain = "rpc", error = %error, "RPC admin snapshot refresh enqueue failed");
                continue;
            }
            let _ = reply_rx.recv_timeout(Duration::from_secs(1));
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
