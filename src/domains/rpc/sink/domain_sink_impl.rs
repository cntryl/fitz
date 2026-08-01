use super::state_model::{
    rpc_admin_snapshot_due, rpc_timeout_sweep_interval, Arc, AtomicBool, DeliveryError, Duration,
    Envelope, Instant, Ordering, Route, RouteAddress, RpcDomainActor, RpcDomainCommand,
    RpcDomainCore, RpcDomainRuntime, RpcDomainSink, RpcLiveCounts, RpcPendingErrorDelivery,
    RpcPendingRequest, RpcQueuedDispatch, RpcSessionCleanupResult, RpcWorkerCleanupResult,
    RpcWorkerDispatch, RPC_BACKPRESSURE_ERROR, RPC_TIMEOUT_ERROR, RPC_WORKER_NOT_FOUND_ERROR,
};
#[cfg(test)]
use super::state_model::{RpcQueuedRequest, RpcWorker};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
#[cfg(not(test))]
use crate::domains::rpc::{
    RpcClientForwardedResponse, RpcClientForwardedResponseBody, RpcWorkerRequestDelivery,
};
use crate::runtime::routing::RouteFamily;

impl RpcDomainActor {
    pub(super) fn new(core: Arc<RpcDomainCore>, active: Arc<AtomicBool>) -> Self {
        Self { core, active }
    }

    pub(super) fn route_address() -> RouteAddress {
        RouteAddress::new(RouteFamily::new(0), Route::new("internal://domain/rpc"))
    }

    pub(super) fn runtime(&self) -> RpcDomainRuntime<'_> {
        RpcDomainRuntime {
            core: &self.core,
            active: &self.active,
        }
    }
}

impl RpcDomainSink {
    pub(super) fn runtime(&self) -> RpcDomainRuntime<'_> {
        RpcDomainRuntime {
            core: &self.core,
            active: &self.active,
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
        if let Some(runtime) = self.family_runtime.as_ref() {
            runtime.stop();
        }
        self.actor.stop();
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(crate) fn timeout_sweep_interval(&self) -> Duration {
        self.runtime().timeout_sweep_interval()
    }

    pub(crate) fn expire_timed_out_requests(&self) {
        if let (Some(runtime), Some(families)) =
            (self.family_runtime.as_ref(), self.family_families.as_ref())
        {
            for family in families.iter().copied() {
                if let Err(error) = runtime.try_enqueue(
                    family,
                    crate::runtime::FamilyActorLane::Control,
                    RpcDomainCommand::ExpireTimedOutRequestsAt(Instant::now(), None),
                ) {
                    tracing::warn!(
                        domain = "rpc",
                        family = family.id(),
                        error = %error,
                        "RPC timeout sweep enqueue failed"
                    );
                }
            }
            return;
        }

        if let Err(error) =
            self.actor
                .try_send_high_priority(RpcDomainCommand::ExpireTimedOutRequestsAt(
                    Instant::now(),
                    None,
                ))
        {
            tracing::warn!(domain = "rpc", error = %error, "RPC timeout sweep enqueue failed");
        }
    }

    #[cfg(test)]
    pub(super) fn expire_timed_out_requests_at(&self, now: Instant) {
        if let (Some(runtime), Some(families)) =
            (self.family_runtime.as_ref(), self.family_families.as_ref())
        {
            for family in families.iter().copied() {
                let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
                if let Err(error) = runtime.try_enqueue(
                    family,
                    crate::runtime::FamilyActorLane::Control,
                    RpcDomainCommand::ExpireTimedOutRequestsAt(now, Some(reply_tx)),
                ) {
                    tracing::warn!(
                        domain = "rpc",
                        family = family.id(),
                        error = %error,
                        "RPC timeout sweep enqueue failed"
                    );
                    continue;
                }
                let _ = reply_rx.recv_timeout(Duration::from_secs(1));
            }
            return;
        }

        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) =
            self.actor
                .try_send_high_priority(RpcDomainCommand::ExpireTimedOutRequestsAt(
                    now,
                    Some(reply_tx),
                ))
        {
            tracing::warn!(domain = "rpc", error = %error, "RPC timeout sweep enqueue failed");
            return;
        }

        let _ = reply_rx.recv_timeout(Duration::from_secs(1));
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.actor.is_running()
            && self
                .family_runtime
                .as_ref()
                .is_none_or(crate::runtime::FamilyActorPoolRuntime::is_running)
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.family_runtime.as_ref().map_or_else(
            || self.actor.health_snapshot(),
            crate::runtime::FamilyActorPoolRuntime::health_snapshot,
        )
    }

    #[cfg(test)]
    pub(crate) fn panic_actor_for_tests(&self) {
        if let (Some(runtime), Some(family)) = (
            self.family_runtime.as_ref(),
            self.family_families
                .as_ref()
                .and_then(|families| families.first()),
        ) {
            let _ = runtime.try_enqueue(
                *family,
                crate::runtime::FamilyActorLane::Control,
                RpcDomainCommand::PanicForTests,
            );
            return;
        }
        let _ = self
            .actor
            .try_send_high_priority(RpcDomainCommand::PanicForTests);
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.actor.stop();
    }

    #[cfg(test)]
    pub(super) fn register_worker_for_tests(&self, worker: RpcWorker) {
        self.core.state.lock().register_worker(worker);
    }

    #[cfg(test)]
    pub(super) fn track_pending_request_for_tests(
        &self,
        correlation_id: uuid::Uuid,
        pending: RpcPendingRequest,
    ) {
        self.core.state.lock().pending.track_pending_for_family(
            pending.family,
            correlation_id,
            pending,
        );
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
        if let (Some(runtime), Some(families)) =
            (self.family_runtime.as_ref(), self.family_families.as_ref())
        {
            let mut total = RpcLiveCounts::default();
            for family in families.iter().copied() {
                let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
                if let Err(error) = runtime.try_enqueue(
                    family,
                    crate::runtime::FamilyActorLane::Control,
                    RpcDomainCommand::ReadLiveCounts(reply_tx),
                ) {
                    tracing::warn!(
                        domain = "rpc",
                        family = family.id(),
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
            return total;
        }

        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(RpcDomainCommand::ReadLiveCounts(reply_tx))
        {
            tracing::warn!(domain = "rpc", error = %error, "RPC live-count query enqueue failed");
            return RpcLiveCounts::default();
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(super) fn sync_admin_snapshot(&self) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let (Some(runtime), Some(family)) = (
            self.family_runtime.as_ref(),
            self.family_families
                .as_ref()
                .and_then(|families| families.first()),
        ) {
            if let Err(error) = runtime.try_enqueue(
                *family,
                crate::runtime::FamilyActorLane::Control,
                RpcDomainCommand::SyncAdminSnapshot(Some(reply_tx)),
            ) {
                tracing::warn!(domain = "rpc", error = %error, "RPC admin snapshot enqueue failed");
                return;
            }
            let _ = reply_rx.recv_timeout(Duration::from_secs(1));
            return;
        }

        if let Err(error) = self
            .actor
            .try_send_high_priority(RpcDomainCommand::SyncAdminSnapshot(Some(reply_tx)))
        {
            tracing::warn!(domain = "rpc", error = %error, "RPC admin snapshot enqueue failed");
            return;
        }

        let _ = reply_rx.recv_timeout(Duration::from_secs(1));
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        if let (Some(runtime), Some(family)) = (
            self.family_runtime.as_ref(),
            self.family_families
                .as_ref()
                .and_then(|families| families.first()),
        ) {
            if let Err(error) = runtime.try_enqueue(
                *family,
                crate::runtime::FamilyActorLane::Control,
                RpcDomainCommand::RefreshAdminSnapshotIfDirty(None),
            ) {
                tracing::warn!(domain = "rpc", error = %error, "RPC admin snapshot refresh enqueue failed");
            }
            return;
        }

        if let Err(error) = self
            .actor
            .try_send_high_priority(RpcDomainCommand::RefreshAdminSnapshotIfDirty(None))
        {
            tracing::warn!(domain = "rpc", error = %error, "RPC admin snapshot refresh enqueue failed");
        }
    }

    #[cfg(test)]
    pub(super) fn apply_session_cleanup(&self, session_id: u64) -> RpcSessionCleanupResult {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) =
            self.actor
                .try_send_high_priority(RpcDomainCommand::ApplySessionCleanupForTests(
                    session_id, reply_tx,
                ))
        {
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
        if let Err(error) =
            self.actor
                .try_send_high_priority(RpcDomainCommand::ApplyWorkerUnsubscribeForTests(
                    worker_addr.clone(),
                    session_id,
                    reply_tx,
                ))
        {
            tracing::warn!(domain = "rpc", error = %error, "RPC worker unsubscribe enqueue failed");
            return RpcWorkerCleanupResult::default();
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or_default()
    }
}

impl RpcDomainRuntime<'_> {
    fn u64_to_usize_saturating(value: u64) -> usize {
        usize::try_from(value).unwrap_or(usize::MAX)
    }

    fn elapsed_us_saturating(start: Instant) -> u64 {
        start.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
    }

    pub(super) fn release_global_pending(&self, count: usize) {
        if count == 0 {
            return;
        }
        let _ = self.global_pending_count.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| Some(current.saturating_sub(count)),
        );
    }

    pub(crate) fn timeout_sweep_interval(&self) -> Duration {
        rpc_timeout_sweep_interval(self.request_timeout)
    }

    pub(super) fn live_counts(&self) -> RpcLiveCounts {
        let state = self.state.lock();
        let workers = state.registration_count();
        RpcLiveCounts {
            workers,
            pending_requests: state.live_request_count(),
        }
    }

    pub(super) fn counter_inc(&self, name: &str) {
        if let Some(ref metrics) = self.metrics {
            metrics.counter_inc(name);
        }
    }

    pub(super) fn counter_add(&self, name: &str, amount: u64) {
        if let Some(ref metrics) = self.metrics {
            metrics.counter_add(name, amount);
        }
    }

    pub(super) fn gauge_set(&self, name: &str, value: u64) {
        if let Some(ref metrics) = self.metrics {
            metrics.gauge_set(name, value);
            if name == "rpc_pending_requests" {
                metrics.set_pending_request_count(Self::u64_to_usize_saturating(value));
            }
        }
    }

    pub(super) fn histogram_observe_us(&self, name: &str, value_us: u64) {
        if let Some(ref metrics) = self.metrics {
            metrics.histogram_observe_us(name, value_us);
        }
    }

    pub(super) fn histogram_observe_elapsed_us(&self, name: &str, start: Instant) {
        self.histogram_observe_us(name, Self::elapsed_us_saturating(start));
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            let counts = self.core.aggregate_live_counts();
            metrics.set_worker_count(counts.workers);
            metrics.set_pending_request_count(counts.pending_requests);
        }
    }

    pub(super) fn expire_timed_out_requests_inline_if_due(&self) {
        let now_elapsed_us = Self::elapsed_us_saturating(self.snapshot_epoch);
        let interval_us = self
            .timeout_sweep_interval()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX);
        let last_elapsed_us = self.last_inline_timeout_elapsed_us.load(Ordering::Relaxed);

        if now_elapsed_us.saturating_sub(last_elapsed_us) < interval_us {
            return;
        }

        if self
            .last_inline_timeout_elapsed_us
            .compare_exchange(
                last_elapsed_us,
                now_elapsed_us,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.expire_timed_out_requests_at(Instant::now());
        }
    }

    pub(super) fn expire_timed_out_requests_at(&self, now: Instant) {
        let timeout_result = {
            let mut state = self.state.lock();
            state.expire_timed_out(now)
        };

        if timeout_result.removed_pending == 0 {
            return;
        }

        self.release_global_pending(timeout_result.removed_pending);
        let timeout_delivery_count = timeout_result.timeout_deliveries.len();
        self.gauge_set("rpc_pending_requests", timeout_result.pending_len as u64);
        self.counter_add(
            "rpc_request_timeouts_total",
            timeout_result.removed_pending as u64,
        );
        self.counter_add(
            "rpc_cleanup_pending_removed_total",
            timeout_result.removed_pending as u64,
        );
        if timeout_result.closed_caller_drops > 0 {
            self.counter_add(
                "rpc_timeout_errors_dropped_total",
                timeout_result.closed_caller_drops as u64,
            );
            self.counter_add(
                "rpc_responses_dropped_closed_caller_total",
                timeout_result.closed_caller_drops as u64,
            );
        }
        self.schedule_admin_snapshot(false);
        self.dispatch_all_queued_requests();

        tracing::debug!(
            domain = "rpc",
            removed_pending = timeout_result.removed_pending,
            delivered_timeouts = timeout_delivery_count,
            closed_caller_drops = timeout_result.closed_caller_drops,
            pending_len = timeout_result.pending_len,
            "RPC request timeout sweep applied"
        );

        self.forward_pending_error_deliveries(
            timeout_result.timeout_deliveries,
            crate::dispatch::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
            RPC_TIMEOUT_ERROR,
            "rpc_timeout_errors_forwarded_total",
            "rpc_timeout_errors_dropped_total",
        );
    }

    pub(super) fn remove_pending_request_for_family(
        &self,
        family: crate::runtime::routing::RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> Option<(RpcPendingRequest, usize)> {
        let removed = {
            let mut state = self.state.lock();
            state.remove_pending_request_for_family(family, correlation_id)
        };

        self.gauge_set(
            "rpc_pending_requests",
            removed.as_ref().map_or_else(
                || self.pending_request_count() as u64,
                |(_, pending_len)| *pending_len as u64,
            ),
        );
        if removed.is_some() {
            self.release_global_pending(1);
        }
        removed
    }

    pub(super) fn apply_session_cleanup(&self, session_id: u64) -> RpcSessionCleanupResult {
        let cleanup_result = {
            let mut state = self.state.lock();
            state.cleanup_session(session_id)
        };

        self.gauge_set("rpc_pending_requests", cleanup_result.pending_len as u64);
        self.release_global_pending(cleanup_result.removed_pending);
        if cleanup_result.removed_workers > 0 {
            self.counter_add(
                "rpc_cleanup_workers_removed_total",
                cleanup_result.removed_workers as u64,
            );
        }
        if cleanup_result.detached_callers > 0 {
            self.counter_add(
                "rpc_cleanup_callers_detached_total",
                cleanup_result.detached_callers as u64,
            );
        }
        if cleanup_result.removed_pending > 0 {
            self.counter_add(
                "rpc_cleanup_pending_removed_total",
                cleanup_result.removed_pending as u64,
            );
        }
        if cleanup_result.removed_workers > 0
            || cleanup_result.detached_callers > 0
            || cleanup_result.removed_pending > 0
        {
            self.schedule_admin_snapshot(false);
        }
        self.refresh_metrics_gauges();

        tracing::debug!(
            domain = "rpc",
            session_id,
            removed_workers = cleanup_result.removed_workers,
            detached_callers = cleanup_result.detached_callers,
            removed_pending = cleanup_result.removed_pending,
            pending_len = cleanup_result.pending_len,
            "RPC session cleanup applied"
        );

        cleanup_result
    }

    pub(super) fn apply_worker_unsubscribe(
        &self,
        worker_addr: &RouteAddress,
        session_id: u64,
    ) -> RpcWorkerCleanupResult {
        let cleanup_result = {
            let mut state = self.state.lock();
            state.unregister_worker(worker_addr, session_id)
        };

        self.gauge_set("rpc_pending_requests", cleanup_result.pending_len as u64);
        self.release_global_pending(cleanup_result.removed_pending);
        if cleanup_result.removed_workers > 0 {
            self.counter_add(
                "rpc_cleanup_workers_removed_total",
                cleanup_result.removed_workers as u64,
            );
        }
        if cleanup_result.removed_pending > 0 {
            self.counter_add(
                "rpc_cleanup_pending_removed_total",
                cleanup_result.removed_pending as u64,
            );
        }
        if cleanup_result.removed_workers > 0 || cleanup_result.removed_pending > 0 {
            self.schedule_admin_snapshot(false);
        }
        self.refresh_metrics_gauges();

        tracing::debug!(
            domain = "rpc",
            worker = worker_addr.route().as_str(),
            session_id,
            removed_workers = cleanup_result.removed_workers,
            removed_pending = cleanup_result.removed_pending,
            pending_len = cleanup_result.pending_len,
            "RPC worker cleanup applied"
        );

        cleanup_result
    }

    pub(super) fn forward_pending_error_deliveries(
        &self,
        error_deliveries: Vec<RpcPendingErrorDelivery>,
        error_code: u16,
        error_message: &'static str,
        forwarded_counter: &str,
        dropped_counter: &str,
    ) {
        if error_deliveries.is_empty() {
            return;
        }

        for delivery in error_deliveries {
            let correlation_id = delivery.correlation_id;

            #[cfg(test)]
            let response_envelope = {
                let mut error_body_encoder =
                    crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(96);
                let mut response_encoder =
                    crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(128);
                let error_body = crate::dispatch::protocol::rpc_codec::encode_error_body_into(
                    error_code,
                    error_message,
                    &mut error_body_encoder,
                );
                let error_response = crate::domains::rpc::protocol::RpcResponse::single(
                    correlation_id,
                    bytes::Bytes::from(error_body),
                );
                let encoded_response =
                    crate::dispatch::protocol::rpc_codec::encode_response_message_into(
                        &error_response,
                        &mut response_encoder,
                    );
                let response_ctx = FrameContext::new(
                    delivery.caller_session_id,
                    crate::dispatch::protocol::frame::ChannelId::Rpc,
                    crate::dispatch::protocol::tlv::MessageType::new(303),
                    bytes::Bytes::from(encoded_response),
                    *delivery.caller_inbox_addr.family(),
                );
                Envelope::new(delivery.caller_inbox_addr, response_ctx)
            };

            #[cfg(not(test))]
            let response_envelope = {
                let route_family = *delivery.caller_inbox_addr.family();
                Envelope::new(
                    delivery.caller_inbox_addr,
                    RpcClientForwardedResponse::new(
                        delivery.caller_session_id,
                        route_family,
                        RpcClientForwardedResponseBody::TerminalError {
                            correlation_id,
                            code: error_code,
                            message: error_message,
                        },
                    ),
                )
            };

            if let Err(error) = self.router.route(response_envelope) {
                self.counter_inc(dropped_counter);
                tracing::warn!(
                    domain = "rpc",
                    correlation_id = %correlation_id,
                    error_code,
                    error = ?error,
                    "Failed to forward RPC terminal error to requester"
                );
            } else {
                self.counter_inc(forwarded_counter);
            }
        }
    }

    pub(super) fn forward_worker_disconnect_errors(
        &self,
        disconnect_deliveries: Vec<RpcPendingErrorDelivery>,
    ) {
        self.forward_pending_error_deliveries(
            disconnect_deliveries,
            crate::dispatch::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
            RPC_WORKER_NOT_FOUND_ERROR,
            "rpc_worker_disconnect_errors_forwarded_total",
            "rpc_worker_disconnect_errors_dropped_total",
        );
    }

    pub(super) fn pending_request_count(&self) -> usize {
        self.live_counts().pending_requests
    }

    pub(super) fn forward_request_to_worker(
        &self,
        req: &crate::domains::rpc::protocol::RpcRequest,
        worker: &RpcWorkerDispatch,
    ) -> Result<(), crate::runtime::RouteError> {
        #[cfg(test)]
        let request_envelope = {
            let mut payload_encoder =
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let request_bytes = crate::dispatch::protocol::rpc_codec::encode_request_into(
                req,
                &mut payload_encoder,
            );
            let request_ctx = FrameContext::new(
                worker.session_id,
                crate::dispatch::protocol::frame::ChannelId::Rpc,
                crate::dispatch::protocol::tlv::MessageType::new(302),
                bytes::Bytes::from(request_bytes),
                *worker.addr.family(),
            );
            Envelope::new(worker.inbox_addr.clone(), request_ctx)
        };

        #[cfg(not(test))]
        let request_envelope = Envelope::new(
            worker.inbox_addr.clone(),
            RpcWorkerRequestDelivery::new(worker.session_id, *worker.addr.family(), req.clone()),
        );

        self.router.route(request_envelope)
    }

    pub(super) fn dispatch_queued_requests_for_family(
        &self,
        family: crate::runtime::routing::RouteFamily,
    ) {
        let mut snapshot_dirty = false;
        loop {
            let next_dispatch = self.state.lock().next_ready_dispatch_for_family(family);
            let Some(dispatch) = next_dispatch else {
                break;
            };
            self.forward_queued_dispatch(&dispatch);
            snapshot_dirty = true;
        }
        if snapshot_dirty {
            self.schedule_admin_snapshot(false);
        }
    }

    pub(super) fn forward_queued_dispatch(&self, dispatch: &RpcQueuedDispatch) {
        self.gauge_set("rpc_pending_requests", dispatch.live_request_count as u64);

        match self.forward_request_to_worker(&dispatch.request, &dispatch.worker) {
            Ok(()) => {
                self.counter_inc("rpc_requests_dispatched_total");
            }
            Err(
                crate::runtime::RouteError::RouteNotFound(_)
                | crate::runtime::RouteError::DeliveryFailed(
                    _,
                    DeliveryError::ActorStopped | DeliveryError::SinkPanicked,
                ),
            ) => {
                self.counter_inc("rpc_request_forward_errors_total");
                let cleanup_result = self.apply_session_cleanup(dispatch.worker.session_id);
                self.forward_worker_disconnect_errors(cleanup_result.disconnect_deliveries);
            }
            Err(crate::runtime::RouteError::DeliveryFailed(
                _,
                DeliveryError::MailboxFull { .. } | DeliveryError::HighLaneFull { .. },
            )) => {
                self.counter_inc("rpc_request_forward_errors_total");
                self.counter_inc("rpc_backpressure_rejects_total");
                if let Some((pending, pending_len)) = self.remove_pending_request_for_family(
                    *dispatch.worker.addr.family(),
                    &dispatch.request.correlation_id,
                ) {
                    if let Some(caller_inbox_addr) = pending.caller_inbox_addr {
                        self.forward_pending_error_deliveries(
                            vec![RpcPendingErrorDelivery {
                                correlation_id: dispatch.request.correlation_id,
                                caller_session_id: pending.caller_session_id,
                                caller_inbox_addr,
                            }],
                            crate::dispatch::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
                            RPC_BACKPRESSURE_ERROR,
                            "rpc_backpressure_errors_forwarded_total",
                            "rpc_backpressure_errors_dropped_total",
                        );
                    } else {
                        self.counter_inc("rpc_backpressure_errors_dropped_total");
                        self.counter_inc("rpc_responses_dropped_closed_caller_total");
                        tracing::warn!(
                            domain = "rpc",
                            correlation_id = %dispatch.request.correlation_id,
                            "Dropped RPC backpressure error because caller session was already closed"
                        );
                    }
                    self.gauge_set("rpc_pending_requests", pending_len as u64);
                }
            }
        }
    }

    pub(super) fn dispatch_all_queued_requests(&self) {
        let mut families: Vec<crate::runtime::routing::RouteFamily> = {
            let state = self.state.lock();
            state.routes.keys().map(|(family, _)| *family).collect()
        };
        families.sort_by_key(crate::runtime::routing::RouteFamily::id);
        families.dedup();
        for family in families {
            self.dispatch_queued_requests_for_family(family);
        }
    }

    pub(super) fn refresh_admin_snapshot_if_dirty(&self) {
        self.maybe_sync_admin_snapshot(false);
    }

    /// Mark the admin snapshot dirty. Forced calls refresh immediately; regular
    /// hot-path updates coalesce until an admin read or another forced refresh.
    pub(super) fn schedule_admin_snapshot(&self, force: bool) {
        if force {
            self.snapshot_dirty.store(true, Ordering::Relaxed);
            self.maybe_sync_admin_snapshot(true);
            return;
        }

        if self
            .snapshot_dirty
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        self.maybe_sync_admin_snapshot(false);
    }

    /// Sync the admin snapshot when the snapshot interval elapses or a caller forces it.
    ///
    /// Even forced snapshots are still point-in-time copies of the sink's current
    /// in-memory state, not linearizable reads of concurrent RPC activity.
    pub(super) fn maybe_sync_admin_snapshot(&self, force: bool) {
        #[cfg(feature = "bench-no-snapshot")]
        if !force {
            return;
        }

        let now_elapsed_us = Self::elapsed_us_saturating(self.snapshot_epoch);
        let last_snapshot_elapsed_us = self.last_snapshot_elapsed_us.load(Ordering::Relaxed);
        let snapshot_dirty = self.snapshot_dirty.load(Ordering::Relaxed);

        if !rpc_admin_snapshot_due(
            snapshot_dirty,
            force,
            now_elapsed_us,
            last_snapshot_elapsed_us,
        ) {
            return;
        }

        if self
            .snapshot_syncing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        if !self.snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.snapshot_syncing.store(false, Ordering::Release);
            return;
        }

        let snapshot_start = Instant::now();
        self.sync_admin_snapshot();
        let snapshot_time_us = Self::elapsed_us_saturating(snapshot_start);
        self.last_snapshot_elapsed_us.store(
            Self::elapsed_us_saturating(self.snapshot_epoch),
            Ordering::Relaxed,
        );
        self.snapshot_syncing.store(false, Ordering::Release);
        self.histogram_observe_us("rpc_admin_snapshot_us", snapshot_time_us);
    }
}
