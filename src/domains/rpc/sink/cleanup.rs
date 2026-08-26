//! Session/worker disconnect cleanup and stale queued-request rejection.
//!
//! `SessionCleanup` is delivered on the high-priority mailbox lane, so it can
//! pass an older, already-queued normal-lane request from the same session.
//! Remembering the cleaned-up session lets that stale request fail instead of
//! silently recreating a worker registration or pending request for a session
//! that is already gone and will never be cleaned up again.

use super::response_forwarder::RpcResponseForwarder;
use super::state_model::{
    Envelope, RouteAddress, RpcDomainRuntime, RpcPendingErrorDelivery, RpcSessionCleanupResult,
    RpcWorkerCleanupResult, RPC_WORKER_NOT_FOUND_ERROR,
};
use std::collections::{HashSet, VecDeque};

/// Bounded record of sessions `apply_session_cleanup` has already run for as
/// part of disconnect cleanup.
pub(super) struct CleanedUpSessions {
    order: VecDeque<u64>,
    seen: HashSet<u64>,
    capacity: usize,
}

impl CleanedUpSessions {
    #[must_use]
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            order: VecDeque::new(),
            seen: HashSet::new(),
            capacity: capacity.max(1),
        }
    }

    pub(super) fn mark(&mut self, session_id: u64) {
        if self.seen.insert(session_id) {
            self.order.push_back(session_id);
            if self.order.len() > self.capacity {
                if let Some(oldest) = self.order.pop_front() {
                    self.seen.remove(&oldest);
                }
            }
        }
    }

    pub(super) fn contains(&self, session_id: u64) -> bool {
        self.seen.contains(&session_id)
    }
}

impl RpcDomainRuntime<'_> {
    pub(super) fn is_cleaned_up_session(&self, session_id: u64) -> bool {
        self.cleaned_up_sessions.lock().contains(session_id)
    }

    pub(super) fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            // Mark first so an older normal-lane request that cleanup jumped
            // over cannot recreate a worker registration or pending request
            // for this session below.
            self.cleaned_up_sessions.lock().mark(cleanup.session_id);
            let cleanup_result = self.apply_session_cleanup(cleanup.session_id);
            self.forward_worker_disconnect_errors(cleanup_result.disconnect_deliveries);
            return true;
        }

        false
    }

    pub(super) fn apply_session_cleanup(&self, session_id: u64) -> RpcSessionCleanupResult {
        let cleanup_result = {
            let mut state = self.state.lock();
            state.cleanup_session(session_id)
        };

        self.gauge_set("rpc_pending_requests", cleanup_result.pending_len as u64);
        self.release_global_pending(cleanup_result.removed_pending);
        if cleanup_result.removed_registrations > 0 {
            self.counter_add(
                "rpc_cleanup_workers_removed_total",
                cleanup_result.removed_registrations as u64,
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
        if cleanup_result.removed_registrations > 0
            || cleanup_result.detached_callers > 0
            || cleanup_result.removed_pending > 0
        {
            self.schedule_admin_snapshot(false);
        }
        self.refresh_metrics_gauges();

        tracing::debug!(
            domain = "rpc",
            session_id,
            removed_workers = cleanup_result.removed_registrations,
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
            state.unregister_registration(worker_addr, session_id)
        };

        self.gauge_set("rpc_pending_requests", cleanup_result.pending_len as u64);
        self.release_global_pending(cleanup_result.removed_pending);
        if cleanup_result.removed_registrations > 0 {
            self.counter_add(
                "rpc_cleanup_workers_removed_total",
                cleanup_result.removed_registrations as u64,
            );
        }
        if cleanup_result.removed_pending > 0 {
            self.counter_add(
                "rpc_cleanup_pending_removed_total",
                cleanup_result.removed_pending as u64,
            );
        }
        if cleanup_result.removed_registrations > 0 || cleanup_result.removed_pending > 0 {
            self.schedule_admin_snapshot(false);
        }
        self.refresh_metrics_gauges();

        tracing::debug!(
            domain = "rpc",
            worker = worker_addr.route().as_str(),
            session_id,
            removed_workers = cleanup_result.removed_registrations,
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
            let response_envelope =
                RpcResponseForwarder::terminal_error_envelope(delivery, error_code, error_message);

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
}
