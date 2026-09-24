//! Session and worker disconnect cleanup.
//!
//! The cleanup protocol (mark-before-release, stale-request rejection) is
//! owned by [`crate::runtime::SessionScoped`]; this file only releases state.

use super::response_forwarder::RpcResponseForwarder;
use super::state_model::{
    RpcFamilyRuntime, RpcPendingErrorDelivery, RpcSessionCleanupResult, RpcWorkerCleanupResult,
    RPC_WORKER_NOT_FOUND_ERROR,
};
use crate::runtime::routing::RouteAddress;
use crate::runtime::{CleanedUpSessions, SessionScoped};

impl SessionScoped for RpcFamilyRuntime<'_> {
    fn cleaned_up_sessions(&mut self) -> &mut CleanedUpSessions {
        &mut self.core.cleaned_up_sessions
    }

    fn release_session_resources(&mut self, session_id: u64) {
        let cleanup_result = self.apply_session_cleanup(session_id);
        self.forward_worker_disconnect_errors(cleanup_result.disconnect_deliveries);
    }
}

impl RpcFamilyRuntime<'_> {
    pub(super) fn apply_session_cleanup(&mut self, session_id: u64) -> RpcSessionCleanupResult {
        let cleanup_result = {
            let state = &mut self.core.state;
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
        &mut self,
        worker_addr: &RouteAddress,
        session_id: u64,
    ) -> RpcWorkerCleanupResult {
        let cleanup_result = {
            let state = &mut self.core.state;
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
        &mut self,
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

            if let Err(error) = self.core.router.route(response_envelope) {
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
        &mut self,
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
