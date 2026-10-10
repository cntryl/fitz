//! Cooperative cancellation controls at the RPC family-actor boundary.

use super::state_model::{
    RpcCancellationDisposition, RpcFamilyRuntime, RpcWorkerCancellation,
    RPC_CANCELLATION_CLOSE_RETRY_INTERVAL,
};
use crate::protocol::rpc_codec::{
    RpcCancellationMessage, RpcCancellationReason, RpcCancellationResult,
};
use crate::runtime::routing::{session_inbox_address, RouteFamily};
use crate::runtime::{Envelope, SessionCloseRequest};
use uuid::Uuid;

impl RpcFamilyRuntime<'_> {
    pub(super) fn handle_cancellation_frame(
        &mut self,
        meta: &crate::runtime::ClientFrameMeta,
        payload: &[u8],
    ) {
        let message = match crate::protocol::rpc_codec::parse_cancellation_message(payload) {
            Ok(message) => message,
            Err(error) => {
                self.counter_inc("rpc_cancellation_malformed_total");
                tracing::warn!(
                    domain = "rpc",
                    session_id = meta.session_id,
                    error = %error,
                    "Malformed RPC cancellation control"
                );
                return;
            }
        };

        match message {
            RpcCancellationMessage::CallerCancel {
                correlation_id,
                reason,
            } => self.handle_caller_cancellation(meta, correlation_id, reason),
            RpcCancellationMessage::WorkerCleanupAck { correlation_id } => {
                self.handle_worker_cancellation_ack(meta, correlation_id);
            }
        }
    }

    fn handle_caller_cancellation(
        &mut self,
        meta: &crate::runtime::ClientFrameMeta,
        correlation_id: Uuid,
        reason: RpcCancellationReason,
    ) {
        self.counter_inc("rpc_cancellation_requested_total");
        let disposition = self.core.state.cancel_request_for_caller(
            meta.route_family,
            &correlation_id,
            meta.session_id,
            std::time::Instant::now(),
            self.core.cancellation_grace_period,
        );
        let result = match disposition {
            RpcCancellationDisposition::QueuedRemoved => {
                self.release_global_pending(1);
                let pending_len = self.core.state.live_request_count();
                self.gauge_set("rpc_pending_requests", pending_len as u64);
                self.schedule_admin_snapshot(false);
                self.counter_inc("rpc_cancellation_queued_removed_total");
                RpcCancellationResult::QueuedRemoved
            }
            RpcCancellationDisposition::Dispatched {
                worker_session_id,
                supports_cancellation,
            } => {
                self.schedule_admin_snapshot(false);
                if supports_cancellation {
                    let cancellation = RpcWorkerCancellation {
                        family: meta.route_family,
                        correlation_id,
                        worker_session_id,
                        reason,
                    };
                    if self.forward_worker_cancellation(&cancellation) {
                        RpcCancellationResult::Forwarded
                    } else {
                        RpcCancellationResult::ForwardingFailed
                    }
                } else {
                    self.counter_inc("rpc_cancellation_unsupported_total");
                    RpcCancellationResult::WorkerUnsupported
                }
            }
            RpcCancellationDisposition::AlreadyCancelled => {
                self.counter_inc("rpc_cancellation_duplicate_total");
                RpcCancellationResult::AlreadyTerminal
            }
            RpcCancellationDisposition::UnauthorizedOrUnknown => {
                self.counter_inc("rpc_cancellation_unknown_or_unauthorized_total");
                RpcCancellationResult::UnauthorizedOrUnknown
            }
        };
        self.forward_cancellation_result(
            meta.session_id,
            meta.route_family,
            correlation_id,
            result,
        );
    }

    fn handle_worker_cancellation_ack(
        &mut self,
        meta: &crate::runtime::ClientFrameMeta,
        correlation_id: Uuid,
    ) {
        let Some(_) = self.core.state.acknowledge_cancellation(
            meta.route_family,
            &correlation_id,
            meta.session_id,
        ) else {
            // Supporting workers acknowledge every completed invocation, even
            // if the caller completed normally before a late cancellation.
            self.counter_inc("rpc_cancellation_ack_ignored_total");
            return;
        };

        self.counter_inc("rpc_cancellation_acknowledged_total");
        self.release_global_pending(1);
        let pending_len = self.core.state.live_request_count();
        self.gauge_set("rpc_pending_requests", pending_len as u64);
        self.schedule_admin_snapshot(false);
        self.dispatch_queued_requests_for_family(meta.route_family);
    }

    /// Busy time reported by the transport frame loop behind a worker's inbox.
    pub(super) fn worker_frame_loop_busy_time(
        router: &crate::runtime::Router,
        family: RouteFamily,
        worker_session_id: u64,
        now: std::time::Instant,
    ) -> Option<std::time::Duration> {
        router
            .resolve_sink(&session_inbox_address(family, worker_session_id))?
            .frame_loop_busy_time(now)
    }

    pub(super) fn forward_worker_cancellation(
        &mut self,
        cancellation: &RpcWorkerCancellation,
    ) -> bool {
        let busy_time = Self::worker_frame_loop_busy_time(
            &self.core.router,
            cancellation.family,
            cancellation.worker_session_id,
            std::time::Instant::now(),
        );
        self.core.state.pending.start_grace_busy_accounting(
            super::state_model::RpcCorrelationKey {
                family: cancellation.family,
                correlation_id: cancellation.correlation_id,
            },
            busy_time,
        );
        let worker_id = self
            .core
            .state
            .pending
            .pending_for_key(&super::state_model::RpcCorrelationKey {
                family: cancellation.family,
                correlation_id: cancellation.correlation_id,
            })
            .expect("canceled invocation remains tracked")
            .dispatch_info
            .worker_correlation_id;
        let bytes = crate::protocol::rpc_codec::encode_worker_cancel_tlv_frame(
            &worker_id,
            cancellation.reason,
        );
        let destination =
            session_inbox_address(cancellation.family, cancellation.worker_session_id);
        let delivery = crate::domains::rpc::protocol::RpcLifecycleControlDelivery {
            session_id: cancellation.worker_session_id,
            frame: bytes,
        };
        match self.core.router.route(Envelope::new(destination, delivery)) {
            Ok(()) => {
                self.counter_inc("rpc_cancellation_forwarded_total");
                true
            }
            Err(error) => {
                self.counter_inc("rpc_cancellation_forwarding_failed_total");
                tracing::warn!(
                    domain = "rpc",
                    family = cancellation.family.id(),
                    correlation_id = %cancellation.correlation_id,
                    worker_session_id = cancellation.worker_session_id,
                    error = %error,
                    "RPC cancellation could not be delivered to worker"
                );
                self.request_worker_session_close(
                    cancellation.family,
                    cancellation.worker_session_id,
                    "RPC cancellation control delivery failed",
                );
                false
            }
        }
    }

    pub(super) fn request_worker_session_close(
        &mut self,
        family: RouteFamily,
        worker_session_id: u64,
        reason: &'static str,
    ) {
        self.core
            .state
            .mark_worker_close_requested(family, worker_session_id);
        let destination = session_inbox_address(family, worker_session_id);
        let request = SessionCloseRequest { reason };
        match self.core.router.route(Envelope::new(destination, request)) {
            Ok(()) => {
                self.counter_inc("rpc_cancellation_worker_close_requested_total");
            }
            Err(error) => {
                self.counter_inc("rpc_cancellation_worker_close_signal_failures_total");
                self.core.state.retry_close_for_worker(
                    family,
                    worker_session_id,
                    std::time::Instant::now() + RPC_CANCELLATION_CLOSE_RETRY_INTERVAL,
                );
                tracing::warn!(
                    domain = "rpc",
                    family = family.id(),
                    worker_session_id,
                    error = %error,
                    "RPC worker close signal could not be delivered; retry scheduled"
                );
            }
        }
    }

    fn forward_cancellation_result(
        &mut self,
        session_id: u64,
        family: RouteFamily,
        correlation_id: Uuid,
        result: RpcCancellationResult,
    ) {
        let frame =
            crate::protocol::rpc_codec::encode_cancel_result_tlv_frame(&correlation_id, result);
        let destination = session_inbox_address(family, session_id);
        let delivery =
            crate::domains::rpc::protocol::RpcLifecycleControlDelivery { session_id, frame };
        if let Err(error) = self.core.router.route(Envelope::new(destination, delivery)) {
            self.counter_inc("rpc_cancellation_result_dropped_total");
            tracing::debug!(
                domain = "rpc",
                family = family.id(),
                session_id,
                correlation_id = %correlation_id,
                error = %error,
                "RPC cancellation result could not be delivered"
            );
        }
    }
}
