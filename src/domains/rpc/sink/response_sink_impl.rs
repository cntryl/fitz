use super::response_forwarder::RpcResponseForwarder;
use super::state_model::{
    session_inbox_address, Envelope, Instant, RpcDeliveryOutcome as DeliveryOutcome,
    RpcDomainRuntime, RpcPendingDispatchInfo, RpcPendingErrorDelivery,
    RpcPendingResponseDisposition, RpcResponseState, RpcState, RPC_CORRELATION_NOT_FOUND_ERROR,
    RPC_INVALID_SEQUENCE_ERROR, RPC_WRONG_WORKER_ERROR,
};
use crate::domains::rpc::protocol::RpcResponse;

struct ResponseStateContext<'a> {
    envelope: &'a Envelope,
    meta: &'a crate::runtime::ClientFrameMeta,
    state: parking_lot::MutexGuard<'a, RpcState>,
    state_wait_us: u64,
    state_hold_start: Option<Instant>,
    pending_route_lookup_us: u64,
}

fn elapsed_micros_u64(start: Instant) -> u64 {
    start.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
}

fn elapsed_micros_optional(start: Option<Instant>) -> u64 {
    start.map_or(0, elapsed_micros_u64)
}

impl RpcDomainRuntime<'_> {
    pub(super) fn handle_response_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        resp: &RpcResponse,
    ) -> DeliveryOutcome {
        self.counter_inc("rpc_responses_total");

        let metrics_enabled = self.metrics.is_some();
        let state_wait_start = metrics_enabled.then(Instant::now);
        let mut state = self.state.lock();
        let state_wait_us = elapsed_micros_optional(state_wait_start);
        let state_hold_start = metrics_enabled.then(Instant::now);
        let pending_route_lookup_start = metrics_enabled.then(Instant::now);
        let caller_info = RpcResponseState::pending_for_response(
            &mut *state,
            meta.route_family,
            &resp.correlation_id,
            meta.session_id,
            resp.seq,
            resp.stream_end,
        );
        let pending_route_lookup_us = elapsed_micros_optional(pending_route_lookup_start);
        let context = ResponseStateContext {
            envelope,
            meta,
            state,
            state_wait_us,
            state_hold_start,
            pending_route_lookup_us,
        };

        match caller_info {
            RpcPendingResponseDisposition::WrongWorker {
                owner_worker_session_id,
            } => self.handle_wrong_response_worker(context, resp, owner_worker_session_id),
            RpcPendingResponseDisposition::Forward {
                pending: caller_info,
                removed_pending,
            } => self.handle_forwarded_response(context, resp, &caller_info, removed_pending),
            RpcPendingResponseDisposition::InvalidSequence {
                pending: caller_info,
                expected_seq,
            } => self.handle_invalid_response_sequence(context, resp, &caller_info, expected_seq),
            RpcPendingResponseDisposition::Missing => {
                self.handle_missing_response_pending(context, resp)
            }
        }
    }

    fn handle_wrong_response_worker(
        &self,
        context: ResponseStateContext<'_>,
        resp: &RpcResponse,
        owner_worker_session_id: u64,
    ) -> DeliveryOutcome {
        let ResponseStateContext {
            envelope,
            meta,
            state,
            state_wait_us,
            state_hold_start,
            pending_route_lookup_us,
        } = context;
        let state_hold_us = elapsed_micros_optional(state_hold_start);
        let pending_len = RpcResponseState::live_count(&*state);
        drop(state);

        self.histogram_observe_us("rpc_pending_route_lookup_us", pending_route_lookup_us);
        self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
        self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);
        self.counter_inc("rpc_responses_rejected_wrong_worker_total");

        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
            session_inbox_address(*envelope.destination().family(), meta.session_id)
        });
        self.forward_pending_error_deliveries(
            vec![RpcPendingErrorDelivery {
                correlation_id: resp.correlation_id,
                caller_session_id: meta.session_id,
                caller_inbox_addr: worker_inbox_addr,
            }],
            crate::dispatch::protocol::error_codes::rpc::ERR_RPC_WRONG_WORKER,
            RPC_WRONG_WORKER_ERROR,
            "rpc_worker_ownership_errors_forwarded_total",
            "rpc_worker_ownership_errors_dropped_total",
        );

        tracing::warn!(
            domain = "rpc",
            correlation_id = %resp.correlation_id,
            pending_len,
            expected_worker_session_id = owner_worker_session_id,
            received_worker_session_id = meta.session_id,
            "Rejected RPC response from non-owner worker"
        );
        (None, None, true)
    }

    fn handle_forwarded_response(
        &self,
        context: ResponseStateContext<'_>,
        resp: &RpcResponse,
        caller_info: &RpcPendingDispatchInfo,
        removed_pending: bool,
    ) -> DeliveryOutcome {
        let ResponseStateContext {
            envelope: _,
            meta,
            mut state,
            state_wait_us,
            state_hold_start,
            pending_route_lookup_us,
        } = context;
        let mut state_changed = false;
        if removed_pending {
            self.release_global_pending(1);
            let completion_latency_us = elapsed_micros_u64(caller_info.submitted_at_instant);
            RpcResponseState::release_dispatch(
                &mut *state,
                caller_info,
                Some(completion_latency_us),
            );
            let live_request_count = RpcResponseState::live_count(&*state);
            self.histogram_observe_us("rpc_pending_route_remove_us", pending_route_lookup_us);
            self.histogram_observe_us("rpc_pending_untrack_us", pending_route_lookup_us);
            self.gauge_set("rpc_pending_requests", live_request_count as u64);
            state_changed = true;
        }

        let state_hold_us = elapsed_micros_optional(state_hold_start);
        drop(state);

        self.histogram_observe_us("rpc_pending_route_lookup_us", pending_route_lookup_us);
        self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
        self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);

        self.forward_response_to_requester(meta, resp, caller_info);

        tracing::debug!(
            domain = "rpc",
            correlation_id = %resp.correlation_id,
            stream_end = resp.stream_end,
            "Response forwarded to requester"
        );

        if state_changed {
            self.schedule_admin_snapshot(false);
            self.dispatch_queued_requests_for_family(
                caller_info
                    .caller_inbox_addr
                    .as_ref()
                    .map_or(caller_info.family, |addr| *addr.family()),
            );
        }

        (None, state_changed.then_some(false), false)
    }

    fn forward_response_to_requester(
        &self,
        meta: &crate::runtime::ClientFrameMeta,
        resp: &RpcResponse,
        caller_info: &RpcPendingDispatchInfo,
    ) {
        let metrics_enabled = self.metrics.is_some();
        let response_forward_start = metrics_enabled.then(Instant::now);
        if let Some(forward_envelope) =
            RpcResponseForwarder::response_envelope(meta, resp, caller_info)
        {
            if let Err(error) = self.router.route(forward_envelope) {
                self.counter_inc("rpc_response_forward_errors_total");
                tracing::warn!(
                    domain = "rpc",
                    correlation_id = %resp.correlation_id,
                    error = ?error,
                    "Failed to forward response to requester"
                );
            }
        } else {
            self.counter_inc("rpc_responses_dropped_closed_caller_total");
        }
        if let Some(response_forward_start) = response_forward_start {
            self.histogram_observe_elapsed_us("rpc_response_forward_us", response_forward_start);
        }
    }

    fn handle_invalid_response_sequence(
        &self,
        context: ResponseStateContext<'_>,
        resp: &RpcResponse,
        caller_info: &RpcPendingDispatchInfo,
        expected_seq: u64,
    ) -> DeliveryOutcome {
        let ResponseStateContext {
            envelope,
            meta,
            mut state,
            state_wait_us,
            state_hold_start,
            pending_route_lookup_us,
        } = context;
        RpcResponseState::release_dispatch(&mut *state, caller_info, None);
        self.release_global_pending(1);
        let live_request_count = RpcResponseState::live_count(&*state);
        let state_hold_us = elapsed_micros_optional(state_hold_start);
        drop(state);

        self.histogram_observe_us("rpc_pending_route_lookup_us", pending_route_lookup_us);
        self.histogram_observe_us("rpc_pending_route_remove_us", pending_route_lookup_us);
        self.histogram_observe_us("rpc_pending_untrack_us", pending_route_lookup_us);
        self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
        self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);
        self.gauge_set("rpc_pending_requests", live_request_count as u64);
        self.counter_inc("rpc_response_invalid_sequence_total");
        self.counter_inc("rpc_cleanup_pending_removed_total");
        self.schedule_admin_snapshot(false);
        self.dispatch_queued_requests_for_family(
            caller_info
                .caller_inbox_addr
                .as_ref()
                .map_or(caller_info.family, |addr| *addr.family()),
        );

        if let Some(caller_inbox_addr) = caller_info.caller_inbox_addr.clone() {
            self.forward_pending_error_deliveries(
                vec![RpcPendingErrorDelivery {
                    correlation_id: resp.correlation_id,
                    caller_session_id: caller_info.caller_session_id,
                    caller_inbox_addr,
                }],
                crate::dispatch::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
                RPC_INVALID_SEQUENCE_ERROR,
                "rpc_invalid_sequence_errors_forwarded_total",
                "rpc_invalid_sequence_errors_dropped_total",
            );
        } else {
            self.counter_inc("rpc_invalid_sequence_errors_dropped_total");
        }

        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
            session_inbox_address(*envelope.destination().family(), meta.session_id)
        });
        self.forward_pending_error_deliveries(
            vec![RpcPendingErrorDelivery {
                correlation_id: resp.correlation_id,
                caller_session_id: meta.session_id,
                caller_inbox_addr: worker_inbox_addr,
            }],
            crate::dispatch::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
            RPC_INVALID_SEQUENCE_ERROR,
            "rpc_worker_protocol_errors_forwarded_total",
            "rpc_worker_protocol_errors_dropped_total",
        );

        tracing::warn!(
            domain = "rpc",
            correlation_id = %resp.correlation_id,
            expected_seq,
            received_seq = resp.seq,
            "Rejected RPC response with invalid sequence"
        );
        (None, Some(false), true)
    }

    fn handle_missing_response_pending(
        &self,
        context: ResponseStateContext<'_>,
        resp: &RpcResponse,
    ) -> DeliveryOutcome {
        let ResponseStateContext {
            envelope,
            meta,
            state,
            state_wait_us,
            state_hold_start,
            pending_route_lookup_us,
        } = context;
        let state_hold_us = elapsed_micros_optional(state_hold_start);
        let live_request_count = RpcResponseState::live_count(&*state);
        drop(state);

        self.histogram_observe_us("rpc_pending_route_lookup_us", pending_route_lookup_us);
        self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
        self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);
        self.counter_inc("rpc_responses_missing_pending_total");

        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
            session_inbox_address(*envelope.destination().family(), meta.session_id)
        });
        self.forward_pending_error_deliveries(
            vec![RpcPendingErrorDelivery {
                correlation_id: resp.correlation_id,
                caller_session_id: meta.session_id,
                caller_inbox_addr: worker_inbox_addr,
            }],
            crate::dispatch::protocol::error_codes::rpc::ERR_CORRELATION_NOT_FOUND,
            RPC_CORRELATION_NOT_FOUND_ERROR,
            "rpc_correlation_errors_forwarded_total",
            "rpc_correlation_errors_dropped_total",
        );
        tracing::warn!(
            domain = "rpc",
            correlation_id = %resp.correlation_id,
            pending_len = live_request_count,
            "No pending request for response"
        );
        (None, None, true)
    }
}
