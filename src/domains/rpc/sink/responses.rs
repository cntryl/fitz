use super::response_forwarder::RpcResponseForwarder;
use super::state_model::{
    session_inbox_address, Envelope, Instant, RpcDeliveryOutcome as DeliveryOutcome,
    RpcDomainRuntime, RpcPendingDispatchInfo, RpcPendingErrorDelivery,
    RpcPendingResponseDisposition, RpcResponseState, RpcState, RPC_CORRELATION_NOT_FOUND_ERROR,
    RPC_INVALID_SEQUENCE_ERROR, RPC_RESPONSE_UNDELIVERABLE_ERROR, RPC_WRONG_WORKER_ERROR,
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

/// Delivery attempts for one response chunk before the RPC is ended.
///
/// RPC RESPONSE has no acknowledgement from the broker back to the worker, so
/// a worker can never learn that a chunk failed to reach the caller and
/// cannot resend it - every supported SDK simply advances to the next chunk,
/// or closes after the terminal one. A retry budget greater than one attempt
/// therefore waits for a resend that will never come: a nonterminal chunk's
/// "retry" silently becomes an invalid-sequence error on the NEXT chunk the
/// worker sends, and a terminal chunk's "retry" just sits pending until the
/// caller's own timeout. The broker must end the RPC on the first failed
/// forward instead.
pub(in crate::domains::rpc::sink) const MAX_RESPONSE_DELIVERY_ATTEMPTS: u32 = 1;

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
                stream_end,
            } => self.handle_forwarded_response(context, resp, &caller_info, stream_end),
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
        stream_end: bool,
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
        drop(state);

        self.histogram_observe_us("rpc_pending_route_lookup_us", pending_route_lookup_us);
        self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
        self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);

        // Forward before touching the request's cursor. A stream whose cursor
        // moved past a chunk the caller never received would present every
        // later chunk as contiguous.
        if !self.forward_response_to_requester(meta, resp, caller_info) {
            return self.handle_undeliverable_response(envelope, meta, resp, caller_info);
        }

        let mut state = self.core.state.lock();
        let completed = RpcResponseState::commit_response_delivery(
            &mut *state,
            meta.route_family,
            &resp.correlation_id,
            stream_end,
        );
        let mut state_changed = false;
        if stream_end && completed {
            let completion_latency_us = elapsed_micros_u64(caller_info.submitted_at_instant);
            RpcResponseState::release_dispatch(
                &mut *state,
                caller_info,
                Some(completion_latency_us),
            );
            let live_request_count = RpcResponseState::live_count(&*state);
            drop(state);
            self.release_global_pending(1);
            self.histogram_observe_us("rpc_pending_route_remove_us", pending_route_lookup_us);
            self.histogram_observe_us("rpc_pending_untrack_us", pending_route_lookup_us);
            self.gauge_set("rpc_pending_requests", live_request_count as u64);
            state_changed = true;
        } else {
            drop(state);
        }

        tracing::debug!(
            domain = "rpc",
            correlation_id = %resp.correlation_id,
            stream_end,
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

    /// Handle a chunk the caller could not receive.
    ///
    /// Ends the RPC immediately: RPC RESPONSE has no ACK, so the worker that
    /// sent this chunk has no way to learn delivery failed and will never
    /// resend it. Waiting would only delay a failure the caller is going to
    /// see either way, while leaving the worker producing into a stream that
    /// no longer has a live listener.
    fn handle_undeliverable_response(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        resp: &RpcResponse,
        caller_info: &RpcPendingDispatchInfo,
    ) -> DeliveryOutcome {
        let failures = {
            let mut state = self.core.state.lock();
            RpcResponseState::record_delivery_failure(
                &mut *state,
                meta.route_family,
                &resp.correlation_id,
            )
        };
        self.counter_inc("rpc_response_delivery_retries_total");
        if failures < MAX_RESPONSE_DELIVERY_ATTEMPTS {
            tracing::debug!(
                domain = "rpc",
                correlation_id = %resp.correlation_id,
                seq = resp.seq,
                failures,
                "Caller outbound saturated; chunk stays retryable at its sequence"
            );
            return (None, None, false);
        }

        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
            session_inbox_address(*envelope.destination().family(), meta.session_id)
        });
        self.terminate_undeliverable_stream(meta, resp, caller_info, worker_inbox_addr);
        (None, Some(false), true)
    }

    /// Forward one response chunk, reporting whether the caller received it.
    ///
    /// The boolean matters: a chunk the caller never got leaves a hole in the
    /// stream, and every later chunk would arrive looking contiguous. Callers
    /// of this method must end the RPC rather than continue past a `false`.
    fn forward_response_to_requester(
        &self,
        meta: &crate::runtime::ClientFrameMeta,
        resp: &RpcResponse,
        caller_info: &RpcPendingDispatchInfo,
    ) -> bool {
        let metrics_enabled = self.metrics.is_some();
        let response_forward_start = metrics_enabled.then(Instant::now);
        let delivered = if let Some(forward_envelope) =
            RpcResponseForwarder::response_envelope(meta, resp, caller_info)
        {
            if let Err(error) = self.router.route(forward_envelope) {
                self.counter_inc("rpc_response_forward_errors_total");
                tracing::warn!(
                    domain = "rpc",
                    correlation_id = %resp.correlation_id,
                    seq = resp.seq,
                    error = ?error,
                    "Failed to forward response to requester"
                );
                false
            } else {
                true
            }
        } else {
            self.counter_inc("rpc_responses_dropped_closed_caller_total");
            false
        };
        if let Some(response_forward_start) = response_forward_start {
            self.histogram_observe_elapsed_us("rpc_response_forward_us", response_forward_start);
        }
        delivered
    }

    /// End an RPC whose response chunk could not reach the caller.
    ///
    /// Backpressure may reject or terminate an RPC, but it must never drop a
    /// chunk and keep forwarding later ones. Both sides are told: the caller
    /// so it sees a terminated stream instead of a sequence gap, and the
    /// worker so it stops producing into a stream that no longer exists.
    fn terminate_undeliverable_stream(
        &self,
        meta: &crate::runtime::ClientFrameMeta,
        resp: &RpcResponse,
        caller_info: &RpcPendingDispatchInfo,
        worker_inbox_addr: crate::runtime::routing::RouteAddress,
    ) {
        {
            let mut state = self.core.state.lock();
            if RpcResponseState::abandon_pending(
                &mut *state,
                meta.route_family,
                &resp.correlation_id,
            )
            .is_none()
            {
                // Another path already ended this request.
                return;
            }
            RpcResponseState::release_dispatch(&mut *state, caller_info, None);
            let live_request_count = RpcResponseState::live_count(&*state);
            self.gauge_set("rpc_pending_requests", live_request_count as u64);
        }
        self.release_global_pending(1);
        self.counter_inc("rpc_streams_terminated_undeliverable_total");
        self.counter_inc("rpc_cleanup_pending_removed_total");
        self.schedule_admin_snapshot(false);
        self.dispatch_queued_requests_for_family(
            caller_info
                .caller_inbox_addr
                .as_ref()
                .map_or(caller_info.family, |addr| *addr.family()),
        );

        // This path is only reached after the worker has already produced at
        // least one response chunk, so the call may have partially executed.
        // `ERR_RPC_BACKPRESSURE` is documented and spec-classified (REQ-PROTO-012)
        // as safe to retry - it means "never accepted" - which is not true
        // here. Use the domain's indeterminate/backend-error code instead,
        // matching the `indeterminate_error_code` convention used elsewhere
        // for "outcome unknown, do not blindly retry" so a client cannot
        // safely re-invoke a non-idempotent call whose side effects may have
        // already run.
        if let Some(caller_inbox_addr) = caller_info.caller_inbox_addr.clone() {
            self.forward_pending_error_deliveries(
                vec![RpcPendingErrorDelivery {
                    correlation_id: resp.correlation_id,
                    caller_session_id: caller_info.caller_session_id,
                    caller_inbox_addr,
                }],
                crate::dispatch::protocol::error_codes::rpc::ERR_BACKEND_ERROR,
                RPC_RESPONSE_UNDELIVERABLE_ERROR,
                "rpc_undeliverable_stream_errors_forwarded_total",
                "rpc_undeliverable_stream_errors_dropped_total",
            );
        } else {
            self.counter_inc("rpc_undeliverable_stream_errors_dropped_total");
        }

        self.forward_pending_error_deliveries(
            vec![RpcPendingErrorDelivery {
                correlation_id: resp.correlation_id,
                caller_session_id: meta.session_id,
                caller_inbox_addr: worker_inbox_addr,
            }],
            crate::dispatch::protocol::error_codes::rpc::ERR_BACKEND_ERROR,
            RPC_RESPONSE_UNDELIVERABLE_ERROR,
            "rpc_worker_stream_cancels_forwarded_total",
            "rpc_worker_stream_cancels_dropped_total",
        );

        tracing::warn!(
            domain = "rpc",
            correlation_id = %resp.correlation_id,
            seq = resp.seq,
            "Terminated RPC stream: response chunk could not be delivered to the caller"
        );
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
