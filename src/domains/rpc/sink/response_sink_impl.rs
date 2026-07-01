use super::state_model::{
    session_inbox_address, Envelope, Instant, RouteFamily, RpcDeliveryOutcome as DeliveryOutcome,
    RpcDomainRuntime, RpcPendingErrorDelivery, RpcPendingRequest, RpcPendingResponseDisposition,
    RpcState, RPC_CORRELATION_NOT_FOUND_ERROR, RPC_INVALID_SEQUENCE_ERROR, RPC_WRONG_WORKER_ERROR,
};
use crate::domains::rpc::protocol::RpcResponse;
#[cfg(not(test))]
use crate::domains::rpc::{
    RpcClientForwardedResponse, RpcClientForwardedResponseBody, RpcWorkerAck,
};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;

struct ResponseStateContext<'a> {
    envelope: &'a Envelope,
    meta: &'a crate::runtime::ClientFrameMeta,
    state: parking_lot::MutexGuard<'a, RpcState>,
    state_wait_us: u64,
    state_hold_start: Instant,
    pending_route_lookup_us: u64,
}

struct AckStateContext<'a> {
    envelope: &'a Envelope,
    meta: &'a crate::runtime::ClientFrameMeta,
    state: parking_lot::MutexGuard<'a, RpcState>,
    state_wait_us: u64,
    state_hold_start: Instant,
}

fn elapsed_micros_u64(start: Instant) -> u64 {
    start.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
}

impl RpcDomainRuntime<'_> {
    pub(super) fn handle_response_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        resp: &RpcResponse,
    ) -> DeliveryOutcome {
        self.counter_inc("rpc_responses_total");

        let state_wait_start = Instant::now();
        let mut state = self.state.lock();
        let state_wait_us = elapsed_micros_u64(state_wait_start);
        let state_hold_start = Instant::now();
        let pending_route_lookup_start = Instant::now();
        let wrong_worker_owner = state
            .pending
            .worker_session_id(&resp.correlation_id)
            .filter(|owner_session_id| *owner_session_id != meta.session_id);
        let caller_info = wrong_worker_owner.is_none().then(|| {
            state
                .pending
                .pending_for_response(&resp.correlation_id, resp.seq, resp.stream_end)
        });
        let pending_route_lookup_us = elapsed_micros_u64(pending_route_lookup_start);
        let context = ResponseStateContext {
            envelope,
            meta,
            state,
            state_wait_us,
            state_hold_start,
            pending_route_lookup_us,
        };

        if let Some(owner_worker_session_id) = wrong_worker_owner {
            return self.handle_wrong_response_worker(context, resp, owner_worker_session_id);
        }

        match caller_info.expect("caller info should be present when owner matched") {
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

    pub(super) fn handle_ack_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        correlation_id: uuid::Uuid,
    ) -> DeliveryOutcome {
        let state_wait_start = Instant::now();
        let state = self.state.lock();
        let state_wait_us = elapsed_micros_u64(state_wait_start);
        let state_hold_start = Instant::now();
        let wrong_worker_owner = state
            .pending
            .worker_session_id(&correlation_id)
            .filter(|owner_session_id| *owner_session_id != meta.session_id);
        let context = AckStateContext {
            envelope,
            meta,
            state,
            state_wait_us,
            state_hold_start,
        };

        if let Some(owner_worker_session_id) = wrong_worker_owner {
            return self.handle_wrong_ack_worker(context, correlation_id, owner_worker_session_id);
        }

        self.handle_ack_cleanup(context, correlation_id)
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
        let state_hold_us = elapsed_micros_u64(state_hold_start);
        let pending_len = state.live_request_count();
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
            crate::protocol::error_codes::rpc::ERR_RPC_WRONG_WORKER,
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
        caller_info: &RpcPendingRequest,
        removed_pending: bool,
    ) -> DeliveryOutcome {
        let ResponseStateContext {
            envelope,
            meta,
            mut state,
            state_wait_us,
            state_hold_start,
            pending_route_lookup_us,
        } = context;
        let mut state_changed = false;
        let mut dispatch_route = None;

        if removed_pending {
            let completion_latency_us = elapsed_micros_u64(caller_info.submitted_at_instant);
            state.release_worker_for_pending(caller_info, Some(completion_latency_us));
            let live_request_count = state.live_request_count();
            self.histogram_observe_us("rpc_pending_route_remove_us", pending_route_lookup_us);
            self.histogram_observe_us("rpc_pending_untrack_us", pending_route_lookup_us);
            self.gauge_set("rpc_pending_requests", live_request_count as u64);
            state_changed = true;
            dispatch_route = Some(caller_info.route.clone());
        }

        let state_hold_us = elapsed_micros_u64(state_hold_start);
        drop(state);

        self.histogram_observe_us("rpc_pending_route_lookup_us", pending_route_lookup_us);
        self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
        self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);

        self.forward_response_to_requester(meta, resp, caller_info);
        self.forward_ack_to_worker(envelope, meta, resp.correlation_id);

        tracing::debug!(
            domain = "rpc",
            correlation_id = %resp.correlation_id,
            stream_end = resp.stream_end,
            "Response forwarded to requester and ACK sent to worker"
        );

        if let Some(route) = dispatch_route {
            self.schedule_admin_snapshot(false);
            self.dispatch_queued_requests_for_route(&route);
        }

        (None, state_changed.then_some(false), false)
    }

    fn forward_response_to_requester(
        &self,
        meta: &crate::runtime::ClientFrameMeta,
        resp: &RpcResponse,
        caller_info: &RpcPendingRequest,
    ) {
        let response_forward_start = Instant::now();
        #[cfg(not(test))]
        let _ = meta;

        if let Some(caller_inbox_addr) = caller_info.caller_inbox_addr.as_ref() {
            #[cfg(test)]
            let forward_envelope = {
                let mut payload_encoder =
                    crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
                let encoded_response = crate::protocol::rpc_codec::encode_response_message_into(
                    resp,
                    &mut payload_encoder,
                );
                let forward_ctx = FrameContext::new(
                    caller_info.caller_session_id,
                    super::mailbox_adapter::test_protocol_channel_from_client(meta.channel),
                    crate::protocol::tlv::MessageType::new(303),
                    bytes::Bytes::from(encoded_response),
                    *caller_inbox_addr.family(),
                );
                Envelope::new(caller_inbox_addr.clone(), forward_ctx)
            };

            #[cfg(not(test))]
            let forward_envelope = Envelope::new(
                caller_inbox_addr.clone(),
                RpcClientForwardedResponse::new(
                    caller_info.caller_session_id,
                    *caller_inbox_addr.family(),
                    RpcClientForwardedResponseBody::Response(resp.clone()),
                ),
            );

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
        self.histogram_observe_elapsed_us("rpc_response_forward_us", response_forward_start);
    }

    fn forward_ack_to_worker(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        correlation_id: uuid::Uuid,
    ) {
        let ack_forward_start = Instant::now();
        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
            session_inbox_address(*envelope.destination().family(), meta.session_id)
        });
        #[cfg(test)]
        let ack_envelope = {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(64);
            let ack_payload =
                crate::protocol::rpc_codec::encode_ack_into(&correlation_id, &mut payload_encoder);
            let ack_ctx = FrameContext::new(
                meta.session_id,
                super::mailbox_adapter::test_protocol_channel_from_client(meta.channel),
                crate::protocol::tlv::MessageType::new(304),
                bytes::Bytes::from(ack_payload),
                RouteFamily::from_u32(envelope.destination().family().id()),
            );
            Envelope::new(worker_inbox_addr, ack_ctx)
        };

        #[cfg(not(test))]
        let ack_envelope = Envelope::new(
            worker_inbox_addr,
            RpcWorkerAck::new(
                meta.session_id,
                RouteFamily::from_u32(envelope.destination().family().id()),
                correlation_id,
            ),
        );

        if let Err(error) = self.router.route(ack_envelope) {
            self.counter_inc("rpc_ack_forward_errors_total");
            tracing::warn!(
                domain = "rpc",
                correlation_id = %correlation_id,
                error = ?error,
                "Failed to send ACK to worker"
            );
        } else {
            self.counter_inc("rpc_worker_acks_total");
        }
        self.histogram_observe_elapsed_us("rpc_ack_forward_us", ack_forward_start);
    }

    fn handle_invalid_response_sequence(
        &self,
        context: ResponseStateContext<'_>,
        resp: &RpcResponse,
        caller_info: &RpcPendingRequest,
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
        state.release_worker_for_pending(caller_info, None);
        let live_request_count = state.live_request_count();
        let state_hold_us = elapsed_micros_u64(state_hold_start);
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
        self.dispatch_queued_requests_for_route(&caller_info.route);

        if let Some(caller_inbox_addr) = caller_info.caller_inbox_addr.clone() {
            self.forward_pending_error_deliveries(
                vec![RpcPendingErrorDelivery {
                    correlation_id: resp.correlation_id,
                    caller_session_id: caller_info.caller_session_id,
                    caller_inbox_addr,
                }],
                crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
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
            crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
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
        let state_hold_us = elapsed_micros_u64(state_hold_start);
        let live_request_count = state.live_request_count();
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
            crate::protocol::error_codes::rpc::ERR_CORRELATION_NOT_FOUND,
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

    fn handle_wrong_ack_worker(
        &self,
        context: AckStateContext<'_>,
        correlation_id: uuid::Uuid,
        owner_worker_session_id: u64,
    ) -> DeliveryOutcome {
        let AckStateContext {
            envelope,
            meta,
            state,
            state_wait_us,
            state_hold_start,
        } = context;
        let state_hold_us = elapsed_micros_u64(state_hold_start);
        let pending_len = state.live_request_count();
        drop(state);

        self.histogram_observe_us("rpc_ack_state_wait_us", state_wait_us);
        self.histogram_observe_us("rpc_ack_state_hold_us", state_hold_us);
        self.counter_inc("rpc_acks_rejected_wrong_worker_total");

        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
            session_inbox_address(*envelope.destination().family(), meta.session_id)
        });
        self.forward_pending_error_deliveries(
            vec![RpcPendingErrorDelivery {
                correlation_id,
                caller_session_id: meta.session_id,
                caller_inbox_addr: worker_inbox_addr,
            }],
            crate::protocol::error_codes::rpc::ERR_RPC_WRONG_WORKER,
            RPC_WRONG_WORKER_ERROR,
            "rpc_worker_ownership_errors_forwarded_total",
            "rpc_worker_ownership_errors_dropped_total",
        );

        tracing::warn!(
            domain = "rpc",
            correlation_id = %correlation_id,
            pending_len,
            expected_worker_session_id = owner_worker_session_id,
            received_worker_session_id = meta.session_id,
            "Rejected RPC ACK from non-owner worker"
        );
        (None, None, true)
    }

    fn handle_ack_cleanup(
        &self,
        context: AckStateContext<'_>,
        correlation_id: uuid::Uuid,
    ) -> DeliveryOutcome {
        let AckStateContext {
            mut state,
            state_wait_us,
            state_hold_start,
            ..
        } = context;
        let pending_route_remove_start = Instant::now();
        let removed_pending = state.remove_pending_request(&correlation_id);
        let pending_route_remove_us = elapsed_micros_u64(pending_route_remove_start);
        let state_hold_us = elapsed_micros_u64(state_hold_start);
        let dispatch_route = removed_pending
            .as_ref()
            .map(|(pending, _)| pending.route.clone());
        drop(state);

        self.histogram_observe_us("rpc_pending_route_remove_us", pending_route_remove_us);
        self.histogram_observe_us("rpc_ack_state_wait_us", state_wait_us);
        self.histogram_observe_us("rpc_ack_state_hold_us", state_hold_us);
        let removed_pending_found = removed_pending.is_some();
        if let Some((_, pending_len)) = removed_pending {
            self.histogram_observe_us("rpc_pending_untrack_us", pending_route_remove_us);
            self.gauge_set("rpc_pending_requests", pending_len as u64);
            self.counter_inc("rpc_cleanup_acks_total");
            self.schedule_admin_snapshot(false);
            if let Some(route) = dispatch_route {
                self.dispatch_queued_requests_for_route(&route);
            }
        } else {
            self.counter_inc("rpc_cleanup_acks_missing_pending_total");
        }
        tracing::debug!(
            domain = "rpc",
            correlation_id = %correlation_id,
            "Request acknowledged and cleaned up"
        );
        (
            None,
            removed_pending_found.then_some(false),
            !removed_pending_found,
        )
    }
}
