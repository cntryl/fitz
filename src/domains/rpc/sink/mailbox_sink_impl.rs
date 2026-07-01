use super::state_model::{
    session_inbox_address, DeliveryError, Envelope, Instant, MailboxSink, Ordering, RouteFamily,
    RpcClientRequest, RpcClientResponseBody, RpcDomainSink, RpcPendingErrorDelivery,
    RpcPendingRequest, RpcPendingResponseDisposition, RpcQueuedRequest, RpcState, RpcWorker,
    RPC_BACKPRESSURE_ERROR, RPC_CORRELATION_NOT_FOUND_ERROR, RPC_DUPLICATE_CORRELATION_ERROR,
    RPC_INVALID_SEQUENCE_ERROR, RPC_MAX_PENDING_REQUESTS, RPC_NO_WORKERS_ERROR,
    RPC_WORKER_NOT_FOUND_ERROR, RPC_WRONG_WORKER_ERROR,
};
use crate::domains::rpc::protocol::{RpcMessage, RpcRequest, RpcResponse};
#[cfg(not(test))]
use crate::domains::rpc::{
    RpcClientForwardedResponse, RpcClientForwardedResponseBody, RpcWorkerAck,
};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;

impl MailboxSink for RpcDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(&envelope) {
            return Ok(());
        }
        self.ensure_active()?;
        Self::log_delivery(&envelope);

        let request = Self::extract_request(&envelope)?;
        let meta = request.meta;
        let request_started = self.record_request_start();
        Self::log_parse_start(meta);

        let rpc_msg = self.parse_request_message(request.message, request_started)?;
        let (response, snapshot_policy, request_failed) =
            self.handle_rpc_message(&envelope, &meta, rpc_msg);

        self.complete_request(
            &envelope,
            meta,
            response,
            snapshot_policy,
            request_failed,
            request_started,
        );

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

type DeliveryOutcome = (Option<RpcClientResponseBody>, Option<bool>, bool);

impl RpcDomainSink {
    fn handle_rpc_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        rpc_msg: RpcMessage,
    ) -> DeliveryOutcome {
        match rpc_msg {
            RpcMessage::RegisterWorker { worker_addr } => {
                self.handle_register_worker_message(envelope, meta, worker_addr)
            }
            RpcMessage::UnregisterWorker { worker_addr } => {
                self.handle_unregister_worker_message(meta, worker_addr)
            }
            RpcMessage::Request(req) => self.handle_request_message(envelope, meta, req),
            RpcMessage::Response(resp) => self.handle_response_message(envelope, meta, resp),
            RpcMessage::Ack { correlation_id } => {
                self.handle_ack_message(envelope, meta, correlation_id)
            }
            RpcMessage::Deliver(_) => (
                Some(RpcClientResponseBody::Error(
                    "Deliver not valid client message".to_string(),
                )),
                None,
                true,
            ),
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn handle_register_worker_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        worker_addr: crate::runtime::routing::RouteAddress,
    ) -> DeliveryOutcome {
        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
            session_inbox_address(*envelope.destination().family(), meta.session_id)
        });
        {
            let mut state = self.state.lock();
            let route_state = state.ensure_route_state(worker_addr.route());
            route_state.register_worker(RpcWorker::new(
                worker_addr.clone(),
                worker_inbox_addr,
                meta.session_id,
            ));
        }
        self.dispatch_queued_requests_for_route(worker_addr.route());
        tracing::debug!(
            domain = "rpc",
            worker = worker_addr.route().as_str(),
            session = meta.session_id,
            "Worker registered"
        );
        self.refresh_metrics_gauges();
        (
            Some(RpcClientResponseBody::Ok { data: vec![] }),
            Some(true),
            false,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn handle_unregister_worker_message(
        &self,
        meta: &crate::runtime::ClientFrameMeta,
        worker_addr: crate::runtime::routing::RouteAddress,
    ) -> DeliveryOutcome {
        let cleanup_result = self.apply_worker_unsubscribe(&worker_addr, meta.session_id);
        self.forward_worker_disconnect_errors(cleanup_result.disconnect_deliveries);
        tracing::debug!(
            domain = "rpc",
            worker = worker_addr.route().as_str(),
            session = meta.session_id,
            removed_workers = cleanup_result.removed_workers,
            removed_pending = cleanup_result.removed_pending,
            "Worker unregistered"
        );
        (
            Some(RpcClientResponseBody::Ok { data: vec![] }),
            Some(true),
            false,
        )
    }

    fn handle_request_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
    ) -> DeliveryOutcome {
        self.expire_timed_out_requests();
        self.counter_inc("rpc_requests_total");
        let caller_inbox_addr = envelope
            .source()
            .cloned()
            .unwrap_or_else(|| session_inbox_address(meta.route_family, meta.session_id));

        let state_wait_start = Instant::now();
        let mut state = self.state.lock();
        let state_wait_us = Self::elapsed_micros_u64(state_wait_start);
        let state_hold_start = Instant::now();
        let route_registry_lookup_start = Instant::now();
        let route_registry_lookup_us = Self::elapsed_micros_u64(route_registry_lookup_start);
        let duplicate_correlation = state.contains_correlation(&req.correlation_id);
        let route_exists = state.has_registered_workers(&req.route);
        let total_live_requests = state.live_request_count();
        let route_requires_queue = state.route_state(&req.route).is_some_and(|route_state| {
            route_state.has_queued_requests() || !route_state.has_available_worker()
        });

        if duplicate_correlation {
            return self.reject_duplicate_request(
                req,
                state,
                state_hold_start,
                state_wait_us,
                route_registry_lookup_us,
            );
        }
        if !route_exists {
            return self.reject_missing_worker_route(
                req,
                state,
                state_hold_start,
                state_wait_us,
                route_registry_lookup_us,
                0,
            );
        }
        if total_live_requests >= RPC_MAX_PENDING_REQUESTS {
            return self.reject_pending_capacity(
                req,
                state,
                state_hold_start,
                state_wait_us,
                route_registry_lookup_us,
            );
        }
        if route_requires_queue {
            return self.handle_queued_request_path(
                meta,
                req,
                caller_inbox_addr,
                state,
                state_hold_start,
                state_wait_us,
                route_registry_lookup_us,
            );
        }

        self.handle_immediate_request_dispatch(
            meta,
            req,
            caller_inbox_addr,
            state,
            state_hold_start,
            state_wait_us,
            route_registry_lookup_us,
        )
    }

    fn handle_response_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        resp: RpcResponse,
    ) -> DeliveryOutcome {
        self.counter_inc("rpc_responses_total");

        let state_wait_start = Instant::now();
        let mut state = self.state.lock();
        let state_wait_us = Self::elapsed_micros_u64(state_wait_start);
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
        let pending_route_lookup_us = Self::elapsed_micros_u64(pending_route_lookup_start);

        if let Some(owner_worker_session_id) = wrong_worker_owner {
            return self.handle_wrong_response_worker(
                envelope,
                meta,
                resp,
                state,
                state_wait_us,
                state_hold_start,
                pending_route_lookup_us,
                owner_worker_session_id,
            );
        }

        match caller_info.expect("caller info should be present when owner matched") {
            RpcPendingResponseDisposition::Forward {
                pending: caller_info,
                removed_pending,
            } => self.handle_forwarded_response(
                envelope,
                meta,
                resp,
                state,
                state_wait_us,
                state_hold_start,
                pending_route_lookup_us,
                caller_info,
                removed_pending,
            ),
            RpcPendingResponseDisposition::InvalidSequence {
                pending: caller_info,
                expected_seq,
            } => self.handle_invalid_response_sequence(
                envelope,
                meta,
                resp,
                state,
                state_wait_us,
                state_hold_start,
                pending_route_lookup_us,
                caller_info,
                expected_seq,
            ),
            RpcPendingResponseDisposition::Missing => self.handle_missing_response_pending(
                envelope,
                meta,
                resp,
                state,
                state_wait_us,
                state_hold_start,
                pending_route_lookup_us,
            ),
        }
    }

    fn handle_ack_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        correlation_id: uuid::Uuid,
    ) -> DeliveryOutcome {
        let state_wait_start = Instant::now();
        let state = self.state.lock();
        let state_wait_us = Self::elapsed_micros_u64(state_wait_start);
        let state_hold_start = Instant::now();
        let wrong_worker_owner = state
            .pending
            .worker_session_id(&correlation_id)
            .filter(|owner_session_id| *owner_session_id != meta.session_id);

        if let Some(owner_worker_session_id) = wrong_worker_owner {
            return self.handle_wrong_ack_worker(
                envelope,
                meta,
                correlation_id,
                state,
                state_wait_us,
                state_hold_start,
                owner_worker_session_id,
            );
        }

        self.handle_ack_cleanup(correlation_id, state, state_wait_us, state_hold_start)
    }

    fn observe_request_state_metrics(
        &self,
        route_registry_lookup_us: u64,
        state_wait_us: u64,
        state_hold_us: u64,
        worker_selection_us: u64,
    ) {
        self.histogram_observe_us("rpc_route_registry_lookup_us", route_registry_lookup_us);
        self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
        self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
        self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
        self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
    }

    #[allow(clippy::needless_pass_by_value)]
    fn reject_duplicate_request(
        &self,
        req: RpcRequest,
        state: parking_lot::MutexGuard<'_, RpcState>,
        state_hold_start: Instant,
        state_wait_us: u64,
        route_registry_lookup_us: u64,
    ) -> DeliveryOutcome {
        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
        drop(state);
        self.observe_request_state_metrics(
            route_registry_lookup_us,
            state_wait_us,
            state_hold_us,
            0,
        );
        self.counter_inc("rpc_requests_rejected_duplicate_correlation_total");
        tracing::warn!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            "Rejected request due to duplicate live correlation"
        );
        (
            Some(RpcClientResponseBody::CodeError {
                code: crate::protocol::error_codes::rpc::ERR_RPC_DUPLICATE_CORRELATION,
                message: RPC_DUPLICATE_CORRELATION_ERROR.to_string(),
            }),
            None,
            true,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn reject_missing_worker_route(
        &self,
        _req: RpcRequest,
        state: parking_lot::MutexGuard<'_, RpcState>,
        state_hold_start: Instant,
        state_wait_us: u64,
        route_registry_lookup_us: u64,
        worker_selection_us: u64,
    ) -> DeliveryOutcome {
        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
        drop(state);
        self.observe_request_state_metrics(
            route_registry_lookup_us,
            state_wait_us,
            state_hold_us,
            worker_selection_us,
        );
        self.counter_inc("rpc_requests_rejected_no_worker_total");
        (
            Some(RpcClientResponseBody::CodeError {
                code: crate::protocol::error_codes::rpc::ERR_ROUTE_NOT_REGISTERED,
                message: RPC_NO_WORKERS_ERROR.to_string(),
            }),
            None,
            true,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn reject_pending_capacity(
        &self,
        req: RpcRequest,
        state: parking_lot::MutexGuard<'_, RpcState>,
        state_hold_start: Instant,
        state_wait_us: u64,
        route_registry_lookup_us: u64,
    ) -> DeliveryOutcome {
        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
        drop(state);
        self.observe_request_state_metrics(
            route_registry_lookup_us,
            state_wait_us,
            state_hold_us,
            0,
        );
        self.counter_inc("rpc_requests_rejected_backpressure_total");
        tracing::warn!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            pending_requests = RPC_MAX_PENDING_REQUESTS,
            "Rejected request due to RPC pending capacity"
        );
        (
            Some(RpcClientResponseBody::CodeError {
                code: crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
                message: RPC_BACKPRESSURE_ERROR.to_string(),
            }),
            None,
            true,
        )
    }

    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    fn handle_queued_request_path(
        &self,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
        caller_inbox_addr: crate::runtime::routing::RouteAddress,
        mut state: parking_lot::MutexGuard<'_, RpcState>,
        state_hold_start: Instant,
        state_wait_us: u64,
        route_registry_lookup_us: u64,
    ) -> DeliveryOutcome {
        let queued_len = state
            .route_state(&req.route)
            .map_or(0, |route_state| route_state.queued_len());

        if queued_len >= self.route_pending_capacity {
            let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
            drop(state);
            self.observe_request_state_metrics(
                route_registry_lookup_us,
                state_wait_us,
                state_hold_us,
                0,
            );
            self.counter_inc("rpc_requests_rejected_backpressure_total");
            tracing::warn!(
                domain = "rpc",
                correlation_id = %req.correlation_id,
                route = req.route.as_str(),
                route_pending_capacity = self.route_pending_capacity,
                "Rejected request because the route-local RPC queue is full"
            );
            return (
                Some(RpcClientResponseBody::CodeError {
                    code: crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
                    message: RPC_BACKPRESSURE_ERROR.to_string(),
                }),
                None,
                true,
            );
        }

        let pending_track_start = Instant::now();
        let expires_at = Instant::now() + self.request_timeout;
        state.queue_request(
            req.correlation_id,
            RpcQueuedRequest::from_request(
                req.clone(),
                meta.session_id,
                caller_inbox_addr,
                expires_at,
            ),
        );
        let live_request_count = state.live_request_count() as u64;
        let pending_track_us = Self::elapsed_micros_u64(pending_track_start);
        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
        drop(state);

        self.observe_request_state_metrics(
            route_registry_lookup_us,
            state_wait_us,
            state_hold_us,
            0,
        );
        self.histogram_observe_us("rpc_pending_track_us", pending_track_us);
        self.histogram_observe_us("rpc_pending_route_index_us", 0);
        self.gauge_set("rpc_pending_requests", live_request_count);
        self.schedule_admin_snapshot(false);
        self.dispatch_queued_requests_for_route(&req.route);

        tracing::debug!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            live_request_count,
            "Request queued on route-local RPC pending queue"
        );

        (
            Some(RpcClientResponseBody::Ok { data: vec![] }),
            None,
            false,
        )
    }

    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    fn handle_immediate_request_dispatch(
        &self,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
        caller_inbox_addr: crate::runtime::routing::RouteAddress,
        mut state: parking_lot::MutexGuard<'_, RpcState>,
        state_hold_start: Instant,
        state_wait_us: u64,
        route_registry_lookup_us: u64,
    ) -> DeliveryOutcome {
        let worker_selection_start = Instant::now();
        let selected_worker = state
            .route_state(&req.route)
            .and_then(super::state_model::RpcRouteState::claim_worker);
        let worker_selection_us = Self::elapsed_micros_u64(worker_selection_start);

        let Some(worker) = selected_worker else {
            return self.reject_missing_worker_route(
                req,
                state,
                state_hold_start,
                state_wait_us,
                route_registry_lookup_us,
                worker_selection_us,
            );
        };

        let expires_at = Instant::now() + self.request_timeout;
        let pending_track_start = Instant::now();
        state.pending.track_pending(
            req.correlation_id,
            RpcPendingRequest::from_dispatch(
                &req,
                meta.session_id,
                caller_inbox_addr,
                worker.addr.clone(),
                worker.session_id,
                expires_at,
            ),
        );
        let live_request_count = state.live_request_count() as u64;
        let pending_track_us = Self::elapsed_micros_u64(pending_track_start);
        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
        drop(state);

        self.observe_request_state_metrics(
            route_registry_lookup_us,
            state_wait_us,
            state_hold_us,
            worker_selection_us,
        );
        self.histogram_observe_us("rpc_pending_track_us", pending_track_us);
        self.histogram_observe_us("rpc_pending_route_index_us", 0);
        self.gauge_set("rpc_pending_requests", live_request_count);
        self.schedule_admin_snapshot(false);

        let request_forward_start = Instant::now();
        let forward_result = self.forward_request_to_worker(&req, &worker);
        self.histogram_observe_elapsed_us("rpc_request_forward_us", request_forward_start);

        match forward_result {
            Ok(()) => {
                self.counter_inc("rpc_requests_dispatched_total");
                tracing::debug!(
                    domain = "rpc",
                    correlation_id = %req.correlation_id,
                    route = req.route.as_str(),
                    "Request forwarded to worker"
                );
                (
                    Some(RpcClientResponseBody::Ok { data: vec![] }),
                    Some(false),
                    false,
                )
            }
            Err(
                crate::runtime::RouteError::RouteNotFound(_)
                | crate::runtime::RouteError::DeliveryFailed(_, DeliveryError::ActorStopped),
            ) => self.handle_disconnected_worker_dispatch(req, worker.session_id),
            Err(crate::runtime::RouteError::DeliveryFailed(
                _,
                DeliveryError::MailboxFull { .. } | DeliveryError::HighLaneFull { .. },
            )) => self.handle_backpressured_worker_dispatch(req),
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn handle_disconnected_worker_dispatch(
        &self,
        req: RpcRequest,
        worker_session_id: u64,
    ) -> DeliveryOutcome {
        self.counter_inc("rpc_request_forward_errors_total");
        let cleanup_result = self.apply_session_cleanup(worker_session_id);
        let disconnect_deliveries = cleanup_result
            .disconnect_deliveries
            .into_iter()
            .filter(|delivery| delivery.correlation_id != req.correlation_id)
            .collect();
        self.forward_worker_disconnect_errors(disconnect_deliveries);
        tracing::warn!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            worker_session_id,
            "Worker disconnected before request dispatch completed"
        );
        (
            Some(RpcClientResponseBody::CodeError {
                code: crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
                message: RPC_WORKER_NOT_FOUND_ERROR.to_string(),
            }),
            None,
            true,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn handle_backpressured_worker_dispatch(&self, req: RpcRequest) -> DeliveryOutcome {
        self.counter_inc("rpc_request_forward_errors_total");
        let pending_len = self
            .remove_pending_request(&req.correlation_id)
            .map(|(_, pending_len)| pending_len)
            .unwrap_or_default();
        tracing::warn!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            pending_len,
            "Failed to forward request to worker due to backpressure"
        );
        (
            Some(RpcClientResponseBody::CodeError {
                code: crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
                message: RPC_BACKPRESSURE_ERROR.to_string(),
            }),
            None,
            true,
        )
    }

    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    fn handle_wrong_response_worker(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        resp: RpcResponse,
        state: parking_lot::MutexGuard<'_, RpcState>,
        state_wait_us: u64,
        state_hold_start: Instant,
        pending_route_lookup_us: u64,
        owner_worker_session_id: u64,
    ) -> DeliveryOutcome {
        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
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

    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    fn handle_forwarded_response(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        resp: RpcResponse,
        mut state: parking_lot::MutexGuard<'_, RpcState>,
        state_wait_us: u64,
        state_hold_start: Instant,
        pending_route_lookup_us: u64,
        caller_info: RpcPendingRequest,
        removed_pending: bool,
    ) -> DeliveryOutcome {
        let mut state_changed = false;
        let mut dispatch_route = None;

        if removed_pending {
            let completion_latency_us = Self::elapsed_micros_u64(caller_info.submitted_at_instant);
            state.release_worker_for_pending(&caller_info, Some(completion_latency_us));
            let live_request_count = state.live_request_count();
            self.histogram_observe_us("rpc_pending_route_remove_us", pending_route_lookup_us);
            self.histogram_observe_us("rpc_pending_untrack_us", pending_route_lookup_us);
            self.gauge_set("rpc_pending_requests", live_request_count as u64);
            state_changed = true;
            dispatch_route = Some(caller_info.route.clone());
        }

        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
        drop(state);

        self.histogram_observe_us("rpc_pending_route_lookup_us", pending_route_lookup_us);
        self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
        self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);

        self.forward_response_to_requester(meta, &resp, &caller_info);
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

            if let Err(e) = self.router.route(forward_envelope) {
                self.counter_inc("rpc_response_forward_errors_total");
                tracing::warn!(
                    domain = "rpc",
                    correlation_id = %resp.correlation_id,
                    error = ?e,
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

        if let Err(e) = self.router.route(ack_envelope) {
            self.counter_inc("rpc_ack_forward_errors_total");
            tracing::warn!(
                domain = "rpc",
                correlation_id = %correlation_id,
                error = ?e,
                "Failed to send ACK to worker"
            );
        } else {
            self.counter_inc("rpc_worker_acks_total");
        }
        self.histogram_observe_elapsed_us("rpc_ack_forward_us", ack_forward_start);
    }

    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    fn handle_invalid_response_sequence(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        resp: RpcResponse,
        mut state: parking_lot::MutexGuard<'_, RpcState>,
        state_wait_us: u64,
        state_hold_start: Instant,
        pending_route_lookup_us: u64,
        caller_info: RpcPendingRequest,
        expected_seq: u64,
    ) -> DeliveryOutcome {
        state.release_worker_for_pending(&caller_info, None);
        let live_request_count = state.live_request_count();
        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
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

        if let Some(caller_inbox_addr) = caller_info.caller_inbox_addr {
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

    #[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
    fn handle_missing_response_pending(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        resp: RpcResponse,
        state: parking_lot::MutexGuard<'_, RpcState>,
        state_wait_us: u64,
        state_hold_start: Instant,
        pending_route_lookup_us: u64,
    ) -> DeliveryOutcome {
        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
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

    #[allow(clippy::too_many_arguments)]
    fn handle_wrong_ack_worker(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        correlation_id: uuid::Uuid,
        state: parking_lot::MutexGuard<'_, RpcState>,
        state_wait_us: u64,
        state_hold_start: Instant,
        owner_worker_session_id: u64,
    ) -> DeliveryOutcome {
        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
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
        correlation_id: uuid::Uuid,
        mut state: parking_lot::MutexGuard<'_, RpcState>,
        state_wait_us: u64,
        state_hold_start: Instant,
    ) -> DeliveryOutcome {
        let pending_route_remove_start = Instant::now();
        let removed_pending = state.remove_pending_request(&correlation_id);
        let pending_route_remove_us = Self::elapsed_micros_u64(pending_route_remove_start);
        let state_hold_us = Self::elapsed_micros_u64(state_hold_start);
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

    fn elapsed_micros_u64(start: Instant) -> u64 {
        start.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
    }

    fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            let cleanup_result = self.apply_session_cleanup(cleanup.session_id);
            self.forward_worker_disconnect_errors(cleanup_result.disconnect_deliveries);
            return true;
        }

        false
    }

    fn ensure_active(&self) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        Ok(())
    }

    fn log_delivery(envelope: &Envelope) {
        tracing::debug!(
            domain = "rpc",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "RPC domain sink: received envelope"
        );
    }

    fn extract_request(envelope: &Envelope) -> Result<RpcClientRequest, DeliveryError> {
        Self::request_from_envelope(envelope).ok_or_else(|| {
            tracing::warn!(domain = "rpc", "Envelope payload was not RpcClientRequest");
            DeliveryError::ActorStopped
        })
    }

    fn record_request_start(&self) -> Option<Instant> {
        self.metrics
            .as_ref()
            .map(crate::domains::rpc::RpcMetrics::record_request_start)
    }

    fn log_parse_start(meta: crate::runtime::ClientFrameMeta) {
        tracing::debug!(
            domain = "rpc",
            session = meta.session_id,
            msg_type = meta.message_type,
            "RPC: parsing request"
        );
    }

    fn parse_request_message(
        &self,
        message: Result<crate::domains::rpc::protocol::RpcMessage, String>,
        request_started: Option<Instant>,
    ) -> Result<crate::domains::rpc::protocol::RpcMessage, DeliveryError> {
        match message {
            Ok(msg) => Ok(msg),
            Err(e) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(domain = "rpc", error = %e, "Failed to parse RPC message");
                Err(DeliveryError::ActorStopped)
            }
        }
    }

    fn complete_request(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: Option<RpcClientResponseBody>,
        snapshot_policy: Option<bool>,
        request_failed: bool,
        request_started: Option<Instant>,
    ) {
        if let Some(force_snapshot) = snapshot_policy {
            self.schedule_admin_snapshot(force_snapshot);
        }

        if let Some(response) = response {
            self.route_rpc_client_response(envelope, meta, &response);
        }

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if request_failed {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }
    }
}
