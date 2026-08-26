//! Request admission decisions and delivery to workers.
//!
//! Covers the path from "a parsed `Request` message" through admission
//! (accept/queue/reject), forwarding to a worker, and draining the
//! route-local queue once a worker becomes available again.

#[cfg(test)]
use super::state_model::RPC_MSG_TYPE_REQUEST;
use super::state_model::{
    session_inbox_address, DeliveryError, Envelope, Instant, RpcDeliveryOutcome as DeliveryOutcome,
    RpcDomainRuntime, RpcPendingErrorDelivery, RpcPendingRequest, RpcQueuedDispatch,
    RpcRequestRejection, RpcRequestState, RpcWorkerDispatch, RPC_BACKPRESSURE_ERROR,
    RPC_DUPLICATE_CORRELATION_ERROR, RPC_MAX_PENDING_REQUESTS, RPC_NO_WORKERS_ERROR,
    RPC_WORKER_NOT_FOUND_ERROR,
};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::domains::rpc::protocol::RpcRequest;
#[cfg(not(test))]
use crate::domains::rpc::RpcWorkerRequestDelivery;

struct RejectionSpec {
    metric: &'static str,
    error_code: u16,
    message: &'static str,
    reason: &'static str,
}

const REJECTION_SPECS: [RejectionSpec; 4] = [
    RejectionSpec {
        metric: "rpc_requests_rejected_duplicate_correlation_total",
        error_code: crate::dispatch::protocol::error_codes::rpc::ERR_RPC_DUPLICATE_CORRELATION,
        message: RPC_DUPLICATE_CORRELATION_ERROR,
        reason: "duplicate live correlation",
    },
    RejectionSpec {
        metric: "rpc_requests_rejected_no_worker_total",
        error_code: crate::dispatch::protocol::error_codes::rpc::ERR_ROUTE_NOT_REGISTERED,
        message: RPC_NO_WORKERS_ERROR,
        reason: "no matching worker registration",
    },
    RejectionSpec {
        metric: "rpc_requests_rejected_backpressure_total",
        error_code: crate::dispatch::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
        message: RPC_BACKPRESSURE_ERROR,
        reason: "global pending capacity",
    },
    RejectionSpec {
        metric: "rpc_requests_rejected_backpressure_total",
        error_code: crate::dispatch::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
        message: RPC_BACKPRESSURE_ERROR,
        reason: "route pending capacity",
    },
];

/// Aggregate name the admin `backpressure_rejects_total` field reads.
pub(in crate::domains::rpc::sink) const RPC_BACKPRESSURE_REJECTS_METRIC: &str =
    "rpc_backpressure_rejects_total";

impl RpcDomainRuntime<'_> {
    pub(super) fn handle_request_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
    ) -> DeliveryOutcome {
        self.expire_timed_out_requests_inline_if_due();
        self.counter_inc("rpc_requests_total");
        let caller_inbox_addr = envelope
            .source()
            .cloned()
            .unwrap_or_else(|| session_inbox_address(meta.route_family, meta.session_id));

        let metrics_enabled = self.metrics.is_some();
        let state_wait_start = metrics_enabled.then(Instant::now);
        let mut state = self.state.lock();
        let state_wait_us = state_wait_start.map_or(0, Self::elapsed_micros_u64);
        let state_hold_start = metrics_enabled.then(Instant::now);
        let dispatch = RpcRequestState::dispatch_or_queue(
            &mut *state,
            req,
            meta.session_id,
            caller_inbox_addr,
            self.request_timeout,
            self.route_pending_capacity,
            RPC_MAX_PENDING_REQUESTS,
            self.enforce_global_pending_count
                .then_some(self.global_pending_count.as_ref()),
        );
        let state_hold_us = state_hold_start.map_or(0, Self::elapsed_micros_u64);
        drop(state);

        self.observe_request_state_metrics(0, state_wait_us, state_hold_us, 0);

        match dispatch {
            super::state_model::RpcRequestDispatch::Rejected { request, reason } => {
                self.reject_with_spec(envelope, meta, &request, reason)
            }
            super::state_model::RpcRequestDispatch::Queued {
                route,
                correlation_id,
                live_request_count,
            } => self.accept_queued_request(
                &route,
                meta.route_family,
                correlation_id,
                live_request_count,
            ),
            super::state_model::RpcRequestDispatch::Immediate {
                request,
                registration,
                live_request_count,
            } => self.forward_immediate_request(
                envelope,
                meta,
                request,
                &registration,
                live_request_count,
            ),
        }
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

    fn reject_with_spec(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        req: &RpcRequest,
        reason: RpcRequestRejection,
    ) -> DeliveryOutcome {
        let spec = &REJECTION_SPECS[reason as usize];
        self.counter_inc(spec.metric);
        // The admin surface reads one aggregate name. Without this, admission
        // control rejections were invisible in `backpressure_rejects_total`
        // even though they are exactly what it is meant to report.
        if spec.error_code == crate::dispatch::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE {
            self.counter_inc(RPC_BACKPRESSURE_REJECTS_METRIC);
        }
        tracing::warn!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            reason = spec.reason,
            "Rejected RPC request"
        );
        self.reject_request_with_terminal_error(envelope, *meta, req, spec.error_code, spec.message)
    }

    fn accept_queued_request(
        &self,
        route: &crate::runtime::routing::Route,
        family: crate::runtime::routing::RouteFamily,
        correlation_id: uuid::Uuid,
        live_request_count: usize,
    ) -> DeliveryOutcome {
        self.histogram_observe_us("rpc_pending_track_us", 0);
        self.histogram_observe_us("rpc_pending_route_index_us", 0);
        self.gauge_set("rpc_pending_requests", live_request_count as u64);
        self.schedule_admin_snapshot(false);
        self.dispatch_queued_requests_for_family(family);

        tracing::debug!(
            domain = "rpc",
            correlation_id = %correlation_id,
            route = route.as_str(),
            live_request_count,
            "Request queued on route-local RPC pending queue"
        );

        (None, None, false)
    }

    fn forward_immediate_request(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
        worker: &RpcWorkerDispatch,
        live_request_count: usize,
    ) -> DeliveryOutcome {
        self.histogram_observe_us("rpc_pending_track_us", 0);
        self.histogram_observe_us("rpc_pending_route_index_us", 0);
        self.gauge_set("rpc_pending_requests", live_request_count as u64);
        self.schedule_admin_snapshot(false);

        let metrics_enabled = self.metrics.is_some();
        let request_forward_start = metrics_enabled.then(Instant::now);
        let forward_result = self.forward_request_to_worker(&req, worker);
        if let Some(request_forward_start) = request_forward_start {
            self.histogram_observe_elapsed_us("rpc_request_forward_us", request_forward_start);
        }

        match forward_result {
            Ok(()) => {
                self.counter_inc("rpc_requests_dispatched_total");
                tracing::debug!(
                    domain = "rpc",
                    correlation_id = %req.correlation_id,
                    route = req.route.as_str(),
                    "Request forwarded to worker"
                );
                (None, Some(false), false)
            }
            Err(
                crate::runtime::RouteError::RouteNotFound(_)
                | crate::runtime::RouteError::DeliveryFailed(
                    _,
                    DeliveryError::ActorStopped
                    | DeliveryError::Timeout
                    | DeliveryError::SinkPanicked
                    | DeliveryError::InvalidPayload { .. },
                ),
            ) => self.handle_disconnected_worker_dispatch(envelope, meta, req, worker.session_id),
            Err(crate::runtime::RouteError::DeliveryFailed(
                _,
                DeliveryError::MailboxFull { .. } | DeliveryError::HighLaneFull { .. },
            )) => self.handle_backpressured_worker_dispatch(envelope, meta, req),
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn handle_disconnected_worker_dispatch(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
        worker_session_id: u64,
    ) -> DeliveryOutcome {
        self.counter_inc("rpc_request_forward_errors_total");
        let cleanup_result = self.apply_session_cleanup(worker_session_id);
        let disconnect_deliveries = cleanup_result
            .disconnect_deliveries
            .into_iter()
            .filter(|delivery| {
                delivery.correlation_id != req.correlation_id
                    || *delivery.caller_inbox_addr.family() != meta.route_family
            })
            .collect();
        self.forward_worker_disconnect_errors(disconnect_deliveries);
        tracing::warn!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            worker_session_id,
            "Worker disconnected before request dispatch completed"
        );
        self.reject_request_with_terminal_error(
            envelope,
            *meta,
            &req,
            crate::dispatch::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
            RPC_WORKER_NOT_FOUND_ERROR,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn handle_backpressured_worker_dispatch(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
    ) -> DeliveryOutcome {
        self.counter_inc("rpc_request_forward_errors_total");
        // Inline dispatch backpressure counts toward the same aggregate as the
        // deferred path; previously only the deferred path was visible.
        self.counter_inc(RPC_BACKPRESSURE_REJECTS_METRIC);
        let pending_len = self
            .remove_pending_request_for_family(meta.route_family, &req.correlation_id)
            .map(|(_, pending_len)| pending_len)
            .unwrap_or_default();
        tracing::warn!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            pending_len,
            "Failed to forward request to worker due to backpressure"
        );
        self.reject_request_with_terminal_error(
            envelope,
            *meta,
            &req,
            crate::dispatch::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
            RPC_BACKPRESSURE_ERROR,
        )
    }

    pub(super) fn reject_request_with_terminal_error(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        req: &RpcRequest,
        code: u16,
        message: &'static str,
    ) -> DeliveryOutcome {
        self.route_rpc_terminal_error_response(envelope, meta, req.correlation_id, code, message);
        (None, None, true)
    }

    pub(super) fn elapsed_micros_u64(start: Instant) -> u64 {
        start.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
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
                crate::dispatch::protocol::tlv::MessageType::new(RPC_MSG_TYPE_REQUEST),
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

        match self.forward_request_to_worker(&dispatch.request, &dispatch.registration) {
            Ok(()) => {
                self.counter_inc("rpc_requests_dispatched_total");
            }
            Err(
                crate::runtime::RouteError::RouteNotFound(_)
                | crate::runtime::RouteError::DeliveryFailed(
                    _,
                    DeliveryError::ActorStopped
                    | DeliveryError::Timeout
                    | DeliveryError::SinkPanicked
                    | DeliveryError::InvalidPayload { .. },
                ),
            ) => {
                self.counter_inc("rpc_request_forward_errors_total");
                let cleanup_result = self.apply_session_cleanup(dispatch.registration.session_id);
                self.forward_worker_disconnect_errors(cleanup_result.disconnect_deliveries);
            }
            Err(crate::runtime::RouteError::DeliveryFailed(
                _,
                DeliveryError::MailboxFull { .. } | DeliveryError::HighLaneFull { .. },
            )) => {
                self.counter_inc("rpc_request_forward_errors_total");
                self.counter_inc(RPC_BACKPRESSURE_REJECTS_METRIC);
                if let Some((pending, pending_len)) = self.remove_pending_request_for_family(
                    *dispatch.registration.addr.family(),
                    &dispatch.request.correlation_id,
                ) {
                    if let Some(caller_inbox_addr) = pending.dispatch_info.caller_inbox_addr {
                        self.forward_pending_error_deliveries(
                            vec![RpcPendingErrorDelivery {
                                correlation_id: dispatch.request.correlation_id,
                                caller_session_id: pending.dispatch_info.caller_session_id,
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
}
