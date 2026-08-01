use super::state_model::{
    session_inbox_address, DeliveryError, Envelope, Instant, MailboxSink, Ordering,
    RpcClientRequest, RpcClientResponseBody, RpcDeliveryOutcome as DeliveryOutcome, RpcDomainActor,
    RpcDomainCommand, RpcDomainRuntime, RpcDomainSink, RpcWorker, RPC_BACKPRESSURE_ERROR,
    RPC_DUPLICATE_CORRELATION_ERROR, RPC_MAX_PENDING_REQUESTS, RPC_NO_WORKERS_ERROR,
    RPC_WORKER_NOT_FOUND_ERROR,
};
use crate::domains::rpc::protocol::{RpcMessage, RpcRequest};
use crate::runtime::{Actor, Context};

impl MailboxSink for RpcDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.actor.is_running()
            || self
                .family_runtime
                .as_ref()
                .is_some_and(|runtime| !runtime.is_running())
        {
            return Err(DeliveryError::ActorStopped);
        }
        if self.family_runtime.is_some() {
            self.deliver_to_family(envelope, false)
        } else {
            self.deliver_to_actor(envelope, false)
        }
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.actor.is_running()
            || self
                .family_runtime
                .as_ref()
                .is_some_and(|runtime| !runtime.is_running())
        {
            return Err(DeliveryError::ActorStopped);
        }
        if self.family_runtime.is_some() {
            self.deliver_to_family(envelope, true)
        } else {
            self.deliver_to_actor(envelope, true)
        }
    }
}

impl Actor for RpcDomainActor {
    type Message = RpcDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.runtime();
        match msg {
            RpcDomainCommand::Deliver(envelope, reply) => {
                let _ = reply.send(runtime.deliver_envelope(&envelope));
            }
            RpcDomainCommand::ExpireTimedOutRequestsAt(now, reply) => {
                runtime.expire_timed_out_requests_at(now);
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            RpcDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(runtime.live_counts());
            }
            #[cfg(test)]
            RpcDomainCommand::SyncAdminSnapshot(reply) => {
                runtime.sync_admin_snapshot();
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            RpcDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                runtime.refresh_admin_snapshot_if_dirty();
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            #[cfg(test)]
            RpcDomainCommand::ApplySessionCleanupForTests(session_id, reply) => {
                let _ = reply.send(runtime.apply_session_cleanup(session_id));
            }
            #[cfg(test)]
            RpcDomainCommand::ApplyWorkerUnsubscribeForTests(worker_addr, session_id, reply) => {
                let _ = reply.send(runtime.apply_worker_unsubscribe(&worker_addr, session_id));
            }
            #[cfg(test)]
            RpcDomainCommand::PanicForTests => {
                panic!("test RPC domain actor panic");
            }
        }
    }
}

impl RpcDomainSink {
    fn deliver_to_family(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        let Some(runtime) = self.family_runtime.as_ref() else {
            return Err(DeliveryError::ActorStopped);
        };
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let family = *envelope.destination().family();
        let command = RpcDomainCommand::Deliver(envelope, reply_tx);
        let lane = if high_priority {
            crate::runtime::FamilyActorLane::Control
        } else {
            crate::runtime::FamilyActorLane::Normal
        };
        runtime
            .try_enqueue(family, lane, command)
            .map_err(Self::family_enqueue_error)?;

        // Family delivery is called synchronously by the async transport edge.
        // Client responses are routed by the actor itself; waiting here would
        // block a Tokio worker while the synchronous domain actor runs.
        drop(reply_rx);
        Ok(())
    }

    fn family_enqueue_error(error: crate::runtime::FamilyActorEnqueueError) -> DeliveryError {
        match error {
            crate::runtime::FamilyActorEnqueueError::NormalLaneFull => DeliveryError::MailboxFull {
                capacity: crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
                current_len: crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
            },
            crate::runtime::FamilyActorEnqueueError::ControlLaneFull => {
                DeliveryError::HighLaneFull {
                    capacity: crate::runtime::FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
                    current_len: crate::runtime::FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
                }
            }
            crate::runtime::FamilyActorEnqueueError::UnknownFamily
            | crate::runtime::FamilyActorEnqueueError::ActorStopped => DeliveryError::ActorStopped,
        }
    }

    fn deliver_to_actor(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = RpcDomainCommand::Deliver(envelope, reply_tx);
        let enqueue_result = if high_priority {
            self.actor.try_send_high_priority(command)
        } else {
            self.actor.try_send(command)
        };
        enqueue_result?;

        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap_or(Err(DeliveryError::ActorStopped))
    }
}

impl RpcDomainRuntime<'_> {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;
        Self::log_delivery(envelope);

        let request = Self::extract_request(envelope)?;
        let meta = request.meta;
        let request_started = self.record_request_start();

        if !Self::valid_request_envelope(envelope, meta) {
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_rpc_client_response(
                envelope,
                response_meta,
                &RpcClientResponseBody::Error("route family mismatch".to_string()),
            );
            return Ok(());
        }

        Self::log_parse_start(meta);

        let Some(rpc_msg) = self.parse_request_message(
            envelope,
            meta,
            request.message,
            &request.raw_payload,
            request_started,
        ) else {
            return Ok(());
        };

        if !Self::valid_rpc_message(envelope, meta, &rpc_msg) {
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_rpc_client_response(
                envelope,
                response_meta,
                &RpcClientResponseBody::Error("route family mismatch".to_string()),
            );
            return Ok(());
        }

        let (response, snapshot_policy, request_failed) =
            self.handle_rpc_message(envelope, &meta, rpc_msg);

        self.complete_request(
            envelope,
            meta,
            response,
            snapshot_policy,
            request_failed,
            request_started,
        );

        Ok(())
    }

    fn handle_rpc_message(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        rpc_msg: RpcMessage,
    ) -> DeliveryOutcome {
        match rpc_msg {
            RpcMessage::RegisterWorker {
                worker_addr,
                max_concurrent,
            } => self.handle_register_worker_message(envelope, meta, worker_addr, max_concurrent),
            RpcMessage::UnregisterWorker { worker_addr } => {
                self.handle_unregister_worker_message(meta, worker_addr)
            }
            RpcMessage::Request(req) => self.handle_request_message(envelope, meta, req),
            RpcMessage::Response(resp) => self.handle_response_message(envelope, meta, &resp),
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
        max_concurrent: usize,
    ) -> DeliveryOutcome {
        let worker_inbox_addr = envelope.source().cloned().unwrap_or_else(|| {
            session_inbox_address(*envelope.destination().family(), meta.session_id)
        });
        {
            let mut state = self.state.lock();
            if matches!(
                state.register_worker(RpcWorker::new(
                    worker_addr.clone(),
                    worker_inbox_addr,
                    meta.session_id,
                    max_concurrent,
                )),
                super::state_model::RpcWorkerRegistration::WildcardLimit
            ) {
                return (
                    Some(RpcClientResponseBody::CodeError {
                        code: crate::dispatch::protocol::error_codes::rpc::ERR_SUBSCRIPTION_LIMIT,
                        message: "wildcard subscription limit exceeded (128 per session)"
                            .to_string(),
                    }),
                    Some(false),
                    false,
                );
            }
        }
        self.dispatch_queued_requests_for_family(*worker_addr.family());
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
        let dispatch = state.dispatch_or_queue_request_with_global_count(
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
            super::state_model::RpcRequestDispatch::Duplicate { request } => {
                self.reject_duplicate_request(envelope, meta, request)
            }
            super::state_model::RpcRequestDispatch::NoWorkers { request } => {
                self.reject_missing_worker_route(envelope, meta, request)
            }
            super::state_model::RpcRequestDispatch::GlobalCapacityFull { request } => {
                self.reject_pending_capacity(envelope, meta, request)
            }
            super::state_model::RpcRequestDispatch::RouteCapacityFull { request } => {
                self.reject_route_capacity(envelope, meta, request)
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
                worker,
                live_request_count,
            } => {
                self.forward_immediate_request(envelope, meta, request, &worker, live_request_count)
            }
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

    #[allow(clippy::needless_pass_by_value)]
    fn reject_duplicate_request(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
    ) -> DeliveryOutcome {
        self.counter_inc("rpc_requests_rejected_duplicate_correlation_total");
        tracing::warn!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            "Rejected request due to duplicate live correlation"
        );
        self.reject_request_with_terminal_error(
            envelope,
            *meta,
            &req,
            crate::dispatch::protocol::error_codes::rpc::ERR_RPC_DUPLICATE_CORRELATION,
            RPC_DUPLICATE_CORRELATION_ERROR,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn reject_missing_worker_route(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
    ) -> DeliveryOutcome {
        self.counter_inc("rpc_requests_rejected_no_worker_total");
        self.reject_request_with_terminal_error(
            envelope,
            *meta,
            &req,
            crate::dispatch::protocol::error_codes::rpc::ERR_ROUTE_NOT_REGISTERED,
            RPC_NO_WORKERS_ERROR,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn reject_pending_capacity(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
    ) -> DeliveryOutcome {
        self.counter_inc("rpc_requests_rejected_backpressure_total");
        tracing::warn!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            pending_requests = RPC_MAX_PENDING_REQUESTS,
            "Rejected request due to RPC pending capacity"
        );
        self.reject_request_with_terminal_error(
            envelope,
            *meta,
            &req,
            crate::dispatch::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
            RPC_BACKPRESSURE_ERROR,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn reject_route_capacity(
        &self,
        envelope: &Envelope,
        meta: &crate::runtime::ClientFrameMeta,
        req: RpcRequest,
    ) -> DeliveryOutcome {
        self.counter_inc("rpc_requests_rejected_backpressure_total");
        tracing::warn!(
            domain = "rpc",
            correlation_id = %req.correlation_id,
            route = req.route.as_str(),
            route_pending_capacity = self.route_pending_capacity,
            "Rejected request because the route-local RPC queue is full"
        );
        self.reject_request_with_terminal_error(
            envelope,
            *meta,
            &req,
            crate::dispatch::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
            RPC_BACKPRESSURE_ERROR,
        )
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
        worker: &super::state_model::RpcWorkerDispatch,
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
                    DeliveryError::ActorStopped | DeliveryError::SinkPanicked,
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

    fn reject_request_with_terminal_error(
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
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: Result<
            crate::domains::rpc::protocol::RpcMessage,
            crate::domains::rpc::protocol::RpcDecodeError,
        >,
        raw_payload: &[u8],
        request_started: Option<Instant>,
    ) -> Option<crate::domains::rpc::protocol::RpcMessage> {
        match message {
            Ok(msg) => Some(msg),
            Err(e) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(domain = "rpc", error = %e, "Failed to parse RPC message");
                let (error_code, error_message) = match &e {
                    crate::domains::rpc::protocol::RpcDecodeError::InvalidCallRoute(_) => (
                        crate::dispatch::protocol::error_codes::rpc::ERR_INVALID_ROUTE,
                        "Invalid RPC call route",
                    ),
                    crate::domains::rpc::protocol::RpcDecodeError::InvalidRegistrationPattern(
                        _,
                    ) => (
                        crate::dispatch::protocol::error_codes::rpc::ERR_INVALID_SUBSCRIPTION_PATTERN,
                        "Invalid RPC registration pattern",
                    ),
                    crate::domains::rpc::protocol::RpcDecodeError::StructurallyUndecodable(_) => (
                        crate::dispatch::protocol::error_codes::rpc::ERR_BACKEND_ERROR,
                        "RPC message parse failed",
                    ),
                };
                if meta.message_type == 302 {
                    if let Ok(correlation_id) =
                        crate::dispatch::protocol::rpc_codec::extract_request_correlation_id(
                            raw_payload,
                        )
                    {
                        self.route_rpc_terminal_error_response(
                            envelope,
                            Self::response_meta_for_source(envelope, meta),
                            correlation_id,
                            error_code,
                            error_message,
                        );
                        return None;
                    }
                }
                self.route_rpc_client_response(
                    envelope,
                    Self::response_meta_for_source(envelope, meta),
                    &RpcClientResponseBody::CodeError {
                        code: error_code,
                        message: error_message.to_string(),
                    },
                );
                None
            }
        }
    }

    fn valid_request_envelope(envelope: &Envelope, meta: crate::runtime::ClientFrameMeta) -> bool {
        meta.route_family == *envelope.destination().family()
            && envelope
                .source()
                .is_none_or(|source| *source.family() == meta.route_family)
    }

    fn response_meta_for_source(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
    ) -> crate::runtime::ClientFrameMeta {
        envelope.source().map_or(meta, |source| {
            let mut response_meta = meta;
            response_meta.route_family = *source.family();
            response_meta
        })
    }

    fn valid_rpc_message(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: &crate::domains::rpc::protocol::RpcMessage,
    ) -> bool {
        use crate::domains::rpc::protocol::RpcMessage;

        match message {
            RpcMessage::RegisterWorker { worker_addr, .. }
            | RpcMessage::UnregisterWorker { worker_addr } => {
                *worker_addr.family() == meta.route_family
            }
            RpcMessage::Request(request) => request.family_id == meta.route_family,
            RpcMessage::Response(_) => true,
            RpcMessage::Deliver(_) => {
                meta.channel == crate::runtime::ClientChannel::Internal
                    && envelope.source().is_none()
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
