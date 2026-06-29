use super::state_model::*;

impl MailboxSink for RpcDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(&envelope) {
            return Ok(());
        }
        self.ensure_active()?;
        self.log_delivery(&envelope);

        let request = self.extract_request(&envelope)?;
        let meta = request.meta;
        let request_started = self.record_request_start();
        self.log_parse_start(meta);

        let rpc_msg = self.parse_request_message(request.message, request_started)?;

        use crate::domains::rpc::protocol::RpcMessage;

        let (response, snapshot_policy, request_failed) = match rpc_msg {
            RpcMessage::RegisterWorker { worker_addr } => {
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
            RpcMessage::UnregisterWorker { worker_addr } => {
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
            RpcMessage::Request(req) => {
                self.expire_timed_out_requests();
                self.counter_inc("rpc_requests_total");
                let caller_inbox_addr = envelope
                    .source()
                    .cloned()
                    .unwrap_or_else(|| session_inbox_address(meta.route_family, meta.session_id));

                let state_wait_start = Instant::now();
                let mut state = self.state.lock();
                let state_wait_us = state_wait_start.elapsed().as_micros() as u64;
                let state_hold_start = Instant::now();
                let route_registry_lookup_start = Instant::now();
                let route_registry_lookup_us =
                    route_registry_lookup_start.elapsed().as_micros() as u64;
                let duplicate_correlation = state.contains_correlation(&req.correlation_id);
                let route_exists = state.has_registered_workers(&req.route);
                let total_live_requests = state.live_request_count();
                let route_requires_queue =
                    state.route_state(&req.route).is_some_and(|route_state| {
                        route_state.has_queued_requests() || !route_state.has_available_worker()
                    });
                let mut worker_selection_us = 0_u64;

                if duplicate_correlation {
                    let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                    drop(state);

                    self.histogram_observe_us(
                        "rpc_route_registry_lookup_us",
                        route_registry_lookup_us,
                    );
                    self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                    self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                    self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                    self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
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
                } else if !route_exists {
                    let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                    drop(state);

                    self.histogram_observe_us(
                        "rpc_route_registry_lookup_us",
                        route_registry_lookup_us,
                    );
                    self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                    self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                    self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                    self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
                    self.counter_inc("rpc_requests_rejected_no_worker_total");
                    (
                        Some(RpcClientResponseBody::CodeError {
                            code: crate::protocol::error_codes::rpc::ERR_ROUTE_NOT_REGISTERED,
                            message: RPC_NO_WORKERS_ERROR.to_string(),
                        }),
                        None,
                        true,
                    )
                } else if total_live_requests >= RPC_MAX_PENDING_REQUESTS {
                    let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                    drop(state);

                    self.histogram_observe_us(
                        "rpc_route_registry_lookup_us",
                        route_registry_lookup_us,
                    );
                    self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                    self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                    self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                    self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
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
                } else if route_requires_queue {
                    let queued_len = state
                        .route_state(&req.route)
                        .map_or(0, |route_state| route_state.queued_len());

                    if queued_len >= self.route_pending_capacity {
                        let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                        drop(state);

                        self.histogram_observe_us(
                            "rpc_route_registry_lookup_us",
                            route_registry_lookup_us,
                        );
                        self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                        self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
                        self.counter_inc("rpc_requests_rejected_backpressure_total");
                        tracing::warn!(
                            domain = "rpc",
                            correlation_id = %req.correlation_id,
                            route = req.route.as_str(),
                            route_pending_capacity = self.route_pending_capacity,
                            "Rejected request because the route-local RPC queue is full"
                        );
                        (
                            Some(RpcClientResponseBody::CodeError {
                                code: crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
                                message: RPC_BACKPRESSURE_ERROR.to_string(),
                            }),
                            None,
                            true,
                        )
                    } else {
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
                        let pending_track_us = pending_track_start.elapsed().as_micros() as u64;
                        let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                        drop(state);

                        self.histogram_observe_us(
                            "rpc_route_registry_lookup_us",
                            route_registry_lookup_us,
                        );
                        self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                        self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
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
                } else {
                    let worker_selection_start = Instant::now();
                    let selected_worker = state
                        .route_state(&req.route)
                        .and_then(|route_state| route_state.claim_worker());
                    worker_selection_us = worker_selection_start.elapsed().as_micros() as u64;

                    if let Some(worker) = selected_worker {
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
                        let pending_track_us = pending_track_start.elapsed().as_micros() as u64;
                        let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                        drop(state);

                        self.histogram_observe_us(
                            "rpc_route_registry_lookup_us",
                            route_registry_lookup_us,
                        );
                        self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                        self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
                        self.histogram_observe_us("rpc_pending_track_us", pending_track_us);
                        self.histogram_observe_us("rpc_pending_route_index_us", 0);
                        self.gauge_set("rpc_pending_requests", live_request_count);
                        self.schedule_admin_snapshot(false);

                        let request_forward_start = Instant::now();
                        let forward_result = self.forward_request_to_worker(&req, &worker);
                        self.histogram_observe_elapsed_us(
                            "rpc_request_forward_us",
                            request_forward_start,
                        );

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
                                | crate::runtime::RouteError::DeliveryFailed(
                                    _,
                                    DeliveryError::ActorStopped,
                                ),
                            ) => {
                                self.counter_inc("rpc_request_forward_errors_total");
                                let cleanup_result = self.apply_session_cleanup(worker.session_id);
                                let disconnect_deliveries = cleanup_result
                                    .disconnect_deliveries
                                    .into_iter()
                                    .filter(|delivery| {
                                        delivery.correlation_id != req.correlation_id
                                    })
                                    .collect();
                                self.forward_worker_disconnect_errors(disconnect_deliveries);
                                tracing::warn!(
                                    domain = "rpc",
                                    correlation_id = %req.correlation_id,
                                    route = req.route.as_str(),
                                    worker_session_id = worker.session_id,
                                    "Worker disconnected before request dispatch completed"
                                );
                                (
                                    Some(RpcClientResponseBody::CodeError {
                                        code:
                                            crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
                                        message: RPC_WORKER_NOT_FOUND_ERROR.to_string(),
                                    }),
                                    None,
                                    true,
                                )
                            }
                            Err(crate::runtime::RouteError::DeliveryFailed(
                                _,
                                DeliveryError::MailboxFull { .. }
                                | DeliveryError::HighLaneFull { .. },
                            )) => {
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
                                        code:
                                            crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
                                        message: RPC_BACKPRESSURE_ERROR.to_string(),
                                    }),
                                    None,
                                    true,
                                )
                            }
                        }
                    } else {
                        let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                        drop(state);

                        self.histogram_observe_us(
                            "rpc_route_registry_lookup_us",
                            route_registry_lookup_us,
                        );
                        self.histogram_observe_us("rpc_dispatch_state_lock_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_wait_us", state_wait_us);
                        self.histogram_observe_us("rpc_dispatch_state_hold_us", state_hold_us);
                        self.histogram_observe_us("rpc_worker_selection_us", worker_selection_us);
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
                }
            }
            RpcMessage::Response(resp) => {
                self.counter_inc("rpc_responses_total");

                let state_wait_start = Instant::now();
                let mut state = self.state.lock();
                let state_wait_us = state_wait_start.elapsed().as_micros() as u64;
                let state_hold_start = Instant::now();
                let pending_route_lookup_start = Instant::now();
                let wrong_worker_owner = state
                    .pending
                    .worker_session_id(&resp.correlation_id)
                    .filter(|owner_session_id| *owner_session_id != meta.session_id);
                let caller_info = wrong_worker_owner.is_none().then(|| {
                    state.pending.pending_for_response(
                        &resp.correlation_id,
                        resp.seq,
                        resp.stream_end,
                    )
                });
                let pending_route_lookup_us =
                    pending_route_lookup_start.elapsed().as_micros() as u64;
                let mut state_changed = false;
                let mut dispatch_route = None;

                let response_result = if let Some(owner_worker_session_id) = wrong_worker_owner {
                    let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                    let pending_len = state.live_request_count();
                    drop(state);

                    self.histogram_observe_us(
                        "rpc_pending_route_lookup_us",
                        pending_route_lookup_us,
                    );
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
                } else {
                    match caller_info.expect("caller info should be present when owner matched") {
                        RpcPendingResponseDisposition::Forward {
                            pending: caller_info,
                            removed_pending,
                        } => {
                            let pending_lookup_us = pending_route_lookup_us;
                            if removed_pending {
                                let completion_latency_us =
                                    caller_info.submitted_at_instant.elapsed().as_micros() as u64;
                                state.release_worker_for_pending(
                                    &caller_info,
                                    Some(completion_latency_us),
                                );
                                let live_request_count = state.live_request_count();
                                self.histogram_observe_us(
                                    "rpc_pending_route_remove_us",
                                    pending_lookup_us,
                                );
                                self.histogram_observe_us(
                                    "rpc_pending_untrack_us",
                                    pending_lookup_us,
                                );
                                self.gauge_set("rpc_pending_requests", live_request_count as u64);
                                state_changed = true;
                                dispatch_route = Some(caller_info.route.clone());
                            }

                            let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                            drop(state);

                            self.histogram_observe_us(
                                "rpc_pending_route_lookup_us",
                                pending_route_lookup_us,
                            );
                            self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
                            self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);

                            let response_forward_start = Instant::now();
                            if let Some(caller_inbox_addr) = caller_info.caller_inbox_addr.as_ref()
                            {
                                #[cfg(test)]
                                let forward_envelope = {
                                    let mut payload_encoder =
                                        crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
                                    let encoded_response =
                                        crate::protocol::rpc_codec::encode_response_message_into(
                                            &resp,
                                            &mut payload_encoder,
                                        );
                                    let forward_ctx = FrameContext::new(
                                        caller_info.caller_session_id,
                                        super::mailbox_adapter::test_protocol_channel_from_client(
                                            meta.channel,
                                        ),
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
                            self.histogram_observe_elapsed_us(
                                "rpc_response_forward_us",
                                response_forward_start,
                            );

                            let ack_forward_start = Instant::now();
                            let worker_inbox_addr =
                                envelope.source().cloned().unwrap_or_else(|| {
                                    session_inbox_address(
                                        *envelope.destination().family(),
                                        meta.session_id,
                                    )
                                });
                            #[cfg(test)]
                            let ack_envelope = {
                                let mut payload_encoder =
                                    crate::protocol::payload_codec::PayloadEncoder::with_capacity(
                                        64,
                                    );
                                let ack_payload = crate::protocol::rpc_codec::encode_ack_into(
                                    &resp.correlation_id,
                                    &mut payload_encoder,
                                );
                                let ack_ctx = FrameContext::new(
                                    meta.session_id,
                                    super::mailbox_adapter::test_protocol_channel_from_client(
                                        meta.channel,
                                    ),
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
                                    resp.correlation_id,
                                ),
                            );

                            if let Err(e) = self.router.route(ack_envelope) {
                                self.counter_inc("rpc_ack_forward_errors_total");
                                tracing::warn!(
                                    domain = "rpc",
                                    correlation_id = %resp.correlation_id,
                                    error = ?e,
                                    "Failed to send ACK to worker"
                                );
                            } else {
                                self.counter_inc("rpc_worker_acks_total");
                            }
                            self.histogram_observe_elapsed_us(
                                "rpc_ack_forward_us",
                                ack_forward_start,
                            );

                            tracing::debug!(
                                domain = "rpc",
                                correlation_id = %resp.correlation_id,
                                stream_end = resp.stream_end,
                                "Response forwarded to requester and ACK sent to worker"
                            );
                            (None, state_changed.then_some(false), false)
                        }
                        RpcPendingResponseDisposition::InvalidSequence {
                            pending: caller_info,
                            expected_seq,
                        } => {
                            state.release_worker_for_pending(&caller_info, None);
                            let live_request_count = state.live_request_count();
                            let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                            drop(state);
                            self.histogram_observe_us(
                                "rpc_pending_route_lookup_us",
                                pending_route_lookup_us,
                            );
                            self.histogram_observe_us(
                                "rpc_pending_route_remove_us",
                                pending_route_lookup_us,
                            );
                            self.histogram_observe_us(
                                "rpc_pending_untrack_us",
                                pending_route_lookup_us,
                            );
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

                            let worker_inbox_addr =
                                envelope.source().cloned().unwrap_or_else(|| {
                                    session_inbox_address(
                                        *envelope.destination().family(),
                                        meta.session_id,
                                    )
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
                            (None, state_changed.then_some(false), true)
                        }
                        RpcPendingResponseDisposition::Missing => {
                            let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                            let live_request_count = state.live_request_count();
                            drop(state);
                            self.histogram_observe_us(
                                "rpc_pending_route_lookup_us",
                                pending_route_lookup_us,
                            );
                            self.histogram_observe_us("rpc_response_state_wait_us", state_wait_us);
                            self.histogram_observe_us("rpc_response_state_hold_us", state_hold_us);
                            self.counter_inc("rpc_responses_missing_pending_total");
                            let worker_inbox_addr =
                                envelope.source().cloned().unwrap_or_else(|| {
                                    session_inbox_address(
                                        *envelope.destination().family(),
                                        meta.session_id,
                                    )
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
                            (None, state_changed.then_some(false), true)
                        }
                    }
                };

                if let Some(route) = dispatch_route {
                    self.schedule_admin_snapshot(false);
                    self.dispatch_queued_requests_for_route(&route);
                }

                response_result
            }
            RpcMessage::Ack { correlation_id } => {
                let state_wait_start = Instant::now();
                let mut state = self.state.lock();
                let state_wait_us = state_wait_start.elapsed().as_micros() as u64;
                let state_hold_start = Instant::now();
                let wrong_worker_owner = state
                    .pending
                    .worker_session_id(&correlation_id)
                    .filter(|owner_session_id| *owner_session_id != meta.session_id);
                let mut dispatch_route = None;

                if let Some(owner_worker_session_id) = wrong_worker_owner {
                    let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
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
                } else {
                    let pending_route_remove_start = Instant::now();
                    let removed_pending = state.remove_pending_request(&correlation_id);
                    let pending_route_remove_us =
                        pending_route_remove_start.elapsed().as_micros() as u64;
                    let state_hold_us = state_hold_start.elapsed().as_micros() as u64;
                    if let Some((pending, _)) = removed_pending.as_ref() {
                        dispatch_route = Some(pending.route.clone());
                    }
                    drop(state);

                    self.histogram_observe_us(
                        "rpc_pending_route_remove_us",
                        pending_route_remove_us,
                    );
                    self.histogram_observe_us("rpc_ack_state_wait_us", state_wait_us);
                    self.histogram_observe_us("rpc_ack_state_hold_us", state_hold_us);
                    let removed_pending_found = removed_pending.is_some();
                    if let Some((_, pending_len)) = removed_pending {
                        self.histogram_observe_us(
                            "rpc_pending_untrack_us",
                            pending_route_remove_us,
                        );
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
            RpcMessage::Deliver(_) => (
                Some(RpcClientResponseBody::Error(
                    "Deliver not valid client message".to_string(),
                )),
                None,
                true,
            ),
        };

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

impl RpcDomainSink {
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

    fn log_delivery(&self, envelope: &Envelope) {
        tracing::debug!(
            domain = "rpc",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "RPC domain sink: received envelope"
        );
    }

    fn extract_request(&self, envelope: &Envelope) -> Result<RpcClientRequest, DeliveryError> {
        Self::request_from_envelope(envelope).ok_or_else(|| {
            tracing::warn!(domain = "rpc", "Envelope payload was not RpcClientRequest");
            DeliveryError::ActorStopped
        })
    }

    fn record_request_start(&self) -> Option<Instant> {
        self.metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start())
    }

    fn log_parse_start(&self, meta: crate::runtime::ClientFrameMeta) {
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
