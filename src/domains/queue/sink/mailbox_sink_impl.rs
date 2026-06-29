use super::model::*;

impl MailboxSink for QueueDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        tracing::debug!(
            domain = "queue",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Queue domain sink: received envelope"
        );

        let request = match Self::request_from_envelope(&envelope) {
            Some(request) => request,
            None => {
                tracing::warn!(
                    domain = "queue",
                    "Envelope payload was not QueueClientRequest"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        let meta = request.meta;
        let route_addr = envelope.destination();
        let route_family = *route_addr.family();
        let request_started = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start());

        let parsed_frame = match request.frame {
            Ok(msg) => msg,
            Err(reason) => {
                let response = crate::domains::queue::QueueResponse::BadRequest { reason };
                self.route_queue_response(&envelope, meta, &response);
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                return Ok(());
            }
        };

        tracing::debug!(
            domain = "queue",
            session = meta.session_id,
            msg_type = meta.message_type,
            "Parsed Queue message successfully"
        );

        self.maybe_sweep_idle_actors();

        if let QueueClientFrame::Sub(sub_msg) = parsed_frame {
            let (response, initial_watch_snapshot) = match sub_msg {
                QueueSubscriptionMessage::Watch {
                    family_id,
                    pattern,
                    session_id,
                    subscriber,
                } => {
                    let pattern_str = pattern.as_str();
                    let parsed_pattern = crate::runtime::matcher::Pattern::new(pattern_str);
                    let subscription_id = {
                        let mut families = self.families.lock();
                        let state = families
                            .entry(family_id.as_u64())
                            .or_insert_with(RoutedSubscriptionSet::new);

                        if let Some(id) = state.find_existing_id(session_id, pattern_str) {
                            id
                        } else {
                            let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                            state.insert(
                                family_id,
                                QueueSubscription {
                                    pattern: parsed_pattern.clone(),
                                    session_id,
                                    subscription_id: id,
                                    subscriber: subscriber.clone(),
                                },
                            );
                            id
                        }
                    };

                    (
                        crate::domains::queue::QueueResponse::WatchOk { subscription_id },
                        Some((
                            family_id,
                            parsed_pattern,
                            session_id,
                            subscription_id,
                            subscriber,
                        )),
                    )
                }
                QueueSubscriptionMessage::Unwatch {
                    family_id,
                    pattern,
                    session_id,
                    ..
                } => {
                    let mut families = self.families.lock();
                    let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
                        state.remove_session_pattern(family_id, session_id, pattern.as_str());
                        state.is_empty()
                    } else {
                        false
                    };
                    if remove_family {
                        families.remove(&family_id.as_u64());
                    }
                    (crate::domains::queue::QueueResponse::UnwatchOk, None)
                }
            };

            self.route_queue_response(&envelope, meta, &response);
            self.mark_admin_snapshot_dirty();
            if let Some((family_id, pattern, session_id, subscription_id, subscriber)) =
                initial_watch_snapshot
            {
                self.emit_current_ready_notifications_for_watch(
                    family_id,
                    &pattern,
                    session_id,
                    subscription_id,
                    &subscriber,
                );
            }

            if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
                if Self::queue_response_is_failure(&response) {
                    metrics.record_failure(started_at);
                } else {
                    metrics.record_success(started_at);
                }
            }
            return Ok(());
        }

        use crate::domains::queue::protocol::QueueMessage;

        let queue_msg = match parsed_frame {
            QueueClientFrame::Op(msg) => msg,
            QueueClientFrame::Sub(_) => unreachable!(),
        };

        // Capture the operation kind before the consuming match for operation-specific metrics.
        #[derive(Clone, Copy)]
        enum QueueOpKind {
            Send,
            Receive,
            Extend,
            Ack,
            Other,
        }
        let op_kind = match &queue_msg {
            QueueMessage::Send { .. } => QueueOpKind::Send,
            QueueMessage::Receive { .. } => QueueOpKind::Receive,
            QueueMessage::Extend { .. } => QueueOpKind::Extend,
            QueueMessage::Ack { .. } => QueueOpKind::Ack,
            _ => QueueOpKind::Other,
        };

        let (response, ready_notification, should_mark_admin_snapshot_dirty) = match queue_msg {
            QueueMessage::Send {
                family_id,
                route,
                body,
                delay_seconds,
            } => match Self::queue_key_for_route(family_id, &route) {
                Ok(key) => {
                    let notification_key = key.clone();
                    let actor_lock_start = Instant::now();
                    let (actor_handle, created_actor) = match self.get_or_create_actor(key) {
                        Ok(actor) => actor,
                        Err(message) => {
                            return self.route_queue_recovery_error(
                                &envelope,
                                meta,
                                request_started,
                                message,
                            );
                        }
                    };
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                        actor_lock_start.elapsed().as_micros() as u64,
                    );
                    let mut actor = actor_handle.lock();
                    let actor_exec_start = Instant::now();
                    actor.process_due_work();
                    let resp = actor.handle_send(body, delay_seconds);
                    let notification =
                        self.record_ready_state(&notification_key, actor.admin_snapshot());
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                        actor_exec_start.elapsed().as_micros() as u64,
                    );
                    let _ = created_actor;
                    (
                        resp,
                        notification.map(|event| (notification_key.clone(), event)),
                        true,
                    )
                }
                Err(response) => (response, None, false),
            },
            QueueMessage::Receive {
                family_id,
                route,
                inflight_seconds,
                batch_size,
            } => match Self::queue_key_for_route(family_id, &route) {
                Ok(key) => {
                    let notification_key = key.clone();
                    let actor_lock_start = Instant::now();
                    let (actor_handle, created_actor) = match self.get_or_create_actor(key) {
                        Ok(actor) => actor,
                        Err(message) => {
                            return self.route_queue_recovery_error(
                                &envelope,
                                meta,
                                request_started,
                                message,
                            );
                        }
                    };
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                        actor_lock_start.elapsed().as_micros() as u64,
                    );
                    let mut actor = actor_handle.lock();
                    let actor_exec_start = Instant::now();
                    actor.process_due_work();
                    let response = actor.handle_receive_for_session(
                        meta.session_id,
                        inflight_seconds,
                        batch_size,
                    );
                    let notification =
                        self.record_ready_state(&notification_key, actor.admin_snapshot());
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                        actor_exec_start.elapsed().as_micros() as u64,
                    );
                    let _ = created_actor;

                    (
                        response,
                        notification.map(|event| (notification_key.clone(), event)),
                        true,
                    )
                }
                Err(response) => (response, None, false),
            },
            QueueMessage::Extend {
                family_id,
                route,
                id,
                token,
                inflight_seconds,
            } => match Self::queue_key_for_route(family_id, &route) {
                Ok(key) => {
                    let notification_key = key.clone();
                    let actor_lock_start = Instant::now();
                    let (actor_handle, created_actor) = match self.get_or_create_actor(key) {
                        Ok(actor) => actor,
                        Err(message) => {
                            return self.route_queue_recovery_error(
                                &envelope,
                                meta,
                                request_started,
                                message,
                            );
                        }
                    };
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                        actor_lock_start.elapsed().as_micros() as u64,
                    );
                    let mut actor = actor_handle.lock();
                    let actor_exec_start = Instant::now();
                    actor.process_due_work();
                    let response = actor.handle_extend_for_session(
                        meta.session_id,
                        id,
                        token,
                        inflight_seconds,
                    );
                    let notification =
                        self.record_ready_state(&notification_key, actor.admin_snapshot());
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                        actor_exec_start.elapsed().as_micros() as u64,
                    );
                    let _ = created_actor;
                    (
                        response,
                        notification.map(|event| (notification_key.clone(), event)),
                        true,
                    )
                }
                Err(response) => (response, None, false),
            },
            QueueMessage::Ack {
                family_id,
                route,
                id,
                token,
            } => match Self::queue_key_for_route(family_id, &route) {
                Ok(key) => {
                    let notification_key = key.clone();
                    let actor_lock_start = Instant::now();
                    let (actor_handle, created_actor) = match self.get_or_create_actor(key) {
                        Ok(actor) => actor,
                        Err(message) => {
                            return self.route_queue_recovery_error(
                                &envelope,
                                meta,
                                request_started,
                                message,
                            );
                        }
                    };
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                        actor_lock_start.elapsed().as_micros() as u64,
                    );
                    let mut actor = actor_handle.lock();
                    let actor_exec_start = Instant::now();
                    actor.process_due_work();
                    let response = actor.handle_ack_for_session(meta.session_id, id, token);
                    let notification =
                        self.record_ready_state(&notification_key, actor.admin_snapshot());
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                        actor_exec_start.elapsed().as_micros() as u64,
                    );
                    let _ = created_actor;
                    (
                        response,
                        notification.map(|event| (notification_key.clone(), event)),
                        true,
                    )
                }
                Err(response) => (response, None, false),
            },
            QueueMessage::InflightExpired { .. } => (
                crate::domains::queue::QueueResponse::Error {
                    message: "InflightExpired is an internal message".to_string(),
                },
                None,
                false,
            ),
        };
        if should_mark_admin_snapshot_dirty {
            self.mark_admin_snapshot_dirty();
            self.mark_fast_flush_dirty(route_family);
        }

        if let Some((key, notification)) = ready_notification {
            self.route_queue_ready_notification(&key, notification);
        }

        self.route_queue_response(&envelope, meta, &response);

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if Self::queue_response_is_failure(&response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
                match op_kind {
                    QueueOpKind::Send => metrics.record_enqueue(started_at),
                    QueueOpKind::Receive => metrics.record_reserve(started_at),
                    QueueOpKind::Ack => metrics.record_complete(),
                    QueueOpKind::Extend => metrics.record_extend(),
                    QueueOpKind::Other => {}
                }
            }
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl QueueDomainSink {
    fn request_from_envelope(envelope: &Envelope) -> Option<QueueClientRequest> {
        if let Some(request) = envelope.payload::<QueueClientRequest>() {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                Self::session_inbox_address(frame_ctx.route_family, frame_ctx.session_id)
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::protocol::queue_codec::parse_frame(
                &frame_ctx,
                &frame_ctx.payload,
                frame_ctx.route_family,
                frame_ctx.session_id,
                subscriber,
            )
            .map(|frame| match frame {
                crate::protocol::queue_codec::ParsedQueueFrame::Op(message) => {
                    QueueClientFrame::Op(message)
                }
                crate::protocol::queue_codec::ParsedQueueFrame::Sub(message) => {
                    QueueClientFrame::Sub(message)
                }
            });
            Some(QueueClientRequest::new(meta, parsed))
        }

        #[cfg(not(test))]
        {
            None
        }
    }
}

#[cfg(test)]
fn test_client_channel_from_protocol(
    channel: crate::protocol::frame::ChannelId,
) -> crate::runtime::ClientChannel {
    match channel {
        crate::protocol::frame::ChannelId::Control => crate::runtime::ClientChannel::Control,
        crate::protocol::frame::ChannelId::Pub => crate::runtime::ClientChannel::Pub,
        crate::protocol::frame::ChannelId::Sub => crate::runtime::ClientChannel::Sub,
        crate::protocol::frame::ChannelId::Rpc => crate::runtime::ClientChannel::Rpc,
        crate::protocol::frame::ChannelId::Lease => crate::runtime::ClientChannel::Lease,
        crate::protocol::frame::ChannelId::Internal => crate::runtime::ClientChannel::Internal,
    }
}
