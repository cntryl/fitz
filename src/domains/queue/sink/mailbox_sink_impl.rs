use super::model::{
    obs, DeliveryError, Duration, Envelope, Instant, MailboxSink, Ordering, QueueClientFrame,
    QueueClientRequest, QueueDomainActor, QueueDomainCommand, QueueDomainCore, QueueDomainRuntime,
    QueueDomainSink, QueueReadyNotification, QueueSubscription, QueueSubscriptionMessage,
    RoutedSubscriptionSet,
};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;
use crate::runtime::routing::RouteFamily;
use crate::runtime::{Actor, Context};

type ReadyNotificationEvent = (crate::domains::queue::QueueKey, QueueReadyNotification);

#[derive(Clone, Copy)]
struct OperationRequestContext<'a> {
    envelope: &'a Envelope,
    meta: crate::runtime::ClientFrameMeta,
    request_started: Option<Instant>,
}

struct OperationOutcome {
    response: crate::domains::queue::QueueResponse,
    ready_notification: Option<ReadyNotificationEvent>,
    mark_admin_snapshot_dirty: bool,
}

#[derive(Clone, Copy)]
struct ExtendOperation {
    session_id: u64,
    id: crate::domains::queue::MessageId,
    token: u64,
    inflight_seconds: u64,
}

#[derive(Clone, Copy)]
enum QueueOpKind {
    Send,
    Receive,
    Extend,
    Ack,
    InflightExpired,
}

impl MailboxSink for QueueDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_actor(envelope, false)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_actor(envelope, true)
    }
}

impl Actor for QueueDomainActor {
    type Message = QueueDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.runtime();
        match msg {
            QueueDomainCommand::Deliver(envelope, reply) => {
                let _ = reply.send(runtime.deliver_envelope(&envelope));
            }
        }
    }
}

impl QueueDomainSink {
    fn deliver_to_actor(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = QueueDomainCommand::Deliver(envelope, reply_tx);
        let enqueue_result = if high_priority {
            self.actor.try_send_high_priority(command)
        } else {
            self.actor.try_send(command)
        };
        enqueue_result?;

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or(Err(DeliveryError::ActorStopped))
    }
}

impl QueueDomainRuntime<'_> {
    fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        self.core.deliver_envelope(envelope)
    }
}

impl QueueDomainCore {
    fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;
        Self::log_delivery(envelope);

        let Some(request) = Self::extract_request(envelope)? else {
            return Ok(());
        };
        let meta = request.meta;
        let route_family = *envelope.destination().family();
        let request_started = self.record_request_start();

        let Some(parsed_frame) =
            self.parse_request_frame(envelope, meta, request.frame, request_started)
        else {
            return Ok(());
        };

        self.maybe_sweep_idle_actors();

        match parsed_frame {
            QueueClientFrame::Sub(sub_msg) => {
                self.handle_subscription_frame(envelope, meta, request_started, sub_msg);
                Ok(())
            }
            QueueClientFrame::Op(queue_msg) => {
                self.handle_actor_operation_frame(
                    envelope,
                    meta,
                    request_started,
                    route_family,
                    queue_msg,
                );
                Ok(())
            }
        }
    }

    fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
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
            domain = "queue",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Queue domain sink: received envelope"
        );
    }

    fn extract_request(envelope: &Envelope) -> Result<Option<QueueClientRequest>, DeliveryError> {
        if let Some(request) = Self::request_from_envelope(envelope) {
            return Ok(Some(request));
        }

        tracing::warn!(
            domain = "queue",
            "Envelope payload was not QueueClientRequest"
        );
        Err(DeliveryError::ActorStopped)
    }

    fn record_request_start(&self) -> Option<Instant> {
        self.metrics
            .as_ref()
            .map(crate::domains::queue::QueueMetrics::record_request_start)
    }

    fn parse_request_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        frame: Result<QueueClientFrame, String>,
        request_started: Option<Instant>,
    ) -> Option<QueueClientFrame> {
        let parsed_frame = match frame {
            Ok(frame) => frame,
            Err(reason) => {
                let response = crate::domains::queue::QueueResponse::BadRequest { reason };
                self.route_queue_response(envelope, meta, &response);
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                return None;
            }
        };

        tracing::debug!(
            domain = "queue",
            session = meta.session_id,
            msg_type = meta.message_type,
            "Parsed Queue message successfully"
        );

        Some(parsed_frame)
    }

    fn handle_subscription_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<Instant>,
        sub_msg: QueueSubscriptionMessage,
    ) {
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

        self.route_queue_response(envelope, meta, &response);
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

        self.record_operation_metrics(request_started, &response, QueueOpKind::InflightExpired);
    }

    fn handle_actor_operation_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<Instant>,
        route_family: RouteFamily,
        queue_msg: crate::domains::queue::protocol::QueueMessage,
    ) {
        let request_context = OperationRequestContext {
            envelope,
            meta,
            request_started,
        };
        let op_kind = Self::classify_operation(&queue_msg);
        let outcome = match queue_msg {
            crate::domains::queue::protocol::QueueMessage::Send {
                family_id,
                route,
                body,
                delay_seconds,
            } => match self.handle_enqueue_operation(
                family_id,
                &route,
                body,
                delay_seconds,
                request_context,
            ) {
                Some(outcome) => outcome,
                None => return,
            },
            crate::domains::queue::protocol::QueueMessage::Receive {
                family_id,
                route,
                inflight_seconds,
                batch_size,
            } => match self.handle_receive_operation(
                family_id,
                &route,
                meta.session_id,
                inflight_seconds,
                batch_size,
                request_context,
            ) {
                Some(outcome) => outcome,
                None => return,
            },
            crate::domains::queue::protocol::QueueMessage::Extend {
                family_id,
                route,
                id,
                token,
                inflight_seconds,
            } => match self.handle_extend_operation(
                family_id,
                &route,
                ExtendOperation {
                    session_id: meta.session_id,
                    id,
                    token,
                    inflight_seconds,
                },
                request_context,
            ) {
                Some(outcome) => outcome,
                None => return,
            },
            crate::domains::queue::protocol::QueueMessage::Ack {
                family_id,
                route,
                id,
                token,
            } => match self.handle_ack_operation(
                family_id,
                &route,
                meta.session_id,
                id,
                token,
                request_context,
            ) {
                Some(outcome) => outcome,
                None => return,
            },
            crate::domains::queue::protocol::QueueMessage::InflightExpired { .. } => {
                OperationOutcome {
                    response: crate::domains::queue::QueueResponse::Error {
                        message: "InflightExpired is an internal message".to_string(),
                    },
                    ready_notification: None,
                    mark_admin_snapshot_dirty: false,
                }
            }
        };

        if outcome.mark_admin_snapshot_dirty {
            self.mark_admin_snapshot_dirty();
            self.mark_fast_flush_dirty(route_family);
        }

        if let Some((key, notification)) = outcome.ready_notification {
            self.route_queue_ready_notification(&key, notification);
        }

        self.route_queue_response(envelope, meta, &outcome.response);
        self.record_operation_metrics(request_started, &outcome.response, op_kind);
    }

    fn handle_enqueue_operation(
        &self,
        family_id: RouteFamily,
        route: &crate::runtime::routing::Route,
        body: bytes::Bytes,
        delay_seconds: Option<u64>,
        request_context: OperationRequestContext<'_>,
    ) -> Option<OperationOutcome> {
        let key = match Self::queue_key_for_route(family_id, route) {
            Ok(key) => key,
            Err(response) => {
                return Some(OperationOutcome {
                    response,
                    ready_notification: None,
                    mark_admin_snapshot_dirty: false,
                });
            }
        };

        self.with_actor_for_operation(&key, request_context, |actor| {
            actor.handle_send(body, delay_seconds)
        })
        .map(|(response, notification)| OperationOutcome {
            response,
            ready_notification: notification,
            mark_admin_snapshot_dirty: true,
        })
    }

    fn handle_receive_operation(
        &self,
        family_id: RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        inflight_seconds: u64,
        batch_size: Option<usize>,
        request_context: OperationRequestContext<'_>,
    ) -> Option<OperationOutcome> {
        let key = match Self::queue_key_for_route(family_id, route) {
            Ok(key) => key,
            Err(response) => {
                return Some(OperationOutcome {
                    response,
                    ready_notification: None,
                    mark_admin_snapshot_dirty: false,
                });
            }
        };

        self.with_actor_for_operation(&key, request_context, |actor| {
            actor.handle_receive_for_session(session_id, inflight_seconds, batch_size)
        })
        .map(|(response, notification)| OperationOutcome {
            response,
            ready_notification: notification,
            mark_admin_snapshot_dirty: true,
        })
    }

    fn handle_extend_operation(
        &self,
        family_id: RouteFamily,
        route: &crate::runtime::routing::Route,
        extend: ExtendOperation,
        request_context: OperationRequestContext<'_>,
    ) -> Option<OperationOutcome> {
        let key = match Self::queue_key_for_route(family_id, route) {
            Ok(key) => key,
            Err(response) => {
                return Some(OperationOutcome {
                    response,
                    ready_notification: None,
                    mark_admin_snapshot_dirty: false,
                });
            }
        };

        self.with_actor_for_operation(&key, request_context, |actor| {
            actor.handle_extend_for_session(
                extend.session_id,
                extend.id,
                extend.token,
                extend.inflight_seconds,
            )
        })
        .map(|(response, notification)| OperationOutcome {
            response,
            ready_notification: notification,
            mark_admin_snapshot_dirty: true,
        })
    }

    fn handle_ack_operation(
        &self,
        family_id: RouteFamily,
        route: &crate::runtime::routing::Route,
        session_id: u64,
        id: crate::domains::queue::MessageId,
        token: u64,
        request_context: OperationRequestContext<'_>,
    ) -> Option<OperationOutcome> {
        let key = match Self::queue_key_for_route(family_id, route) {
            Ok(key) => key,
            Err(response) => {
                return Some(OperationOutcome {
                    response,
                    ready_notification: None,
                    mark_admin_snapshot_dirty: false,
                });
            }
        };

        self.with_actor_for_operation(&key, request_context, |actor| {
            actor.handle_ack_for_session(session_id, id, token)
        })
        .map(|(response, notification)| OperationOutcome {
            response,
            ready_notification: notification,
            mark_admin_snapshot_dirty: true,
        })
    }

    fn with_actor_for_operation<F>(
        &self,
        key: &crate::domains::queue::QueueKey,
        request_context: OperationRequestContext<'_>,
        operation: F,
    ) -> Option<(
        crate::domains::queue::QueueResponse,
        Option<ReadyNotificationEvent>,
    )>
    where
        F: FnOnce(&mut crate::domains::queue::QueueActor) -> crate::domains::queue::QueueResponse,
    {
        let actor_lock_start = Instant::now();
        let (actor_handle, _) = match self.get_or_create_actor(key.clone()) {
            Ok(actor) => actor,
            Err(message) => {
                self.route_queue_recovery_error(
                    request_context.envelope,
                    request_context.meta,
                    request_context.request_started,
                    message,
                );
                return None;
            }
        };
        self.observe_histogram_us(
            obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
            Self::u128_to_u64_saturating(actor_lock_start.elapsed().as_micros()),
        );

        let mut actor = actor_handle.lock();
        let actor_exec_start = Instant::now();
        actor.process_due_work();
        let response = operation(&mut actor);
        let notification = self.record_ready_state(key, actor.admin_snapshot());
        self.observe_histogram_us(
            obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
            Self::u128_to_u64_saturating(actor_exec_start.elapsed().as_micros()),
        );

        Some((response, notification.map(|event| (key.clone(), event))))
    }

    fn classify_operation(
        queue_msg: &crate::domains::queue::protocol::QueueMessage,
    ) -> QueueOpKind {
        match queue_msg {
            crate::domains::queue::protocol::QueueMessage::Send { .. } => QueueOpKind::Send,
            crate::domains::queue::protocol::QueueMessage::Receive { .. } => QueueOpKind::Receive,
            crate::domains::queue::protocol::QueueMessage::Extend { .. } => QueueOpKind::Extend,
            crate::domains::queue::protocol::QueueMessage::Ack { .. } => QueueOpKind::Ack,
            crate::domains::queue::protocol::QueueMessage::InflightExpired { .. } => {
                QueueOpKind::InflightExpired
            }
        }
    }

    fn record_operation_metrics(
        &self,
        request_started: Option<Instant>,
        response: &crate::domains::queue::QueueResponse,
        op_kind: QueueOpKind,
    ) {
        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if Self::queue_response_is_failure(response) {
                metrics.record_failure(started_at);
                return;
            }

            metrics.record_success(started_at);
            match op_kind {
                QueueOpKind::Send => metrics.record_enqueue(started_at),
                QueueOpKind::Receive => metrics.record_reserve(started_at),
                QueueOpKind::Ack => metrics.record_complete(),
                QueueOpKind::Extend => metrics.record_extend(),
                QueueOpKind::InflightExpired => {}
            }
        }
    }

    fn u128_to_u64_saturating(value: u128) -> u64 {
        value.try_into().unwrap_or(u64::MAX)
    }

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
