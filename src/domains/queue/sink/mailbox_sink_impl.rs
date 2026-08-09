use super::model::{
    obs, DeliveryError, Duration, Envelope, Instant, MailboxSink, Ordering, PendingQueueReserve,
    QueueClientFrame, QueueClientRequest, QueueDomainActor, QueueDomainCommand, QueueDomainCore,
    QueueDomainRuntime, QueueDomainSink, QueueLiveCounts, QueueReadyNotification,
    QueueSubscription, QueueSubscriptionMessage, RoutedSubscriptionSet, VecDeque,
    QUEUE_ACTOR_REPLY_TIMEOUT,
};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::routing::RouteFamily;
use crate::runtime::{Actor, Context};

type ReadyNotificationEvent = (crate::domains::queue::QueueKey, QueueReadyNotification);

mod pending_reserves;
mod runtime_adapter;
mod wildcard_receive;

#[derive(Clone, Copy)]
struct OperationRequestContext<'a> {
    envelope: &'a Envelope,
    meta: crate::runtime::ClientFrameMeta,
    request_started: Option<Instant>,
}

struct OperationOutcome {
    response: crate::domains::queue::QueueResponse,
    ready_notifications: Vec<ReadyNotificationEvent>,
    mark_admin_snapshot_dirty: bool,
}

type SubscriptionOutcome = (
    crate::domains::queue::QueueResponse,
    Option<(
        RouteFamily,
        crate::runtime::matcher::Pattern,
        u64,
        u64,
        crate::runtime::routing::RouteAddress,
    )>,
    bool,
);

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
            QueueDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                runtime.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            QueueDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(runtime.live_counts());
            }
            QueueDomainCommand::CleanupSession(session_id, reply) => {
                runtime.cleanup_session(session_id);
                let _ = reply.send(());
            }
            QueueDomainCommand::SweepRuntimeStateAt(now, reply) => {
                runtime.sweep_runtime_state_at(now);
                let _ = reply.send(());
            }
            QueueDomainCommand::ReplayDeadLetter(key, id, reply) => {
                let _ = reply.send(runtime.replay_dead_letter(&key, id));
            }
            QueueDomainCommand::PurgeDeadLetter(key, id, reply) => {
                let _ = reply.send(runtime.purge_dead_letter(&key, id));
            }
            #[cfg(test)]
            QueueDomainCommand::PanicForTests => {
                panic!("test Queue domain actor panic");
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
            .recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT)
            .unwrap_or(Err(DeliveryError::ActorStopped))
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

        if meta.route_family != route_family
            || envelope
                .source()
                .is_some_and(|source| *source.family() != meta.route_family)
        {
            let response = crate::domains::queue::QueueResponse::BadRequest {
                reason: "route family mismatch".to_string(),
            };
            let response_meta = envelope.source().map_or(meta, |source| {
                let mut response_meta = meta;
                response_meta.route_family = *source.family();
                response_meta
            });
            self.route_queue_response(envelope, response_meta, &response);
            if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
                metrics.record_failure(started_at);
            }
            return Ok(());
        }

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
        let (response, initial_watch_snapshot, state_changed) = match sub_msg {
            QueueSubscriptionMessage::Watch {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self.handle_watch_subscription(
                envelope, meta, family_id, &pattern, session_id, subscriber,
            ),
            QueueSubscriptionMessage::Unwatch {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => self.handle_unwatch_subscription(
                envelope,
                meta,
                family_id,
                &pattern,
                session_id,
                &subscriber,
            ),
        };

        self.route_queue_response(envelope, meta, &response);
        if state_changed {
            self.mark_admin_snapshot_dirty();
        }
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

    fn handle_watch_subscription(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    ) -> SubscriptionOutcome {
        if !Self::valid_subscription_request(envelope, meta, family_id, session_id, &subscriber) {
            return (
                crate::domains::queue::QueueResponse::BadRequest {
                    reason: "route family mismatch".to_string(),
                },
                None,
                false,
            );
        }
        let pattern_str = pattern.as_str();
        let parsed_pattern = match crate::runtime::DomainKind::Queue
            .descriptor()
            .compile_registration_pattern(pattern_str)
        {
            Ok(pattern) => pattern,
            Err(reason) => {
                return (
                    crate::domains::queue::QueueResponse::InvalidSubscriptionPattern { reason },
                    None,
                    false,
                );
            }
        };
        let (subscription_id, state_changed) = {
            let mut families = self.families.lock();
            let state = families
                .entry(family_id.as_u64())
                .or_insert_with(RoutedSubscriptionSet::new);

            if let Some(id) = state.find_existing_id(session_id, pattern_str) {
                (id, false)
            } else {
                if state.wildcard_registration_limit_reached(session_id, &parsed_pattern) {
                    return (
                        crate::domains::queue::QueueResponse::SubscriptionLimit,
                        None,
                        false,
                    );
                }
                let Ok(id) = self.next_sub_id.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |current| current.checked_add(1),
                ) else {
                    let state_empty = state.is_empty();
                    if state_empty {
                        families.remove(&family_id.as_u64());
                    }
                    return (
                        crate::domains::queue::QueueResponse::BadRequest {
                            reason: "subscription ID space exhausted".to_string(),
                        },
                        None,
                        false,
                    );
                };
                state.insert(
                    family_id,
                    QueueSubscription {
                        pattern: parsed_pattern.clone(),
                        session_id,
                        subscription_id: id,
                        subscriber: subscriber.clone(),
                    },
                );
                (id, true)
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
            state_changed,
        )
    }

    fn handle_unwatch_subscription(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: RouteFamily,
        pattern: &crate::runtime::routing::Route,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> SubscriptionOutcome {
        if !Self::valid_subscription_request(envelope, meta, family_id, session_id, subscriber) {
            return (
                crate::domains::queue::QueueResponse::BadRequest {
                    reason: "route family mismatch".to_string(),
                },
                None,
                false,
            );
        }
        if let Err(reason) = crate::runtime::DomainKind::Queue
            .descriptor()
            .compile_registration_pattern(pattern.as_str())
        {
            return (
                crate::domains::queue::QueueResponse::InvalidSubscriptionPattern { reason },
                None,
                false,
            );
        }

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
        (crate::domains::queue::QueueResponse::UnwatchOk, None, true)
    }

    fn valid_subscription_request(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        family_id: RouteFamily,
        session_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) -> bool {
        family_id == meta.route_family
            && *subscriber.family() == family_id
            && session_id == meta.session_id
            && envelope.source().is_none_or(|source| source == subscriber)
    }

    fn handle_actor_operation_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<Instant>,
        route_family: RouteFamily,
        queue_msg: crate::domains::queue::protocol::QueueMessage,
    ) {
        if Self::queue_message_family(&queue_msg)
            .is_some_and(|family_id| family_id != meta.route_family)
        {
            let response = crate::domains::queue::QueueResponse::BadRequest {
                reason: "route family mismatch".to_string(),
            };
            self.route_queue_response(envelope, meta, &response);
            if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
                metrics.record_failure(started_at);
            }
            return;
        }

        let op_kind = Self::classify_operation(&queue_msg);
        let wait_seconds = match &queue_msg {
            crate::domains::queue::protocol::QueueMessage::Receive { wait_seconds, .. } => {
                *wait_seconds
            }
            _ => None,
        };
        let wake_route = match &queue_msg {
            crate::domains::queue::protocol::QueueMessage::Send { route, .. } => {
                Some(route.clone())
            }
            _ => None,
        };
        let pending_message = queue_msg.clone();
        let Some(outcome) =
            self.dispatch_actor_operation(envelope, meta, request_started, queue_msg)
        else {
            return;
        };

        if matches!(
            &outcome.response,
            crate::domains::queue::QueueResponse::Received { messages } if messages.is_empty()
        ) || matches!(
            &outcome.response,
            crate::domains::queue::QueueResponse::ReceivedRouted { messages } if messages.is_empty()
        ) {
            if let Some(wait_seconds) = wait_seconds.filter(|seconds| *seconds > 0) {
                if let Some(source) = envelope.source() {
                    let mut message = pending_message;
                    if let crate::domains::queue::protocol::QueueMessage::Receive {
                        wait_seconds,
                        ..
                    } = &mut message
                    {
                        *wait_seconds = None;
                    }
                    let deadline = Instant::now()
                        .checked_add(Duration::from_secs(wait_seconds))
                        .unwrap_or_else(Instant::now);
                    self.pending_reserves.lock().push_back(PendingQueueReserve {
                        envelope: Envelope::from_route(
                            source.clone(),
                            envelope.destination().clone(),
                            (),
                        ),
                        meta,
                        request_started,
                        message,
                        deadline,
                    });
                    return;
                }
            }
        }

        if outcome.mark_admin_snapshot_dirty {
            self.mark_admin_snapshot_dirty();
            self.mark_fast_flush_dirty(route_family);
        }

        for (key, notification) in outcome.ready_notifications {
            self.route_queue_ready_notification(&key, notification);
        }

        self.route_queue_response(envelope, meta, &outcome.response);
        self.record_operation_metrics(request_started, &outcome.response, op_kind);
        if let Some(route) = wake_route.as_ref() {
            self.wake_pending_reserves_for_route(meta.route_family, route, Instant::now());
        }
    }

    fn queue_message_family(
        queue_msg: &crate::domains::queue::protocol::QueueMessage,
    ) -> Option<RouteFamily> {
        match queue_msg {
            crate::domains::queue::protocol::QueueMessage::Send { family_id, .. }
            | crate::domains::queue::protocol::QueueMessage::Receive { family_id, .. }
            | crate::domains::queue::protocol::QueueMessage::Extend { family_id, .. }
            | crate::domains::queue::protocol::QueueMessage::Ack { family_id, .. } => {
                Some(*family_id)
            }
            crate::domains::queue::protocol::QueueMessage::InflightExpired { .. } => None,
        }
    }

    fn dispatch_actor_operation(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<Instant>,
        queue_msg: crate::domains::queue::protocol::QueueMessage,
    ) -> Option<OperationOutcome> {
        let request_context = OperationRequestContext {
            envelope,
            meta,
            request_started,
        };
        let outcome = match queue_msg {
            crate::domains::queue::protocol::QueueMessage::Send {
                family_id,
                route,
                body,
                delay_seconds,
            } => self.handle_enqueue_operation(
                family_id,
                &route,
                body,
                delay_seconds,
                request_context,
            )?,
            crate::domains::queue::protocol::QueueMessage::Receive {
                family_id,
                route,
                inflight_seconds,
                batch_size,
                wait_seconds: _,
            } => self.handle_receive_operation(
                family_id,
                &route,
                meta.session_id,
                inflight_seconds,
                batch_size,
                request_context,
            )?,
            crate::domains::queue::protocol::QueueMessage::Extend {
                family_id,
                route,
                id,
                token,
                inflight_seconds,
            } => self.handle_extend_operation(
                family_id,
                &route,
                ExtendOperation {
                    session_id: meta.session_id,
                    id,
                    token,
                    inflight_seconds,
                },
                request_context,
            )?,
            crate::domains::queue::protocol::QueueMessage::Ack {
                family_id,
                route,
                id,
                token,
            } => self.handle_ack_operation(
                family_id,
                &route,
                meta.session_id,
                id,
                token,
                request_context,
            )?,
            crate::domains::queue::protocol::QueueMessage::InflightExpired { .. } => {
                OperationOutcome {
                    response: crate::domains::queue::QueueResponse::Error {
                        message: "InflightExpired is an internal message".to_string(),
                    },
                    ready_notifications: Vec::new(),
                    mark_admin_snapshot_dirty: false,
                }
            }
        };

        Some(outcome)
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
                    ready_notifications: Vec::new(),
                    mark_admin_snapshot_dirty: false,
                });
            }
        };

        self.with_actor_for_operation(&key, request_context, |actor| {
            actor.handle_send(body, delay_seconds)
        })
        .map(|(response, notification)| OperationOutcome {
            response,
            ready_notifications: notification.into_iter().collect(),
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
        if let Ok(key) = Self::queue_key_for_route(family_id, route) {
            return self
                .with_actor_for_operation(&key, request_context, |actor| {
                    actor.handle_receive_for_session(session_id, inflight_seconds, batch_size)
                })
                .map(|(response, notification)| OperationOutcome {
                    response,
                    ready_notifications: notification.into_iter().collect(),
                    mark_admin_snapshot_dirty: true,
                });
        }

        let pattern = match Self::wildcard_queue_selector(route) {
            Ok(pattern) => pattern,
            Err(response) => {
                return Some(OperationOutcome {
                    response,
                    ready_notifications: Vec::new(),
                    mark_admin_snapshot_dirty: false,
                });
            }
        };
        Some(self.handle_wildcard_receive(
            family_id,
            &pattern,
            session_id,
            inflight_seconds,
            batch_size,
        ))
    }

    fn wildcard_queue_selector(
        route: &crate::runtime::routing::Route,
    ) -> Result<crate::runtime::matcher::Pattern, crate::domains::queue::QueueResponse> {
        if !route.as_str().contains('*') {
            return Err(crate::domains::queue::QueueResponse::BadRequest {
                reason: format!("invalid queue route: {}", route.as_str()),
            });
        }
        let pattern = crate::runtime::DomainKind::Queue
            .descriptor()
            .compile_registration_pattern(route.as_str())
            .map_err(|reason| crate::domains::queue::QueueResponse::BadRequest { reason })?;
        if !pattern.is_wildcard() {
            return Err(crate::domains::queue::QueueResponse::BadRequest {
                reason: format!("invalid queue route: {}", route.as_str()),
            });
        }
        Ok(pattern)
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
                    ready_notifications: Vec::new(),
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
            ready_notifications: notification.into_iter().collect(),
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
                    ready_notifications: Vec::new(),
                    mark_admin_snapshot_dirty: false,
                });
            }
        };

        self.with_actor_for_operation(&key, request_context, |actor| {
            actor.handle_ack_for_session(session_id, id, token)
        })
        .map(|(response, notification)| OperationOutcome {
            response,
            ready_notifications: notification.into_iter().collect(),
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
        let (actor_handle, _) = match self.get_or_create_actor(key) {
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
        let counts = actor.live_counts();
        if counts.total() > 0 {
            self.known_queue_keys.lock().insert(key.clone());
        }
        let notification = self.record_ready_state(key, counts);
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
            let parsed = crate::dispatch::protocol::queue_codec::parse_frame(
                &frame_ctx,
                &frame_ctx.payload,
                frame_ctx.route_family,
                frame_ctx.session_id,
                subscriber,
            )
            .map(|frame| match frame {
                crate::dispatch::protocol::queue_codec::ParsedQueueFrame::Op(message) => {
                    QueueClientFrame::Op(message)
                }
                crate::dispatch::protocol::queue_codec::ParsedQueueFrame::Sub(message) => {
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
    channel: crate::dispatch::protocol::frame::ChannelId,
) -> crate::runtime::ClientChannel {
    match channel {
        crate::dispatch::protocol::frame::ChannelId::Control => {
            crate::runtime::ClientChannel::Control
        }
        crate::dispatch::protocol::frame::ChannelId::Pub => crate::runtime::ClientChannel::Pub,
        crate::dispatch::protocol::frame::ChannelId::Sub => crate::runtime::ClientChannel::Sub,
        crate::dispatch::protocol::frame::ChannelId::Rpc => crate::runtime::ClientChannel::Rpc,
        crate::dispatch::protocol::frame::ChannelId::Lease => crate::runtime::ClientChannel::Lease,
        crate::dispatch::protocol::frame::ChannelId::Internal => {
            crate::runtime::ClientChannel::Internal
        }
    }
}
