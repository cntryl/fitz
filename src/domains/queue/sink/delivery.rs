//! Ready-notification fan-out and per-operation actor dispatch.

use super::model::{
    obs, Envelope, Instant, QueueDomainCore, QueueNotification, QueueReadyNotification,
};
use crate::runtime::routing::RouteFamily;

mod pending_reserves;
mod wildcard_receive;

type ReadyNotificationEvent = (crate::domains::queue::QueueKey, QueueReadyNotification);

#[derive(Clone, Copy)]
pub(super) struct OperationRequestContext<'a> {
    pub(super) envelope: &'a Envelope,
    pub(super) meta: crate::runtime::ClientFrameMeta,
    pub(super) request_started: Option<Instant>,
}

pub(super) struct OperationOutcome {
    pub(super) response: crate::domains::queue::QueueResponse,
    pub(super) ready_notifications: Vec<ReadyNotificationEvent>,
    pub(super) mark_admin_snapshot_dirty: bool,
}

#[derive(Clone, Copy)]
pub(super) struct ExtendOperation {
    pub(super) session_id: u64,
    pub(super) id: crate::domains::queue::MessageId,
    pub(super) token: u64,
    pub(super) inflight_seconds: u64,
}

#[derive(Clone, Copy)]
pub(in crate::domains::queue::sink) enum QueueOpKind {
    Send,
    Receive,
    Extend,
    Ack,
    InflightExpired,
}

impl QueueDomainCore {
    pub(super) fn rollback_undeliverable_receive(
        &self,
        family_id: RouteFamily,
        requested_route: &crate::runtime::routing::Route,
        session_id: u64,
        response: &crate::domains::queue::QueueResponse,
    ) {
        let reservations: Vec<_> = match response {
            crate::domains::queue::QueueResponse::Received { messages } => messages
                .iter()
                .map(|message| (requested_route.clone(), message.id, message.token))
                .collect(),
            crate::domains::queue::QueueResponse::ReceivedRouted { messages } => messages
                .iter()
                .map(|routed| {
                    (
                        routed.route.clone(),
                        routed.message.id,
                        routed.message.token,
                    )
                })
                .collect(),
            _ => return,
        };

        let mut released = Vec::new();
        let mut actors = self.actors.lock();
        for (route, id, token) in reservations {
            let Ok(key) = Self::queue_key_for_route(family_id, &route) else {
                continue;
            };
            let Some(warm_actor) = actors.get_mut(&key) else {
                continue;
            };
            let mut actor = warm_actor.actor.lock();
            if actor.release_undelivered_reservation(session_id, id, token) {
                let notification = self.record_ready_state(&key, actor.live_counts());
                released.push((key, notification));
            }
        }
        drop(actors);

        if released.is_empty() {
            return;
        }
        self.mark_admin_snapshot_dirty();
        for (key, notification) in released {
            if let Some(notification) = notification {
                self.route_queue_ready_notification(&key, notification);
            }
            let route = Self::queue_ready_route(&key);
            self.wake_pending_reserves_for_route(key.family, &route, Instant::now());
        }
    }

    pub(super) fn queue_ready_route(
        key: &crate::domains::queue::QueueKey,
    ) -> crate::runtime::routing::Route {
        crate::runtime::routing::Route::new(format!(
            "queue://{}/{}/{}",
            key.realm, key.area, key.resource
        ))
    }

    pub(super) fn route_queue_notify_to_subscription(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &crate::runtime::routing::Route,
        counts: crate::domains::queue::QueueActorLiveCounts,
    ) {
        #[cfg(test)]
        {
            let payload = crate::dispatch::protocol::queue_codec::encode_notify(
                subscription_id,
                route,
                QueueNotification {
                    ready_messages: counts.ready as u64,
                    delayed_messages: counts.delayed as u64,
                    inflight_messages: counts.inflight as u64,
                },
            );
            let notify_ctx = super::model::FrameContext::new(
                session_id,
                crate::dispatch::protocol::frame::ChannelId::Sub,
                crate::dispatch::protocol::tlv::MessageType::new(
                    crate::dispatch::protocol::queue_codec::msg_type::NOTIFY,
                ),
                bytes::Bytes::from(payload),
                *subscriber.family(),
            );
            let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);
            if self.router.route(notify_envelope).is_err() {
                crate::observability::counter_inc(
                    crate::domains::queue::metrics::METRIC_NOTIFY_DROPS_TOTAL,
                );
            }
        }

        #[cfg(not(test))]
        {
            let notification = crate::domains::queue::QueueClientNotification::new(
                session_id,
                *subscriber.family(),
                subscription_id,
                route.clone(),
                QueueNotification {
                    ready_messages: counts.ready as u64,
                    delayed_messages: counts.delayed as u64,
                    inflight_messages: counts.inflight as u64,
                },
            );
            let notify_envelope = Envelope::new(subscriber.clone(), notification);
            if self.router.route(notify_envelope).is_err() {
                crate::observability::counter_inc(
                    crate::domains::queue::metrics::METRIC_NOTIFY_DROPS_TOTAL,
                );
            }
        }
    }

    pub(super) fn route_queue_ready_notification(
        &self,
        key: &crate::domains::queue::QueueKey,
        notification: QueueReadyNotification,
    ) {
        let route = Self::queue_ready_route(key);
        let targets = {
            let families = self.families.lock();
            let mut targets = Vec::new();
            if let Some(state) = families.get(&notification.family_id.as_u64()) {
                state.for_each_matching_route(
                    notification.family_id,
                    route.as_str(),
                    |subscription| {
                        targets.push((
                            subscription.session_id,
                            subscription.subscription_id,
                            subscription.subscriber.clone(),
                        ));
                    },
                );
            }
            targets
        };

        for (session_id, subscription_id, subscriber) in targets {
            self.route_queue_notify_to_subscription(
                session_id,
                subscription_id,
                &subscriber,
                &route,
                notification.counts,
            );
        }
    }

    pub(super) fn emit_current_ready_notifications_for_watch(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) {
        let actors = self.actors.lock();
        let ready_snapshots: Vec<_> = actors
            .iter()
            .filter(|(key, _)| key.family == family_id)
            .filter_map(|(key, warm_actor)| {
                let counts = warm_actor.actor.lock().live_counts();
                let route = Self::queue_ready_route(key);
                (counts.ready > 0 && pattern.matches(&route)).then_some((route, counts))
            })
            .collect();
        drop(actors);

        for (route, counts) in ready_snapshots {
            self.route_queue_notify_to_subscription(
                session_id,
                subscription_id,
                subscriber,
                &route,
                counts,
            );
        }
    }

    pub(super) fn dispatch_actor_operation(
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
                    let mut response_bytes_remaining =
                        crate::domains::queue::protocol::MAX_QUEUE_RESPONSE_PAYLOAD_BYTES
                            - crate::domains::queue::protocol::RECEIVED_RESPONSE_HEADER_BYTES;
                    actor
                        .handle_receive_for_session_with_wire_budget(
                            session_id,
                            inflight_seconds,
                            batch_size,
                            &mut response_bytes_remaining,
                            crate::domains::queue::protocol::RESERVED_MESSAGE_WIRE_OVERHEAD_BYTES,
                        )
                        .0
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

    pub(super) fn classify_operation(
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

    pub(super) fn record_operation_metrics(
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
}
