//! Ready-notification fan-out and per-operation actor dispatch.

use super::model::{QueueFamilyState, QueueReadyNotification};
use crate::domain::QueueNotification;
use crate::observability as obs;
#[cfg(test)]
use crate::wire::frame_context::FrameContext;
use fitz_core::runtime::routing::RouteFamily;
use fitz_core::runtime::Envelope;
use std::time::Instant;

mod pending_reserves;
mod wildcard_receive;

type ReadyNotificationEvent = (crate::domain::QueueKey, QueueReadyNotification);

#[derive(Clone, Copy)]
pub(super) struct OperationRequestContext<'a> {
    pub(super) envelope: &'a Envelope,
    pub(super) meta: fitz_core::runtime::ClientFrameMeta,
    pub(super) request_started: Option<Instant>,
}

pub(super) struct OperationOutcome {
    pub(super) response: crate::domain::QueueResponse,
    pub(super) ready_notifications: Vec<ReadyNotificationEvent>,
    pub(super) mark_admin_snapshot_dirty: bool,
}

#[derive(Clone, Copy)]
pub(super) struct ExtendOperation {
    pub(super) session_id: u64,
    pub(super) id: crate::domain::MessageId,
    pub(super) token: u64,
    pub(super) inflight_seconds: u64,
}

#[derive(Clone, Copy)]
struct NackOperation {
    session_id: u64,
    id: crate::domain::MessageId,
    token: u64,
    visibility_seconds: u64,
}

#[derive(Clone, Copy)]
pub(in crate::domain::sink) enum QueueOpKind {
    Send,
    Receive,
    Extend,
    Ack,
    Nack,
    InflightExpired,
}

impl QueueFamilyState {
    pub(super) fn rollback_undeliverable_receive(
        &mut self,
        family_id: RouteFamily,
        requested_route: &fitz_core::runtime::routing::Route,
        session_id: u64,
        response: &crate::domain::QueueResponse,
    ) {
        let reservations: Vec<_> = match response {
            crate::domain::QueueResponse::Received { messages } => messages
                .iter()
                .map(|message| (requested_route.clone(), message.id, message.token))
                .collect(),
            crate::domain::QueueResponse::ReceivedRouted { messages } => messages
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

        let mut released_counts = Vec::new();
        for (route, id, token) in reservations {
            let Ok(key) = Self::queue_key_for_route(family_id, &route) else {
                continue;
            };
            let Some(warm_actor) = self.actor_registry.get_mut(&key) else {
                continue;
            };
            if warm_actor
                .actor
                .release_undelivered_reservation(session_id, id, token)
            {
                released_counts.push((key, warm_actor.actor.live_counts()));
            }
        }
        let released = released_counts
            .into_iter()
            .map(|(key, counts)| {
                let notification = self.record_ready_state(&key, counts);
                (key, notification)
            })
            .collect::<Vec<_>>();

        if released.is_empty() {
            return;
        }
        self.mark_admin_snapshot_dirty();
        for (key, notification) in released {
            self.notify_queue_ready_and_wake_reserves(&key, notification, Instant::now());
        }
    }

    pub(super) fn queue_ready_route(
        key: &crate::domain::QueueKey,
    ) -> fitz_core::runtime::routing::Route {
        fitz_core::runtime::routing::Route::new(format!(
            "queue://{}/{}/{}",
            key.realm, key.area, key.resource
        ))
    }

    pub(super) fn route_queue_notify_to_subscription(
        &mut self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &fitz_core::runtime::routing::RouteAddress,
        route: &fitz_core::runtime::routing::Route,
        counts: crate::domain::QueueActorLiveCounts,
    ) {
        #[cfg(test)]
        {
            let payload = crate::wire::queue_codec::encode_notify(
                subscription_id,
                route,
                QueueNotification {
                    ready_messages: counts.ready as u64,
                    delayed_messages: counts.delayed as u64,
                    inflight_messages: counts.inflight as u64,
                },
            );
            let notify_ctx = FrameContext::new(
                session_id,
                crate::wire::frame::ChannelId::Sub,
                crate::wire::message::MessageType::new(crate::wire::queue_codec::msg_type::NOTIFY),
                bytes::Bytes::from(payload),
                *subscriber.family(),
            );
            let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);
            if self.router.route(notify_envelope).is_err() {
                fitz_core::observability::counter_inc(
                    crate::domain::metrics::METRIC_NOTIFY_DROPS_TOTAL,
                );
            }
        }

        #[cfg(not(test))]
        {
            let notification = crate::domain::QueueClientNotification::new(
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
                fitz_core::observability::counter_inc(
                    crate::domain::metrics::METRIC_NOTIFY_DROPS_TOTAL,
                );
            }
        }
    }

    pub(super) fn route_queue_ready_notification(
        &mut self,
        key: &crate::domain::QueueKey,
        notification: QueueReadyNotification,
    ) {
        let route = Self::queue_ready_route(key);
        let targets = {
            let families = &self.families;
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

    pub(super) fn notify_queue_ready_and_wake_reserves(
        &mut self,
        key: &crate::domain::QueueKey,
        notification: Option<QueueReadyNotification>,
        now: Instant,
    ) {
        if let Some(notification) = notification {
            self.route_queue_ready_notification(key, notification);
        }
        let route = Self::queue_ready_route(key);
        self.wake_pending_reserves_for_route(key.family, &route, now);
    }

    pub(super) fn emit_current_ready_notifications_for_watch(
        &mut self,
        family_id: fitz_core::runtime::routing::RouteFamily,
        pattern: &fitz_core::runtime::matcher::Pattern,
        session_id: u64,
        subscription_id: u64,
        subscriber: &fitz_core::runtime::routing::RouteAddress,
    ) {
        let ready_snapshots: Vec<_> = self
            .actor_registry
            .iter()
            .filter(|(key, _)| key.family == family_id)
            .filter_map(|(key, warm_actor)| {
                let counts = warm_actor.actor.live_counts();
                let route = Self::queue_ready_route(key);
                (counts.ready > 0 && pattern.matches(&route)).then_some((route, counts))
            })
            .collect();

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
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        request_started: Option<Instant>,
        queue_msg: crate::domain::protocol::QueueMessage,
    ) -> Option<OperationOutcome> {
        let request_context = OperationRequestContext {
            envelope,
            meta,
            request_started,
        };
        let outcome = match queue_msg {
            crate::domain::protocol::QueueMessage::Send {
                family_id,
                route,
                body,
                delay_seconds,
                ttl_hours,
            } => self.handle_enqueue_operation(
                family_id,
                &route,
                body,
                delay_seconds,
                ttl_hours,
                request_context,
            )?,
            crate::domain::protocol::QueueMessage::Receive {
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
            crate::domain::protocol::QueueMessage::Extend {
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
            crate::domain::protocol::QueueMessage::Ack {
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
            crate::domain::protocol::QueueMessage::Nack {
                family_id,
                route,
                id,
                token,
                visibility_seconds,
            } => self.handle_nack_operation(
                family_id,
                &route,
                NackOperation {
                    session_id: meta.session_id,
                    id,
                    token,
                    visibility_seconds,
                },
                request_context,
            )?,
            crate::domain::protocol::QueueMessage::InflightExpired { .. } => OperationOutcome {
                response: crate::domain::QueueResponse::Error {
                    message: "InflightExpired is an internal message".to_string(),
                },
                ready_notifications: Vec::new(),
                mark_admin_snapshot_dirty: false,
            },
        };

        Some(outcome)
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_enqueue_operation(
        &mut self,
        family_id: RouteFamily,
        route: &fitz_core::runtime::routing::Route,
        body: bytes::Bytes,
        delay_seconds: Option<u64>,
        ttl_hours: Option<std::num::NonZeroU32>,
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
            actor.handle_send_with_ttl(body, delay_seconds, ttl_hours)
        })
        .map(|(response, notification)| OperationOutcome {
            response,
            ready_notifications: notification.into_iter().collect(),
            mark_admin_snapshot_dirty: true,
        })
    }

    fn handle_receive_operation(
        &mut self,
        family_id: RouteFamily,
        route: &fitz_core::runtime::routing::Route,
        session_id: u64,
        inflight_seconds: u64,
        batch_size: Option<usize>,
        request_context: OperationRequestContext<'_>,
    ) -> Option<OperationOutcome> {
        if let Ok(key) = Self::queue_key_for_route(family_id, route) {
            let response_payload_limit = self.response_payload_limit;
            return self
                .with_actor_for_operation(&key, request_context, |actor| {
                    let mut response_bytes_remaining = response_payload_limit
                        - crate::domain::protocol::RECEIVED_RESPONSE_HEADER_BYTES;
                    actor
                        .handle_receive_for_session_with_wire_budget(
                            session_id,
                            inflight_seconds,
                            batch_size,
                            &mut response_bytes_remaining,
                            crate::domain::protocol::RESERVED_MESSAGE_WIRE_OVERHEAD_BYTES,
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
        route: &fitz_core::runtime::routing::Route,
    ) -> Result<fitz_core::runtime::matcher::Pattern, crate::domain::QueueResponse> {
        if !route.as_str().contains('*') {
            return Err(crate::domain::QueueResponse::BadRequest {
                reason: format!("invalid queue route: {}", route.as_str()),
            });
        }
        let pattern = fitz_core::runtime::DomainKind::Queue
            .descriptor()
            .compile_registration_pattern(route.as_str())
            .map_err(|reason| crate::domain::QueueResponse::BadRequest { reason })?;
        if !pattern.is_wildcard() {
            return Err(crate::domain::QueueResponse::BadRequest {
                reason: format!("invalid queue route: {}", route.as_str()),
            });
        }
        Ok(pattern)
    }

    fn handle_extend_operation(
        &mut self,
        family_id: RouteFamily,
        route: &fitz_core::runtime::routing::Route,
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

    fn handle_nack_operation(
        &mut self,
        family_id: RouteFamily,
        route: &fitz_core::runtime::routing::Route,
        nack: NackOperation,
        request_context: OperationRequestContext<'_>,
    ) -> Option<OperationOutcome> {
        let key = match Self::queue_key_for_route(family_id, route) {
            Ok(key) => key,
            Err(response) => {
                return Some(OperationOutcome {
                    response,
                    ready_notifications: Vec::new(),
                    mark_admin_snapshot_dirty: false,
                })
            }
        };
        self.with_actor_for_operation(&key, request_context, |actor| {
            actor.handle_nack_for_session(
                nack.session_id,
                nack.id,
                nack.token,
                nack.visibility_seconds,
            )
        })
        .map(|(response, notification)| OperationOutcome {
            response,
            ready_notifications: notification.into_iter().collect(),
            mark_admin_snapshot_dirty: true,
        })
    }

    fn handle_ack_operation(
        &mut self,
        family_id: RouteFamily,
        route: &fitz_core::runtime::routing::Route,
        session_id: u64,
        id: crate::domain::MessageId,
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
        &mut self,
        key: &crate::domain::QueueKey,
        request_context: OperationRequestContext<'_>,
        operation: F,
    ) -> Option<(crate::domain::QueueResponse, Option<ReadyNotificationEvent>)>
    where
        F: FnOnce(&mut crate::domain::QueueActor) -> crate::domain::QueueResponse,
    {
        let actor_lock_start = Instant::now();
        let ((response, counts, actor_lock_us, actor_exec_us), _, reconstructed) = match self
            .with_actor(key, |actor| {
                let actor_lock_us =
                    Self::u128_to_u64_saturating(actor_lock_start.elapsed().as_micros());
                let actor_exec_start = Instant::now();
                actor.process_due_work();
                let response = operation(actor);
                let actor_exec_us =
                    Self::u128_to_u64_saturating(actor_exec_start.elapsed().as_micros());
                (response, actor.live_counts(), actor_lock_us, actor_exec_us)
            }) {
            Ok(outcome) => outcome,
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
        let counts = if reconstructed {
            self.actor_registry
                .get_mut(key)
                .map_or(counts, |warm_actor| warm_actor.actor.live_counts())
        } else {
            counts
        };
        self.observe_histogram_us(obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY, actor_lock_us);
        if counts.total() > 0 {
            self.reservation_book.insert_key(key.clone());
        }
        let notification = self.record_ready_state(key, counts);
        if counts.ready > 0
            && matches!(&response, crate::domain::QueueResponse::Received { messages } if messages.is_empty())
        {
            // A bounded empty read can leave unverified recovered heads. Let
            // the next sweep signal availability again for watch consumers.
            self.ready_states.remove(key);
        }
        self.observe_histogram_us(obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY, actor_exec_us);

        Some((response, notification.map(|event| (key.clone(), event))))
    }

    pub(super) fn classify_operation(
        queue_msg: &crate::domain::protocol::QueueMessage,
    ) -> QueueOpKind {
        match queue_msg {
            crate::domain::protocol::QueueMessage::Send { .. } => QueueOpKind::Send,
            crate::domain::protocol::QueueMessage::Receive { .. } => QueueOpKind::Receive,
            crate::domain::protocol::QueueMessage::Extend { .. } => QueueOpKind::Extend,
            crate::domain::protocol::QueueMessage::Ack { .. } => QueueOpKind::Ack,
            crate::domain::protocol::QueueMessage::Nack { .. } => QueueOpKind::Nack,
            crate::domain::protocol::QueueMessage::InflightExpired { .. } => {
                QueueOpKind::InflightExpired
            }
        }
    }

    pub(super) fn record_operation_metrics(
        &mut self,
        request_started: Option<Instant>,
        response: &crate::domain::QueueResponse,
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
                QueueOpKind::Nack => metrics.record_nack(),
                QueueOpKind::Extend => metrics.record_extend(),
                QueueOpKind::InflightExpired => {}
            }
        }
    }

    fn u128_to_u64_saturating(value: u128) -> u64 {
        value.try_into().unwrap_or(u64::MAX)
    }
}
