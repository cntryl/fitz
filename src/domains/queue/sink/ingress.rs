//! Envelope intake: cleanup/request extraction, frame parsing, and operation
//! dispatch entry point.

#[cfg(test)]
use super::model::FrameContext;
use super::model::{
    DeliveryError, Duration, Envelope, Instant, PendingQueueReserve, QueueClientFrame,
    QueueClientRequest, QueueDomainCore,
};
use crate::runtime::routing::RouteFamily;

impl QueueDomainCore {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
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

        // This request was already queued (on the normal lane) before this
        // session's disconnect cleanup ran (on the high-priority/control-plane
        // lane) and jumped ahead of it. Reject rather than silently
        // recreating a subscription or pending reserve for a session that is
        // already gone and will never be cleaned up again.
        if self.is_cleaned_up_session(meta.session_id) {
            let response = crate::domains::queue::QueueResponse::BadRequest {
                reason: "session already closed".to_string(),
            };
            self.route_queue_response(envelope, meta, &response);
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
        // `QueueDomainCore::cleanup_session` runs inline rather than through an
        // actor command, so there is no reply deadline to surface here.
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            // Mark first so an older normal-lane request that cleanup jumped
            // over cannot recreate a subscription or pending reserve for
            // this session below. `cleanup.rs` is the sole source of truth
            // for cleaned-up-session state.
            self.mark_cleaned_up_session(cleanup.session_id);
            self.cleanup_session(cleanup.session_id);
            return true;
        }

        false
    }

    fn ensure_active(&self) -> Result<(), DeliveryError> {
        crate::runtime::ingress_support::ensure_actor_active(&self.active)
    }

    fn log_delivery(envelope: &Envelope) {
        crate::runtime::ingress_support::log_envelope_received(
            "queue",
            "Queue domain sink: received envelope",
            envelope,
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
        let receive_route = match &queue_msg {
            crate::domains::queue::protocol::QueueMessage::Receive { route, .. } => {
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
                if envelope.source().is_some() {
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
                        envelope: envelope.clone_for_deferred_reply(),
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

        let delivered = self.route_queue_response(envelope, meta, &outcome.response);
        if !delivered {
            if let Some(route) = receive_route.as_ref() {
                self.rollback_undeliverable_receive(
                    meta.route_family,
                    route,
                    meta.session_id,
                    &outcome.response,
                );
            }
        }
        self.record_operation_metrics(request_started, &outcome.response, op_kind);
        if let Some(route) = wake_route.as_ref() {
            self.wake_pending_reserves_for_route(meta.route_family, route, Instant::now());
        }
    }

    pub(in crate::domains::queue::sink) fn queue_message_family(
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
