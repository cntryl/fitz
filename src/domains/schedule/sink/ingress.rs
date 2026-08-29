//! Envelope ingress: validate an inbound envelope, parse it into a Schedule
//! request, and dispatch to the subscriptions/definitions/response layers.

use super::model::{DeliveryError, Envelope, Ordering, ScheduleDomainRuntime};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;

impl ScheduleDomainRuntime<'_> {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;

        if self.handle_domain_publish_envelope(envelope) {
            return Ok(());
        }

        Self::log_delivery(envelope);

        let Some(request) = Self::extract_request(envelope)? else {
            return Ok(());
        };
        let meta = request.meta;
        let request_started = self.record_request_start();

        if !Self::valid_request_envelope(envelope, meta) {
            let response = crate::domains::schedule::ScheduleResponse::Error(
                crate::domains::schedule::ScheduleFailure::new(
                    crate::domains::schedule::ScheduleFailureCategory::InvalidTarget,
                    "route family mismatch",
                ),
            );
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_schedule_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

        // This request was already queued (on the normal lane) before this
        // session's disconnect cleanup ran (on the high-priority lane) and
        // jumped ahead of it. Reject rather than silently recreating a
        // subscription for a session that is already gone and will never be
        // cleaned up again.
        if self.is_cleaned_up_session(meta.session_id) {
            let response = crate::domains::schedule::ScheduleResponse::Error(
                crate::domains::schedule::ScheduleFailure::new(
                    crate::domains::schedule::ScheduleFailureCategory::InvalidTarget,
                    "session already closed",
                ),
            );
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_schedule_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

        let Some(schedule_msg) =
            self.parse_request_message(envelope, meta, request.message, request_started)
        else {
            return Ok(());
        };

        if !Self::valid_schedule_message(envelope, meta, &schedule_msg) {
            let response = crate::domains::schedule::ScheduleResponse::Error(
                crate::domains::schedule::ScheduleFailure::new(
                    crate::domains::schedule::ScheduleFailureCategory::InvalidTarget,
                    "route family mismatch",
                ),
            );
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_schedule_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

        let route_addr = envelope.destination();
        let route_family = *route_addr.family();

        let subscribe_rollback = match &schedule_msg {
            crate::domains::schedule::ScheduleMessage::Subscribe {
                family_id,
                route,
                session_id,
                ..
            } => {
                let existed = self
                    .core
                    .sub_families
                    .lock()
                    .get(family_id)
                    .and_then(|state| state.find_existing_id(*session_id, route.as_str()))
                    .is_some();
                (!existed).then_some((*family_id, route.clone(), *session_id))
            }
            _ => None,
        };
        let Some((response, schedule_snapshot_dirty)) = self.dispatch_schedule_message(
            envelope,
            meta,
            request_started,
            route_family,
            schedule_msg,
        ) else {
            return Ok(());
        };

        if schedule_snapshot_dirty {
            self.schedule_admin_snapshot(false);
        }

        let delivered = self.route_schedule_response(envelope, meta, &response, request_started);
        if !delivered
            && matches!(
                response,
                crate::domains::schedule::ScheduleResponse::SubscribeOk { .. }
            )
        {
            if let Some((family_id, route, session_id)) = subscribe_rollback {
                self.rollback_undeliverable_schedule_subscribe(family_id, &route, session_id);
            }
        }

        Ok(())
    }

    fn ensure_active(&self) -> Result<(), DeliveryError> {
        crate::runtime::ingress_support::ensure_actor_active(self.active)
    }

    fn handle_domain_publish_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            if *envelope.destination().family() != event.family_id {
                self.core
                    .live_publish_failures
                    .fetch_add(1, Ordering::Relaxed);
                return true;
            }
            self.handle_domain_publish(event);
            return true;
        }

        false
    }

    fn log_delivery(envelope: &Envelope) {
        crate::runtime::ingress_support::log_envelope_received(
            "schedule",
            "Schedule domain sink: received envelope",
            envelope,
        );
    }

    fn extract_request(
        envelope: &Envelope,
    ) -> Result<Option<crate::domains::schedule::ScheduleClientRequest>, DeliveryError> {
        if let Some(request) = Self::request_from_envelope(envelope) {
            Ok(Some(request))
        } else {
            tracing::warn!(
                domain = "schedule",
                "Envelope payload was not ScheduleClientRequest"
            );
            Err(DeliveryError::ActorStopped)
        }
    }

    fn record_request_start(&self) -> Option<std::time::Instant> {
        self.core
            .metrics
            .as_ref()
            .map(crate::domains::schedule::ScheduleMetrics::record_request_start)
    }

    fn parse_request_message(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: Result<
            crate::domains::schedule::ScheduleMessage,
            crate::domains::schedule::ScheduleFailure,
        >,
        request_started: Option<std::time::Instant>,
    ) -> Option<crate::domains::schedule::ScheduleMessage> {
        match message {
            Ok(message) => Some(message),
            Err(error) => {
                tracing::warn!(
                    domain = "schedule",
                    error = %error,
                    "Failed to parse schedule message"
                );
                let response = crate::domains::schedule::ScheduleResponse::Error(error);
                let response_meta = Self::response_meta_for_source(envelope, meta);
                self.route_schedule_response(envelope, response_meta, &response, request_started);
                None
            }
        }
    }

    pub(super) fn dispatch_schedule_message(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        route_family: crate::runtime::routing::RouteFamily,
        schedule_msg: crate::domains::schedule::ScheduleMessage,
    ) -> Option<(crate::domains::schedule::ScheduleResponse, bool)> {
        use crate::domains::schedule::ScheduleResponse;

        match &schedule_msg {
            crate::domains::schedule::ScheduleMessage::Subscribe {
                family_id,
                route,
                session_id,
                subscriber,
            } => {
                return Some((
                    self.apply_subscribe_message(
                        *family_id,
                        route,
                        *session_id,
                        subscriber.clone(),
                    ),
                    false,
                ));
            }
            crate::domains::schedule::ScheduleMessage::Unsubscribe {
                family_id,
                route,
                session_id,
                ..
            } => {
                return Some((
                    self.apply_unsubscribe_message(*family_id, route, *session_id),
                    false,
                ));
            }
            crate::domains::schedule::ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                self.unsubscribe_all(*session_id);
                return Some((ScheduleResponse::Ok, false));
            }
            _ => {}
        }

        let mut actors = self.core.actors.lock();
        let actor = match self.get_or_create_actor(&mut actors, route_family) {
            Ok(actor) => actor,
            Err(error) => {
                let response = ScheduleResponse::Error(
                    crate::domains::schedule::ScheduleFailure::parse(error),
                );
                self.route_schedule_response(envelope, meta, &response, request_started);
                return None;
            }
        };

        Some(self.apply_schedule_message(actor, schedule_msg))
    }

    fn request_from_envelope(
        envelope: &Envelope,
    ) -> Option<crate::domains::schedule::ScheduleClientRequest> {
        if let Some(request) = envelope.payload::<crate::domains::schedule::ScheduleClientRequest>()
        {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                crate::runtime::routing::RouteAddress::new(
                    *envelope.destination().family(),
                    crate::runtime::routing::Route::new(format!(
                        "inbox://session/{}",
                        frame_ctx.session_id
                    )),
                )
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::dispatch::protocol::schedule_codec::parse_request(
                &frame_ctx,
                &frame_ctx.payload,
                *envelope.destination().family(),
                crate::session::SessionId(frame_ctx.session_id),
                subscriber,
            );
            Some(crate::domains::schedule::ScheduleClientRequest::new(
                meta, parsed,
            ))
        }

        #[cfg(not(test))]
        {
            None
        }
    }

    fn valid_request_envelope(envelope: &Envelope, meta: crate::runtime::ClientFrameMeta) -> bool {
        meta.route_family == *envelope.destination().family()
            && envelope
                .source()
                .is_none_or(|source| *source.family() == meta.route_family)
    }

    pub(super) fn response_meta_for_source(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
    ) -> crate::runtime::ClientFrameMeta {
        envelope.source().map_or(meta, |source| {
            let mut response_meta = meta;
            response_meta.route_family = *source.family();
            response_meta
        })
    }

    fn valid_schedule_message(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: &crate::domains::schedule::ScheduleMessage,
    ) -> bool {
        use crate::domains::schedule::ScheduleMessage;

        match message {
            ScheduleMessage::Subscribe {
                family_id,
                session_id,
                subscriber,
                ..
            }
            | ScheduleMessage::Unsubscribe {
                family_id,
                session_id,
                subscriber,
                ..
            } => {
                *family_id == meta.route_family
                    && *session_id == meta.session_id
                    && *subscriber.family() == *family_id
                    && envelope.source().is_none_or(|source| source == subscriber)
            }
            ScheduleMessage::UnsubscribeAll {
                session_id,
                subscriber,
            } => {
                *session_id == meta.session_id
                    && *subscriber.family() == meta.route_family
                    && envelope.source().is_none_or(|source| source == subscriber)
            }
            ScheduleMessage::Create { .. }
            | ScheduleMessage::CreateBatch { .. }
            | ScheduleMessage::Cancel { .. }
            | ScheduleMessage::List { .. }
            | ScheduleMessage::ListV2 { .. } => true,
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
