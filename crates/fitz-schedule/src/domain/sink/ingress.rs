//! Envelope ingress: validate an inbound envelope, parse it into a Schedule
//! request, and dispatch to the subscriptions/definitions/response layers.

use super::model::ScheduleDomainRuntime;
#[cfg(test)]
use crate::wire::frame_context::FrameContext;
use fitz_core::runtime::SessionScoped as _;
use fitz_core::runtime::{DeliveryError, Envelope};

impl ScheduleDomainRuntime<'_> {
    pub(super) fn deliver_envelope(&mut self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
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
            let response =
                crate::domain::ScheduleResponse::Error(crate::domain::ScheduleFailure::new(
                    crate::domain::ScheduleFailureCategory::InvalidTarget,
                    "route family mismatch",
                ));
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_schedule_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

        // This request was already queued (on the normal lane) before this
        // session's disconnect cleanup ran (on the high-priority lane) and
        // jumped ahead of it. Reject rather than silently recreating a
        // subscription for a session that is already gone and will never be
        // cleaned up again.
        if self.is_cleaned_up_request(meta.session_id, envelope) {
            let response =
                crate::domain::ScheduleResponse::Error(crate::domain::ScheduleFailure::new(
                    crate::domain::ScheduleFailureCategory::InvalidTarget,
                    "session already closed",
                ));
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
            let response =
                crate::domain::ScheduleResponse::Error(crate::domain::ScheduleFailure::new(
                    crate::domain::ScheduleFailureCategory::InvalidTarget,
                    "route family mismatch",
                ));
            let response_meta = Self::response_meta_for_source(envelope, meta);
            self.route_schedule_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

        let route_addr = envelope.destination();
        let route_family = *route_addr.family();

        let subscribe_rollback = match &schedule_msg {
            crate::domain::ScheduleMessage::Subscribe {
                family_id,
                route,
                session_id,
                ..
            } => {
                debug_assert_eq!(*family_id, self.core.route_family);
                let existed = self
                    .core
                    .subscriptions
                    .find_existing_id(*session_id, route.as_str())
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
                crate::domain::ScheduleResponse::SubscribeOk { .. }
            )
        {
            if let Some((family_id, route, session_id)) = subscribe_rollback {
                self.rollback_undeliverable_schedule_subscribe(family_id, &route, session_id);
            }
        }

        Ok(())
    }

    fn handle_domain_publish_envelope(&mut self, envelope: &Envelope) -> bool {
        if let Some(event) = envelope.payload::<fitz_core::runtime::DomainPublishEvent>() {
            if *envelope.destination().family() != event.family_id {
                self.core.live_publish_failures = self.core.live_publish_failures.saturating_add(1);
                return true;
            }
            self.handle_domain_publish(event);
            return true;
        }

        false
    }

    fn log_delivery(envelope: &Envelope) {
        fitz_core::runtime::ingress_support::log_envelope_received(
            "schedule",
            "Schedule domain sink: received envelope",
            envelope,
        );
    }

    fn extract_request(
        envelope: &Envelope,
    ) -> Result<Option<crate::domain::ScheduleClientRequest>, DeliveryError> {
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

    fn record_request_start(&mut self) -> Option<std::time::Instant> {
        self.core
            .metrics
            .as_ref()
            .map(crate::domain::ScheduleMetrics::record_request_start)
    }

    fn parse_request_message(
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        message: Result<crate::domain::ScheduleMessage, crate::domain::ScheduleFailure>,
        request_started: Option<std::time::Instant>,
    ) -> Option<crate::domain::ScheduleMessage> {
        match message {
            Ok(message) => Some(message),
            Err(error) => {
                tracing::warn!(
                    domain = "schedule",
                    error = %error,
                    "Failed to parse schedule message"
                );
                let response = crate::domain::ScheduleResponse::Error(error);
                let response_meta = Self::response_meta_for_source(envelope, meta);
                self.route_schedule_response(envelope, response_meta, &response, request_started);
                None
            }
        }
    }

    pub(super) fn dispatch_schedule_message(
        &mut self,
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        request_started: Option<std::time::Instant>,
        route_family: fitz_core::runtime::routing::RouteFamily,
        schedule_msg: crate::domain::ScheduleMessage,
    ) -> Option<(crate::domain::ScheduleResponse, bool)> {
        use crate::domain::ScheduleResponse;

        match &schedule_msg {
            crate::domain::ScheduleMessage::Subscribe {
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
            crate::domain::ScheduleMessage::Unsubscribe {
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
            crate::domain::ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                self.unsubscribe_all(*session_id);
                return Some((ScheduleResponse::Ok, false));
            }
            _ => {}
        }

        debug_assert_eq!(route_family, self.core.route_family);
        let mut family_actor = self.core.actor.take();
        let actor = match self.get_or_create_actor(&mut family_actor) {
            Ok(actor) => actor,
            Err(error) => {
                self.core.actor = family_actor;
                let response =
                    ScheduleResponse::Error(crate::domain::ScheduleFailure::operation_error(error));
                self.route_schedule_response(envelope, meta, &response, request_started);
                return None;
            }
        };

        let outcome = self.apply_schedule_message(actor, schedule_msg);
        self.core.actor = family_actor;
        Some(outcome)
    }

    fn request_from_envelope(envelope: &Envelope) -> Option<crate::domain::ScheduleClientRequest> {
        if let Some(request) = envelope.payload::<crate::domain::ScheduleClientRequest>() {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                fitz_core::runtime::routing::RouteAddress::new(
                    *envelope.destination().family(),
                    fitz_core::runtime::routing::Route::new(format!(
                        "inbox://session/{}",
                        frame_ctx.session_id
                    )),
                )
            });
            let meta = fitz_core::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            )
            .with_correlation(frame_ctx.correlation);
            let parsed = crate::wire::schedule_codec::parse_request(
                &frame_ctx,
                &frame_ctx.payload,
                *envelope.destination().family(),
                fitz_core::session::SessionId(frame_ctx.session_id),
                subscriber,
            );
            Some(crate::domain::ScheduleClientRequest::new(meta, parsed))
        }

        #[cfg(not(test))]
        {
            None
        }
    }

    fn valid_request_envelope(
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
    ) -> bool {
        meta.route_family == *envelope.destination().family()
            && envelope
                .source()
                .is_none_or(|source| *source.family() == meta.route_family)
    }

    pub(super) fn response_meta_for_source(
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
    ) -> fitz_core::runtime::ClientFrameMeta {
        envelope.source().map_or(meta, |source| {
            let mut response_meta = meta;
            response_meta.route_family = *source.family();
            response_meta
        })
    }

    fn valid_schedule_message(
        envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        message: &crate::domain::ScheduleMessage,
    ) -> bool {
        use crate::domain::ScheduleMessage;

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
            | ScheduleMessage::List { .. } => true,
        }
    }
}

#[cfg(test)]
fn test_client_channel_from_protocol(
    channel: crate::wire::frame::ChannelId,
) -> fitz_core::runtime::ClientChannel {
    match channel {
        crate::wire::frame::ChannelId::Control => fitz_core::runtime::ClientChannel::Control,
        crate::wire::frame::ChannelId::Pub => fitz_core::runtime::ClientChannel::Pub,
        crate::wire::frame::ChannelId::Sub => fitz_core::runtime::ClientChannel::Sub,
        crate::wire::frame::ChannelId::Rpc => fitz_core::runtime::ClientChannel::Rpc,
        crate::wire::frame::ChannelId::Lease => fitz_core::runtime::ClientChannel::Lease,
        crate::wire::frame::ChannelId::Internal => fitz_core::runtime::ClientChannel::Internal,
    }
}
