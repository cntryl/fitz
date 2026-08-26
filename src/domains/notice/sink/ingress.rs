//! Envelope ingress: validate an inbound envelope, parse it into a Notice
//! request, and dispatch to the subscription/publish/response layers.

#[cfg(test)]
use super::{test_client_channel_from_protocol, FrameContext};
use super::{Envelope, NoticeDomainCore, NoticeMetrics};
use crate::runtime::DeliveryError;
use std::sync::atomic::Ordering;
use std::time::Instant;

impl NoticeDomainCore {
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
            self.reject_with(envelope, meta, "route family mismatch", request_started);
            return Ok(());
        }

        // This request was already queued (on the normal lane) before this
        // session's disconnect cleanup ran (on the high-priority lane) and
        // jumped ahead of it. Reject rather than silently recreating a
        // subscription for a session that is already gone and will never be
        // cleaned up again.
        if self.is_cleaned_up_session(meta.session_id) {
            self.reject_with(envelope, meta, "session already closed", request_started);
            return Ok(());
        }

        Self::log_parse_start(meta);

        let Some(notice_msg) =
            self.parse_notice_message(envelope, meta, request.message, request_started)
        else {
            return Ok(());
        };

        if !Self::valid_notice_message(envelope, meta, &notice_msg) {
            self.reject_with(envelope, meta, "route family mismatch", request_started);
            return Ok(());
        }

        let (response_opt, should_sync_admin_snapshot) = self.dispatch_notice_message(notice_msg);
        if should_sync_admin_snapshot {
            self.mark_admin_snapshot_dirty();
        }

        if let Some(response) = response_opt {
            self.route_notice_response(envelope, meta, &response, request_started);
        } else if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            metrics.record_success(started_at);
        }

        Ok(())
    }

    fn ensure_active(&self) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        Ok(())
    }

    fn handle_domain_publish_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            if *envelope.destination().family() != event.family_id {
                self.counter_add("fitz_notice_publish_family_mismatch_total", 1);
                return true;
            }
            self.handle_domain_publish(event);
            return true;
        }

        false
    }

    fn log_delivery(envelope: &Envelope) {
        tracing::debug!(
            domain = "notice",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Notice domain sink: received envelope"
        );
    }

    fn extract_request(
        envelope: &Envelope,
    ) -> Result<Option<crate::domains::notice::NoticeClientRequest>, DeliveryError> {
        if let Some(request) = Self::request_from_envelope(envelope) {
            Ok(Some(request))
        } else {
            tracing::warn!(
                domain = "notice",
                "Envelope payload was not NoticeClientRequest"
            );
            Err(DeliveryError::ActorStopped)
        }
    }

    fn record_request_start(&self) -> Option<Instant> {
        self.metrics
            .as_ref()
            .map(NoticeMetrics::record_request_start)
    }

    fn log_parse_start(meta: crate::runtime::ClientFrameMeta) {
        tracing::debug!(
            domain = "notice",
            session = meta.session_id,
            msg_type = meta.message_type,
            "Notice: parsing request"
        );
    }

    fn parse_notice_message(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: Result<crate::domains::notice::protocol::NotificationMessage, String>,
        request_started: Option<Instant>,
    ) -> Option<crate::domains::notice::protocol::NotificationMessage> {
        match message {
            Ok(message) => Some(message),
            Err(error) => {
                tracing::warn!(domain = "notice", error = %error, "Failed to parse notice message");
                self.reject_with(envelope, meta, &error, request_started);
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

    fn valid_notice_message(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        message: &crate::domains::notice::protocol::NotificationMessage,
    ) -> bool {
        use crate::domains::notice::protocol::NotificationMessage;

        match message {
            NotificationMessage::Publish(publish) => publish.family_id == meta.route_family,
            NotificationMessage::Subscribe(subscribe) => {
                subscribe.family_id == meta.route_family
                    && subscribe.session_id.0 == meta.session_id
                    && *subscribe.subscriber.family() == subscribe.family_id
                    && envelope
                        .source()
                        .is_none_or(|source| source == &subscribe.subscriber)
            }
            NotificationMessage::Unsubscribe(unsubscribe) => {
                unsubscribe.family_id == meta.route_family
                    && unsubscribe.session_id.0 == meta.session_id
            }
            NotificationMessage::UnsubscribeAll(unsubscribe_all) => {
                unsubscribe_all.session_id.0 == meta.session_id
                    && *unsubscribe_all.subscriber.family() == meta.route_family
                    && envelope
                        .source()
                        .is_none_or(|source| source == &unsubscribe_all.subscriber)
            }
            NotificationMessage::Deliver(_) => false,
        }
    }

    fn request_from_envelope(
        envelope: &Envelope,
    ) -> Option<crate::domains::notice::NoticeClientRequest> {
        if let Some(request) = envelope.payload::<crate::domains::notice::NoticeClientRequest>() {
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
            let parsed = crate::dispatch::protocol::notice_codec::parse_request(
                &frame_ctx,
                &frame_ctx.payload,
                *envelope.destination().family(),
                crate::session::SessionId(frame_ctx.session_id),
                subscriber,
            );
            Some(crate::domains::notice::NoticeClientRequest::new(
                meta, parsed,
            ))
        }

        #[cfg(not(test))]
        {
            None
        }
    }
}
