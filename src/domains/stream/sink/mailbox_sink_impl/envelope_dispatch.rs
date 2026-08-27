//! Envelope intake: classify what arrived, hand it to the right frame
//! handler, and route the reply back to the client session.

#[cfg(test)]
use super::FrameContext;
use super::{
    DeliveryError, Envelope, Ordering, StreamClientFrame, StreamClientRequest,
    StreamClientResponseBody, StreamDomainCore, STREAM_OPERATIONS_TOTAL,
};
#[cfg(test)]
use super::{Route, RouteAddress};

impl StreamDomainCore {
    pub(in crate::domains::stream::sink) fn deliver_envelope(
        &self,
        envelope: &Envelope,
    ) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;

        if self.handle_domain_publish_envelope(envelope) {
            return Ok(());
        }

        let Some(request) = Self::extract_request(envelope)? else {
            return Ok(());
        };
        let meta = request.meta;
        let request_started = self.record_request_start();

        if meta.route_family != *envelope.destination().family()
            || envelope
                .source()
                .is_some_and(|source| *source.family() != meta.route_family)
        {
            let response = Self::stream_error_response("route family mismatch");
            let response_meta = envelope.source().map_or(meta, |source| {
                let mut response_meta = meta;
                response_meta.route_family = *source.family();
                response_meta
            });
            self.route_stream_response(envelope, response_meta, &response, request_started);
            return Ok(());
        }

        let Some(parsed_frame) =
            self.parse_request_frame(envelope, meta, request.frame, request_started)
        else {
            return Ok(());
        };

        let session_mutation = matches!(
            parsed_frame,
            StreamClientFrame::Sub(_)
                | StreamClientFrame::Op(
                    crate::domains::stream::protocol::StreamMessage::Begin { .. }
                        | crate::domains::stream::protocol::StreamMessage::Append { .. }
                        | crate::domains::stream::protocol::StreamMessage::Commit { .. }
                        | crate::domains::stream::protocol::StreamMessage::Rollback { .. }
                )
        );
        if session_mutation && self.cleaned_up_sessions.lock().contains(meta.session_id) {
            let response = Self::stream_error_response("session has been cleaned up");
            self.route_stream_response(envelope, meta, &response, request_started);
            return Ok(());
        }

        self.record_operation();

        match parsed_frame {
            StreamClientFrame::Sub(sub_msg) => {
                self.handle_subscription_frame(envelope, meta, request_started, sub_msg);
                Ok(())
            }
            StreamClientFrame::Op(stream_msg) => {
                self.handle_actor_operation_frame(envelope, meta, request_started, stream_msg);
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

    fn handle_domain_publish_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            if *envelope.destination().family() != event.family_id {
                crate::observability::counter_inc("fitz_stream_publish_family_mismatch_total");
                return true;
            }
            self.handle_domain_publish(event);
            return true;
        }

        false
    }

    fn extract_request(envelope: &Envelope) -> Result<Option<StreamClientRequest>, DeliveryError> {
        Ok(Some(
            Self::request_from_envelope(envelope).ok_or(DeliveryError::ActorStopped)?,
        ))
    }

    fn record_request_start(&self) -> Option<std::time::Instant> {
        self.metrics
            .as_ref()
            .map(crate::domains::stream::StreamMetrics::record_request_start)
    }

    fn parse_request_frame(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        frame: Result<StreamClientFrame, String>,
        request_started: Option<std::time::Instant>,
    ) -> Option<StreamClientFrame> {
        match frame {
            Ok(frame) => Some(frame),
            Err(error) => {
                let response = Self::stream_error_response(error);
                self.route_stream_response(envelope, meta, &response, request_started);
                None
            }
        }
    }

    fn record_operation(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.counter_inc(STREAM_OPERATIONS_TOTAL);
        } else {
            crate::observability::counter_inc(STREAM_OPERATIONS_TOTAL);
        }
    }

    pub(super) fn request_from_envelope(envelope: &Envelope) -> Option<StreamClientRequest> {
        if let Some(request) = envelope.payload::<StreamClientRequest>() {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                RouteAddress::new(
                    *envelope.destination().family(),
                    Route::new(format!("inbox://session/{}", frame_ctx.session_id)),
                )
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::dispatch::protocol::stream_codec::parse_request(
                &frame_ctx,
                &frame_ctx.payload,
                *envelope.destination().family(),
                crate::session::SessionId(frame_ctx.session_id),
                subscriber,
            );
            Some(StreamClientRequest::new(meta, parsed))
        }

        #[cfg(not(test))]
        {
            None
        }
    }

    pub(super) fn route_stream_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &StreamClientResponseBody,
        request_started: Option<std::time::Instant>,
    ) -> bool {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes = crate::dispatch::protocol::stream_codec::encode_response_into(
                &mut payload_encoder,
                meta.message_type,
                response,
            );
            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::dispatch::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx =
            crate::domains::stream::StreamClientResponse::new(meta, response.clone());

        let delivered = if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            if let Err(error) = self.router.route(response_envelope) {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.record_response_drop();
                } else {
                    crate::observability::counter_inc(
                        crate::domains::stream::metrics::METRIC_RESPONSE_DROPS_TOTAL,
                    );
                }
                tracing::warn!(
                    domain = "stream",
                    session_id = meta.session_id,
                    route_family = meta.route_family.as_u64(),
                    error = %error,
                    "Dropped best-effort Stream response"
                );
                false
            } else {
                true
            }
        } else {
            false
        };

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if Self::stream_response_is_failure(response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }
        delivered
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

#[cfg(test)]
fn test_protocol_channel_from_client(
    channel: crate::runtime::ClientChannel,
) -> crate::dispatch::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => {
            crate::dispatch::protocol::frame::ChannelId::Control
        }
        crate::runtime::ClientChannel::Pub => crate::dispatch::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::dispatch::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::dispatch::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => {
            crate::dispatch::protocol::frame::ChannelId::Internal
        }
    }
}
