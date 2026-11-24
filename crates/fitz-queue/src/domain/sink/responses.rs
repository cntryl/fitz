//! Response and recovery-error routing back to clients.

use super::model::QueueFamilyState;
#[cfg(test)]
use crate::wire::frame_context::FrameContext;
use fitz_core::runtime::Envelope;
use std::time::Instant;

impl QueueFamilyState {
    /// Count a response the actor produced but the transport could not carry.
    fn record_response_route_failure(&mut self) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.counter_inc(crate::domain::metrics::METRIC_RESPONSE_ROUTE_FAILURES_TOTAL);
        }
    }

    pub(super) fn route_queue_response(
        &mut self,
        request_envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        response: &crate::domain::QueueResponse,
    ) -> bool {
        #[cfg(feature = "benchkit")]
        let started = Instant::now();
        if request_envelope.source().is_none() || !request_envelope.try_claim_reply() {
            tracing::debug!(
                domain = "queue",
                session = meta.session_id,
                "Suppressed Queue response after another terminal response won"
            );
            return false;
        }
        let delivered;
        #[cfg(test)]
        {
            let response_bytes =
                crate::wire::queue_codec::encode_response(meta.message_type, response);
            let response_ctx = FrameContext::new(
                meta.session_id,
                crate::wire::test_support::channel_id_from_client(meta.channel),
                crate::wire::message::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
            .with_correlation(meta.correlation);
            delivered = if let Some(response_envelope) = request_envelope.try_reply_to(response_ctx)
            {
                if let Err(error) = self.router.route(response_envelope) {
                    self.record_response_route_failure();
                    tracing::warn!(
                        domain = "queue",
                        session = meta.session_id,
                        error = ?error,
                        "Failed to route queue response"
                    );
                    false
                } else {
                    true
                }
            } else {
                false
            };
        }

        #[cfg(not(test))]
        {
            let response = crate::domain::QueueClientResponse::new(meta, response.clone());
            delivered = if let Some(response_envelope) = request_envelope.try_reply_to(response) {
                if let Err(error) = self.router.route(response_envelope) {
                    self.record_response_route_failure();
                    tracing::warn!(
                        domain = "queue",
                        session = meta.session_id,
                        error = ?error,
                        "Failed to route queue response"
                    );
                    false
                } else {
                    true
                }
            } else {
                false
            };
        }
        #[cfg(feature = "benchkit")]
        {
            use crate::domain::actor::ack_timing::{observe, Phase};
            use crate::wire::queue_codec::msg_type;
            // Reply cloning/routing ends at the session inbox, before socket encoding.
            match meta.message_type {
                msg_type::RESERVE => observe(Phase::ReserveReplyDelivery, started.elapsed()),
                msg_type::COMPLETE => observe(Phase::AckReplyDelivery, started.elapsed()),
                _ => {}
            }
        }
        delivered
    }

    pub(super) fn route_queue_recovery_error(
        &mut self,
        request_envelope: &Envelope,
        meta: fitz_core::runtime::ClientFrameMeta,
        request_started: Option<Instant>,
        message: String,
    ) {
        tracing::error!(
            domain = "queue",
            family = meta.route_family.as_u64(),
            error = %message,
            "Queue actor recovery failed"
        );
        let response = crate::domain::QueueResponse::Error { message };
        self.route_queue_response(request_envelope, meta, &response);
        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            metrics.record_failure(started_at);
        }
    }

    pub(super) fn queue_response_is_failure(response: &crate::domain::QueueResponse) -> bool {
        matches!(
            response,
            crate::domain::QueueResponse::InvalidToken
                | crate::domain::QueueResponse::InflightExpired
                | crate::domain::QueueResponse::NotFound
                | crate::domain::QueueResponse::QueueNotFound
                | crate::domain::QueueResponse::Busy
                | crate::domain::QueueResponse::BadRequest { .. }
                | crate::domain::QueueResponse::Error { .. }
        )
    }
}
