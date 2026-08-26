//! Response and recovery-error routing back to clients.

#[cfg(test)]
use super::model::FrameContext;
use super::model::{Envelope, Instant, QueueDomainCore};

impl QueueDomainCore {
    /// Count a response the actor produced but the transport could not carry.
    fn record_response_route_failure(&self) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics
                .counter_inc(crate::domains::queue::metrics::METRIC_RESPONSE_ROUTE_FAILURES_TOTAL);
        }
    }

    pub(super) fn route_queue_response(
        &self,
        request_envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::queue::QueueResponse,
    ) {
        #[cfg(test)]
        {
            let response_bytes = crate::dispatch::protocol::queue_codec::encode_response(
                meta.message_type,
                response,
            );
            let response_ctx = FrameContext::new(
                meta.session_id,
                crate::protocol::test_support::channel_id_from_client(meta.channel),
                crate::dispatch::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            );
            if let Some(response_envelope) = request_envelope.try_reply_to(response_ctx) {
                if let Err(error) = self.router.route(response_envelope) {
                    self.record_response_route_failure();
                    tracing::warn!(
                        domain = "queue",
                        session = meta.session_id,
                        error = ?error,
                        "Failed to route queue response"
                    );
                }
            }
        }

        #[cfg(not(test))]
        {
            let response = crate::domains::queue::QueueClientResponse::new(meta, response.clone());
            if let Some(response_envelope) = request_envelope.try_reply_to(response) {
                if let Err(error) = self.router.route(response_envelope) {
                    self.record_response_route_failure();
                    tracing::warn!(
                        domain = "queue",
                        session = meta.session_id,
                        error = ?error,
                        "Failed to route queue response"
                    );
                }
            }
        }
    }

    pub(super) fn route_queue_recovery_error(
        &self,
        request_envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        request_started: Option<Instant>,
        message: String,
    ) {
        tracing::error!(
            domain = "queue",
            family = meta.route_family.as_u64(),
            error = %message,
            "Queue actor recovery failed"
        );
        let response = crate::domains::queue::QueueResponse::Error { message };
        self.route_queue_response(request_envelope, meta, &response);
        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            metrics.record_failure(started_at);
        }
    }

    pub(super) fn queue_response_is_failure(
        response: &crate::domains::queue::QueueResponse,
    ) -> bool {
        matches!(
            response,
            crate::domains::queue::QueueResponse::InvalidToken
                | crate::domains::queue::QueueResponse::InflightExpired
                | crate::domains::queue::QueueResponse::NotFound
                | crate::domains::queue::QueueResponse::QueueNotFound
                | crate::domains::queue::QueueResponse::BadRequest { .. }
                | crate::domains::queue::QueueResponse::Error { .. }
        )
    }
}
