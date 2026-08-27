//! Response encoding and best-effort routing back to the requester.

#[cfg(test)]
use super::FrameContext;
use super::{Envelope, Instant, NoticeDomainCore};

impl NoticeDomainCore {
    pub(super) fn reject_with(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        reason: &str,
        request_started: Option<Instant>,
    ) {
        let response = Self::error_response(reason);
        let response_meta = Self::response_meta_for_source(envelope, meta);
        self.route_notice_response(envelope, response_meta, &response, request_started);
    }

    fn error_response(reason: &str) -> crate::domains::notice::NoticeResponse {
        crate::domains::notice::NoticeResponse::Error(reason.to_string())
    }

    fn response_meta_for_source(
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
    ) -> crate::runtime::ClientFrameMeta {
        envelope.source().map_or(meta, |source| {
            let mut response_meta = meta;
            response_meta.route_family = *source.family();
            response_meta
        })
    }

    pub(super) fn route_notice_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::notice::NoticeResponse,
        request_started: Option<Instant>,
    ) -> bool {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes = crate::dispatch::protocol::notice_codec::encode_response_into(
                response,
                &mut payload_encoder,
            );
            FrameContext::new(
                meta.session_id,
                crate::protocol::test_support::channel_id_from_client(meta.channel),
                crate::dispatch::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx =
            crate::domains::notice::NoticeClientResponse::new(meta, response.clone());

        let delivered = if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            if let Err(error) = self.router.route(response_envelope) {
                if let Some(metrics) = self.metrics.as_ref() {
                    metrics.record_response_drop();
                } else {
                    crate::observability::counter_inc(
                        crate::domains::notice::metrics::METRIC_RESPONSE_DROPS_TOTAL,
                    );
                }
                tracing::warn!(
                    domain = "notice",
                    session_id = meta.session_id,
                    route_family = meta.route_family.as_u64(),
                    error = %error,
                    "Dropped best-effort Notice response"
                );
                false
            } else {
                true
            }
        } else {
            false
        };

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if response.is_failure() {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }
        delivered
    }
}
