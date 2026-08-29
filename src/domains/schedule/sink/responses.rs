//! Response encoding and best-effort routing back to the requester.

use super::model::{Envelope, ScheduleDomainRuntime};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;

impl ScheduleDomainRuntime<'_> {
    pub(super) fn route_schedule_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::schedule::ScheduleResponse,
        request_started: Option<std::time::Instant>,
    ) -> bool {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes = crate::dispatch::protocol::schedule_codec::encode_response_into(
                &mut payload_encoder,
                meta.message_type,
                response,
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
            crate::domains::schedule::ScheduleClientResponse::new(meta, response.clone());

        let delivered = if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            if let Err(error) = self.core.router.route(response_envelope) {
                if let Some(metrics) = self.core.metrics.as_ref() {
                    metrics.record_response_drop();
                } else {
                    crate::observability::counter_inc(
                        crate::domains::schedule::metrics::METRIC_RESPONSE_DROPS_TOTAL,
                    );
                }
                tracing::warn!(
                    domain = "schedule",
                    session_id = meta.session_id,
                    route_family = meta.route_family.as_u64(),
                    error = %error,
                    "Dropped best-effort Schedule response"
                );
                false
            } else {
                true
            }
        } else {
            false
        };

        if let (Some(metrics), Some(started_at)) = (self.core.metrics.as_ref(), request_started) {
            if Self::schedule_response_is_failure(response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }
        delivered
    }
}
