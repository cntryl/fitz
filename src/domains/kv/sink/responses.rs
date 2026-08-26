//! Response-envelope construction and routing.

use super::state::KvDomainRuntime;
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope};

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

impl KvDomainRuntime<'_> {
    pub(super) fn route_kv_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::kv::KvResponse,
        request_started: std::time::Instant,
    ) -> Result<(), DeliveryError> {
        #[cfg(test)]
        let response_ctx = {
            let response_bytes = crate::dispatch::protocol::kv::encode_response(response);
            tracing::trace!(
                domain = "kv",
                session = meta.session_id,
                response_len = response_bytes.len(),
                "KV response encoded"
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
        let response_ctx = crate::domains::kv::KvClientResponse::new(meta, response.clone());

        let Some(response_envelope) = envelope.try_reply_to(response_ctx) else {
            self.record_response_metrics(response, request_started);
            tracing::warn!(
                domain = "kv",
                session = meta.session_id,
                "Cannot route response: envelope has no source address"
            );
            return Ok(());
        };

        match self.core.router.route(response_envelope) {
            Ok(()) => {
                self.record_response_metrics(response, request_started);
                tracing::debug!(
                    domain = "kv",
                    session = meta.session_id,
                    "KV message handled and response routed"
                );
                Ok(())
            }
            Err(error) => {
                self.record_request_metrics(true, request_started);
                tracing::warn!(
                    domain = "kv",
                    session = meta.session_id,
                    error = ?error,
                    "Failed to route response"
                );
                // Preserve why delivery failed. Reporting backpressure as a
                // stopped actor discards the occupancy the caller needs to tell
                // a transient full mailbox from a dead one.
                Err(match error {
                    crate::runtime::RouteError::DeliveryFailed(_, delivery_error) => delivery_error,
                    crate::runtime::RouteError::RouteNotFound(_) => DeliveryError::ActorStopped,
                })
            }
        }
    }
}
