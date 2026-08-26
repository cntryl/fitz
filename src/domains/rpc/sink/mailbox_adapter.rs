#[cfg(test)]
use super::state_model::RPC_MSG_TYPE_RESPONSE;
use super::state_model::{Envelope, RpcClientRequest, RpcClientResponseBody, RpcDomainRuntime};
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
#[cfg(not(test))]
use crate::domains::rpc::{
    RpcClientForwardedResponse, RpcClientForwardedResponseBody, RpcClientResponse,
};

impl RpcDomainRuntime<'_> {
    pub(super) fn request_from_envelope(envelope: &Envelope) -> Option<RpcClientRequest> {
        if let Some(request) = envelope.payload::<RpcClientRequest>() {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::dispatch::protocol::rpc_codec::parse_request(
                &frame_ctx,
                &frame_ctx.payload,
                *envelope.destination().family(),
            );
            Some(RpcClientRequest::new_with_payload(
                meta,
                parsed,
                frame_ctx.payload,
            ))
        }

        #[cfg(not(test))]
        {
            None
        }
    }

    pub(super) fn route_rpc_client_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &RpcClientResponseBody,
    ) {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes = crate::dispatch::protocol::rpc_codec::encode_response_into(
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
        let response_ctx = RpcClientResponse::new(meta, response.clone());

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            if let Err(error) = self.router.route(response_envelope) {
                Self::record_response_drop(meta.session_id, "response", &error);
            }
        }
    }

    pub(super) fn route_rpc_terminal_error_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        correlation_id: uuid::Uuid,
        code: u16,
        message: &'static str,
    ) {
        #[cfg(test)]
        let response_ctx = {
            let mut response_encoder =
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(
                    crate::dispatch::protocol::rpc_codec::terminal_error_response_message_capacity(
                        message,
                    ),
                );
            let mut error_encoder =
                crate::dispatch::protocol::payload_codec::PayloadEncoder::with_capacity(
                    crate::dispatch::protocol::rpc_codec::error_body_capacity(message),
                );
            let response_bytes =
                crate::dispatch::protocol::rpc_codec::encode_terminal_error_response_message_into(
                    &correlation_id,
                    code,
                    message,
                    &mut response_encoder,
                    &mut error_encoder,
                );
            FrameContext::new(
                meta.session_id,
                crate::protocol::test_support::channel_id_from_client(meta.channel),
                crate::dispatch::protocol::tlv::MessageType::new(RPC_MSG_TYPE_RESPONSE),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx = RpcClientForwardedResponse::new(
            meta.session_id,
            meta.route_family,
            RpcClientForwardedResponseBody::TerminalError {
                correlation_id,
                code,
                message,
            },
        );

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            if let Err(error) = self.router.route(response_envelope) {
                Self::record_response_drop(meta.session_id, "terminal_error", &error);
            }
        }
    }

    /// Record a client response the router could not deliver.
    ///
    /// A dropped RPC response is invisible to the caller until its own timeout
    /// expires, and a dropped terminal error converts a fast failure into that
    /// same silent wait. Per Law 7 this only reports the drop; delivery is not
    /// retried and correctness does not depend on the counter existing.
    fn record_response_drop(
        session_id: u64,
        response_kind: &'static str,
        error: &crate::runtime::RouteError,
    ) {
        crate::observability::counter_inc(
            crate::domains::rpc::metrics::METRIC_RESPONSE_DROPS_TOTAL,
        );
        tracing::warn!(
            domain = "rpc",
            session = session_id,
            response_kind = response_kind,
            error = %error,
            "Failed to route RPC client response"
        );
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
