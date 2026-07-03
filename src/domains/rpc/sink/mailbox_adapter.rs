use super::state_model::{Envelope, RpcClientRequest, RpcClientResponseBody, RpcDomainRuntime};
#[cfg(not(test))]
use crate::domains::rpc::{
    RpcClientForwardedResponse, RpcClientForwardedResponseBody, RpcClientResponse,
};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;

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
            let parsed = crate::protocol::rpc_codec::parse_request(
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
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes =
                crate::protocol::rpc_codec::encode_response_into(response, &mut payload_encoder);
            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx = RpcClientResponse::new(meta, response.clone());

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            let _ = self.router.route(response_envelope);
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
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(
                    crate::protocol::rpc_codec::terminal_error_response_message_capacity(message),
                );
            let mut error_encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(
                crate::protocol::rpc_codec::error_body_capacity(message),
            );
            let response_bytes =
                crate::protocol::rpc_codec::encode_terminal_error_response_message_into(
                    &correlation_id,
                    code,
                    message,
                    &mut response_encoder,
                    &mut error_encoder,
                );
            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::protocol::tlv::MessageType::new(303),
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
            let _ = self.router.route(response_envelope);
        }
    }
}

#[cfg(test)]
fn test_client_channel_from_protocol(
    channel: crate::protocol::frame::ChannelId,
) -> crate::runtime::ClientChannel {
    match channel {
        crate::protocol::frame::ChannelId::Control => crate::runtime::ClientChannel::Control,
        crate::protocol::frame::ChannelId::Pub => crate::runtime::ClientChannel::Pub,
        crate::protocol::frame::ChannelId::Sub => crate::runtime::ClientChannel::Sub,
        crate::protocol::frame::ChannelId::Rpc => crate::runtime::ClientChannel::Rpc,
        crate::protocol::frame::ChannelId::Lease => crate::runtime::ClientChannel::Lease,
        crate::protocol::frame::ChannelId::Internal => crate::runtime::ClientChannel::Internal,
    }
}

#[cfg(test)]
pub(super) fn test_protocol_channel_from_client(
    channel: crate::runtime::ClientChannel,
) -> crate::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => crate::protocol::frame::ChannelId::Control,
        crate::runtime::ClientChannel::Pub => crate::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => crate::protocol::frame::ChannelId::Internal,
    }
}
