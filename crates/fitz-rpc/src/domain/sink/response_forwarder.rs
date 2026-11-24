#[cfg(test)]
use super::state_model::RPC_MSG_TYPE_RESPONSE;
use super::state_model::{RpcPendingDispatchInfo, RpcPendingErrorDelivery};
#[cfg(not(test))]
use crate::domain::{RpcClientForwardedResponse, RpcClientForwardedResponseBody};
#[cfg(test)]
use crate::wire::frame_context::FrameContext;
use fitz_core::runtime::Envelope;

/// Builds requester-facing envelopes without owning dispatch or pending-state policy.
pub(super) struct RpcResponseForwarder;

impl RpcResponseForwarder {
    pub(super) fn response_envelope(
        meta: &fitz_core::runtime::ClientFrameMeta,
        response: &crate::domain::protocol::RpcResponse,
        caller: &RpcPendingDispatchInfo,
    ) -> Option<Envelope> {
        let caller_inbox_addr = caller.caller_inbox_addr.as_ref()?;

        #[cfg(test)]
        {
            let mut encoder = crate::wire::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes =
                crate::wire::rpc_codec::encode_response_message_into(response, &mut encoder);
            let context = FrameContext::new(
                caller.caller_session_id,
                crate::wire::test_support::channel_id_from_client(meta.channel),
                crate::wire::message::MessageType::new(RPC_MSG_TYPE_RESPONSE),
                bytes::Bytes::from(response_bytes),
                *caller_inbox_addr.family(),
            );
            Some(Envelope::new(caller_inbox_addr.clone(), context))
        }

        #[cfg(not(test))]
        {
            let _ = meta;
            Some(Envelope::new(
                caller_inbox_addr.clone(),
                RpcClientForwardedResponse::new(
                    caller.caller_session_id,
                    *caller_inbox_addr.family(),
                    RpcClientForwardedResponseBody::Response(response.clone()),
                ),
            ))
        }
    }

    pub(super) fn terminal_error_envelope(
        delivery: RpcPendingErrorDelivery,
        error_code: u16,
        error_message: &'static str,
    ) -> Envelope {
        let correlation_id = delivery.correlation_id;

        #[cfg(test)]
        {
            let mut error_encoder = crate::wire::payload_codec::PayloadEncoder::with_capacity(96);
            let mut response_encoder =
                crate::wire::payload_codec::PayloadEncoder::with_capacity(128);
            let error_body = crate::wire::rpc_codec::encode_error_body_into(
                error_code,
                error_message,
                &mut error_encoder,
            );
            let response = crate::domain::protocol::RpcResponse::single(
                correlation_id,
                bytes::Bytes::from(error_body),
            );
            let encoded = crate::wire::rpc_codec::encode_response_message_into(
                &response,
                &mut response_encoder,
            );
            let context = FrameContext::new(
                delivery.caller_session_id,
                crate::wire::frame::ChannelId::Rpc,
                crate::wire::message::MessageType::new(RPC_MSG_TYPE_RESPONSE),
                bytes::Bytes::from(encoded),
                *delivery.caller_inbox_addr.family(),
            );
            Envelope::new(delivery.caller_inbox_addr, context)
        }

        #[cfg(not(test))]
        {
            let family = *delivery.caller_inbox_addr.family();
            Envelope::new(
                delivery.caller_inbox_addr,
                RpcClientForwardedResponse::new(
                    delivery.caller_session_id,
                    family,
                    RpcClientForwardedResponseBody::TerminalError {
                        correlation_id,
                        code: error_code,
                        message: error_message,
                    },
                ),
            )
        }
    }
}
