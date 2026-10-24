//! Refuse transport-buffer admission without dispatch or releasing a live slot.

use super::SessionOutboundSink;
use crate::protocol::wire2::header::{decode_message, MessageError};

impl SessionOutboundSink {
    pub(crate) fn reject_unqueued_frame(
        &self,
        session_id: u64,
        frame: &[u8],
    ) -> Result<(), String> {
        if !self.connection.hello_sent() {
            return Err("transport capacity exhausted before HELLO".to_owned());
        }
        let limit = u32::try_from(self.max_message_bytes).unwrap_or(u32::MAX);
        let (kind, correlation, payload, code) = match decode_message(frame, limit) {
            Ok((header, payload)) => (
                header.message_type,
                header.correlation,
                payload,
                crate::protocol::error_codes::protocol::ERR_BUSY,
            ),
            Err(MessageError::ReservedFlags {
                message_type,
                correlation,
                ..
            }) => (
                message_type,
                correlation,
                &[][..],
                crate::protocol::error_codes::protocol::ERR_UNSUPPORTED,
            ),
            Err(error) => return Err(error.to_string()),
        };
        if kind == 500
            && code == crate::protocol::error_codes::protocol::ERR_BUSY
            && frame[2] & 1 == 0
        {
            return Ok(());
        }
        if kind == 1
            || kind == 303
            || (kind == 304 && payload.first() != Some(&1))
            || correlation.is_none()
            || crate::protocol::manifest::client_entry(crate::protocol::message::MessageType::new(
                kind,
            ))
            .is_err()
        {
            return Err("required protocol traffic could not be buffered".to_owned());
        }
        let reason = "transport frame buffer at capacity; request was not accepted";
        let (kind, correlation, body) = if kind == 302 {
            let id = crate::protocol::rpc_codec::extract_request_correlation_id(payload)?;
            (
                303,
                None,
                crate::protocol::rpc_codec::encode_terminal_error_response_message_payload(
                    &id, code, reason,
                ),
            )
        } else {
            (
                kind,
                correlation,
                crate::protocol::error_codes::encode_error_body(code, reason).into(),
            )
        };
        self.send_unadmitted_response(session_id, kind, correlation, &body)
            .map_err(|error| format!("cannot deliver transport refusal: {error:?}"))
    }
}

#[cfg(test)]
mod tests;
