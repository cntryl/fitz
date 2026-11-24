use super::{Session, SessionError};
use crate::observability as obs;
use crate::protocol::wire3::extensions::DecodedPayload;
use crate::protocol::wire3::header::{MessageError, MessageHeader};

impl Session {
    pub(super) fn validate_payload<'a>(
        &mut self,
        wire_payload: &'a [u8],
        header: &MessageHeader,
    ) -> Result<DecodedPayload<'a>, SessionError> {
        let decoded = crate::protocol::wire3::extensions::decode_payload(
            wire_payload,
            header.message_type,
            crate::protocol::wire3::extensions::ExtensionDirection::ClientToBroker,
            header.flags.is_extensions_present(),
        )
        .map_err(|error| {
            crate::observability::counter_inc(obs::METRIC_TLV_DECODE_ERRORS);
            crate::observability::counter_inc(obs::METRIC_FRAMES_MALFORMED);
            SessionError::Decode(MessageError::InvalidExtensions(error))
        })?;
        if header.message_type == crate::protocol::wire3::extensions::CONNECT_MESSAGE_TYPE {
            self.capability_ids.clear();
            if let Some(ids) = decoded.capability_ids {
                for chunk in ids.as_chunks::<4>().0 {
                    let id = u32::from_be_bytes(*chunk);
                    if crate::protocol::wire3::BROKER_CAPABILITY_IDS.contains(&id) {
                        self.capability_ids.push(id);
                    }
                }
            }
        }
        if decoded.fields.queue_ttl_hours.is_some()
            && !self
                .capability_ids
                .contains(&crate::protocol::wire3::QUEUE_MESSAGE_TTL_CAPABILITY_ID)
        {
            return Err(SessionError::Unsupported {
                message_type: header.message_type,
                correlation: header.correlation,
                reason: "queue message TTL capability was not negotiated",
            });
        }
        if header.message_type == crate::protocol::wire3::extensions::CONNECT_MESSAGE_TYPE
            && !header.flags.is_extensions_present()
            && wire_payload.get(..4).is_some_and(|version_range| {
                let min_version = u16::from_be_bytes([version_range[0], version_range[1]]);
                let max_version = u16::from_be_bytes([version_range[2], version_range[3]]);
                crate::protocol::wire3::handshake::negotiate_version(
                    min_version,
                    max_version,
                    crate::protocol::wire3::WIRE_VERSION,
                    crate::protocol::wire3::WIRE_VERSION,
                )
                .is_some()
            })
        {
            crate::observability::counter_inc(obs::METRIC_TLV_DECODE_ERRORS);
            crate::observability::counter_inc(obs::METRIC_FRAMES_MALFORMED);
            return Err(SessionError::Decode(MessageError::InvalidExtensions(
                crate::protocol::wire3::extensions::ExtensionError::CapabilityListMissing,
            )));
        }
        let entry = crate::protocol::manifest::client_entry(
            crate::protocol::message::MessageType::new(header.message_type),
        )
        .map_err(|_| SessionError::Unsupported {
            message_type: header.message_type,
            correlation: header.correlation,
            reason: "unsupported or server-to-client message type",
        })?;
        if entry
            .required_capability_id
            .is_some_and(|id| !self.capability_ids.contains(&id))
        {
            return Err(SessionError::Unsupported {
                message_type: header.message_type,
                correlation: header.correlation,
                reason: "required message capability was not negotiated",
            });
        }
        Ok(decoded)
    }
}
