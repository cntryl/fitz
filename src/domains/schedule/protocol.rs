use bytes::Bytes;
use crate::protocol::tlv::{TlvDecoder};

/// TLV payload for schedules. MUST be TLV encoded.
/// Fields required:
/// - cron: string
/// - resource: string
/// - operation: string
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulePayload {
    pub cron: String,
    pub resource: String,
    pub operation: String,
}

impl SchedulePayload {
    pub fn decode(input: &[u8]) -> Result<Self, String> {
        // Use TlvDecoder to decode records and extract string fields
        let decoder = TlvDecoder::new();
        let records = decoder
            .decode_all(input)
            .map_err(|e| format!("tlv decode error: {}", e))?;

        let mut cron: Option<String> = None;
        let mut resource: Option<String> = None;
        let mut operation: Option<String> = None;

        for rec in records {
            // Type numbers chosen arbitrarily and documented internally
            match rec.msg_type.0 {
                1 => cron = Some(String::from_utf8(rec.value.to_vec()).unwrap_or_default()),
                2 => resource = Some(String::from_utf8(rec.value.to_vec()).unwrap_or_default()),
                3 => operation = Some(String::from_utf8(rec.value.to_vec()).unwrap_or_default()),
                _ => (),
            }
        }

        let cron = cron.ok_or_else(|| "missing field: cron".to_string())?;
        let resource = resource.ok_or_else(|| "missing field: resource".to_string())?;
        let operation = operation.ok_or_else(|| "missing field: operation".to_string())?;

        Ok(Self {
            cron,
            resource,
            operation,
        })
    }

    pub fn encode(&self) -> Bytes {
        // Encode as TLV records with types 1,2,3 using TlvEncoder
        use crate::protocol::tlv::{TlvEncoder, MessageType};
        let mut enc = TlvEncoder::new();
        enc.encode(MessageType(1), self.cron.as_bytes());
        enc.encode(MessageType(2), self.resource.as_bytes());
        enc.encode(MessageType(3), self.operation.as_bytes());
        enc.finish()
    }
}

