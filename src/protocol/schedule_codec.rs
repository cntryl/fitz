//! Schedule domain codec - delayed/recurring tasks
//!
//! Encodes/decodes TLV messages for the schedule domain.
//! Supports Create, Cancel, List operations.

use crate::domains::schedule::protocol::SchedulePayload;
use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};

/// Schedule operation messages
#[derive(Debug, Clone)]
pub enum ScheduleMessage {
    /// Create a new schedule
    Create { payload: SchedulePayload },
    /// Cancel an existing schedule
    Cancel { schedule_id: String },
    /// List all schedules
    List,
}

/// Response from schedule operations
#[derive(Debug, Clone)]
pub enum ScheduleResponse {
    /// Operation succeeded with optional schedule ID
    Ok { schedule_id: Option<String> },
    /// Operation failed with error message
    Error(String),
}

/// Parse incoming message from TLV-encoded bytes
pub fn parse_request(ctx: &FrameContext, payload: &[u8]) -> Result<ScheduleMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type.0 {
        700 => parse_create(&mut dec),
        701 => parse_cancel(&mut dec),
        702 => parse_list(&mut dec),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &ScheduleResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();

    match response {
        ScheduleResponse::Ok { schedule_id } => {
            enc.put_u8(0); // success flag
            enc.put_optional_string(schedule_id.as_deref());
        }
        ScheduleResponse::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }

    enc.finish()
}

// ===== Helper Parsers =====

fn parse_create(dec: &mut TlvDecoder) -> Result<ScheduleMessage, String> {
    let payload_bytes = dec.get_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    let payload = SchedulePayload::decode(&payload_bytes)
        .map_err(|e| format!("Failed to decode schedule payload: {}", e))?;

    Ok(ScheduleMessage::Create { payload })
}

fn parse_cancel(dec: &mut TlvDecoder) -> Result<ScheduleMessage, String> {
    let schedule_id = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::Cancel { schedule_id })
}

fn parse_list(_dec: &mut TlvDecoder) -> Result<ScheduleMessage, String> {
    // List operation takes no parameters
    Ok(ScheduleMessage::List)
}
