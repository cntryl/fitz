//! Schedule domain codec - delayed/recurring tasks
//!
//! Encodes/decodes TLV messages for the schedule domain.
//! Supports Create, Cancel, List operations.

use crate::domains::schedule::protocol::SchedulePayload;
use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::SessionId;

/// Schedule operation messages
#[derive(Debug, Clone)]
pub enum ScheduleMessage {
    /// Create a new schedule
    Create { payload: SchedulePayload },
    /// Cancel an existing schedule
    Cancel { schedule_id: String },
    /// List all schedules
    List,
    /// Subscribe to schedule fire notifications (client -> server)
    Subscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Unsubscribe from schedule fire notifications (client -> server)
    Unsubscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Unsubscribe all schedule subscriptions for a session (called on disconnect)
    UnsubscribeAll {
        session_id: u64,
        subscriber: RouteAddress,
    },
}

/// Response from schedule operations
#[derive(Debug, Clone)]
pub enum ScheduleResponse {
    /// Operation succeeded with optional schedule ID
    Ok { schedule_id: Option<String> },
    /// Operation failed with error message
    Error(String),
}

/// Parse incoming message from TLV-encoded bytes.
///
/// `route_family`, `session_id`, and `subscriber` are injected by the
/// session layer — they are never read from the wire payload.
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<ScheduleMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type.0 {
        700 => parse_create(&mut dec),
        701 => parse_cancel(&mut dec),
        702 => parse_list(&mut dec),
        703 => parse_subscribe(&mut dec, route_family, session_id, subscriber),
        704 => parse_unsubscribe(&mut dec, route_family, session_id, subscriber),
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

/// Wire format: `[string pattern]`
fn parse_subscribe(
    dec: &mut TlvDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<ScheduleMessage, String> {
    let pattern_str = dec.get_string()?;
    let pattern = Route::new(pattern_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::Subscribe {
        family_id: route_family,
        pattern,
        session_id: session_id.0,
        subscriber,
    })
}

/// Wire format: `[string pattern]`
fn parse_unsubscribe(
    dec: &mut TlvDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<ScheduleMessage, String> {
    let pattern_str = dec.get_string()?;
    let pattern = Route::new(pattern_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::Unsubscribe {
        family_id: route_family,
        pattern,
        session_id: session_id.0,
        subscriber,
    })
}

/// Encode a SCHEDULE_NOTIFY (705) payload.
///
/// Wire format: `[u64 subscription_id][string route][bytes payload]`
pub fn encode_notify(subscription_id: u64, route: &Route, payload: &[u8]) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.put_u64(subscription_id);
    enc.put_string(route.as_str());
    enc.put_bytes(payload);
    enc.finish()
}
