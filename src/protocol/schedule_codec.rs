//! Schedule domain codec - delayed/recurring tasks
//!
//! Encodes/decodes TLV messages for the schedule domain.
//! Supports Create, Cancel, List operations with route-based identity.

use bytes::Bytes;
use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::SessionId;

/// Schedule operation messages
#[derive(Debug, Clone)]
pub enum ScheduleMessage {
    /// Create or update a schedule (route is identity, upsert)
    Create {
        route: String,
        cron: String,
        payload: Bytes,
    },
    /// Cancel an existing schedule by route
    Cancel { route: String },
    /// List all schedules
    List,
    /// Subscribe to schedule fire notifications by pattern (client -> server)
    Subscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Unsubscribe from schedule fire notifications by pattern (client -> server)
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
    /// Operation succeeded (no schedule_id returned - route is identity)
    Ok,
    /// LIST operation: returns all schedules as (route, cron, payload) tuples
    ListDefs(Vec<ScheduleListEntry>),
    /// Operation failed with error message
    Error(String),
}

/// Single schedule entry in LIST response
#[derive(Debug, Clone)]
pub struct ScheduleListEntry {
    pub route: String,
    pub cron: String,
    pub payload: Bytes,
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
        ScheduleResponse::Ok => {
            enc.put_u8(0); // success flag
        }
        ScheduleResponse::ListDefs(entries) => {
            enc.put_u8(0); // success flag
            for entry in entries {
                enc.put_u8(1); // has_entry
                enc.put_string(&entry.route);
                enc.put_string(&entry.cron);
                enc.put_bytes(&entry.payload);
            }
            enc.put_u8(0); // end sentinel
        }
        ScheduleResponse::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }

    enc.finish()
}

// ===== Helper Parsers =====

/// Parse CREATE message
/// Wire format: [string route][string cron][bytes payload]
fn parse_create(dec: &mut TlvDecoder) -> Result<ScheduleMessage, String> {
    let route = dec.get_string()?;
    let cron = dec.get_string()?;
    let payload = Bytes::from(dec.get_bytes()?);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::Create {
        route,
        cron,
        payload,
    })
}

/// Parse CANCEL message
/// Wire format: [string route]
fn parse_cancel(dec: &mut TlvDecoder) -> Result<ScheduleMessage, String> {
    let route = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::Cancel { route })
}

/// Parse LIST message (no parameters)
fn parse_list(_dec: &mut TlvDecoder) -> Result<ScheduleMessage, String> {
    Ok(ScheduleMessage::List)
}

/// Parse SUBSCRIBE message
/// Wire format: [string pattern]
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

/// Parse UNSUBSCRIBE message
/// Wire format: [string pattern]
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
/// Wire format: [bytes payload]
/// Payload is what was stored with the schedule (fanout data)
pub fn encode_notify(payload: &[u8]) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    enc.put_bytes(payload);
    enc.finish()
}
