//! Schedule domain codec - delayed/recurring tasks
//!
//! Encodes/decodes TLV messages for the schedule domain.
//! Supports Create, Cancel, List operations with route-based identity.

use crate::domains::schedule::{ScheduleMessage, ScheduleResponse};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::SessionId;

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
    let mut dec = PayloadDecoder::new(payload);

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
    let mut enc = PayloadEncoder::new();

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
fn parse_create(dec: &mut PayloadDecoder) -> Result<ScheduleMessage, String> {
    let route = dec.get_string()?;
    let cron = dec.get_string()?;
    let payload = dec.get_bytes()?;

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
fn parse_cancel(dec: &mut PayloadDecoder) -> Result<ScheduleMessage, String> {
    let route = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::Cancel { route })
}

/// Parse LIST message (no parameters)
fn parse_list(_dec: &mut PayloadDecoder) -> Result<ScheduleMessage, String> {
    Ok(ScheduleMessage::List)
}

/// Parse SUBSCRIBE message
/// Wire format: [string pattern]
fn parse_subscribe(
    dec: &mut PayloadDecoder,
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
    dec: &mut PayloadDecoder,
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
    let mut enc = PayloadEncoder::new();
    enc.put_bytes(payload);
    enc.finish()
}
