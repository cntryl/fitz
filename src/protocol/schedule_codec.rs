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

/// Extract the schedule route or pattern needed for authorization.
pub fn extract_auth_route(msg_type: u16, payload: &[u8]) -> Result<Option<&str>, String> {
    let mut dec = PayloadDecoder::new(payload);

    match msg_type {
        700 => {
            let route = dec.get_string_ref()?;
            dec.get_string_ref()?;
            dec.skip_bytes()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        701 | 703 | 704 => {
            let route = dec.get_string_ref()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        702 => {
            dec.get_optional_u64()?;
            dec.get_optional_u64()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(None)
        }
        _ => Err(format!("Unknown operation: {}", msg_type)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &ScheduleResponse) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_response_into(&mut enc, response)
}

pub fn encode_response_into(enc: &mut PayloadEncoder, response: &ScheduleResponse) -> Vec<u8> {
    enc.clear();

    match response {
        ScheduleResponse::Ok => {
            enc.put_u8(0); // success flag
        }
        ScheduleResponse::SubscribeOk { subscription_id } => {
            enc.put_u8(0); // success flag
            enc.put_optional_u64(Some(*subscription_id));
        }
        ScheduleResponse::ListDefs {
            entries,
            total_count,
        } => {
            enc.put_u8(0); // success flag
            enc.put_u64(*total_count); // total count of all schedules
            for entry in entries.iter() {
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

/// Parse LIST message
/// Wire format (optional): [u64 offset][u64 limit]
/// If no parameters provided, defaults to offset=0, limit=100
fn parse_list(dec: &mut PayloadDecoder) -> Result<ScheduleMessage, String> {
    let offset = dec.get_optional_u64()?.unwrap_or(0);
    let limit = dec.get_optional_u64()?.unwrap_or(100);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::List { offset, limit })
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
/// Wire format: [u64 subscription_id][bytes payload]
/// Payload is what was stored with the schedule (fanout data)
pub fn encode_notify(subscription_id: u64, payload: &[u8]) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_notify_into(&mut enc, subscription_id, payload)
}

pub fn encode_notify_into(
    enc: &mut PayloadEncoder,
    subscription_id: u64,
    payload: &[u8],
) -> Vec<u8> {
    enc.clear();
    enc.put_u64(subscription_id);
    enc.put_bytes(payload);
    enc.finish()
}

#[cfg(test)]
mod tests {
    use super::{encode_notify, encode_response};
    use crate::domains::schedule::ScheduleResponse;

    #[test]
    fn should_encode_subscribe_response_with_subscription_id() {
        // Arrange
        let payload = encode_response(&ScheduleResponse::SubscribeOk {
            subscription_id: 42,
        });

        // Act

        // Assert
        assert_eq!(payload[0], 0);
        assert_eq!(payload[1], 1);
        assert_eq!(&payload[2..10], &42u64.to_be_bytes());
    }

    #[test]
    fn should_encode_schedule_notify_with_subscription_id() {
        // Arrange
        let payload = encode_notify(7, b"fire");

        // Act

        // Assert
        assert_eq!(&payload[0..8], &7u64.to_be_bytes());
        assert_eq!(&payload[8..12], &(4u32).to_be_bytes());
        assert_eq!(&payload[12..], b"fire");
    }
}
