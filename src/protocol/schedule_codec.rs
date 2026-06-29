//! Schedule domain codec for durable timing intent.
//!
//! Encodes and decodes TLV messages for schedule definition management and
//! ephemeral live notifications.

use crate::domains::schedule::{ScheduleCreateEntry, ScheduleMessage, ScheduleResponse};
use crate::protocol::error_codes::schedule as schedule_error_codes;
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
        706 => parse_create_batch(&mut dec),
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
        706 => Err("schedule batch create requires multi-route authorization".to_string()),
        702 => {
            if dec.remaining() == 0 {
                return Ok(None);
            }

            dec.get_optional_u64()?;
            dec.get_optional_u64()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(None)
        }
        _ => Err(format!("Unknown operation: {msg_type}")),
    }
}

pub fn extract_batch_auth_routes(payload: &[u8]) -> Result<Vec<&str>, String> {
    let mut dec = PayloadDecoder::new(payload);
    let entry_count = dec.get_u32()? as usize;
    let mut routes = Vec::with_capacity(entry_count.min(256));

    for _ in 0..entry_count {
        let route = dec.get_string_ref()?;
        dec.get_string_ref()?;
        dec.skip_bytes()?;
        routes.push(route);
    }

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(routes)
}

/// Encode domain response to TLV-encoded bytes
#[must_use]
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
            return crate::protocol::error_codes::encode_error_body_into(
                schedule_error_code_for_message(e),
                e,
                enc,
            );
        }
    }

    enc.finish()
}

fn schedule_error_code_for_message(message: &str) -> u16 {
    match message {
        "schedule not found" => schedule_error_codes::ERR_SCHEDULE_NOT_FOUND,
        "Cron expression must have exactly 5 fields" => schedule_error_codes::ERR_INVALID_CRON,
        "schedule route must be schedule://{realm}/{area}/{resource}/{operation}" => {
            schedule_error_codes::ERR_INVALID_TARGET
        }
        "schedule route scheme must be schedule" => schedule_error_codes::ERR_INVALID_TARGET,
        "schedule route must not contain wildcards" => schedule_error_codes::ERR_INVALID_TARGET,
        "schedule subscription state is owned by the schedule domain sink" => {
            schedule_error_codes::ERR_INVALID_TARGET
        }
        _ => schedule_error_codes::ERR_PARSE_ERROR,
    }
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

fn parse_create_batch(dec: &mut PayloadDecoder) -> Result<ScheduleMessage, String> {
    let entry_count = dec.get_u32()? as usize;
    let mut entries = Vec::with_capacity(entry_count.min(256));

    for _ in 0..entry_count {
        entries.push(ScheduleCreateEntry {
            route: dec.get_string()?,
            cron: dec.get_string()?,
            payload: dec.get_bytes()?,
        });
    }

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::CreateBatch { entries })
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
    if dec.remaining() == 0 {
        return Ok(ScheduleMessage::List {
            offset: 0,
            limit: 100,
        });
    }

    let offset = dec.get_optional_u64()?.unwrap_or(0);
    let limit = dec.get_optional_u64()?.unwrap_or(100);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::List { offset, limit })
}

/// Parse SUBSCRIBE message.
/// Wire format: [string exact_route]
fn parse_subscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<ScheduleMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::Subscribe {
        family_id: route_family,
        route,
        session_id: session_id.0,
        subscriber,
    })
}

/// Parse UNSUBSCRIBE message.
/// Wire format: [string exact_route]
fn parse_unsubscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<ScheduleMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ScheduleMessage::Unsubscribe {
        family_id: route_family,
        route,
        session_id: session_id.0,
        subscriber,
    })
}

/// Encode an ephemeral SCHEDULE_NOTIFY (705) payload.
///
/// Wire format: [u64 subscription_id][bytes payload]
/// Payload is the stored schedule payload handed to the live notify path. The
/// notification itself is not durably replayed as a delivery artifact.
#[must_use]
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
    use super::{encode_notify, encode_response, extract_batch_auth_routes};
    use crate::domains::schedule::ScheduleResponse;
    use crate::protocol::error_codes::schedule as schedule_error_codes;
    use crate::protocol::payload_codec::PayloadEncoder;

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

    #[test]
    fn should_encode_typed_schedule_error_for_known_failure() {
        // Arrange
        let response = ScheduleResponse::Error("schedule not found".to_string());

        // Act
        let payload = encode_response(&response);

        // Assert
        assert_eq!(payload[0], 1);
        assert_eq!(
            &payload[1..5],
            &(schedule_error_codes::ERR_SCHEDULE_NOT_FOUND as u32).to_be_bytes()
        );
    }

    #[test]
    fn should_extract_schedule_batch_auth_routes() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_u32(2);
        enc.put_string("schedule://acme/jobs/backup/run");
        enc.put_string("0 2 * * *");
        enc.put_bytes(b"backup");
        enc.put_string("schedule://acme/jobs/report/run");
        enc.put_string("15 6 * * *");
        enc.put_bytes(b"report");
        let payload = enc.finish();

        // Act
        let routes = extract_batch_auth_routes(&payload).expect("batch auth routes");

        // Assert
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0], "schedule://acme/jobs/backup/run");
        assert_eq!(routes[1], "schedule://acme/jobs/report/run");
    }
}
