//! Schedule domain codec for durable timing intent.
//!
//! Encodes and decodes TLV messages for schedule definition management and
//! ephemeral live notifications.

use crate::dispatch::wire::schedule::{
    ScheduleCreateEntry, ScheduleDeliveryMode, ScheduleFailure, ScheduleFailureCategory,
    ScheduleMessage, ScheduleResponse,
};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::SessionId;

const MAX_SCHEDULE_LIST_LIMIT: u64 = 1_000;

/// Parse incoming message from TLV-encoded bytes.
///
/// `route_family`, `session_id`, and `subscriber` are injected by the
/// session layer — they are never read from the wire payload.
///
/// # Errors
///
/// Returns an error when the schedule message type is unsupported or the
/// payload cannot be decoded as the requested schedule operation.
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<ScheduleMessage, ScheduleFailure> {
    let mut dec = PayloadDecoder::new(payload);

    match ctx.msg_type.0 {
        700 => parse_create(&mut dec),
        701 => parse_cancel(&mut dec),
        702 => parse_list(&mut dec),
        707 => parse_list_v2(&mut dec),
        703 => parse_subscribe(&mut dec, route_family, session_id, subscriber),
        704 => parse_unsubscribe(&mut dec, route_family, session_id, subscriber),
        706 => parse_create_batch(&mut dec),
        _ => Err(ScheduleFailure::parse(format!(
            "Unknown operation: {}",
            ctx.msg_type.0
        ))),
    }
}

/// Extract the schedule route or pattern needed for authorization.
///
/// # Errors
///
/// Returns an error when the payload is malformed, has trailing data, or the
/// message type is unsupported for schedule authorization extraction.
pub fn extract_auth_route(msg_type: u16, payload: &[u8]) -> Result<Option<&str>, String> {
    let mut dec = PayloadDecoder::new(payload);

    match msg_type {
        700 => {
            let route = dec.get_string_ref()?;
            dec.get_string_ref()?;
            dec.get_u8()?;
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
            if let Some(limit) = dec.get_optional_u64()? {
                if limit > MAX_SCHEDULE_LIST_LIMIT {
                    return Err(format!(
                        "schedule LIST limit must be at most {MAX_SCHEDULE_LIST_LIMIT}"
                    ));
                }
            }
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(None)
        }
        707 => {
            if dec.remaining() > 0 {
                dec.get_optional_string()?;
                if let Some(limit) = dec.get_optional_u64()? {
                    if limit > MAX_SCHEDULE_LIST_LIMIT {
                        return Err(format!(
                            "schedule LIST limit must be at most {MAX_SCHEDULE_LIST_LIMIT}"
                        ));
                    }
                }
                if !dec.is_complete() {
                    return Err("Trailing data in message".to_string());
                }
            }
            Ok(None)
        }
        _ => Err(format!("Unknown operation: {msg_type}")),
    }
}

/// # Errors
///
/// Returns an error when the batch payload is malformed or contains trailing data.
pub fn extract_batch_auth_routes(payload: &[u8]) -> Result<Vec<&str>, String> {
    let mut dec = PayloadDecoder::new(payload);
    let entry_count = dec.get_u32()? as usize;
    let mut routes = Vec::with_capacity(entry_count.min(256));

    for _ in 0..entry_count {
        let route = dec.get_string_ref()?;
        dec.get_string_ref()?;
        dec.get_u8()?;
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
pub fn encode_response(message_type: u16, response: &ScheduleResponse) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_response_into(&mut enc, message_type, response)
}

pub fn encode_response_into(
    enc: &mut PayloadEncoder,
    message_type: u16,
    response: &ScheduleResponse,
) -> Vec<u8> {
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
                enc.put_u8(entry.delivery_mode as u8);
                enc.put_bytes(&entry.payload);
            }
            enc.put_u8(0); // end sentinel
        }
        ScheduleResponse::ListPage {
            entries,
            has_more,
            continuation,
        } => {
            enc.put_u8(0);
            enc.put_u8(1); // response version
            enc.put_u8(u8::from(*has_more));
            enc.put_optional_string(continuation.as_deref());
            for entry in entries.iter() {
                enc.put_u8(1);
                enc.put_string(&entry.route);
                enc.put_string(&entry.cron);
                enc.put_u8(entry.delivery_mode as u8);
                enc.put_bytes(&entry.payload);
            }
            enc.put_u8(0);
        }
        ScheduleResponse::Error(e) => {
            if message_type == 702 {
                return crate::protocol::error_codes::encode_error_body_into(
                    e.category.code(),
                    &e.message,
                    enc,
                );
            }
            enc.put_u8(1);
            enc.put_string(&e.message);
        }
    }

    enc.finish()
}

// ===== Helper Parsers =====

/// Parse CREATE message
/// Wire format: [string route][string cron][u8 mode][bytes payload]
fn parse_create(dec: &mut PayloadDecoder) -> Result<ScheduleMessage, ScheduleFailure> {
    let route = dec.get_string()?;
    let cron = dec.get_string()?;
    let delivery_mode = ScheduleDeliveryMode::try_from(dec.get_u8()?).map_err(|message| {
        ScheduleFailure::new(ScheduleFailureCategory::InvalidDeliveryMode, message)
    })?;
    let payload = dec.get_bytes()?;

    if !dec.is_complete() {
        return Err(ScheduleFailure::parse("Trailing data in message"));
    }

    Ok(ScheduleMessage::Create {
        route,
        cron,
        delivery_mode,
        payload,
    })
}

fn parse_create_batch(dec: &mut PayloadDecoder) -> Result<ScheduleMessage, ScheduleFailure> {
    let entry_count = dec.get_u32()? as usize;
    let mut entries = Vec::with_capacity(entry_count.min(256));

    for _ in 0..entry_count {
        entries.push(ScheduleCreateEntry {
            route: dec.get_string()?,
            cron: dec.get_string()?,
            delivery_mode: ScheduleDeliveryMode::try_from(dec.get_u8()?).map_err(|message| {
                ScheduleFailure::new(ScheduleFailureCategory::InvalidDeliveryMode, message)
            })?,
            payload: dec.get_bytes()?,
        });
    }

    if !dec.is_complete() {
        return Err(ScheduleFailure::parse("Trailing data in message"));
    }

    Ok(ScheduleMessage::CreateBatch { entries })
}

/// Parse CANCEL message
/// Wire format: [string route]
fn parse_cancel(dec: &mut PayloadDecoder) -> Result<ScheduleMessage, ScheduleFailure> {
    let route = dec.get_string()?;

    if !dec.is_complete() {
        return Err(ScheduleFailure::parse("Trailing data in message"));
    }

    Ok(ScheduleMessage::Cancel { route })
}

/// Parse LIST message
/// Wire format (optional): [u64 offset][u64 limit]
/// If no parameters provided, defaults to offset=0, limit=100
fn parse_list(dec: &mut PayloadDecoder) -> Result<ScheduleMessage, ScheduleFailure> {
    if dec.remaining() == 0 {
        return Ok(ScheduleMessage::List {
            offset: 0,
            limit: 100,
        });
    }

    let offset = dec.get_optional_u64()?.unwrap_or(0);
    let limit = dec.get_optional_u64()?.unwrap_or(100);
    if limit > MAX_SCHEDULE_LIST_LIMIT {
        return Err(ScheduleFailure::new(
            ScheduleFailureCategory::Limit,
            format!("schedule LIST limit must be at most {MAX_SCHEDULE_LIST_LIMIT}"),
        ));
    }

    if !dec.is_complete() {
        return Err(ScheduleFailure::parse("Trailing data in message"));
    }

    Ok(ScheduleMessage::List { offset, limit })
}

/// Parse versioned live ordered LIST: [optional string cursor][u64 limit].
fn parse_list_v2(dec: &mut PayloadDecoder) -> Result<ScheduleMessage, ScheduleFailure> {
    let cursor = dec.get_optional_string()?;
    let limit = dec.get_optional_u64()?.unwrap_or(100);
    if limit > MAX_SCHEDULE_LIST_LIMIT {
        return Err(ScheduleFailure::new(
            ScheduleFailureCategory::Limit,
            format!("schedule LIST limit must be at most {MAX_SCHEDULE_LIST_LIMIT}"),
        ));
    }
    if !dec.is_complete() {
        return Err(ScheduleFailure::parse("Trailing data in message"));
    }
    Ok(ScheduleMessage::ListV2 { cursor, limit })
}

/// Parse SUBSCRIBE message.
/// Wire format: [string `route_pattern`]
fn parse_subscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<ScheduleMessage, ScheduleFailure> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    if !dec.is_complete() {
        return Err(ScheduleFailure::parse("Trailing data in message"));
    }

    Ok(ScheduleMessage::Subscribe {
        family_id: route_family,
        route,
        session_id: session_id.0,
        subscriber,
    })
}

/// Parse UNSUBSCRIBE message.
/// Wire format: [string `route_pattern`]
fn parse_unsubscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<ScheduleMessage, ScheduleFailure> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    if !dec.is_complete() {
        return Err(ScheduleFailure::parse("Trailing data in message"));
    }

    Ok(ScheduleMessage::Unsubscribe {
        family_id: route_family,
        route,
        session_id: session_id.0,
        subscriber,
    })
}

/// Encode an ephemeral `SCHEDULE_NOTIFY` (705) payload.
///
/// Wire format: [u64 `subscription_id`][string exact_route][bytes payload]
/// Payload is the stored schedule payload handed to the live notify path. The
/// notification itself is not durably replayed as a delivery artifact.
#[must_use]
pub fn encode_notify(subscription_id: u64, route: &str, payload: &[u8]) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_notify_into(&mut enc, subscription_id, route, payload)
}

pub fn encode_notify_into(
    enc: &mut PayloadEncoder,
    subscription_id: u64,
    route: &str,
    payload: &[u8],
) -> Vec<u8> {
    enc.clear();
    enc.put_u64(subscription_id);
    enc.put_string(route);
    enc.put_bytes(payload);
    enc.finish()
}

#[cfg(test)]
mod tests {
    use super::{
        encode_notify, encode_response, extract_batch_auth_routes, parse_create, parse_request,
        ScheduleDeliveryMode, ScheduleFailure, ScheduleFailureCategory,
    };
    use crate::dispatch::wire::schedule::ScheduleResponse;
    use crate::protocol::error_codes::schedule as schedule_error_codes;
    use crate::protocol::frame::ChannelId;
    use crate::protocol::frame_context::FrameContext;
    use crate::protocol::payload_codec::PayloadEncoder;
    use crate::protocol::tlv::MessageType;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use crate::session::SessionId;
    use bytes::Bytes;

    #[test]
    fn should_encode_subscribe_response_with_subscription_id() {
        // Arrange
        let payload = encode_response(
            703,
            &ScheduleResponse::SubscribeOk {
                subscription_id: 42,
            },
        );

        // Act

        // Assert
        assert_eq!(payload[0], 0);
        assert_eq!(payload[1], 1);
        assert_eq!(&payload[2..10], &42u64.to_be_bytes());
    }

    #[test]
    fn should_encode_schedule_notify_with_subscription_id() {
        // Arrange
        let payload = encode_notify(7, "schedule://acme/jobs/report/run", b"fire");

        // Act

        // Assert
        assert_eq!(&payload[0..8], &7u64.to_be_bytes());
        let route = b"schedule://acme/jobs/report/run";
        let route_len = u32::try_from(route.len()).expect("route length fits u32");
        assert_eq!(&payload[8..12], &route_len.to_be_bytes());
        assert_eq!(&payload[12..12 + route.len()], route);
        assert_eq!(
            &payload[12 + route.len()..16 + route.len()],
            &(4u32).to_be_bytes()
        );
        assert_eq!(&payload[16 + route.len()..], b"fire");
    }

    #[test]
    fn should_encode_typed_schedule_list_error_for_known_failure() {
        // Arrange
        let response = ScheduleResponse::Error(ScheduleFailure::new(
            ScheduleFailureCategory::NotFound,
            "schedule not found",
        ));

        // Act
        let payload = encode_response(702, &response);

        // Assert
        assert_eq!(payload[0], 1);
        assert_eq!(
            &payload[1..5],
            &u32::from(schedule_error_codes::ERR_SCHEDULE_NOT_FOUND).to_be_bytes()
        );
    }

    #[test]
    fn should_encode_plain_schedule_error_for_non_list_operation() {
        // Arrange
        let response = ScheduleResponse::Error(ScheduleFailure::new(
            ScheduleFailureCategory::NotFound,
            "schedule not found",
        ));

        // Act
        let payload = encode_response(701, &response);

        // Assert
        assert_eq!(&payload[..5], &[1, 0, 0, 0, 18]);
        assert_eq!(&payload[5..], b"schedule not found");
    }

    #[test]
    fn should_extract_schedule_batch_auth_routes() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_u32(2);
        enc.put_string("schedule://acme/jobs/backup/run");
        enc.put_string("0 2 * * *");
        enc.put_u8(ScheduleDeliveryMode::Broadcast as u8);
        enc.put_bytes(b"backup");
        enc.put_string("schedule://acme/jobs/report/run");
        enc.put_string("15 6 * * *");
        enc.put_u8(ScheduleDeliveryMode::Single as u8);
        enc.put_bytes(b"report");
        let payload = enc.finish();

        // Act
        let routes = extract_batch_auth_routes(&payload).expect("batch auth routes");

        // Assert
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0], "schedule://acme/jobs/backup/run");
        assert_eq!(routes[1], "schedule://acme/jobs/report/run");
    }

    #[test]
    fn should_reject_unknown_schedule_delivery_mode() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_string("schedule://acme/jobs/backup/run");
        enc.put_string("0 2 * * *");
        enc.put_u8(2);
        enc.put_bytes(b"backup");
        let payload = enc.finish();
        let mut decoder = crate::protocol::payload_codec::PayloadDecoder::new(&payload);

        // Act
        let result = parse_create(&mut decoder);

        // Assert
        let failure = result.expect_err("invalid mode");
        assert_eq!(failure.message, "invalid schedule delivery mode");
        assert_eq!(
            failure.category.code(),
            schedule_error_codes::ERR_INVALID_DELIVERY_MODE
        );
    }

    #[test]
    fn should_reject_legacy_create_without_delivery_mode() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_string("schedule://acme/jobs/backup/run");
        enc.put_string("0 2 * * *");
        enc.put_bytes(b"backup");
        let payload = enc.finish();
        let mut decoder = crate::protocol::payload_codec::PayloadDecoder::new(&payload);

        // Act
        let result = parse_create(&mut decoder);

        // Assert
        assert!(result.is_err());
    }

    proptest::proptest! {
        #[test]
        fn should_never_panic_given_arbitrary_schedule_payload(
            message_type in 700_u16..=707,
            payload in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512),
        ) {
            // Arrange
            let family = RouteFamily::new(1);
            let context = FrameContext::new(
                1,
                ChannelId::Pub,
                MessageType::new(message_type),
                Bytes::copy_from_slice(&payload),
                family,
            );
            let subscriber = RouteAddress::new(family, Route::new("inbox://session/1"));

            // Act
            let _ = parse_request(&context, &payload, family, SessionId(1), subscriber);

            // Assert
        }
    }
}
