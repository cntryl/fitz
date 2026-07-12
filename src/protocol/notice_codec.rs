//! Notice domain codec - pub/sub notifications
//!
//! Encodes/decodes TLV messages for the notice domain.
//! Supports Publish, Subscribe, Unsubscribe operations.
//!
//! `route_family`, `session_id`, and `subscriber` are server-internal
//! concepts supplied by the session/transport layer — they never appear
//! on the wire.

pub use crate::dispatch::wire::notice::NoticeResponse;
use crate::dispatch::wire::notice::{
    DeliverMessage, NotificationMessage, PublishMessage, SubscribeMessage, UnsubscribeAllMessage,
    UnsubscribeMessage,
};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::SessionId;
use bytes::{BufMut, Bytes, BytesMut};

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// Parse incoming message from TLV-encoded bytes.
///
/// `route_family`, `session_id`, and `subscriber` are injected by the
/// session layer — they are never read from the wire payload.
///
/// # Errors
///
/// Returns an error when the notice message type is unsupported or the
/// payload is malformed for the requested operation.
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<NotificationMessage, String> {
    let mut dec = PayloadDecoder::new(payload);

    match ctx.msg_type.0 {
        500 => parse_publish(ctx, &mut dec, route_family).map(NotificationMessage::Publish),
        501 => parse_subscribe(&mut dec, route_family, session_id, subscriber)
            .map(NotificationMessage::Subscribe),
        502 => parse_unsubscribe(&mut dec, route_family, session_id)
            .map(NotificationMessage::Unsubscribe),
        503 => parse_unsubscribe_all(&mut dec, session_id, subscriber)
            .map(NotificationMessage::UnsubscribeAll),
        504 => parse_deliver(&mut dec).map(NotificationMessage::Deliver),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Extract the publish route or subscription pattern needed for authorization.
///
/// # Errors
///
/// Returns an error when the payload is malformed, has trailing data, or the
/// message type is unsupported for notice authorization extraction.
pub fn extract_auth_route(msg_type: u16, payload: &[u8]) -> Result<Option<&str>, String> {
    let mut dec = PayloadDecoder::new(payload);

    match msg_type {
        500 => {
            let route = dec.get_string_ref()?;
            dec.skip_bytes()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        501 => {
            let pattern = dec.get_string_ref()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(pattern))
        }
        502 => {
            dec.get_u64()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(None)
        }
        503 => {
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(None)
        }
        504 => {
            dec.get_u64()?;
            let route = dec.get_string_ref()?;
            dec.skip_bytes()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        _ => Err(format!("Unknown operation: {msg_type}")),
    }
}

/// Encode domain response to TLV-encoded bytes
#[must_use]
pub fn encode_response(response: &NoticeResponse) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_response_into(response, &mut enc)
}

/// Encode domain response using a reusable payload encoder.
pub fn encode_response_into(response: &NoticeResponse, enc: &mut PayloadEncoder) -> Vec<u8> {
    enc.clear();

    match response {
        NoticeResponse::Ok => {
            enc.put_u8(0); // success flag
        }
        NoticeResponse::SubscribeOk { subscription_id } => {
            enc.put_u8(0); // success flag
            enc.put_optional_u64(Some(*subscription_id));
        }
        NoticeResponse::Error(e) => {
            return crate::protocol::error_codes::encode_error_body_into(
                notice_error_code_for_message(e),
                e,
                enc,
            );
        }
    }

    enc.finish()
}

fn notice_error_code_for_message(message: &str) -> u16 {
    use crate::protocol::error_codes::notice;

    match message {
        "empty pattern" => notice::ERR_INVALID_PATTERN,
        message if message.contains("subscription") && message.contains("limit") => {
            notice::ERR_SUBSCRIPTION_LIMIT
        }
        message if message.contains("route") => notice::ERR_INVALID_ROUTE,
        _ => notice::ERR_BACKEND_ERROR,
    }
}

// ===== Helper Parsers =====

/// Wire format: `[string route][bytes payload]`
fn parse_publish(
    ctx: &FrameContext,
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<PublishMessage, String> {
    let route = Route::from_ref(dec.get_string_ref()?);
    let payload = ctx.payload.slice(dec.get_bytes_range()?);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(PublishMessage {
        family_id: route_family,
        route,
        payload,
    })
}

/// Wire format: `[string pattern]`
fn parse_subscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<SubscribeMessage, String> {
    let pattern = Route::from_ref(dec.get_string_ref()?);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(SubscribeMessage {
        family_id: route_family,
        pattern,
        session_id,
        subscriber,
    })
}

/// Wire format: `[u64 subscription_id]`
fn parse_unsubscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
) -> Result<UnsubscribeMessage, String> {
    let subscription_id = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(UnsubscribeMessage {
        family_id: route_family,
        subscription_id,
        session_id,
    })
}

/// Wire format: `(empty)` — all fields are server-supplied
fn parse_unsubscribe_all(
    dec: &mut PayloadDecoder,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<UnsubscribeAllMessage, String> {
    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(UnsubscribeAllMessage {
        session_id,
        subscriber,
    })
}

/// Encode a NOTICE DELIVER (504) payload.
///
/// Wire format: `[u64 subscription_id][string route][bytes payload]`
///
/// The `subscription_id` allows client-side demultiplexing to the correct handler.
/// The route and payload carry the actual delivery content.
#[must_use]
pub fn encode_notify(subscription_id: u64, route: &Route, payload: &[u8]) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_notify_route_into(subscription_id, route.as_str(), payload, &mut enc)
}

/// Encode NOTICE DELIVER payload using a reusable payload encoder.
pub fn encode_notify_into(
    subscription_id: u64,
    route: &Route,
    payload: &[u8],
    enc: &mut PayloadEncoder,
) -> Vec<u8> {
    encode_notify_route_into(subscription_id, route.as_str(), payload, enc)
}

/// Encode NOTICE DELIVER payload from a raw route string using a reusable payload encoder.
pub fn encode_notify_route_into(
    subscription_id: u64,
    route: &str,
    payload: &[u8],
    enc: &mut PayloadEncoder,
) -> Vec<u8> {
    enc.clear();
    enc.put_u64(subscription_id);
    enc.put_string(route);
    enc.put_bytes(payload);
    enc.finish()
}

/// Encode the route/payload suffix shared by all NOTIFY deliveries for one publish.
///
/// Wire format: `[string route][bytes payload]`
#[must_use]
pub fn encode_notify_shared_suffix(route: &str, payload: &[u8]) -> Bytes {
    let mut suffix = BytesMut::with_capacity(4 + route.len() + 4 + payload.len());
    suffix.put_u32(usize_to_u32_saturating(route.len()));
    suffix.extend_from_slice(route.as_bytes());
    suffix.put_u32(usize_to_u32_saturating(payload.len()));
    suffix.extend_from_slice(payload);
    suffix.freeze()
}

/// Encode a NOTICE DELIVER payload from a pre-encoded shared route/payload suffix.
///
/// Wire format: `[u64 subscription_id][string route][bytes payload]`
pub fn encode_notify_with_shared_suffix(subscription_id: u64, suffix: &Bytes) -> Bytes {
    let mut payload = BytesMut::with_capacity(8 + suffix.len());
    payload.put_u64(subscription_id);
    payload.extend_from_slice(suffix.as_ref());
    payload.freeze()
}

fn parse_deliver(dec: &mut PayloadDecoder) -> Result<DeliverMessage, String> {
    let _subscription_id = dec.get_u64()?;
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let payload = dec.get_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(DeliverMessage { route, payload })
}
