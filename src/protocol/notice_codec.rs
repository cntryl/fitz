//! Notice domain codec - pub/sub notifications
//!
//! Encodes/decodes TLV messages for the notice domain.
//! Supports Publish, Subscribe, Unsubscribe operations.
//!
//! `route_family`, `session_id`, and `subscriber` are server-internal
//! concepts supplied by the session/transport layer — they never appear
//! on the wire.

use crate::domains::notice::protocol::{
    DeliverMessage, NotificationMessage, PublishMessage, SubscribeMessage, UnsubscribeAllMessage,
    UnsubscribeMessage,
};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::SessionId;

/// Response from notice operations
#[derive(Debug, Clone)]
pub enum NoticeResponse {
    /// Operation succeeded with optional subscription ID
    Ok { subscription_id: Option<u64> },
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
) -> Result<NotificationMessage, String> {
    let mut dec = PayloadDecoder::new(payload);

    match ctx.msg_type.0 {
        500 => parse_publish(&mut dec, route_family).map(NotificationMessage::Publish),
        501 => parse_subscribe(&mut dec, route_family, session_id, subscriber)
            .map(NotificationMessage::Subscribe),
        502 => parse_unsubscribe(&mut dec, route_family, session_id, subscriber)
            .map(NotificationMessage::Unsubscribe),
        503 => {
            parse_unsubscribe_all(session_id, subscriber).map(NotificationMessage::UnsubscribeAll)
        }
        504 => parse_deliver(&mut dec).map(NotificationMessage::Deliver),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &NoticeResponse) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_response_into(response, &mut enc)
}

/// Encode domain response using a reusable payload encoder.
pub fn encode_response_into(response: &NoticeResponse, enc: &mut PayloadEncoder) -> Vec<u8> {
    enc.clear();

    match response {
        NoticeResponse::Ok { subscription_id } => {
            enc.put_u8(0); // success flag
            enc.put_optional_u64(*subscription_id);
        }
        NoticeResponse::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }

    enc.finish()
}

// ===== Helper Parsers =====

/// Wire format: `[string route][bytes payload]`
fn parse_publish(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<PublishMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let payload = dec.get_bytes()?;

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
    let pattern_str = dec.get_string()?;
    let pattern = Route::new(pattern_str);

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

/// Wire format: `[string pattern]`
fn parse_unsubscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<UnsubscribeMessage, String> {
    let pattern_str = dec.get_string()?;
    let pattern = Route::new(pattern_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(UnsubscribeMessage {
        family_id: route_family,
        pattern,
        session_id,
        subscriber,
    })
}

/// Wire format: `(empty)` — all fields are server-supplied
fn parse_unsubscribe_all(
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<UnsubscribeAllMessage, String> {
    Ok(UnsubscribeAllMessage {
        session_id,
        subscriber,
    })
}

/// Encode a NOTICE DELIVER (504) payload.
///
/// Wire format: `[u64 subscription_id][string route][bytes payload]`
///
/// The subscription_id allows client-side demultiplexing to the correct handler.
/// The route and payload carry the actual delivery content.
pub fn encode_notify(subscription_id: u64, route: &Route, payload: &[u8]) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_notify_into(subscription_id, route, payload, &mut enc)
}

/// Encode NOTICE DELIVER payload using a reusable payload encoder.
pub fn encode_notify_into(
    subscription_id: u64,
    route: &Route,
    payload: &[u8],
    enc: &mut PayloadEncoder,
) -> Vec<u8> {
    enc.clear();
    enc.put_u64(subscription_id);
    enc.put_string(route.as_str());
    enc.put_bytes(payload);
    enc.finish()
}

fn parse_deliver(dec: &mut PayloadDecoder) -> Result<DeliverMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let payload = dec.get_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(DeliverMessage {
        route: std::sync::Arc::new(route),
        payload: std::sync::Arc::new(payload),
    })
}
