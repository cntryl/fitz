//! Notice domain codec - pub/sub notifications
//!
//! Encodes/decodes TLV messages for the notice domain.
//! Supports Publish, Subscribe, Unsubscribe operations.

use crate::domains::notice::protocol::{
    NotificationMessage, NotifyMessage, PublishMessage, SubscribeMessage, UnsubscribeAllMessage,
    UnsubscribeMessage,
};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};
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

/// Parse incoming message from TLV-encoded bytes
pub fn parse_request(ctx: &FrameContext, payload: &[u8]) -> Result<NotificationMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type.0 {
        100 => parse_publish(&mut dec).map(NotificationMessage::Publish),
        101 => parse_subscribe(&mut dec).map(NotificationMessage::Subscribe),
        102 => parse_unsubscribe(&mut dec).map(NotificationMessage::Unsubscribe),
        103 => parse_unsubscribe_all(&mut dec).map(NotificationMessage::UnsubscribeAll),
        104 => parse_notify(&mut dec).map(NotificationMessage::Notify),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &NoticeResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();

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

fn parse_publish(dec: &mut TlvDecoder) -> Result<PublishMessage, String> {
    let family_id = dec.get_u64()?;
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let payload = dec.get_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(PublishMessage {
        family_id: RouteFamily::new(family_id),
        route,
        payload,
    })
}

fn parse_subscribe(dec: &mut TlvDecoder) -> Result<SubscribeMessage, String> {
    let family_id = dec.get_u64()?;
    let pattern_str = dec.get_string()?;
    let pattern = Route::new(pattern_str);
    let session_id_u64 = dec.get_u64()?;
    let subscriber_str = dec.get_string()?;
    let subscriber = RouteAddress::new(RouteFamily::new(family_id), Route::new(subscriber_str));

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(SubscribeMessage {
        family_id: RouteFamily::new(family_id),
        pattern,
        session_id: SessionId(session_id_u64),
        subscriber,
    })
}

fn parse_unsubscribe(dec: &mut TlvDecoder) -> Result<UnsubscribeMessage, String> {
    let family_id = dec.get_u64()?;
    let pattern_str = dec.get_string()?;
    let pattern = Route::new(pattern_str);
    let session_id_u64 = dec.get_u64()?;
    let subscriber_str = dec.get_string()?;
    let subscriber = RouteAddress::new(RouteFamily::new(family_id), Route::new(subscriber_str));

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(UnsubscribeMessage {
        family_id: RouteFamily::new(family_id),
        pattern,
        session_id: SessionId(session_id_u64),
        subscriber,
    })
}

fn parse_unsubscribe_all(dec: &mut TlvDecoder) -> Result<UnsubscribeAllMessage, String> {
    let session_id_u64 = dec.get_u64()?;
    let family_id = dec.get_u64()?;
    let subscriber_str = dec.get_string()?;
    let subscriber = RouteAddress::new(RouteFamily::new(family_id), Route::new(subscriber_str));

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(UnsubscribeAllMessage {
        session_id: SessionId(session_id_u64),
        subscriber,
    })
}

fn parse_notify(dec: &mut TlvDecoder) -> Result<NotifyMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let payload = dec.get_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(NotifyMessage {
        route: std::sync::Arc::new(route),
        payload: std::sync::Arc::new(payload),
    })
}
