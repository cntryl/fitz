//! Stream domain codec - append-only streaming
//!
//! Encodes/decodes TLV messages for the stream domain.
//! Supports Begin, Append, Commit, Rollback, Read, Last, GetMetadata operations.
//!
//! `route_family` is a server-internal concept supplied by the session layer
//! — it never appears on the wire.

use crate::domains::stream::protocol::{
    IngestMetadata, StreamMessage, StreamSubscriptionMessage, StreamWriteMode,
};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::SessionId;

/// A parsed frame from the stream domain wire protocol.
///
/// Client operation frames (600-606) produce `Op`. Subscription frames (607-608)
/// produce `Sub` and are handled by the sink without reaching `StreamActor`.
#[derive(Debug, Clone)]
pub enum ParsedStreamFrame {
    Op(StreamMessage),
    Sub(StreamSubscriptionMessage),
}

/// Response from stream operations
#[derive(Debug, Clone)]
pub enum StreamResponse {
    /// Operation succeeded with optional session ID and data
    Ok {
        session_id: Option<u64>,
        data: Vec<u8>,
    },
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
) -> Result<ParsedStreamFrame, String> {
    let mut dec = PayloadDecoder::new(payload);

    match ctx.msg_type.0 {
        600 => parse_begin(&mut dec, route_family).map(ParsedStreamFrame::Op),
        601 => parse_append(&mut dec).map(ParsedStreamFrame::Op),
        602 => parse_commit(&mut dec).map(ParsedStreamFrame::Op),
        603 => parse_rollback(&mut dec).map(ParsedStreamFrame::Op),
        604 => parse_read(&mut dec, route_family).map(ParsedStreamFrame::Op),
        605 => parse_last(&mut dec, route_family).map(ParsedStreamFrame::Op),
        606 => parse_get_metadata(&mut dec, route_family).map(ParsedStreamFrame::Op),
        607 => parse_subscribe(&mut dec, route_family, session_id, subscriber)
            .map(ParsedStreamFrame::Sub),
        608 => parse_unsubscribe(&mut dec, route_family, session_id, subscriber)
            .map(ParsedStreamFrame::Sub),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Extract the stream route or pattern needed for authorization.
pub fn extract_auth_route(msg_type: u16, payload: &[u8]) -> Result<Option<&str>, String> {
    let mut dec = PayloadDecoder::new(payload);

    match msg_type {
        600 => {
            let route = dec.get_string_ref()?;
            dec.get_u64()?;
            dec.skip_optional_bytes()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        601 => {
            dec.get_u64()?;
            dec.skip_bytes()?;
            dec.skip_optional_bytes()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(None)
        }
        602 | 603 => {
            dec.get_u64()?;
            if msg_type == 602 {
                dec.get_u8()?;
            }
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(None)
        }
        604 => {
            let route = dec.get_string_ref()?;
            dec.get_u64()?;
            dec.get_u64()?;
            dec.get_optional_u64()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        605..=608 => {
            let route = dec.get_string_ref()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        _ => Err(format!("Unknown operation: {}", msg_type)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &StreamResponse) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_response_into(&mut enc, response)
}

pub fn encode_response_into(enc: &mut PayloadEncoder, response: &StreamResponse) -> Vec<u8> {
    enc.clear();

    match response {
        StreamResponse::Ok { session_id, data } => {
            enc.put_u8(0); // success flag
            enc.put_optional_u64(*session_id);
            enc.put_bytes(data);
        }
        StreamResponse::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }

    enc.finish()
}

// ===== Helper Parsers =====

/// Wire format: `[string route][u64 expected_offset][optional bytes ingest_metadata]`
fn parse_begin(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<StreamMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let expected_offset = dec.get_u64()?;
    let ingest_metadata = dec.get_optional_bytes()?.map(|b| IngestMetadata {
        opaque: b.to_vec().into(),
    });

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Begin {
        family_id: route_family,
        route,
        expected_offset,
        ingest_metadata,
    })
}

/// Wire format: `[u64 session_id][bytes body][optional bytes metadata]`
fn parse_append(dec: &mut PayloadDecoder) -> Result<StreamMessage, String> {
    let session_id = dec.get_u64()?;
    let body = dec.get_bytes()?;
    let metadata = dec.get_optional_bytes()?.map(|b| b.to_vec().into());

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Append {
        session_id,
        body,
        metadata,
    })
}

/// Wire format: `[u64 session_id][u8 mode]` where mode: 0=Buffered, 1=Sync
fn parse_commit(dec: &mut PayloadDecoder) -> Result<StreamMessage, String> {
    let session_id = dec.get_u64()?;
    let mode_byte = dec.get_u8()?;
    let mode = match mode_byte {
        0 => StreamWriteMode::Buffered,
        1 => StreamWriteMode::Sync,
        _ => return Err(format!("Invalid write mode: {}", mode_byte)),
    };

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Commit { session_id, mode })
}

/// Wire format: `[u64 session_id]`
fn parse_rollback(dec: &mut PayloadDecoder) -> Result<StreamMessage, String> {
    let session_id = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Rollback { session_id })
}

/// Wire format: `[string route][u64 from_offset][u64 limit][optional u64 max_bytes]`
fn parse_read(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<StreamMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let from_offset = dec.get_u64()?;
    let limit = dec.get_u64()?;
    let max_bytes = dec.get_optional_u64()?.map(|u| u as usize);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Read {
        family_id: route_family,
        route,
        from_offset,
        limit,
        max_bytes,
    })
}

/// Wire format: `[string route]`
fn parse_last(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<StreamMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Last {
        family_id: route_family,
        route,
    })
}

/// Wire format: `[string route]`
fn parse_get_metadata(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<StreamMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::GetMetadata {
        family_id: route_family,
        route,
    })
}

/// Wire format: `[string pattern]`
fn parse_subscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<StreamSubscriptionMessage, String> {
    let pattern_str = dec.get_string()?;
    let pattern = Route::new(pattern_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamSubscriptionMessage::Subscribe {
        family_id: route_family,
        pattern,
        session_id: session_id.0,
        subscriber,
    })
}

/// Wire format: `[string pattern]`
fn parse_unsubscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: SessionId,
    subscriber: RouteAddress,
) -> Result<StreamSubscriptionMessage, String> {
    let pattern_str = dec.get_string()?;
    let pattern = Route::new(pattern_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamSubscriptionMessage::Unsubscribe {
        family_id: route_family,
        pattern,
        session_id: session_id.0,
        subscriber,
    })
}

/// Encode a STREAM_NOTIFY (609) payload.
///
/// Wire format: `[u64 subscription_id][string route][bytes payload]`
pub fn encode_notify(subscription_id: u64, route: &Route, payload: &[u8]) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_notify_into(&mut enc, subscription_id, route, payload)
}

pub fn encode_notify_into(
    enc: &mut PayloadEncoder,
    subscription_id: u64,
    route: &Route,
    payload: &[u8],
) -> Vec<u8> {
    enc.clear();
    enc.put_u64(subscription_id);
    enc.put_string(route.as_str());
    enc.put_bytes(payload);
    enc.finish()
}
