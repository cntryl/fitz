//! Stream domain codec - append-only streaming
//!
//! Encodes/decodes TLV messages for the stream domain.
//! Supports Begin, Append, Commit, Rollback, Read, Last, GetMetadata operations.
//!
//! `route_family` is a server-internal concept supplied by the session layer
//! — it never appears on the wire.

use crate::domains::stream::protocol::{
    IngestMetadata, StreamClientFrame, StreamClientResponseBody, StreamDiscriminator,
    StreamFilterSet, StreamMessage, StreamSubscriptionMessage, StreamWriteMode,
};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::session::SessionId;

const ERR_STREAM_FILTER_UNSUPPORTED_VERSION: &str = "ERR_STREAM_FILTER_UNSUPPORTED_VERSION";
const ERR_STREAM_FILTER_INVALID_PAYLOAD: &str = "ERR_STREAM_FILTER_INVALID_PAYLOAD";

fn map_stream_filter_decode_error(error: String) -> String {
    if error.contains("missing marker") {
        return format!("{}: {}", ERR_STREAM_FILTER_UNSUPPORTED_VERSION, error);
    }

    format!("{}: {}", ERR_STREAM_FILTER_INVALID_PAYLOAD, error)
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
) -> Result<StreamClientFrame, String> {
    let mut dec = PayloadDecoder::new(payload);

    match ctx.msg_type.0 {
        600 => parse_begin(&mut dec, route_family).map(StreamClientFrame::Op),
        601 => parse_append(&mut dec).map(StreamClientFrame::Op),
        602 => parse_commit(&mut dec).map(StreamClientFrame::Op),
        603 => parse_rollback(&mut dec).map(StreamClientFrame::Op),
        604 => parse_read(&mut dec, route_family).map(StreamClientFrame::Op),
        605 => parse_last(&mut dec, route_family).map(StreamClientFrame::Op),
        606 => parse_get_metadata(&mut dec, route_family).map(StreamClientFrame::Op),
        607 => parse_subscribe(&mut dec, route_family, session_id, subscriber)
            .map(StreamClientFrame::Sub),
        608 => parse_unsubscribe(&mut dec, route_family, session_id, subscriber)
            .map(StreamClientFrame::Sub),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Extract the stream route or pattern needed for authorization.
pub fn extract_auth_route(msg_type: u16, payload: &[u8]) -> Result<Option<&str>, String> {
    let mut dec = PayloadDecoder::new(payload);

    match msg_type {
        600 => {
            let route = dec.get_string_ref()?;
            dec.skip_optional_bytes()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        601 => {
            dec.get_u64()?;
            dec.get_u64()?;
            dec.skip_bytes()?;
            dec.skip_optional_bytes()?;
            if dec.remaining() > 0 {
                dec.get_optional_string()?;
            }
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
            if dec.remaining() > 0 {
                dec.skip_optional_bytes()?;
            }
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
pub fn encode_response(response: &StreamClientResponseBody) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_response_into(&mut enc, response)
}

pub fn encode_response_into(
    enc: &mut PayloadEncoder,
    response: &StreamClientResponseBody,
) -> Vec<u8> {
    enc.clear();

    match response {
        StreamClientResponseBody::Ok { session_id, data } => {
            enc.put_u8(0); // success flag
            enc.put_optional_u64(*session_id);
            enc.put_bytes(data);
        }
        StreamClientResponseBody::Error(e) => {
            return crate::protocol::error_codes::encode_error_body_into(
                stream_error_code_for_message(e),
                e,
                enc,
            );
        }
    }

    enc.finish()
}

fn stream_error_code_for_message(message: &str) -> u16 {
    use crate::protocol::error_codes::stream;

    if message.contains(ERR_STREAM_FILTER_UNSUPPORTED_VERSION) {
        return stream::ERR_STREAM_FILTER_UNSUPPORTED_VERSION;
    }
    if message.contains(ERR_STREAM_FILTER_INVALID_PAYLOAD) {
        return stream::ERR_STREAM_FILTER_INVALID_PAYLOAD;
    }

    match message {
        "session already active" => stream::ERR_SESSION_ALREADY_ACTIVE,
        "session not found" => stream::ERR_SESSION_NOT_FOUND,
        "concurrency conflict" | "ERR_CONCURRENCY_CONFLICT" => stream::ERR_CONCURRENCY_CONFLICT,
        "resource not found" => stream::ERR_RESOURCE_NOT_FOUND,
        message if message.contains("read") || message.contains("bound") => {
            stream::ERR_INVALID_READ_BOUND
        }
        message if message.contains("subscription") && message.contains("pattern") => {
            stream::ERR_INVALID_SUBSCRIPTION_PATTERN
        }
        _ => stream::ERR_BACKEND_ERROR,
    }
}

// ===== Helper Parsers =====

/// Wire format: `[string route][optional bytes ingest_metadata]`
fn parse_begin(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<StreamMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let ingest_metadata = dec.get_optional_bytes()?.map(|b| IngestMetadata {
        opaque: b.to_vec().into(),
    });

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Begin {
        family_id: route_family,
        route,
        ingest_metadata,
    })
}

/// Wire format: `[u64 session_id][u64 expected_offset][bytes body][optional bytes metadata]`
fn parse_append(dec: &mut PayloadDecoder) -> Result<StreamMessage, String> {
    let session_id = dec.get_u64()?;
    let expected_offset = dec.get_u64()?;
    let body = dec.get_bytes()?;
    let metadata = dec.get_optional_bytes()?.map(|b| b.to_vec().into());
    let discriminator = if dec.remaining() > 0 {
        dec.get_optional_string()?.map(StreamDiscriminator)
    } else {
        None
    };

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Append {
        session_id,
        expected_offset,
        body,
        metadata,
        discriminator,
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
    let filter = if dec.remaining() > 0 {
        match dec.get_optional_bytes()? {
            Some(bytes) => Some(
                StreamFilterSet::try_decode(bytes.as_ref())
                    .map_err(map_stream_filter_decode_error)?,
            ),
            None => None,
        }
    } else {
        None
    };

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Read {
        family_id: route_family,
        route,
        from_offset,
        limit,
        max_bytes,
        filter,
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
