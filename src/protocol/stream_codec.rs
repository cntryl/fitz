//! Stream domain codec - append-only streaming
//!
//! Encodes/decodes TLV messages for the stream domain.
//! Supports Begin, Append, Commit, Rollback, Read, Last, GetMetadata operations.
//!
//! `route_family` is a server-internal concept supplied by the session layer
//! — it never appears on the wire.

use crate::domains::stream::protocol::{IngestMetadata, StreamMessage, StreamWriteMode};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};
use crate::runtime::routing::{Route, RouteFamily};

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
/// `route_family` is injected by the session layer — it is never read
/// from the wire payload.
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
    route_family: RouteFamily,
) -> Result<StreamMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type.0 {
        600 => parse_begin(&mut dec, route_family),
        601 => parse_append(&mut dec),
        602 => parse_commit(&mut dec),
        603 => parse_rollback(&mut dec),
        604 => parse_read(&mut dec, route_family),
        605 => parse_last(&mut dec, route_family),
        606 => parse_get_metadata(&mut dec, route_family),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &StreamResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();

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
fn parse_begin(dec: &mut TlvDecoder, route_family: RouteFamily) -> Result<StreamMessage, String> {
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
fn parse_append(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
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
fn parse_commit(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
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
fn parse_rollback(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let session_id = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Rollback { session_id })
}

/// Wire format: `[string route][u64 from_offset][u64 limit][optional u64 max_bytes]`
fn parse_read(dec: &mut TlvDecoder, route_family: RouteFamily) -> Result<StreamMessage, String> {
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
fn parse_last(dec: &mut TlvDecoder, route_family: RouteFamily) -> Result<StreamMessage, String> {
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
fn parse_get_metadata(dec: &mut TlvDecoder, route_family: RouteFamily) -> Result<StreamMessage, String> {
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
