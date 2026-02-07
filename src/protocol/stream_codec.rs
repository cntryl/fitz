//! Stream domain codec - append-only streaming
//!
//! Encodes/decodes TLV messages for the stream domain.
//! Supports Begin, Append, Commit, Rollback, Read, Last, GetMetadata operations.

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

/// Parse incoming message from TLV-encoded bytes
pub fn parse_request(ctx: &FrameContext, payload: &[u8]) -> Result<StreamMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type.0 {
        600 => parse_begin(&mut dec),
        601 => parse_append(&mut dec),
        602 => parse_commit(&mut dec),
        603 => parse_rollback(&mut dec),
        604 => parse_read(&mut dec),
        605 => parse_last(&mut dec),
        606 => parse_get_metadata(&mut dec),
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

fn parse_begin(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let family_id = dec.get_u64()?;
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
        family_id: RouteFamily::new(family_id),
        route,
        expected_offset,
        ingest_metadata,
    })
}

fn parse_append(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let session_id = dec.get_string()?;
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

fn parse_commit(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let session_id = dec.get_string()?;
    let mode_str = dec.get_string()?;
    let mode = match mode_str.as_str() {
        "Buffered" => StreamWriteMode::Buffered,
        "Sync" => StreamWriteMode::Sync,
        _ => return Err(format!("Invalid write mode: {}", mode_str)),
    };

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Commit { session_id, mode })
}

fn parse_rollback(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let session_id = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Rollback { session_id })
}

fn parse_read(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let family_id = dec.get_u64()?;
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let from_offset = dec.get_u64()?;
    let limit = dec.get_u64()?;
    let max_bytes = dec.get_optional_u64()?.map(|u| u as usize);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Read {
        family_id: RouteFamily::new(family_id),
        route,
        from_offset,
        limit,
        max_bytes,
    })
}

fn parse_last(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let family_id = dec.get_u64()?;
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Last {
        family_id: RouteFamily::new(family_id),
        route,
    })
}

fn parse_get_metadata(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let family_id = dec.get_u64()?;
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::GetMetadata {
        family_id: RouteFamily::new(family_id),
        route,
    })
}
