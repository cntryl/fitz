//! Lease domain codec - distributed lock operations
//!
//! Encodes/decodes TLV messages for the lease domain.
//! Supports Acquire, Renew, Release, Query operations.

use crate::domains::lease::protocol::LeaseMessage;
use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};
use crate::runtime::routing::{Route, RouteFamily};

/// Response from lease operations
#[derive(Debug, Clone)]
pub enum LeaseResponse {
    /// Operation succeeded with optional token
    Ok { token: Option<String> },
    /// Operation failed with error message
    Error(String),
}

/// Parse incoming message from TLV-encoded bytes
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
) -> Result<LeaseMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type.0 {
        400 => parse_acquire(&mut dec),
        401 => parse_renew(&mut dec),
        402 => parse_release(&mut dec),
        403 => parse_query(&mut dec),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &LeaseResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();

    match response {
        LeaseResponse::Ok { token } => {
            enc.put_u8(0); // success flag
            enc.put_optional_string(token.as_deref());
        }
        LeaseResponse::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }

    enc.finish()
}

// ===== Helper Parsers =====

fn parse_acquire(dec: &mut TlvDecoder) -> Result<LeaseMessage, String> {
    let family_id = dec.get_u64()?;
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let owner_id = dec.get_string()?;
    let ttl_secs = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Acquire {
        family_id: RouteFamily::new(family_id),
        route,
        owner_id,
        ttl_secs,
    })
}

fn parse_renew(dec: &mut TlvDecoder) -> Result<LeaseMessage, String> {
    let family_id = dec.get_u64()?;
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let owner_id = dec.get_string()?;
    let fencing_token = dec.get_u64()?;
    let ttl_secs = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Renew {
        family_id: RouteFamily::new(family_id),
        route,
        owner_id,
        fencing_token,
        ttl_secs,
    })
}

fn parse_release(dec: &mut TlvDecoder) -> Result<LeaseMessage, String> {
    let family_id = dec.get_u64()?;
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let owner_id = dec.get_string()?;
    let fencing_token = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Release {
        family_id: RouteFamily::new(family_id),
        route,
        owner_id,
        fencing_token,
    })
}

fn parse_query(dec: &mut TlvDecoder) -> Result<LeaseMessage, String> {
    let family_id = dec.get_u64()?;
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Query {
        family_id: RouteFamily::new(family_id),
        route,
    })
}
