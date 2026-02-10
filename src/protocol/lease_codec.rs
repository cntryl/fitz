//! Lease domain codec - distributed lock operations
//!
//! Encodes/decodes TLV messages for the lease domain.
//! Supports Acquire, Renew, Release, Query operations.
//!
//! `route_family` is a server-internal concept supplied by the session layer
//! — it never appears on the wire.

use crate::domains::lease::protocol::LeaseMessage;
use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};
use crate::runtime::routing::{Route, RouteFamily};

/// Response from lease operations
#[derive(Debug, Clone)]
pub enum LeaseResponse {
    /// Operation succeeded with optional token
    Ok { token: Option<u64> },
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
) -> Result<LeaseMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type.0 {
        400 => parse_acquire(&mut dec, route_family),
        401 => parse_renew(&mut dec, route_family),
        402 => parse_release(&mut dec, route_family),
        403 => parse_query(&mut dec, route_family),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &LeaseResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();

    match response {
        LeaseResponse::Ok { token } => {
            enc.put_u8(0); // success flag
            enc.put_optional_u64(*token);
        }
        LeaseResponse::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }

    enc.finish()
}

// ===== Helper Parsers =====

/// Wire format: `[string route][string owner_id][u64 ttl_secs]`
fn parse_acquire(dec: &mut TlvDecoder, route_family: RouteFamily) -> Result<LeaseMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let owner_id = dec.get_string()?;
    let ttl_secs = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Acquire {
        family_id: route_family,
        route,
        owner_id,
        ttl_secs,
    })
}

/// Wire format: `[string route][string owner_id][u64 fencing_token][u64 ttl_secs]`
fn parse_renew(dec: &mut TlvDecoder, route_family: RouteFamily) -> Result<LeaseMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let owner_id = dec.get_string()?;
    let fencing_token = dec.get_u64()?;
    let ttl_secs = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Renew {
        family_id: route_family,
        route,
        owner_id,
        fencing_token,
        ttl_secs,
    })
}

/// Wire format: `[string route][string owner_id][u64 fencing_token]`
fn parse_release(dec: &mut TlvDecoder, route_family: RouteFamily) -> Result<LeaseMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let owner_id = dec.get_string()?;
    let fencing_token = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Release {
        family_id: route_family,
        route,
        owner_id,
        fencing_token,
    })
}

/// Wire format: `[string route]`
fn parse_query(dec: &mut TlvDecoder, route_family: RouteFamily) -> Result<LeaseMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Query {
        family_id: route_family,
        route,
    })
}
