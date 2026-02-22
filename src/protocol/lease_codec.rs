//! Lease domain codec - distributed lock operations
//!
//! Encodes/decodes TLV messages for the lease domain.
//! Supports Acquire, Extend, Release, Query operations.
//!
//! Wire format follows CLIENT_SPEC: ACQUIRE success includes response_type
//! (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued) + fencing_token;
//! EXTEND success is new_fencing_token; RELEASE success is status only;
//! QUERY success is has_holder + optional holder details.
//!
//! `route_family` is a server-internal concept supplied by the session layer
//! — it never appears on the wire.

use crate::domains::lease::protocol::{LeaseMessage, LeaseResponse as DomainLeaseResponse};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{Route, RouteFamily};

/// ACQUIRE success response_type (CLIENT_SPEC)
pub mod acquire_response_type {
    pub const ACQUIRED: u8 = 0;
    pub const ALREADY_HELD: u8 = 1;
    pub const QUEUED: u8 = 2;
    pub const ALREADY_QUEUED: u8 = 3;
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
    let mut dec = PayloadDecoder::new(payload);

    match ctx.msg_type.0 {
        400 => parse_acquire(&mut dec, route_family),
        401 => parse_extend(&mut dec, route_family),
        402 => parse_release(&mut dec, route_family),
        403 => parse_query(&mut dec, route_family),
        407 => parse_subscribe(&mut dec, route_family),
        408 => parse_unsubscribe(&mut dec, route_family),
        409 => Ok(LeaseMessage::UnsubscribeAll),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Encode domain LeaseResponse to wire bytes (CLIENT_SPEC).
///
/// - ACQUIRE success: status=0, response_type (0–3), fencing_token
/// - EXTEND success: status=0, new_fencing_token
/// - RELEASE success: status=0
/// - QUERY success (free): status=0, has_holder=0, pending_waiters=0
/// - QUERY success (held): status=0, has_holder=1, owner_id, ttl_remaining_secs, pending_waiters
/// - All errors: status=1, error_len, error_msg
pub fn encode_domain_response(response: &DomainLeaseResponse) -> Vec<u8> {
    use acquire_response_type::{ACQUIRED, ALREADY_HELD, ALREADY_QUEUED, QUEUED};

    let mut enc = PayloadEncoder::new();

    match response {
        DomainLeaseResponse::Acquired { fencing_token } => {
            enc.put_u8(0);
            enc.put_u8(ACQUIRED);
            enc.put_u64(*fencing_token);
            enc.finish()
        }
        DomainLeaseResponse::AlreadyHeld { fencing_token } => {
            enc.put_u8(0);
            enc.put_u8(ALREADY_HELD);
            enc.put_u64(*fencing_token);
            enc.finish()
        }
        DomainLeaseResponse::Queued { fencing_token } => {
            enc.put_u8(0);
            enc.put_u8(QUEUED);
            enc.put_u64(*fencing_token);
            enc.finish()
        }
        DomainLeaseResponse::AlreadyQueued { fencing_token } => {
            enc.put_u8(0);
            enc.put_u8(ALREADY_QUEUED);
            enc.put_u64(*fencing_token);
            enc.finish()
        }
        DomainLeaseResponse::Extended { fencing_token } => {
            enc.put_u8(0);
            enc.put_u64(*fencing_token);
            enc.finish()
        }
        DomainLeaseResponse::Released => {
            enc.put_u8(0);
            enc.finish()
        }
        DomainLeaseResponse::Status {
            owner_id,
            fencing_token: _,
            expires_in_secs,
            pending_waiters,
        } => {
            enc.put_u8(0);
            enc.put_u8(1); // has_holder=true
            enc.put_string(owner_id);
            enc.put_u64(*expires_in_secs);
            enc.put_u32(*pending_waiters as u32);
            enc.finish()
        }
        DomainLeaseResponse::NotFound => {
            enc.put_u8(0);
            enc.put_u8(0); // has_holder=false
            enc.put_u32(0); // pending_waiters
            enc.finish()
        }
        DomainLeaseResponse::Timeout => encode_error("Timeout"),
        DomainLeaseResponse::QueueFull { pending_count } => {
            encode_error(&format!("QueueFull: {} pending", pending_count))
        }
        DomainLeaseResponse::HeldByOther { current_owner } => {
            encode_error(&format!("HeldByOther: {}", current_owner))
        }
        DomainLeaseResponse::NotHeld => encode_error("NotHeld"),
        DomainLeaseResponse::Fenced { current_token } => {
            encode_error(&format!("Fenced: current_token={}", current_token))
        }
        DomainLeaseResponse::Expired => encode_error("Expired"),
        DomainLeaseResponse::SubscribeOk { subscription_id } => {
            enc.put_u8(0);
            enc.put_u64(*subscription_id);
            enc.finish()
        }
        DomainLeaseResponse::UnsubscribeOk => {
            enc.put_u8(0);
            enc.finish()
        }
    }
}

fn encode_error(msg: &str) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    enc.put_u8(1);
    enc.put_string(msg);
    enc.finish()
}

// ===== Helper Parsers =====

/// Wire format: `[string route][string owner_id][u64 ttl_secs][u32 wait_seconds (optional)]`
fn parse_acquire(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<LeaseMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let owner_id = dec.get_string()?;
    let ttl_secs = dec.get_u64()?;
    // wait_seconds is optional for backward compatibility; defaults to 0
    let wait_seconds = if dec.is_complete() {
        0
    } else {
        dec.get_u32().unwrap_or(0)
    };

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Acquire {
        family_id: route_family,
        route,
        owner_id,
        ttl_secs,
        wait_seconds,
    })
}

/// Wire format: `[string route][string owner_id][u64 fencing_token][u64 ttl_secs]`
fn parse_extend(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<LeaseMessage, String> {
    let route_str = dec.get_string()?;
    let route = Route::new(route_str);
    let owner_id = dec.get_string()?;
    let fencing_token = dec.get_u64()?;
    let ttl_secs = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Extend {
        family_id: route_family,
        route,
        owner_id,
        fencing_token,
        ttl_secs,
    })
}

/// Wire format: `[string route][string owner_id][u64 fencing_token]`
fn parse_release(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<LeaseMessage, String> {
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
fn parse_query(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<LeaseMessage, String> {
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
/// Wire format: `[string pattern]`
fn parse_subscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<LeaseMessage, String> {
    let pattern = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Subscribe {
        family_id: route_family,
        pattern,
    })
}

/// Wire format: `[string pattern]`
fn parse_unsubscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<LeaseMessage, String> {
    let pattern = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Unsubscribe {
        family_id: route_family,
        pattern,
    })
}

/// Encode a lease change notification
///
/// Wire format: `[u64 subscription_id][string route][bytes payload]`
pub fn encode_notify(subscription_id: u64, route: &str, _payload: &[u8]) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    enc.put_u64(subscription_id);
    enc.put_string(route);
    // Minimal payload for leases (just signal the change)
    enc.put_bytes(&[]);
    enc.finish()
}
