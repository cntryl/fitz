//! Lease domain codec - ephemeral lock operations
//!
//! Encodes/decodes TLV messages for the lease domain inside one running broker.
//! Supports Acquire, Extend, Release, Query, Subscribe, and Unsubscribe operations.
//!
//! Wire format follows CLIENT_SPEC: ACQUIRE success includes response_type
//! (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued) + fencing_token;
//! EXTEND success is new_fencing_token; RELEASE success is status only;
//! QUERY success is has_holder + optional holder details.
//!
//! `route_family` is a server-internal concept supplied by the session layer
//! — it never appears on the wire.
//!
//! Lease fencing tokens are process-local; a restart resets the token lineage.

use crate::domains::lease::protocol::{
    LeaseMessage, LeaseResponse as DomainLeaseResponse, LeaseSubscriptionMessage,
};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

/// ACQUIRE success response_type (CLIENT_SPEC)
pub mod acquire_response_type {
    pub const ACQUIRED: u8 = 0;
    pub const ALREADY_HELD: u8 = 1;
    pub const QUEUED: u8 = 2;
    pub const ALREADY_QUEUED: u8 = 3;
}

/// Lease domain message type IDs.
pub mod msg_type {
    pub const ACQUIRE: u16 = 400;
    pub const RENEW: u16 = 401;
    pub const RELEASE: u16 = 402;
    pub const QUERY: u16 = 403;
    pub const SUBSCRIBE: u16 = 407;
    pub const UNSUBSCRIBE: u16 = 408;
    pub const NOTIFY: u16 = 409;
}

#[derive(Debug, Clone)]
pub enum ParsedLeaseFrame {
    Op(LeaseMessage),
    Sub(LeaseSubscriptionMessage),
}

/// Parse an incoming lease frame.
pub fn parse_frame(
    ctx: &FrameContext,
    payload: &[u8],
    route_family: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
) -> Result<ParsedLeaseFrame, String> {
    match ctx.msg_type.0 {
        msg_type::ACQUIRE | msg_type::RENEW | msg_type::RELEASE | msg_type::QUERY => {
            parse_request(ctx.msg_type.0, route_family, payload).map(ParsedLeaseFrame::Op)
        }
        msg_type::SUBSCRIBE => parse_subscribe(
            &mut PayloadDecoder::new(payload),
            route_family,
            session_id,
            subscriber,
        )
        .map(ParsedLeaseFrame::Sub),
        msg_type::UNSUBSCRIBE => parse_unsubscribe(
            &mut PayloadDecoder::new(payload),
            route_family,
            session_id,
            subscriber,
        )
        .map(ParsedLeaseFrame::Sub),
        msg_type::NOTIFY => Err("LEASE_NOTIFY is server-to-client only".to_string()),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type.0)),
    }
}

/// Parse incoming message from TLV-encoded bytes.
///
/// `route_family` is injected by the session layer — it is never read
/// from the wire payload.
pub fn parse_request(
    msg_type: u16,
    route_family: RouteFamily,
    payload: &[u8],
) -> Result<LeaseMessage, String> {
    let mut dec = PayloadDecoder::new(payload);

    match msg_type {
        msg_type::ACQUIRE => parse_acquire(&mut dec, route_family),
        msg_type::RENEW => parse_extend(&mut dec, route_family),
        msg_type::RELEASE => parse_release(&mut dec, route_family),
        msg_type::QUERY => parse_query(&mut dec, route_family),
        _ => Err(format!("Unknown operation: {}", msg_type)),
    }
}

/// Extract the lease route or pattern needed for authorization.
pub fn extract_auth_route(msg_type: u16, payload: &[u8]) -> Result<Option<&str>, String> {
    let mut dec = PayloadDecoder::new(payload);

    match msg_type {
        msg_type::ACQUIRE => {
            let route = dec.get_string_ref()?;
            dec.get_string_ref()?;
            dec.get_u64()?;
            if !dec.is_complete() {
                dec.get_u32()?;
            }
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        msg_type::RENEW => {
            let route = dec.get_string_ref()?;
            dec.get_string_ref()?;
            dec.get_u64()?;
            dec.get_u64()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        msg_type::RELEASE => {
            let route = dec.get_string_ref()?;
            dec.get_string_ref()?;
            dec.get_u64()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        msg_type::QUERY | msg_type::SUBSCRIBE | msg_type::UNSUBSCRIBE => {
            let route = dec.get_string_ref()?;
            if !dec.is_complete() {
                return Err("Trailing data in message".to_string());
            }
            Ok(Some(route))
        }
        msg_type::NOTIFY => Err("LEASE_NOTIFY is server-to-client only".to_string()),
        _ => Err(format!("Unknown operation: {}", msg_type)),
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
    let mut enc = PayloadEncoder::new();
    encode_domain_response_into(&mut enc, response)
}

pub fn encode_domain_response_into(
    enc: &mut PayloadEncoder,
    response: &DomainLeaseResponse,
) -> Vec<u8> {
    use acquire_response_type::{ACQUIRED, ALREADY_HELD, ALREADY_QUEUED, QUEUED};

    enc.clear();

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
        DomainLeaseResponse::Timeout => encode_error_into(
            enc,
            crate::protocol::error_codes::lease::ERR_TIMEOUT,
            "Timeout",
        ),
        DomainLeaseResponse::QueueFull { pending_count } => encode_error_into(
            enc,
            crate::protocol::error_codes::lease::ERR_QUEUE_FULL,
            &format!("QueueFull: {} pending", pending_count),
        ),
        DomainLeaseResponse::HeldByOther { current_owner } => encode_error_into(
            enc,
            crate::protocol::error_codes::lease::ERR_LEASE_HELD,
            &format!("HeldByOther: {}", current_owner),
        ),
        DomainLeaseResponse::NotHeld => encode_error_into(
            enc,
            crate::protocol::error_codes::lease::ERR_LEASE_NOT_FOUND,
            "NotHeld",
        ),
        DomainLeaseResponse::Fenced { current_token } => encode_error_into(
            enc,
            crate::protocol::error_codes::lease::ERR_INVALID_FENCE,
            &format!("Fenced: current_token={}", current_token),
        ),
        DomainLeaseResponse::Expired => encode_error_into(
            enc,
            crate::protocol::error_codes::lease::ERR_LEASE_EXPIRED,
            "Expired",
        ),
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

fn encode_error_into(enc: &mut PayloadEncoder, code: u16, msg: &str) -> Vec<u8> {
    crate::protocol::error_codes::encode_error_body_into(code, msg, enc)
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
    session_id: u64,
    subscriber: RouteAddress,
) -> Result<LeaseSubscriptionMessage, String> {
    let pattern = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseSubscriptionMessage::Subscribe {
        family_id: route_family,
        pattern: Route::new(pattern.as_str()),
        session_id,
        subscriber,
    })
}

/// Wire format: `[string pattern]`
fn parse_unsubscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
) -> Result<LeaseSubscriptionMessage, String> {
    let pattern = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseSubscriptionMessage::Unsubscribe {
        family_id: route_family,
        pattern: Route::new(pattern.as_str()),
        session_id,
        subscriber,
    })
}

/// Encode a lease change notification
///
/// Wire format: `[u64 subscription_id][string route][bytes payload]`
pub fn encode_notify(subscription_id: u64, route: &str, _payload: &[u8]) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_notify_into(&mut enc, subscription_id, route, _payload)
}

pub fn encode_notify_into(
    enc: &mut PayloadEncoder,
    subscription_id: u64,
    route: &str,
    _payload: &[u8],
) -> Vec<u8> {
    enc.clear();
    enc.put_u64(subscription_id);
    enc.put_string(route);
    // Minimal payload for leases (just signal the change)
    enc.put_bytes(&[]);
    enc.finish()
}
