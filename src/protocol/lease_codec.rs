//! Lease domain codec - ephemeral lock operations
//!
//! Encodes/decodes TLV messages for the lease domain inside one running broker.
//! Supports Acquire, Extend, Release, Query, Subscribe, and Unsubscribe operations.
//!
//! Wire format follows `CLIENT_SPEC`: ACQUIRE success includes `response_type`
//! (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued) + `fencing_token`;
//! EXTEND success is `new_fencing_token`; RELEASE success is status only;
//! QUERY success is `has_holder` + optional holder details.
//!
//! `route_family` is a server-internal concept supplied by the session layer
//! — it never appears on the wire.
//!
//! Lease fencing tokens are process-local; a restart resets the token lineage.

use crate::dispatch::wire::lease::{
    session_scoped_owner_id, LeaseKey, LeaseMessage, LeaseResponse as DomainLeaseResponse,
    LeaseSubscriptionMessage, PreparedLeaseOperation,
};
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

/// ACQUIRE success `response_type` (`CLIENT_SPEC`)
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
    pub const LIST: u16 = 410;
}

#[derive(Debug, Clone)]
pub enum ParsedLeaseFrame {
    Op(LeaseMessage),
    Sub(LeaseSubscriptionMessage),
}

/// Parse an incoming lease frame.
///
/// # Errors
///
/// Returns an error when the message type is unknown, server-only, or the
/// payload cannot be decoded as the expected lease frame.
pub fn parse_frame(
    ctx: &FrameContext,
    payload: &[u8],
    route_family: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
) -> Result<ParsedLeaseFrame, String> {
    match ctx.msg_type.0 {
        msg_type::ACQUIRE
        | msg_type::RENEW
        | msg_type::RELEASE
        | msg_type::QUERY
        | msg_type::LIST => {
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
///
/// # Errors
///
/// Returns an error when the payload is malformed for the requested lease
/// operation or the message type is unsupported.
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
        msg_type::LIST => parse_list(&mut dec, route_family),
        _ => Err(format!("Unknown operation: {msg_type}")),
    }
}

pub(crate) fn parse_prepared_request(
    msg_type: u16,
    route_family: RouteFamily,
    session_id: u64,
    payload: &[u8],
) -> Result<PreparedLeaseOperation, String> {
    let mut dec = PayloadDecoder::new(payload);

    match msg_type {
        msg_type::ACQUIRE => parse_prepared_acquire(&mut dec, route_family, session_id),
        msg_type::RENEW => parse_prepared_extend(&mut dec, route_family, session_id),
        msg_type::RELEASE => parse_prepared_release(&mut dec, route_family, session_id),
        msg_type::QUERY => parse_prepared_query(&mut dec, route_family),
        _ => Err(format!("Unknown operation: {msg_type}")),
    }
}

/// Extract the exact Lease route needed for authorization.
///
/// # Errors
///
/// Returns an error when the payload is malformed, has trailing data, or the
/// message type is unsupported for lease authorization extraction.
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
        msg_type::LIST => {
            // Only the selector drives authorization; cursor/limit are
            // request-shaping fields, not part of the auth route.
            let route = dec.get_string_ref()?;
            Ok(Some(route))
        }
        msg_type::NOTIFY => Err("LEASE_NOTIFY is server-to-client only".to_string()),
        _ => Err(format!("Unknown operation: {msg_type}")),
    }
}

/// Encode domain `LeaseResponse` to wire bytes (`CLIENT_SPEC`).
///
/// - ACQUIRE success: status=0, `response_type` (0–3), `fencing_token`
/// - EXTEND success: status=0, `new_fencing_token`
/// - RELEASE success: status=0
/// - QUERY success (free): status=0, `has_holder=0`, `pending_waiters=0`
/// - QUERY success (held): status=0, `has_holder=1`, `owner_id`, `ttl_remaining_secs`, `pending_waiters`
/// - All errors: status=1, `error_len`, `error_msg`
#[must_use]
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

    if let Some(encoded) = encode_plain_error_response_into(enc, response) {
        return encoded;
    }

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
        DomainLeaseResponse::Released | DomainLeaseResponse::UnsubscribeOk => {
            enc.put_u8(0);
            enc.finish()
        }
        DomainLeaseResponse::Status {
            owner_id,
            fencing_token: _,
            expires_in_secs,
            pending_waiters,
        } => encode_status_into(enc, owner_id, *expires_in_secs, *pending_waiters),
        DomainLeaseResponse::ListPage { items, next_cursor } => {
            encode_list_page_into(enc, items, *next_cursor)
        }
        DomainLeaseResponse::NotFound => {
            enc.put_u8(0);
            enc.put_u8(0); // has_holder=false
            enc.put_u32(0); // pending_waiters
            enc.finish()
        }
        DomainLeaseResponse::SubscribeOk { subscription_id } => {
            enc.put_u8(0);
            enc.put_u64(*subscription_id);
            enc.finish()
        }
        DomainLeaseResponse::Error(_)
        | DomainLeaseResponse::InvalidSubscriptionRoute(_)
        | DomainLeaseResponse::InvalidListPattern(_)
        | DomainLeaseResponse::InvalidListCursor
        | DomainLeaseResponse::Timeout
        | DomainLeaseResponse::QueueFull { .. }
        | DomainLeaseResponse::HeldByOther { .. }
        | DomainLeaseResponse::NotHeld
        | DomainLeaseResponse::Fenced { .. }
        | DomainLeaseResponse::Expired => {
            unreachable!("handled by encode_plain_error_response_into above")
        }
    }
}

/// Encodes the plain `[status=1][code][message]` error envelope shared by
/// every Lease failure that carries only a code and a message. Returns
/// `None` for a response with its own success or richer error shape (`Acquired`,
/// `Status`, `ListPage`, `NotFound`, `SubscribeOk`, …), which
/// `encode_domain_response_into` encodes itself. Splitting these out keeps
/// that function under the repository's line-count lint.
fn encode_plain_error_response_into(
    enc: &mut PayloadEncoder,
    response: &DomainLeaseResponse,
) -> Option<Vec<u8>> {
    use crate::protocol::error_codes::lease as err;

    let (code, message) = match response {
        DomainLeaseResponse::Error(message) => (err::ERR_BAD_REQUEST, message.clone()),
        DomainLeaseResponse::InvalidSubscriptionRoute(message) => {
            (err::ERR_INVALID_SUBSCRIPTION_ROUTE, message.clone())
        }
        DomainLeaseResponse::InvalidListPattern(message) => {
            (err::ERR_INVALID_LIST_PATTERN, message.clone())
        }
        DomainLeaseResponse::InvalidListCursor => (
            err::ERR_INVALID_LIST_CURSOR,
            "InvalidListCursor".to_string(),
        ),
        DomainLeaseResponse::Timeout => (err::ERR_TIMEOUT, "Timeout".to_string()),
        DomainLeaseResponse::QueueFull { pending_count } => (
            err::ERR_QUEUE_FULL,
            format!("QueueFull: {pending_count} pending"),
        ),
        DomainLeaseResponse::HeldByOther { current_owner } => {
            (err::ERR_LEASE_HELD, format!("HeldByOther: {current_owner}"))
        }
        DomainLeaseResponse::NotHeld => (err::ERR_LEASE_NOT_FOUND, "NotHeld".to_string()),
        DomainLeaseResponse::Fenced { current_token } => (
            err::ERR_INVALID_FENCE,
            format!("Fenced: current_token={current_token}"),
        ),
        DomainLeaseResponse::Expired => (err::ERR_LEASE_EXPIRED, "Expired".to_string()),
        _ => return None,
    };

    Some(encode_error_into(enc, code, &message))
}

/// Wire format: `[u32 item_count][item...][u8 has_next][u64 snapshot_id][u32 offset]`
/// where each item is `[string route][string owner_id][u64 holder_incarnation]
/// [string acquired_at][u64 expires_in_secs][u32 renewals]`. The trailing
/// `snapshot_id`/`offset` pair is present only when `has_next=1`.
fn encode_list_page_into(
    enc: &mut PayloadEncoder,
    items: &[crate::dispatch::wire::lease::LeaseListItem],
    next_cursor: Option<crate::dispatch::wire::lease::LeaseListCursor>,
) -> Vec<u8> {
    enc.put_u8(0);
    enc.put_u32(usize_to_u32_saturating(items.len()));
    for item in items {
        enc.put_string(item.route.as_str());
        enc.put_string(&item.owner_id);
        enc.put_u64(item.holder_incarnation);
        enc.put_string(&item.acquired_at);
        enc.put_u64(item.expires_in_secs);
        enc.put_u32(item.renewals);
    }
    match next_cursor {
        Some(cursor) => {
            enc.put_u8(1);
            enc.put_u64(cursor.snapshot_id);
            enc.put_u32(cursor.offset);
        }
        None => enc.put_u8(0),
    }
    enc.finish()
}

fn encode_status_into(
    enc: &mut PayloadEncoder,
    owner_id: &str,
    expires_in_secs: u64,
    pending_waiters: usize,
) -> Vec<u8> {
    enc.put_u8(0);
    enc.put_u8(1); // has_holder=true
    enc.put_string(owner_id);
    enc.put_u64(expires_in_secs);
    enc.put_u32(usize_to_u32_saturating(pending_waiters));
    enc.finish()
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
    let route_str = dec.get_string_ref()?;
    let route = Route::from_ref(route_str);
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

fn parse_prepared_acquire(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: u64,
) -> Result<PreparedLeaseOperation, String> {
    let route = dec.get_string_ref()?;
    let key = LeaseKey::from_route_str(route_family, route)
        .ok_or_else(|| "invalid lease route".to_string())?;
    let logical_owner_id = dec.get_string_ref()?;
    if logical_owner_id.len() > crate::domains::lease::protocol::LEASE_MAX_OWNER_ID_BYTES {
        return Err(format!(
            "owner_id exceeds maximum length of {} bytes",
            crate::domains::lease::protocol::LEASE_MAX_OWNER_ID_BYTES
        ));
    }
    let owner_id = session_scoped_owner_id(session_id, logical_owner_id);
    let ttl_secs = dec.get_u64()?;
    let wait_seconds = if dec.is_complete() {
        0
    } else {
        dec.get_u32().unwrap_or(0)
    };

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(PreparedLeaseOperation::Acquire {
        key,
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
    let route_str = dec.get_string_ref()?;
    let route = Route::from_ref(route_str);
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

fn parse_prepared_extend(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: u64,
) -> Result<PreparedLeaseOperation, String> {
    let route = dec.get_string_ref()?;
    let key = LeaseKey::from_route_str(route_family, route)
        .ok_or_else(|| "invalid lease route".to_string())?;
    let owner_id = session_scoped_owner_id(session_id, dec.get_string_ref()?);
    let fencing_token = dec.get_u64()?;
    let ttl_secs = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(PreparedLeaseOperation::Extend {
        key,
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
    let route_str = dec.get_string_ref()?;
    let route = Route::from_ref(route_str);
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

fn parse_prepared_release(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: u64,
) -> Result<PreparedLeaseOperation, String> {
    let route = dec.get_string_ref()?;
    let key = LeaseKey::from_route_str(route_family, route)
        .ok_or_else(|| "invalid lease route".to_string())?;
    let owner_id = session_scoped_owner_id(session_id, dec.get_string_ref()?);
    let fencing_token = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(PreparedLeaseOperation::Release {
        key,
        owner_id,
        fencing_token,
    })
}

/// Wire format: `[string route]`
fn parse_query(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<LeaseMessage, String> {
    let route_str = dec.get_string_ref()?;
    let route = Route::from_ref(route_str);

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::Query {
        family_id: route_family,
        route,
    })
}

/// Wire format: `[string pattern][u8 has_cursor][u64 snapshot_id][u32 offset][u32 limit]`
/// (`snapshot_id`/`offset` are present only when `has_cursor=1`; `limit=0`
/// means "use the server default page size").
fn parse_list(dec: &mut PayloadDecoder, route_family: RouteFamily) -> Result<LeaseMessage, String> {
    let pattern_str = dec.get_string_ref()?;
    let pattern = Route::from_ref(pattern_str);

    let has_cursor = dec.get_u8()? != 0;
    let cursor = if has_cursor {
        let snapshot_id = dec.get_u64()?;
        let offset = dec.get_u32()?;
        Some(crate::dispatch::wire::lease::LeaseListCursor {
            snapshot_id,
            offset,
        })
    } else {
        None
    };
    let limit_raw = dec.get_u32()?;
    let limit = if limit_raw == 0 {
        None
    } else {
        Some(limit_raw)
    };

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseMessage::List {
        family_id: route_family,
        pattern,
        cursor,
        limit,
    })
}

fn parse_prepared_query(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
) -> Result<PreparedLeaseOperation, String> {
    let route = dec.get_string_ref()?;
    let key = LeaseKey::from_route_str(route_family, route)
        .ok_or_else(|| "invalid lease route".to_string())?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(PreparedLeaseOperation::Query { key })
}

/// Wire format: `[string route]`
fn parse_subscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
) -> Result<LeaseSubscriptionMessage, String> {
    let route = dec.get_string_ref()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseSubscriptionMessage::Subscribe {
        family_id: route_family,
        route: Route::from_ref(route),
        session_id,
        subscriber,
    })
}

/// Wire format: `[string route]`
fn parse_unsubscribe(
    dec: &mut PayloadDecoder,
    route_family: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
) -> Result<LeaseSubscriptionMessage, String> {
    let route = dec.get_string_ref()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(LeaseSubscriptionMessage::Unsubscribe {
        family_id: route_family,
        route: Route::from_ref(route),
        session_id,
        subscriber,
    })
}

/// Encode a lease change notification
///
/// Wire format: `[u64 subscription_id][string route][bytes payload]`
#[must_use]
pub fn encode_notify(subscription_id: u64, route: &str, _payload: &[u8]) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    encode_notify_into(&mut enc, subscription_id, route, &[])
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
