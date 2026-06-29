// KV domain TLV message types and codec

use crate::domains::kv::KvError;
use crate::domains::kv::{KvMessage, KvResponse, KvSubscriptionMessage, TxMode};
use crate::protocol::frame_context::FrameContext;
use crate::runtime::routing::{route_exact_triplet, Route, RouteAddress, RouteFamily};

use super::mutation_parsers::{
    parse_delete, parse_delete_range, parse_get, parse_insert, parse_put, parse_scan,
};

/// KV domain message type IDs
pub mod msg_type {
    pub const BEGIN: u16 = 100;
    pub const COMMIT: u16 = 101;
    pub const ROLLBACK: u16 = 102;
    pub const GET: u16 = 103;
    pub const PUT: u16 = 104;
    pub const INSERT: u16 = 105;
    pub const DELETE: u16 = 106;
    pub const DELETE_RANGE: u16 = 107;
    pub const SCAN: u16 = 108;
    pub const SUBSCRIBE: u16 = 109;
    pub const UNSUBSCRIBE: u16 = 110;
    pub const NOTIFY: u16 = 111;
}

#[derive(Debug, Clone)]
pub enum ParsedKvFrame {
    Op(KvMessage),
    Sub(KvSubscriptionMessage),
}

pub fn parse_frame(
    ctx: &FrameContext,
    payload: &[u8],
    route_family: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
) -> Result<ParsedKvFrame, String> {
    match ctx.msg_type.0 {
        msg_type::BEGIN
        | msg_type::COMMIT
        | msg_type::ROLLBACK
        | msg_type::GET
        | msg_type::PUT
        | msg_type::INSERT
        | msg_type::DELETE
        | msg_type::DELETE_RANGE
        | msg_type::SCAN => {
            parse_request(ctx.msg_type.0, route_family, payload).map(ParsedKvFrame::Op)
        }
        msg_type::SUBSCRIBE => {
            parse_subscribe(route_family, session_id, subscriber, payload).map(ParsedKvFrame::Sub)
        }
        msg_type::UNSUBSCRIBE => {
            parse_unsubscribe(route_family, session_id, subscriber, payload).map(ParsedKvFrame::Sub)
        }
        msg_type::NOTIFY => Err("KV_NOTIFY is server-to-client only".to_string()),
        _ => Err(format!("Unknown KV message type: {}", ctx.msg_type.0)),
    }
}

/// Parse KV request from bytes
/// Per CLIENT_SPEC: All operations now include full route on wire.
/// RouteFamily is assigned by the session and must be provided by the caller.
pub fn parse_request(
    msg_type: u16,
    route_family: RouteFamily,
    payload: &[u8],
) -> Result<KvMessage, String> {
    match msg_type {
        msg_type::BEGIN => parse_begin(route_family, payload),
        msg_type::COMMIT => parse_commit(payload),
        msg_type::ROLLBACK => parse_rollback(payload),
        msg_type::GET => parse_get(route_family, payload),
        msg_type::PUT => parse_put(route_family, payload),
        msg_type::INSERT => parse_insert(route_family, payload),
        msg_type::DELETE => parse_delete(route_family, payload),
        msg_type::DELETE_RANGE => parse_delete_range(route_family, payload),
        msg_type::SCAN => parse_scan(route_family, payload),
        _ => Err(format!("Unknown KV message type: {msg_type}")),
    }
}

/// Encode KV response to bytes
pub fn encode_response(response: &KvResponse) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    match response {
        KvResponse::BeginOk { tx_id } => {
            buf.put_u8(0); // status: success
            buf.put_u64(*tx_id);
        }
        KvResponse::SubscribeOk { subscription_id } => {
            buf.put_u8(0); // status: success
            buf.put_u64(*subscription_id);
        }
        KvResponse::UnsubscribeOk => {
            buf.put_u8(0); // status: success
        }
        KvResponse::CommitOk => {
            buf.put_u8(0); // status: success
                           // Empty response for commit ok
        }
        KvResponse::RollbackOk => {
            buf.put_u8(0); // status: success
                           // Empty response for rollback ok
        }
        KvResponse::GetResult { found, value } => {
            buf.put_u8(0); // status: success
            buf.put_u8(if *found { 1 } else { 0 });
            if let Some(v) = value {
                buf.put_u32(v.len() as u32);
                buf.put_slice(v);
            } else {
                buf.put_u32(0);
            }
        }
        KvResponse::PutOk => {
            buf.put_u8(0); // status: success
                           // Empty response for put ok
        }
        KvResponse::InsertOk => {
            buf.put_u8(0); // status: success
                           // Empty response for insert ok
        }
        KvResponse::DeleteOk => {
            buf.put_u8(0); // status: success
                           // Empty response for delete ok
        }
        KvResponse::DeleteRangeOk => {
            buf.put_u8(0); // status: success
                           // Empty response for delete range ok
        }
        KvResponse::ScanResult { items, has_more } => {
            buf.put_u8(0); // status: success
            buf.put_u32(items.len() as u32);
            for item in items {
                buf.put_u32(item.key.len() as u32);
                buf.put_slice(&item.key);
                buf.put_u32(item.value.len() as u32);
                buf.put_slice(&item.value);
            }
            buf.put_u8(if *has_more { 1 } else { 0 });
        }
        KvResponse::Error { error } => {
            return crate::protocol::error_codes::encode_error_body(
                kv_error_code(error),
                &error.to_string(),
            );
        }
    }
    buf
}

fn kv_error_code(error: &KvError) -> u16 {
    use crate::protocol::error_codes::kv;

    match error {
        KvError::InvalidTxId | KvError::NoActiveTx => kv::ERR_TRANSACTION_NOT_FOUND,
        KvError::NotFound => kv::ERR_KEY_NOT_FOUND,
        KvError::Conflict(_) => kv::ERR_ISOLATION_CONFLICT,
        KvError::AlreadyExists => kv::ERR_KEY_EXISTS,
        KvError::RealmMismatch => kv::ERR_REALM_MISMATCH,
        KvError::BackendUnavailable(_) | KvError::BackendError(_) => kv::ERR_BACKEND_ERROR,
        KvError::InvalidRoute(_)
        | KvError::InvalidRequest(_)
        | KvError::InvalidRealm
        | KvError::InvalidRouteFamily
        | KvError::UnknownResource(_)
        | KvError::TxScopeViolation { .. } => kv::ERR_INVALID_ROUTE,
    }
}

// ===== Parsers =====

/// Parse route string into realm, area, resource components
/// Expected format: "kv://realm/area/resource" or just "realm/area/resource"
fn split_route(route_str: &str) -> Option<(&str, &str, &str)> {
    let parts = route_exact_triplet(route_str)?;

    if parts.realm.is_empty() || parts.area.is_empty() || parts.resource.is_empty() {
        return None;
    }

    Some((parts.realm, parts.area, parts.resource))
}

pub(super) fn decode_route_str<'a>(
    route_bytes: &'a [u8],
    op_name: &str,
) -> Result<&'a str, String> {
    std::str::from_utf8(route_bytes).map_err(|_| format!("Invalid UTF-8 in {op_name} route"))
}

fn read_route_str<'a>(
    payload: &'a [u8],
    offset: &mut usize,
    op_name: &str,
) -> Result<&'a str, String> {
    if *offset + 4 > payload.len() {
        return Err(format!("{op_name} route length overflow"));
    }
    let route_len = u32::from_be_bytes([
        payload[*offset],
        payload[*offset + 1],
        payload[*offset + 2],
        payload[*offset + 3],
    ]) as usize;
    *offset += 4;

    if *offset + route_len > payload.len() {
        return Err(format!("{op_name} route overflow"));
    }
    let route_str = decode_route_str(&payload[*offset..*offset + route_len], op_name)?;
    *offset += route_len;
    Ok(route_str)
}

fn parse_route(route_str: &str) -> Result<(String, String, String), String> {
    match split_route(route_str) {
        Some((realm, area, resource)) => {
            Ok((realm.to_string(), area.to_string(), resource.to_string()))
        }
        None => Err(format!(
            "Route must be realm/area/resource, got '{route_str}'"
        )),
    }
}

pub(super) fn parse_route_resource(route_str: &str) -> Result<String, String> {
    match split_route(route_str) {
        Some((_realm, _area, resource)) => Ok(resource.to_string()),
        None => Err(format!(
            "Route must be realm/area/resource, got '{route_str}'"
        )),
    }
}

fn validate_route(route_str: &str) -> Result<(), String> {
    if split_route(route_str).is_some() {
        Ok(())
    } else {
        Err(format!(
            "Route must be realm/area/resource, got '{route_str}'"
        ))
    }
}

/// Extract the KV route needed for authorization without constructing a full request message.
pub fn extract_auth_route(msg_type: u16, payload: &[u8]) -> Result<Option<&str>, String> {
    match msg_type {
        msg_type::BEGIN => {
            if payload.len() < 6 {
                return Err("BEGIN payload too short".to_string());
            }

            let mut offset = 0;
            let route_str = read_route_str(payload, &mut offset, "BEGIN")?;
            validate_route(route_str)?;

            if offset + 2 > payload.len() {
                return Err("BEGIN mode byte missing".to_string());
            }

            Ok(Some(route_str))
        }
        msg_type::SUBSCRIBE | msg_type::UNSUBSCRIBE => {
            let mut offset = 0;
            let route_str = read_route_str(payload, &mut offset, "KV watch")?;
            validate_route(route_str)?;
            if offset != payload.len() {
                return Err("Trailing data in KV watch payload".to_string());
            }
            Ok(Some(route_str))
        }
        msg_type::COMMIT
        | msg_type::ROLLBACK
        | msg_type::GET
        | msg_type::PUT
        | msg_type::INSERT
        | msg_type::DELETE
        | msg_type::DELETE_RANGE
        | msg_type::SCAN => {
            if payload.len() < 12 {
                return Err(format!("{msg_type} payload too short"));
            }

            let mut offset = 8;
            let route_str = read_route_str(payload, &mut offset, "KV")?;
            validate_route(route_str)?;
            Ok(None)
        }
        msg_type::NOTIFY => Err("KV_NOTIFY is server-to-client only".to_string()),
        _ => Err(format!("Unknown KV message type: {msg_type}")),
    }
}

fn parse_commit(payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 route_len][route]
    if payload.len() < 12 {
        return Err("COMMIT payload too short".to_string());
    }

    let mut offset = 0;

    // Read transaction ID (u64)
    let tx_id = u64::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
        payload[offset + 4],
        payload[offset + 5],
        payload[offset + 6],
        payload[offset + 7],
    ]);
    offset += 8;

    // Read route length (u32)
    if offset + 4 > payload.len() {
        return Err("COMMIT route length overflow".to_string());
    }
    let route_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read route
    if offset + route_len > payload.len() {
        return Err("COMMIT route overflow".to_string());
    }
    let route_str = decode_route_str(&payload[offset..offset + route_len], "COMMIT")?;

    // Parse route into realm/area/resource (not used in Commit, but validates wire format)
    validate_route(route_str)?;

    Ok(KvMessage::Commit { tx_id })
}

fn parse_rollback(payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 route_len][route]
    if payload.len() < 12 {
        return Err("ROLLBACK payload too short".to_string());
    }

    let mut offset = 0;

    // Read transaction ID (u64)
    let tx_id = u64::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
        payload[offset + 4],
        payload[offset + 5],
        payload[offset + 6],
        payload[offset + 7],
    ]);
    offset += 8;

    // Read route length (u32)
    if offset + 4 > payload.len() {
        return Err("ROLLBACK route length overflow".to_string());
    }
    let route_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read route
    if offset + route_len > payload.len() {
        return Err("ROLLBACK route overflow".to_string());
    }
    let route_str = decode_route_str(&payload[offset..offset + route_len], "ROLLBACK")?;

    // Parse route into realm/area/resource (not used in Rollback, but validates wire format)
    validate_route(route_str)?;

    Ok(KvMessage::Rollback { tx_id })
}

fn parse_begin(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u32 route_len][route][u8 mode][u8 durability]
    if payload.len() < 6 {
        return Err("BEGIN payload too short".to_string());
    }

    let mut offset = 0;

    // Read route length (u32)
    let route_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read route
    if offset + route_len > payload.len() {
        return Err("BEGIN route overflow".to_string());
    }
    let route_str = decode_route_str(&payload[offset..offset + route_len], "BEGIN")?;
    offset += route_len;

    // Parse route into realm/area/resource
    let (realm, area, resource) = parse_route(route_str)?;

    // Read mode (u8): 0=ReadOnly, 1=ReadWrite
    if offset >= payload.len() {
        return Err("BEGIN mode byte missing".to_string());
    }
    let mode = match payload[offset] {
        0 => TxMode::ReadOnly,
        1 => TxMode::ReadWrite,
        _ => return Err("Invalid transaction mode".to_string()),
    };
    offset += 1;

    // Read durability (u8): 0=buffered, 1=sync (per CLIENT_SPEC)
    if offset >= payload.len() {
        return Err("BEGIN durability byte missing".to_string());
    }
    let write_options = match payload[offset] {
        0 => cntryl_midge::WriteOptions::buffered(),
        1 => cntryl_midge::WriteOptions::sync(),
        value => return Err(format!("Invalid durability mode: {value}")),
    };

    Ok(KvMessage::Begin {
        route_family,
        realm,
        area,
        resource,
        mode,
        write_options,
    })
}

fn parse_subscribe(
    route_family: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
    payload: &[u8],
) -> Result<KvSubscriptionMessage, String> {
    let mut offset = 0;
    let pattern_str = read_route_str(payload, &mut offset, "KV SUBSCRIBE")?;
    validate_route(pattern_str)?;
    if offset != payload.len() {
        return Err("Trailing data in KV SUBSCRIBE payload".to_string());
    }

    Ok(KvSubscriptionMessage::Subscribe {
        family_id: route_family,
        pattern: Route::from_ref(pattern_str),
        session_id,
        subscriber,
    })
}

fn parse_unsubscribe(
    route_family: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
    payload: &[u8],
) -> Result<KvSubscriptionMessage, String> {
    let mut offset = 0;
    let pattern_str = read_route_str(payload, &mut offset, "KV UNSUBSCRIBE")?;
    validate_route(pattern_str)?;
    if offset != payload.len() {
        return Err("Trailing data in KV UNSUBSCRIBE payload".to_string());
    }

    Ok(KvSubscriptionMessage::Unsubscribe {
        family_id: route_family,
        pattern: Route::from_ref(pattern_str),
        session_id,
        subscriber,
    })
}
