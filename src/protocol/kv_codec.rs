//! KV domain TLV message types and codec

use crate::domains::kv::KvError;
use crate::domains::kv::{
    KvMessage, KvNotification, KvResponse, KvSubscriptionMessage, ScanQuery, TxMode,
};
use crate::protocol::frame_context::FrameContext;
use crate::runtime::routing::{route_exact_triplet, Route, RouteAddress, RouteFamily};
use bytes::Bytes;

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
        _ => Err(format!("Unknown KV message type: {}", msg_type)),
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

fn decode_route_str<'a>(route_bytes: &'a [u8], op_name: &str) -> Result<&'a str, String> {
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
            "Route must be realm/area/resource, got '{}'",
            route_str
        )),
    }
}

fn parse_route_resource(route_str: &str) -> Result<String, String> {
    match split_route(route_str) {
        Some((_realm, _area, resource)) => Ok(resource.to_string()),
        None => Err(format!(
            "Route must be realm/area/resource, got '{}'",
            route_str
        )),
    }
}

fn validate_route(route_str: &str) -> Result<(), String> {
    if split_route(route_str).is_some() {
        Ok(())
    } else {
        Err(format!(
            "Route must be realm/area/resource, got '{}'",
            route_str
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
                return Err(format!("{} payload too short", msg_type));
            }

            let mut offset = 8;
            let route_str = read_route_str(payload, &mut offset, "KV")?;
            validate_route(route_str)?;
            Ok(None)
        }
        msg_type::NOTIFY => Err("KV_NOTIFY is server-to-client only".to_string()),
        _ => Err(format!("Unknown KV message type: {}", msg_type)),
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
        value => return Err(format!("Invalid durability mode: {}", value)),
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

pub fn encode_notify(subscription_id: u64, route: &Route, notification: KvNotification) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u64(subscription_id);
    buf.put_u32(route.as_str().len() as u32);
    buf.put_slice(route.as_str().as_bytes());
    buf.put_u64(notification.mutation_count);
    buf
}

fn parse_get(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 route_len][route][u32 key_len][key]
    if payload.len() < 16 {
        return Err("GET payload too short".to_string());
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
        return Err("GET route length overflow".to_string());
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
        return Err("GET route overflow".to_string());
    }
    let route_str = decode_route_str(&payload[offset..offset + route_len], "GET")?;
    offset += route_len;

    // Parse route into realm/area/resource (only resource used in KvMessage)
    let resource = parse_route_resource(route_str)?;

    // Read key length (u32)
    if offset + 4 > payload.len() {
        return Err("GET key length overflow".to_string());
    }
    let key_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read key
    if offset + key_len > payload.len() {
        return Err("GET key overflow".to_string());
    }
    let key = Bytes::copy_from_slice(&payload[offset..offset + key_len]);

    Ok(KvMessage::Get {
        tx_id,
        route_family,
        resource,
        key,
    })
}

fn parse_put(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 route_len][route][u32 key_len][key][u32 value_len][value]
    if payload.len() < 20 {
        return Err("PUT payload too short".to_string());
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
        return Err("PUT route length overflow".to_string());
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
        return Err("PUT route overflow".to_string());
    }
    let route_str = decode_route_str(&payload[offset..offset + route_len], "PUT")?;
    offset += route_len;

    // Parse route into realm/area/resource (only resource used in KvMessage)
    let resource = parse_route_resource(route_str)?;

    // Read key length (u32)
    if offset + 4 > payload.len() {
        return Err("PUT key length overflow".to_string());
    }
    let key_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read key
    if offset + key_len > payload.len() {
        return Err("PUT key overflow".to_string());
    }
    let key = Bytes::copy_from_slice(&payload[offset..offset + key_len]);
    offset += key_len;

    // Read value length (u32)
    if offset + 4 > payload.len() {
        return Err("PUT value length overflow".to_string());
    }
    let value_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read value
    if offset + value_len > payload.len() {
        return Err("PUT value overflow".to_string());
    }
    let value = Bytes::copy_from_slice(&payload[offset..offset + value_len]);

    Ok(KvMessage::Put {
        tx_id,
        route_family,
        resource,
        key,
        value,
    })
}

fn parse_insert(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 route_len][route][u32 key_len][key][u32 value_len][value]
    if payload.len() < 20 {
        return Err("INSERT payload too short".to_string());
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
        return Err("INSERT route length overflow".to_string());
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
        return Err("INSERT route overflow".to_string());
    }
    let route_str = decode_route_str(&payload[offset..offset + route_len], "INSERT")?;
    offset += route_len;

    // Parse route into realm/area/resource (only resource used in KvMessage)
    let resource = parse_route_resource(route_str)?;

    // Read key length (u32)
    if offset + 4 > payload.len() {
        return Err("INSERT key length overflow".to_string());
    }
    let key_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read key
    if offset + key_len > payload.len() {
        return Err("INSERT key overflow".to_string());
    }
    let key = Bytes::copy_from_slice(&payload[offset..offset + key_len]);
    offset += key_len;

    // Read value length (u32)
    if offset + 4 > payload.len() {
        return Err("INSERT value length overflow".to_string());
    }
    let value_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read value
    if offset + value_len > payload.len() {
        return Err("INSERT value overflow".to_string());
    }
    let value = Bytes::copy_from_slice(&payload[offset..offset + value_len]);

    Ok(KvMessage::Insert {
        tx_id,
        route_family,
        resource,
        key,
        value,
    })
}

fn parse_delete(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 route_len][route][u32 key_len][key]
    if payload.len() < 16 {
        return Err("DELETE payload too short".to_string());
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
        return Err("DELETE route length overflow".to_string());
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
        return Err("DELETE route overflow".to_string());
    }
    let route_str = decode_route_str(&payload[offset..offset + route_len], "DELETE")?;
    offset += route_len;

    // Parse route into realm/area/resource (only resource used in KvMessage)
    let resource = parse_route_resource(route_str)?;

    // Read key length (u32)
    if offset + 4 > payload.len() {
        return Err("DELETE key length overflow".to_string());
    }
    let key_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read key
    if offset + key_len > payload.len() {
        return Err("DELETE key overflow".to_string());
    }
    let key = Bytes::copy_from_slice(&payload[offset..offset + key_len]);

    Ok(KvMessage::Delete {
        tx_id,
        route_family,
        resource,
        key,
    })
}

fn parse_delete_range(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 route_len][route][u32 start_len][start][u32 end_len][end]
    if payload.len() < 20 {
        return Err("DELETE_RANGE payload too short".to_string());
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
        return Err("DELETE_RANGE route length overflow".to_string());
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
        return Err("DELETE_RANGE route overflow".to_string());
    }
    let route_str = decode_route_str(&payload[offset..offset + route_len], "DELETE_RANGE")?;
    offset += route_len;

    // Parse route into realm/area/resource (only resource used in KvMessage)
    let resource = parse_route_resource(route_str)?;

    // Read start key length (u32)
    if offset + 4 > payload.len() {
        return Err("DELETE_RANGE start key length overflow".to_string());
    }
    let start_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read start key
    if offset + start_len > payload.len() {
        return Err("DELETE_RANGE start key overflow".to_string());
    }
    let start = Bytes::copy_from_slice(&payload[offset..offset + start_len]);
    offset += start_len;

    // Read end key length (u32)
    if offset + 4 > payload.len() {
        return Err("DELETE_RANGE end key length overflow".to_string());
    }
    let end_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read end key
    if offset + end_len > payload.len() {
        return Err("DELETE_RANGE end key overflow".to_string());
    }
    let end = Bytes::copy_from_slice(&payload[offset..offset + end_len]);

    Ok(KvMessage::DeleteRange {
        tx_id,
        route_family,
        resource,
        start,
        end,
    })
}

fn parse_scan(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 route_len][route][u8 has_start][start?][u8 has_end][end?][u8 has_limit][limit?][u8 reverse]
    if payload.len() < 15 {
        return Err("SCAN payload too short".to_string());
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
        return Err("SCAN route length overflow".to_string());
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
        return Err("SCAN route overflow".to_string());
    }
    let route_str = decode_route_str(&payload[offset..offset + route_len], "SCAN")?;
    offset += route_len;

    // Parse route into realm/area/resource (only resource used in KvMessage)
    let resource = parse_route_resource(route_str)?;

    // Read start key option (u8): 0=None, 1=Some
    if offset >= payload.len() {
        return Err("SCAN start key option byte missing".to_string());
    }
    let start = if payload[offset] == 1 {
        offset += 1;
        // Read start key length (u32)
        if offset + 4 > payload.len() {
            return Err("SCAN start key length overflow".to_string());
        }
        let len = u32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]) as usize;
        offset += 4;
        // Read start key
        if offset + len > payload.len() {
            return Err("SCAN start key overflow".to_string());
        }
        let key = Bytes::copy_from_slice(&payload[offset..offset + len]);
        offset += len;
        Some(key)
    } else {
        offset += 1;
        None
    };

    // Read end key option (u8): 0=None, 1=Some
    if offset >= payload.len() {
        return Err("SCAN end key option byte missing".to_string());
    }
    let end = if payload[offset] == 1 {
        offset += 1;
        // Read end key length (u32)
        if offset + 4 > payload.len() {
            return Err("SCAN end key length overflow".to_string());
        }
        let len = u32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]) as usize;
        offset += 4;
        // Read end key
        if offset + len > payload.len() {
            return Err("SCAN end key overflow".to_string());
        }
        let key = Bytes::copy_from_slice(&payload[offset..offset + len]);
        offset += len;
        Some(key)
    } else {
        offset += 1;
        None
    };

    // Read limit option (u8): 0=None, 1=Some
    let limit = if payload.len() > offset && payload[offset] == 1 {
        offset += 1;
        if offset + 4 > payload.len() {
            return Err("SCAN limit overflow".to_string());
        }
        let l = u32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]) as usize;
        offset += 4;
        Some(l)
    } else {
        if payload.len() > offset {
            offset += 1;
        }
        None
    };

    // Read reverse flag (u8)
    let reverse = if payload.len() > offset {
        payload[offset] != 0
    } else {
        false
    };

    Ok(KvMessage::Scan {
        tx_id,
        route_family,
        resource,
        query: ScanQuery {
            start,
            end,
            limit,
            reverse,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn should_parse_begin_read_write_buffered() {
        // Arrange
        let route = "kv://acme/kv/users";
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u8(1); // ReadWrite
        payload.put_u8(0); // buffered (per CLIENT_SPEC: 0=buffered, 1=sync)

        // Act
        let result = parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload);

        // Assert
        assert!(matches!(result, Ok(KvMessage::Begin { .. })));
    }

    #[test]
    fn should_parse_get_with_key() {
        // Arrange
        let route = "kv://acme/kv/users";
        let key = b"user:1001";
        let mut payload = Vec::new();
        payload.put_u64(1); // tx_id
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u32(key.len() as u32);
        payload.put_slice(key);

        // Act
        let result = parse_request(msg_type::GET, RouteFamily::new(1), &payload);

        // Assert
        assert!(matches!(result, Ok(KvMessage::Get { tx_id: 1, .. })));
    }

    #[test]
    fn should_encode_get_result_found() {
        // Arrange
        let response = KvResponse::GetResult {
            found: true,
            value: Some(Bytes::from("test_value")),
        };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 0); // status: success
        assert_eq!(encoded[1], 1); // found flag
    }

    #[test]
    fn should_encode_get_result_not_found() {
        // Arrange
        let response = KvResponse::GetResult {
            found: false,
            value: None,
        };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 0); // status: success
        assert_eq!(encoded[1], 0); // not found flag
    }

    #[test]
    fn should_parse_begin_with_sync_durability() {
        // Arrange - Per CLIENT_SPEC, durability byte: 0=buffered, 1=sync
        let route = "kv://acme/kv/users";
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u8(1); // ReadWrite
        payload.put_u8(1); // sync durability (per CLIENT_SPEC: 1=sync)

        // Act
        let result = parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload);

        // Assert
        match result {
            Ok(KvMessage::Begin { write_options, .. }) => {
                // Verify that durability byte 1 maps to sync
                assert!(
                    write_options.is_sync(),
                    "Durability byte 1 should map to sync"
                );
            }
            _ => panic!("Expected KvMessage::Begin with sync write options"),
        }
    }

    #[test]
    fn should_parse_begin_with_buffered_durability() {
        // Arrange - Per CLIENT_SPEC, durability byte: 0=buffered, 1=sync
        let route = "kv://acme/kv/users";
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u8(1); // ReadWrite
        payload.put_u8(0); // buffered durability (per CLIENT_SPEC: 0=buffered)

        // Act
        let result = parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload);

        // Assert
        match result {
            Ok(KvMessage::Begin { write_options, .. }) => {
                // Verify that durability byte 0 maps to buffered
                assert!(
                    !write_options.is_sync(),
                    "Durability byte 0 should map to buffered"
                );
            }
            _ => panic!("Expected KvMessage::Begin with buffered write options"),
        }
    }

    #[test]
    fn should_reject_begin_with_invalid_durability() {
        // Arrange
        let route = "kv://acme/kv/users";
        let mut base_payload = Vec::new();
        base_payload.put_u32(route.len() as u32);
        base_payload.put_slice(route.as_bytes());
        base_payload.put_u8(1); // ReadWrite

        // Act
        let results = [2_u8, 255_u8]
            .into_iter()
            .map(|durability| {
                let mut payload = base_payload.clone();
                payload.put_u8(durability);
                parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload)
            })
            .collect::<Vec<_>>();

        // Assert
        assert!(results.iter().all(Result::is_err));
        assert!(results.iter().all(|result| result
            .as_ref()
            .unwrap_err()
            .contains("Invalid durability mode")));
    }

    #[test]
    fn should_reject_begin_with_too_few_route_segments() {
        // Arrange
        let route = "kv://acme/kv";
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u8(1); // ReadWrite
        payload.put_u8(0); // buffered

        // Act
        let result = parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_begin_given_nested_resource_path() {
        // Arrange
        let route = "kv://acme/kv/users/by/id";
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u8(1);
        payload.put_u8(0);

        // Act
        let result = parse_request(msg_type::BEGIN, RouteFamily::new(1), &payload);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_encode_subscribe_ok_response() {
        // Arrange
        let response = KvResponse::SubscribeOk { subscription_id: 9 };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert_eq!(encoded.len(), 9);
        assert_eq!(encoded[0], 0);
        assert_eq!(u64::from_be_bytes(encoded[1..9].try_into().unwrap()), 9);
    }
}
