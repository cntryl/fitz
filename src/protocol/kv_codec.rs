//! KV domain TLV message types and codec

use crate::domains::kv::{KvMessage, KvResponse, ScanQuery, TxMode};
use crate::runtime::routing::RouteFamily;
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
}

/// Parse KV request from bytes
pub fn parse_request(
    msg_type: u16,
    route_family: RouteFamily,
    realm: String,
    area: String,
    payload: &[u8],
) -> Result<KvMessage, String> {
    match msg_type {
        msg_type::BEGIN => parse_begin(route_family, realm, area, payload),
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
            buf.put_u64(*tx_id);
        }
        KvResponse::CommitOk => {
            // Empty response for commit ok
        }
        KvResponse::RollbackOk => {
            // Empty response for rollback ok
        }
        KvResponse::GetResult { found, value } => {
            buf.put_u8(if *found { 1 } else { 0 });
            if let Some(v) = value {
                buf.put_u32(v.len() as u32);
                buf.put_slice(v);
            } else {
                buf.put_u32(0);
            }
        }
        KvResponse::PutOk => {
            // Empty response for put ok
        }
        KvResponse::InsertOk => {
            // Empty response for insert ok
        }
        KvResponse::DeleteOk => {
            // Empty response for delete ok
        }
        KvResponse::DeleteRangeOk => {
            // Empty response for delete range ok
        }
        KvResponse::ScanResult { items, has_more } => {
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
            let error_msg = error.to_string();
            buf.put_u32(error_msg.len() as u32);
            buf.put_slice(error_msg.as_bytes());
        }
    }
    buf
}

// ===== Parsers =====

fn parse_commit(payload: &[u8]) -> Result<KvMessage, String> {
    if payload.len() < 8 {
        return Err("COMMIT payload too short".to_string());
    }

    let tx_id = u64::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);

    Ok(KvMessage::Commit { tx_id })
}

fn parse_rollback(payload: &[u8]) -> Result<KvMessage, String> {
    if payload.len() < 8 {
        return Err("ROLLBACK payload too short".to_string());
    }

    let tx_id = u64::from_be_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);

    Ok(KvMessage::Rollback { tx_id })
}

fn parse_begin(
    route_family: RouteFamily,
    realm: String,
    area: String,
    payload: &[u8],
) -> Result<KvMessage, String> {
    if payload.len() < 10 {
        return Err("BEGIN payload too short".to_string());
    }

    let mut offset = 0;

    // Read resource name length (u32)
    let resource_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Read resource name
    if offset + resource_len > payload.len() {
        return Err("BEGIN resource name overflow".to_string());
    }
    let resource_raw = String::from_utf8(payload[offset..offset + resource_len].to_vec())
        .map_err(|_| "Invalid UTF-8 in resource name".to_string())?;
    offset += resource_len;

    // If realm/area were not provided by the caller, extract from resource path.
    // The client encodes resource as "realm/area/resource".
    let (realm, area, resource) = if realm.is_empty() {
        let parts: Vec<&str> = resource_raw.splitn(3, '/').collect();
        if parts.len() == 3 {
            (
                parts[0].to_string(),
                parts[1].to_string(),
                parts[2].to_string(),
            )
        } else {
            return Err(format!(
                "BEGIN resource path must be realm/area/resource, got '{}'",
                resource_raw
            ));
        }
    } else {
        (realm, area, resource_raw)
    };

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

    // Read durability (u8): 0=synced, 1=buffered
    if offset >= payload.len() {
        return Err("BEGIN durability byte missing".to_string());
    }
    let write_options = if payload[offset] == 0 {
        cntryl_midge::WriteOptions::sync()
    } else {
        cntryl_midge::WriteOptions::buffered()
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

fn parse_get(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 key_len][key]
    // Resource is implicit from transaction context (established at BEGIN).
    if payload.len() < 12 {
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
        resource: String::new(),
        key,
    })
}

fn parse_put(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 key_len][key][u32 value_len][value]
    // Resource is implicit from transaction context (established at BEGIN).
    if payload.len() < 16 {
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
        resource: String::new(),
        key,
        value,
    })
}

fn parse_insert(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 key_len][key][u32 value_len][value]
    // Resource is implicit from transaction context (established at BEGIN).
    if payload.len() < 16 {
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
        resource: String::new(),
        key,
        value,
    })
}

fn parse_delete(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 key_len][key]
    // Resource is implicit from transaction context (established at BEGIN).
    if payload.len() < 12 {
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
        resource: String::new(),
        key,
    })
}

fn parse_delete_range(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u32 start_len][start][u32 end_len][end]
    // Resource is implicit from transaction context (established at BEGIN).
    if payload.len() < 16 {
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
        resource: String::new(),
        start,
        end,
    })
}

fn parse_scan(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    // Wire format per CLIENT_SPEC: [u64 tx_id][u8 start_opt][start?][u8 end_opt][end?][u8 limit_opt][limit?][u8 reverse]
    // Resource is implicit from transaction context (established at BEGIN).
    if payload.len() < 11 {
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
        resource: String::new(),
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
        let resource = "users";
        let mut payload = Vec::new();
        payload.put_u32(resource.len() as u32);
        payload.put_slice(resource.as_bytes());
        payload.put_u8(1); // ReadWrite
        payload.put_u8(1); // buffered

        // Act
        let result = parse_request(
            msg_type::BEGIN,
            RouteFamily::new(1),
            "acme".to_string(),
            "kv".to_string(),
            &payload,
        );

        // Assert
        assert!(matches!(result, Ok(KvMessage::Begin { .. })));
    }

    #[test]
    fn should_parse_get_with_key() {
        // Arrange
        let key = b"user:1001";
        let mut payload = Vec::new();
        payload.put_u64(1); // tx_id
        payload.put_u32(key.len() as u32);
        payload.put_slice(key);

        // Act
        let result = parse_request(
            msg_type::GET,
            RouteFamily::new(1),
            "acme".to_string(),
            "kv".to_string(),
            &payload,
        );

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
        assert_eq!(encoded[0], 1); // found flag
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
        assert_eq!(encoded[0], 0); // not found flag
    }
}
