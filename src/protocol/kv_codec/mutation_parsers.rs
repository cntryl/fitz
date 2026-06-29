use bytes::Bytes;

use crate::domains::kv::{KvMessage, KvNotification, ScanQuery};
use crate::protocol::kv_codec::frame_and_routes::{decode_route_str, parse_route_resource};
use crate::runtime::routing::{Route, RouteFamily};

pub fn encode_notify(subscription_id: u64, route: &Route, notification: KvNotification) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u64(subscription_id);
    buf.put_u32(route.as_str().len() as u32);
    buf.put_slice(route.as_str().as_bytes());
    buf.put_u64(notification.mutation_count);
    buf
}

pub(super) fn parse_get(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
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

pub(super) fn parse_put(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
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

pub(super) fn parse_insert(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
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

pub(super) fn parse_delete(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
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

pub(super) fn parse_delete_range(
    route_family: RouteFamily,
    payload: &[u8],
) -> Result<KvMessage, String> {
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

pub(super) fn parse_scan(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
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
