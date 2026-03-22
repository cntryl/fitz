//! Transport benchmark helpers
//!
//! Minimal TLV frame builders and parsers reused by tier4 benches.

use crate::testkit::transport::{TlvFrameBuilder, TlvFrameParser};
use bytes::BufMut;
use bytes::Bytes;

/// Extract the single TLV field from a test frame for direct-to-sink delivery.
pub fn extract_single_tlv_field(frame: &[u8]) -> (u16, Bytes) {
    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser.next_field_ref().expect("single TLV field");
    (msg_type, Bytes::copy_from_slice(payload))
}

/// Build KV BEGIN frame (msg_type 100)
pub fn build_kv_begin(route: &str, mode: u8, durability: u8) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u32 BE route_len][route][u8 mode][u8 durability]
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.push(mode);
    payload.push(durability);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(100, &payload);
    builder.build()
}

/// Build KV PUT frame (msg_type 104)
pub fn build_kv_put(tx_id: u64, route: &str, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route][u32 BE key_len][key][u32 BE value_len][value]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
    payload.extend_from_slice(value);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(104, &payload);
    builder.build()
}

/// Build KV ROLLBACK frame (msg_type 102)
pub fn build_kv_rollback(tx_id: u64, route: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(102, &payload);
    builder.build()
}

/// Parse KV response
/// Format: [u8 status][optional data...]
pub fn parse_kv_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][...response data...]
    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        return ((msg_type & 0xFF) as u8, status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Parse KV transaction ID from response (big-endian u64)
pub fn parse_kv_tx_id(data: &[u8]) -> Result<u64, String> {
    // BeginOk format: [u8 status][u64 tx_id]
    if data.len() < 9 {
        return Err(format!(
            "TX ID data too short: {} bytes, need 9",
            data.len()
        ));
    }
    let bytes = [
        data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
    ];
    Ok(u64::from_be_bytes(bytes))
}

/// Build NOTICE PUBLISH frame (msg_type 500)
pub fn build_notice_publish(route: &str, data: &[u8]) -> Vec<u8> {
    // Wire format: [string route][bytes payload]
    let mut buf = Vec::new();

    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());
    buf.put_u32(data.len() as u32);
    buf.put_slice(data);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(500, &buf);
    builder.build()
}

/// Build NOTICE SUBSCRIBE frame (msg_type 501)
pub fn build_notice_subscribe(route_pattern: &str) -> Vec<u8> {
    // Wire format: [string pattern]
    let mut buf = Vec::new();
    buf.put_u32(route_pattern.len() as u32);
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(501, &buf);
    builder.build()
}

/// Parse NOTICE response
pub fn parse_notice_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        let data = if payload.len() > 1 {
            payload[1..].to_vec()
        } else {
            Vec::new()
        };
        return ((msg_type & 0xFF) as u8, status, data);
    }

    (0, 1, Vec::new())
}

/// Build QUEUE ENQUEUE frame (msg_type 200)
pub fn build_queue_enqueue(queue_name: &str, data: &[u8]) -> Vec<u8> {
    // Wire format: [u32 route_len][route][u32 body_len][body][u8 has_delay=0]
    let mut payload = Vec::new();
    payload.extend_from_slice(&(queue_name.len() as u32).to_be_bytes());
    payload.extend_from_slice(queue_name.as_bytes());
    payload.extend_from_slice(&(data.len() as u32).to_be_bytes());
    payload.extend_from_slice(data);
    payload.push(0); // has_delay = false

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(200, &payload);
    builder.build()
}

/// Build QUEUE RESERVE frame (msg_type 202)
pub fn build_queue_dequeue(queue_name: &str) -> Vec<u8> {
    // Wire format: [u32 route_len][route][u64 lease_seconds][u8 has_batch=0][u8 has_wait=0]
    let mut payload = Vec::new();
    payload.extend_from_slice(&(queue_name.len() as u32).to_be_bytes());
    payload.extend_from_slice(queue_name.as_bytes());
    payload.extend_from_slice(&30_u64.to_be_bytes());
    payload.push(0); // has_batch_size = false
    payload.push(0); // has_wait = false

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(202, &payload);
    builder.build()
}

/// Parse QUEUE response
pub fn parse_queue_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        return ((msg_type & 0xFF) as u8, status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Build LEASE ACQUIRE frame (msg_type 400)
pub fn build_lease_acquire_immediate(route: &str, owner_id: &str, ttl_secs: i32) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    buf.put_u32(owner_id.len() as u32);
    buf.put_slice(owner_id.as_bytes());

    buf.put_u64(ttl_secs as u64);
    buf.put_u32(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(400, &buf);
    builder.build()
}

/// Build LEASE RELEASE frame (msg_type 402)
pub fn build_lease_release(route: &str, owner_id: &str, token: u64) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    buf.put_u32(owner_id.len() as u32);
    buf.put_slice(owner_id.as_bytes());

    buf.put_u64(token);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(402, &buf);
    builder.build()
}

/// Build LEASE EXTEND frame (msg_type 401)
pub fn build_lease_extend(route: &str, owner_id: &str, token: u64, ttl_secs: i32) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    buf.put_u32(owner_id.len() as u32);
    buf.put_slice(owner_id.as_bytes());

    buf.put_u64(token);
    buf.put_u64(ttl_secs as u64);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(401, &buf);
    builder.build()
}

/// Build LEASE QUERY frame (msg_type 403)
pub fn build_lease_query(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(403, &buf);
    builder.build()
}

/// Parse LEASE response: (msg_type, status, data)
pub fn parse_lease_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        return ((msg_type & 0xFF) as u8, status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Parse lease token from response data
///
/// Wire format for ACQUIRE success:
/// - data[0]: status (0 = success)
/// - data[1]: response_type (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued)
/// - data[2-9]: fencing_token (u64 big-endian)
pub fn parse_lease_token_response(data: &[u8]) -> Result<u64, String> {
    if data.len() < 2 {
        return Err("Token data too short".to_string());
    }

    let status = data[0];
    if status != 0 {
        return Err("Lease operation failed".to_string());
    }

    // data[1] is response_type (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued)
    // All of these include a fencing token
    let response_type = data[1];
    if response_type > 3 {
        return Err(format!("Invalid response_type: {}", response_type));
    }

    if data.len() < 10 {
        return Err("Token data incomplete".to_string());
    }

    let bytes = [
        data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
    ];
    Ok(u64::from_be_bytes(bytes))
}

/// Parse lease token from EXTEND success response data.
///
/// Wire format:
/// - data[0]: status (0 = success)
/// - data[1..9]: new fencing_token (u64 big-endian)
pub fn parse_lease_extend_token_response(data: &[u8]) -> Result<u64, String> {
    if data.len() < 9 {
        return Err("Extend token data too short".to_string());
    }

    let status = data[0];
    if status != 0 {
        return Err("Lease extend operation failed".to_string());
    }

    let bytes = [
        data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
    ];
    Ok(u64::from_be_bytes(bytes))
}

/// Build RPC SUBSCRIBE frame (msg_type 300)
pub fn build_rpc_subscribe(worker_addr: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(worker_addr.len() as u32);
    buf.put_slice(worker_addr.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(300, &buf);
    builder.build()
}

/// Build RPC REQUEST frame (msg_type 302)
pub fn build_rpc_request(route: &str, payload: &[u8]) -> Vec<u8> {
    use uuid::Uuid;

    let mut buf = Vec::new();
    let uuid = Uuid::new_v4();
    buf.put_u32(16);
    buf.put_slice(uuid.as_bytes());

    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    let reply_route = format!("inbox://session/1/{}", uuid);
    buf.put_u32(reply_route.len() as u32);
    buf.put_slice(reply_route.as_bytes());

    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(302, &buf);
    builder.build()
}

/// Build RPC RESPONSE frame (msg_type 303) from worker to route
pub fn build_rpc_response_frame(correlation_id: uuid::Uuid, body: &[u8]) -> Vec<u8> {
    let resp = crate::domains::rpc::protocol::RpcResponse::single(
        correlation_id,
        bytes::Bytes::from(body.to_vec()),
    );
    let payload = crate::protocol::rpc_codec::encode_response_message(&resp);
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(303, &payload);
    builder.build()
}

/// Build RPC ACK frame (msg_type 304) from worker to route
pub fn build_rpc_ack_frame(correlation_id: uuid::Uuid) -> Vec<u8> {
    let payload = crate::protocol::rpc_codec::encode_ack(&correlation_id);
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(304, &payload);
    builder.build()
}

/// Parse RPC response
pub fn parse_rpc_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        return ((msg_type & 0xFF) as u8, status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Build STREAM BEGIN frame (msg_type 600)
pub fn build_stream_begin(route: &str, expected_offset: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());
    buf.put_u64(expected_offset);
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(600, &buf);
    builder.build()
}

/// Build STREAM APPEND frame (msg_type 601)
pub fn build_stream_append(session_id: u64, data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u64(session_id);
    buf.put_u32(data.len() as u32);
    buf.put_slice(data);
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(601, &buf);
    builder.build()
}

/// Build STREAM COMMIT frame (msg_type 602)
pub fn build_stream_commit(session_id: u64, mode: u8) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u64(session_id);
    buf.put_u8(mode);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(602, &buf);
    builder.build()
}

/// Build STREAM READ frame (msg_type 604)
pub fn build_stream_read(route: &str, start_offset: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());
    buf.put_u64(start_offset);
    buf.put_u64(1000);
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

/// Build STREAM LAST frame (msg_type 605)
pub fn build_stream_last(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(605, &buf);
    builder.build()
}

/// Parse STREAM response
pub fn parse_stream_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        return ((msg_type & 0xFF) as u8, status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Parse session_id from STREAM BEGIN response data
pub fn parse_stream_session_id(data: &[u8]) -> Result<u64, String> {
    if data.len() < 2 {
        return Err("Stream response data too short".to_string());
    }

    let status = data[0];
    if status != 0 {
        return Err("Stream BEGIN operation failed".to_string());
    }

    let has_session_id = data[1];
    if has_session_id == 0 {
        return Err("No session_id in response".to_string());
    }

    if data.len() < 10 {
        return Err("Session ID data incomplete".to_string());
    }

    let bytes = [
        data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
    ];
    Ok(u64::from_be_bytes(bytes))
}

/// Build SCHEDULE CREATE frame (msg_type 700)
pub fn build_schedule_create(route: &str, cron: &str, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    buf.put_u32(cron.len() as u32);
    buf.put_slice(cron.as_bytes());

    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(700, &buf);
    builder.build()
}

/// Parse SCHEDULE response
pub fn parse_schedule_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        return ((msg_type & 0xFF) as u8, status, payload.to_vec());
    }

    (0, 1, Vec::new())
}
