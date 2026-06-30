//! Transport benchmark helpers
//!
//! Minimal TLV frame builders and parsers reused by tier4 benches.

use crate::testkit::transport::{TlvFrameBuilder, TlvFrameParser};
use bytes::BufMut;
use bytes::Bytes;

#[inline]
fn u32_len(value: usize) -> u32 {
    u32::try_from(value)
        .unwrap_or_else(|_| panic!("transport frame length exceeds u32::MAX: {value}"))
}

#[inline]
fn u64_from_i32(value: i32) -> u64 {
    u64::try_from(value).unwrap_or_else(|_| panic!("negative TTL seconds are invalid: {value}"))
}

#[inline]
fn msg_type_to_u8(msg_type: u16) -> u8 {
    msg_type.to_le_bytes()[0]
}

#[inline]
fn u32_to_usize(value: u32) -> usize {
    usize::try_from(value).unwrap_or_else(|_| panic!("u32 value does not fit usize: {value}"))
}

/// Extract the single TLV field from a test frame for direct-to-sink delivery.
/// # Panics
/// This function panics when the frame does not contain a single TLV field.
#[must_use]
pub fn extract_single_tlv_field(frame: &[u8]) -> (u16, Bytes) {
    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser.next_field_ref().expect("single TLV field");
    (msg_type, Bytes::copy_from_slice(payload))
}

/// Build KV BEGIN frame (`msg_type` 100)
#[must_use]
pub fn build_kv_begin(route: &str, mode: u8, durability: u8) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u32 BE route_len][route][u8 mode][u8 durability]
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.push(mode);
    payload.push(durability);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(100, &payload);
    builder.build()
}

/// Build KV PUT frame (`msg_type` 104)
#[must_use]
pub fn build_kv_put(tx_id: u64, route: &str, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route][u32 BE key_len][key][u32 BE value_len][value]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&(u32_len(key.len())).to_be_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(u32_len(value.len())).to_be_bytes());
    payload.extend_from_slice(value);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(104, &payload);
    builder.build()
}

/// Build KV COMMIT frame (`msg_type` 101)
#[must_use]
pub fn build_kv_commit(tx_id: u64, route: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(101, &payload);
    builder.build()
}

/// Build KV ROLLBACK frame (`msg_type` 102)
#[must_use]
pub fn build_kv_rollback(tx_id: u64, route: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(u32_len(route.len())).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(102, &payload);
    builder.build()
}

/// Parse KV response
/// Format: [u8 status][optional data...]
#[must_use]
pub fn parse_kv_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][...response data...]
    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        return (msg_type_to_u8(msg_type), status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Parse KV transaction ID from response (big-endian u64).
///
/// # Errors
/// Returns an error when the payload is shorter than 9 bytes.
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

/// Build NOTICE PUBLISH frame (`msg_type` 500)
#[must_use]
pub fn build_notice_publish(route: &str, data: &[u8]) -> Vec<u8> {
    // Wire format: [string route][bytes payload]
    let mut buf = Vec::new();

    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());
    buf.put_u32(u32_len(data.len()));
    buf.put_slice(data);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(500, &buf);
    builder.build()
}

/// Build NOTICE SUBSCRIBE frame (`msg_type` 501)
#[must_use]
pub fn build_notice_subscribe(route_pattern: &str) -> Vec<u8> {
    // Wire format: [string pattern]
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route_pattern.len()));
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(501, &buf);
    builder.build()
}

/// Build NOTICE UNSUBSCRIBE frame (`msg_type` 502)
#[must_use]
pub fn build_notice_unsubscribe(subscription_id: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u64(subscription_id);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(502, &buf);
    builder.build()
}

/// Parse NOTICE response
#[must_use]
pub fn parse_notice_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        let data = if payload.len() > 1 {
            payload[1..].to_vec()
        } else {
            Vec::new()
        };
        return (msg_type_to_u8(msg_type), status, data);
    }

    (0, 1, Vec::new())
}

/// Parse the response from a notice subscription operation.
///
/// # Errors
/// Returns an error when trailing bytes remain in the payload.
pub fn parse_notice_subscription_id(data: &[u8]) -> Result<Option<u64>, String> {
    use crate::protocol::payload_codec::PayloadDecoder;

    let mut decoder = PayloadDecoder::new(data);
    let subscription_id = decoder.get_optional_u64()?;
    if !decoder.is_complete() {
        return Err("Trailing data in notice subscription response".to_string());
    }

    Ok(subscription_id)
}

pub struct NoticeDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub body: Vec<u8>,
}

/// Parse notice delivery payload into a [`NoticeDelivery`] record.
///
/// # Errors
/// Returns an error if the frame does not contain a notice delivery message or
/// if the payload is malformed.
pub fn parse_notice_delivery(frame: &[u8]) -> Result<NoticeDelivery, String> {
    use crate::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field_ref()
        .ok_or_else(|| "Missing notice delivery frame".to_string())?;
    if msg_type != 504 {
        return Err(format!("Unexpected notice delivery msg_type: {msg_type}"));
    }

    let mut dec = PayloadDecoder::new(payload);
    let subscription_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let body = dec.get_bytes()?.to_vec();
    if !dec.is_complete() {
        return Err("Trailing data in notice delivery".to_string());
    }

    Ok(NoticeDelivery {
        msg_type,
        subscription_id,
        route,
        body,
    })
}

/// Build QUEUE ENQUEUE frame (`msg_type` 200)
#[must_use]
pub fn build_queue_enqueue(queue_name: &str, data: &[u8]) -> Vec<u8> {
    // Wire format: [u32 route_len][route][u32 body_len][body][u8 has_delay=0]
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(queue_name.len())).to_be_bytes());
    payload.extend_from_slice(queue_name.as_bytes());
    payload.extend_from_slice(&(u32_len(data.len())).to_be_bytes());
    payload.extend_from_slice(data);
    payload.push(0); // has_delay = false

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(200, &payload);
    builder.build()
}

/// Build QUEUE RESERVE frame (`msg_type` 202)
#[must_use]
pub fn build_queue_dequeue(queue_name: &str) -> Vec<u8> {
    // Wire format: [u32 route_len][route][u64 lease_seconds][u8 has_batch=0]
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(queue_name.len())).to_be_bytes());
    payload.extend_from_slice(queue_name.as_bytes());
    payload.extend_from_slice(&30_u64.to_be_bytes());
    payload.push(0); // has_batch_size = false

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(202, &payload);
    builder.build()
}

/// Build QUEUE WATCH frame (`msg_type` 207).
#[must_use]
pub fn build_queue_watch(queue_name: &str) -> Vec<u8> {
    let pattern = if queue_name.contains('*') || queue_name.ends_with("/ready") {
        queue_name.to_string()
    } else {
        format!("{queue_name}/ready")
    };
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(pattern.len())).to_be_bytes());
    payload.extend_from_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(207, &payload);
    builder.build()
}

/// Build QUEUE RESERVE frame (`msg_type` 202) with an explicit batch size.
#[must_use]
pub fn build_queue_dequeue_batch(queue_name: &str, batch_size: u32) -> Vec<u8> {
    // Wire format: [u32 route_len][route][u64 lease_seconds][u8 has_batch=1][u32 batch]
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(queue_name.len())).to_be_bytes());
    payload.extend_from_slice(queue_name.as_bytes());
    payload.extend_from_slice(&30_u64.to_be_bytes());
    payload.push(1); // has_batch_size = true
    payload.extend_from_slice(&batch_size.to_be_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(202, &payload);
    builder.build()
}

/// Build QUEUE COMPLETE frame (`msg_type` 204)
#[must_use]
pub fn build_queue_complete(queue_name: &str, message_id: u64, token: u64) -> Vec<u8> {
    // Wire format: [u32 route_len][route][u64 message_id][u64 inflight_token]
    let mut payload = Vec::new();
    payload.extend_from_slice(&(u32_len(queue_name.len())).to_be_bytes());
    payload.extend_from_slice(queue_name.as_bytes());
    payload.extend_from_slice(&message_id.to_be_bytes());
    payload.extend_from_slice(&token.to_be_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(204, &payload);
    builder.build()
}

/// Parse QUEUE response
#[must_use]
pub fn parse_queue_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        return (msg_type_to_u8(msg_type), status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Build LEASE ACQUIRE frame (`msg_type` 400)
#[must_use]
pub fn build_lease_acquire_immediate(route: &str, owner_id: &str, ttl_secs: i32) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    buf.put_u32(u32_len(owner_id.len()));
    buf.put_slice(owner_id.as_bytes());

    buf.put_u64(u64_from_i32(ttl_secs));
    buf.put_u32(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(400, &buf);
    builder.build()
}

/// Build LEASE RELEASE frame (`msg_type` 402)
#[must_use]
pub fn build_lease_release(route: &str, owner_id: &str, token: u64) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    buf.put_u32(u32_len(owner_id.len()));
    buf.put_slice(owner_id.as_bytes());

    buf.put_u64(token);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(402, &buf);
    builder.build()
}

/// Build LEASE EXTEND frame (`msg_type` 401)
#[must_use]
pub fn build_lease_extend(route: &str, owner_id: &str, token: u64, ttl_secs: i32) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    buf.put_u32(u32_len(owner_id.len()));
    buf.put_slice(owner_id.as_bytes());

    buf.put_u64(token);
    buf.put_u64(u64_from_i32(ttl_secs));

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(401, &buf);
    builder.build()
}

/// Build LEASE QUERY frame (`msg_type` 403)
#[must_use]
pub fn build_lease_query(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(403, &buf);
    builder.build()
}

/// Parse LEASE response: (`msg_type`, `status`, `data`)
#[must_use]
pub fn parse_lease_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        return (msg_type_to_u8(msg_type), status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Parse lease token from response data
///
/// Wire format for ACQUIRE success:
/// - data[0]: status (0 = success)
/// - `data[1]`: `response_type` (`0=Acquired`, `1=AlreadyHeld`, `2=Queued`, `3=AlreadyQueued`)
/// - `data[2-9]`: `fencing_token` (`u64` big-endian)
///
/// # Errors
/// Returns an error when:
/// - payload is shorter than 10 bytes,
/// - status is non-zero,
/// - or response type is outside the expected 0..=3 range.
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
        return Err(format!("Invalid response_type: {response_type}"));
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
/// - `data[1..9]`: `new_fencing_token` (`u64` big-endian)
///
/// # Errors
/// Returns an error when the payload is shorter than 9 bytes or status is non-zero.
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

/// Build RPC SUBSCRIBE frame (`msg_type` 300)
#[must_use]
pub fn build_rpc_subscribe(worker_addr: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(worker_addr.len()));
    buf.put_slice(worker_addr.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(300, &buf);
    builder.build()
}

/// Build RPC REQUEST frame (`msg_type` 302)
#[must_use]
pub fn build_rpc_request(route: &str, payload: &[u8]) -> Vec<u8> {
    use uuid::Uuid;

    let mut buf = Vec::new();
    let uuid = Uuid::new_v4();
    buf.put_u32(16);
    buf.put_slice(uuid.as_bytes());

    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let reply_route = format!("inbox://session/1/{uuid}");
    buf.put_u32(u32_len(reply_route.len()));
    buf.put_slice(reply_route.as_bytes());

    buf.put_u32(u32_len(payload.len()));
    buf.put_slice(payload);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(302, &buf);
    builder.build()
}

/// Build RPC RESPONSE frame (`msg_type` 303) from worker to route
#[must_use]
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

/// Build RPC ACK frame (`msg_type` 304) from worker to route
#[must_use]
pub fn build_rpc_ack_frame(correlation_id: uuid::Uuid) -> Vec<u8> {
    let payload = crate::protocol::rpc_codec::encode_ack(&correlation_id);
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(304, &payload);
    builder.build()
}

/// Parse RPC response
#[must_use]
pub fn parse_rpc_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        return (msg_type_to_u8(msg_type), status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Build STREAM BEGIN frame (`msg_type` 600)
#[must_use]
pub fn build_stream_begin(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(600, &buf);
    builder.build()
}

/// Build STREAM APPEND frame (`msg_type` 601)
#[must_use]
pub fn build_stream_append(session_id: u64, expected_offset: u64, data: &[u8]) -> Vec<u8> {
    build_stream_append_with_metadata(session_id, expected_offset, data, None)
}

/// Build STREAM APPEND frame (`msg_type` 601) with optional metadata.
#[must_use]
pub fn build_stream_append_with_metadata(
    session_id: u64,
    expected_offset: u64,
    data: &[u8],
    metadata: Option<&[u8]>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u64(session_id);
    buf.put_u64(expected_offset);
    buf.put_u32(u32_len(data.len()));
    buf.put_slice(data);
    match metadata {
        Some(metadata) => {
            buf.put_u8(1);
            buf.put_u32(u32_len(metadata.len()));
            buf.put_slice(metadata);
        }
        None => buf.put_u8(0),
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(601, &buf);
    builder.build()
}

/// Build STREAM COMMIT frame (`msg_type` 602)
#[must_use]
pub fn build_stream_commit(session_id: u64, mode: u8) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u64(session_id);
    buf.put_u8(mode);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(602, &buf);
    builder.build()
}

/// Build STREAM READ frame (`msg_type` 604)
#[must_use]
pub fn build_stream_read(route: &str, start_offset: u64) -> Vec<u8> {
    build_stream_read_with_limit(route, start_offset, 1000)
}

/// Build STREAM READ frame (`msg_type` 604) with a caller-provided limit.
#[must_use]
pub fn build_stream_read_with_limit(route: &str, start_offset: u64, limit: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());
    buf.put_u64(start_offset);
    buf.put_u64(limit);
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

/// Build STREAM LAST frame (`msg_type` 605)
#[must_use]
pub fn build_stream_last(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(605, &buf);
    builder.build()
}

/// Build STREAM `GET_METADATA` frame (`msg_type` 606)
#[must_use]
pub fn build_stream_get_metadata(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(606, &buf);
    builder.build()
}

/// Build STREAM SUBSCRIBE frame (`msg_type` 607)
#[must_use]
pub fn build_stream_subscribe(route_pattern: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route_pattern.len()));
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(607, &buf);
    builder.build()
}

/// Parse STREAM response
#[must_use]
pub fn parse_stream_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        return (msg_type_to_u8(msg_type), status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Parse `session_id` from STREAM BEGIN response data
///
/// # Errors
/// Returns an error if the response indicates failure or is not structurally valid.
pub fn parse_stream_session_id(data: &[u8]) -> Result<u64, String> {
    use crate::protocol::payload_codec::PayloadDecoder;

    let mut decoder = PayloadDecoder::new(data);
    let status = decoder.get_u8()?;
    if status != 0 {
        let error = decoder
            .get_string()
            .unwrap_or_else(|_| "Stream BEGIN operation failed".to_string());
        return Err(error);
    }

    let session_id = decoder
        .get_optional_u64()?
        .ok_or_else(|| "No session_id in response".to_string())?;
    decoder.get_bytes()?;
    if !decoder.is_complete() {
        return Err("Trailing data in stream BEGIN response".to_string());
    }

    Ok(session_id)
}

fn decode_stream_ok_data(payload: &[u8]) -> Result<Bytes, String> {
    use crate::protocol::payload_codec::PayloadDecoder;

    let mut decoder = PayloadDecoder::new(payload);
    let status = decoder.get_u8()?;
    if status != 0 {
        let error = decoder
            .get_string()
            .unwrap_or_else(|_| format!("stream response failed with status {status}"));
        return Err(error);
    }

    let _session_id = decoder.get_optional_u64()?;
    let data = decoder.get_bytes()?;
    if !decoder.is_complete() {
        return Err("Trailing data in stream success response".to_string());
    }

    Ok(data)
}

fn skip_stream_wire_record(
    decoder: &mut crate::protocol::payload_codec::PayloadDecoder<'_>,
) -> Result<(), String> {
    decoder.get_u64()?;
    decoder.get_optional_u64()?;
    decoder.get_optional_u64()?;
    decoder.skip_bytes()?;
    decoder.get_optional_bytes()?;
    decoder.get_u64()?;
    Ok(())
}

fn skip_stream_read_item(
    decoder: &mut crate::protocol::payload_codec::PayloadDecoder<'_>,
) -> Result<(), String> {
    match decoder.get_u8()? {
        0 => skip_stream_wire_record(decoder),
        1 => {
            decoder.get_u64()?;
            decoder.get_u8()?;
            Ok(())
        }
        2 => {
            decoder.get_u64()?;
            decoder.get_u64()?;
            decoder.get_u8()?;
            Ok(())
        }
        other => Err(format!("Unknown stream read item kind: {other}")),
    }
}

/// Count records present in a STREAM READ payload.
///
/// # Errors
/// Returns an error if the payload is malformed.
pub fn count_stream_read_records_from_payload(payload: &[u8]) -> Result<usize, String> {
    use crate::protocol::payload_codec::PayloadDecoder;

    let data = decode_stream_ok_data(payload)?;
    let mut decoder = PayloadDecoder::new(&data);
    let count = u32_to_usize(decoder.get_u32()?);

    for item_index in 0..count {
        if let Err(err) = skip_stream_read_item(&mut decoder) {
            let offset = decoder.offset();
            return Err(format!(
                "stream read item {} failed at offset {}: {} (next byte = {})",
                item_index,
                offset,
                err,
                decoder.peek_u8().unwrap_or(0)
            ));
        }
    }

    decoder.get_u64()?;
    decoder.get_optional_u64()?;
    decoder.get_optional_u64()?;
    decoder.get_u8()?;

    if !decoder.is_complete() {
        return Err("Trailing data in stream read response".to_string());
    }

    Ok(count)
}

/// Parse a STREAM READ response and count its records.
///
/// # Errors
/// Returns an error when the response payload is malformed.
pub fn parse_stream_read_record_count(response: &[u8]) -> Result<usize, String> {
    let (_msg_type, _status, payload) = parse_stream_response(response);
    count_stream_read_records_from_payload(&payload)
}

/// Build SCHEDULE CREATE frame (`msg_type` 700)
#[must_use]
pub fn build_schedule_create(route: &str, cron: &str, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(route.len()));
    buf.put_slice(route.as_bytes());

    buf.put_u32(u32_len(cron.len()));
    buf.put_slice(cron.as_bytes());

    buf.put_u32(u32_len(payload.len()));
    buf.put_slice(payload);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(700, &buf);
    builder.build()
}

/// Build SCHEDULE CREATE BATCH frame (`msg_type` 706).
#[must_use]
pub fn build_schedule_create_batch(entries: &[(&str, &str, &[u8])]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(u32_len(entries.len()));

    for (route, cron, payload) in entries {
        buf.put_u32(u32_len(route.len()));
        buf.put_slice(route.as_bytes());

        buf.put_u32(u32_len(cron.len()));
        buf.put_slice(cron.as_bytes());

        buf.put_u32(u32_len(payload.len()));
        buf.put_slice(payload);
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(706, &buf);
    builder.build()
}

/// Parse SCHEDULE response
#[must_use]
pub fn parse_schedule_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

    if let Some((msg_type, payload)) = parser.next_field_ref() {
        let status = if payload.is_empty() { 1 } else { payload[0] };
        return (msg_type_to_u8(msg_type), status, payload.to_vec());
    }

    (0, 1, Vec::new())
}

/// Ensure a SCHEDULE response reported success.
///
/// # Errors
/// Returns an error when the response is empty or the status is non-zero.
pub fn ensure_schedule_ok(response: &[u8]) -> Result<(), String> {
    let (msg_type, status, _data) = parse_schedule_response(response);

    if msg_type == 0 {
        return Err("Empty SCHEDULE response".to_string());
    }

    if status != 0 {
        return Err(format!(
            "SCHEDULE operation failed (msg_type={msg_type}, status={status})"
        ));
    }

    Ok(())
}
