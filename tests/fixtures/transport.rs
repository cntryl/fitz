//! Transport helpers for end-to-end integration tests
//!
//! Provides connector traits and frame builders for each domain.
//! Tests use generic async functions parameterized by connector type.

#![allow(dead_code)]

// Re-export testkit types for test files
use bytes::BufMut;
pub use fitz::testkit::{
    TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder, TlvFrameParser,
};

// ============================================================================
// GENERIC CONNECTOR TRAITS
// ============================================================================

/// Generic test client trait for sending/receiving frames
#[async_trait::async_trait]
pub trait TestConnectorClient: Send {
    async fn request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), String>;
}

// ============================================================================
// TCP AND WEBSOCKET CONNECTOR WRAPPER STRUCTS
// ============================================================================

pub struct TcpClient(TestClient);
pub struct WsClient(TestWebSocketClient);

#[async_trait::async_trait]
impl TestConnectorClient for TcpClient {
    async fn request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.0.send_frame(frame).await.map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl TestConnectorClient for WsClient {
    async fn request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.0.send_frame(frame).await.map_err(|e| e.to_string())
    }
}

// ============================================================================
// LEASE DOMAIN - CONNECTOR IMPLEMENTATIONS
// ============================================================================

pub struct TcpLeaseConnector(TestClient);
pub struct WsLeaseConnector(TestWebSocketClient);

impl TcpLeaseConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

impl WsLeaseConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
pub trait LeaseConnector: Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
impl LeaseConnector for TcpLeaseConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        TestClient::new(server.tcp_addr)
            .await
            .map(TcpLeaseConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl LeaseConnector for WsLeaseConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        let url = format!("ws://{}", server.ws_addr);
        TestWebSocketClient::connect(&url)
            .await
            .map(WsLeaseConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Build LEASE ACQUIRE frame (msg_type 400)
pub fn build_lease_acquire_immediate(route: &str, owner_id: &str, ttl_secs: i32) -> Vec<u8> {
    // Wire format: [string route][string owner_id][u64 ttl_secs][u32 wait_seconds (optional)]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    // Owner ID (length-prefixed string)
    buf.put_u32(owner_id.len() as u32);
    buf.put_slice(owner_id.as_bytes());

    // TTL seconds (u64)
    buf.put_u64(ttl_secs as u64);

    // Wait seconds (u32, 0 for immediate)
    buf.put_u32(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(400, &buf);
    builder.build()
}

/// Build LEASE RENEW frame (msg_type 401)
pub fn build_lease_renew(route: &str, owner_id: &str, token: u64, ttl_secs: i32) -> Vec<u8> {
    // Wire format: [string route][string owner_id][u64 fencing_token][u64 ttl_secs]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    // Owner ID (length-prefixed string)
    buf.put_u32(owner_id.len() as u32);
    buf.put_slice(owner_id.as_bytes());

    // Fencing token (u64)
    buf.put_u64(token);

    // TTL seconds (u64)
    buf.put_u64(ttl_secs as u64);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(401, &buf);
    builder.build()
}

/// Build LEASE RELEASE frame (msg_type 402)
pub fn build_lease_release(route: &str, owner_id: &str, token: u64) -> Vec<u8> {
    // Wire format: [string route][string owner_id][u64 fencing_token]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    // Owner ID (length-prefixed string)
    buf.put_u32(owner_id.len() as u32);
    buf.put_slice(owner_id.as_bytes());

    // Fencing token (u64)
    buf.put_u64(token);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(402, &buf);
    builder.build()
}

/// Parse LEASE response: (msg_type: u8, status: u8, data: Vec<u8>)
pub fn parse_lease_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][optional u64 token]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        // Return msg_type (as u8), status, and full payload for further parsing
        return ((msg_type & 0xFF) as u8, status, payload);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

/// Parse lease token from ACQUIRE success response data (CLIENT_SPEC).
/// Wire format: [u8 status=0][u8 response_type (0=Acquired,1=AlreadyHeld,2=Queued,3=AlreadyQueued)][u64 BE fencing_token]
pub fn parse_lease_token_response(data: &[u8]) -> Result<u64, String> {
    if data.len() < 10 {
        return Err("Token data too short".to_string());
    }

    let status = data[0];
    if status != 0 {
        return Err("Lease operation failed".to_string());
    }

    // Bytes 2-9: fencing_token (u64 big-endian)
    let bytes = [
        data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
    ];
    Ok(u64::from_be_bytes(bytes))
}

// ============================================================================
// NOTICE DOMAIN - CONNECTOR IMPLEMENTATIONS
// ============================================================================

pub struct TcpNoticeConnector(TestClient);
pub struct WsNoticeConnector(TestWebSocketClient);

impl TcpNoticeConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

impl WsNoticeConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
pub trait NoticeConnector: Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
impl NoticeConnector for TcpNoticeConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        TestClient::new(server.tcp_addr)
            .await
            .map(TcpNoticeConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl NoticeConnector for WsNoticeConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        let url = format!("ws://{}", server.ws_addr);
        TestWebSocketClient::connect(&url)
            .await
            .map(WsNoticeConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Build NOTICE PUBLISH frame (msg_type 500)
pub fn build_notice_publish(route: &str, _realm: &str, data: &[u8]) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string route][bytes payload]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    // Payload (length-prefixed bytes)
    buf.put_u32(data.len() as u32);
    buf.put_slice(data);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(500, &buf);
    builder.build()
}

/// Build NOTICE SUBSCRIBE frame (msg_type 501)
pub fn build_notice_subscribe(route_pattern: &str) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string pattern]
    let mut buf = Vec::new();

    // Pattern (length-prefixed string)
    buf.put_u32(route_pattern.len() as u32);
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(501, &buf);
    builder.build()
}

/// Parse NOTICE response
pub fn parse_notice_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][...response data...]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        // Return msg_type (as u8), status, and data portion (skipping status byte)
        let data = if payload.len() > 1 {
            payload[1..].to_vec()
        } else {
            Vec::new()
        };
        return ((msg_type & 0xFF) as u8, status, data);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

// ============================================================================
// QUEUE DOMAIN - CONNECTOR TRAIT
// ============================================================================

pub struct TcpQueueConnector(TestClient);
pub struct WsQueueConnector(TestWebSocketClient);

impl TcpQueueConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

impl WsQueueConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
pub trait QueueConnector: Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
impl QueueConnector for TcpQueueConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        TestClient::new(server.tcp_addr)
            .await
            .map(TcpQueueConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl QueueConnector for WsQueueConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        let url = format!("ws://{}", server.ws_addr);
        TestWebSocketClient::connect(&url)
            .await
            .map(WsQueueConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
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
    payload.extend_from_slice(&30_u64.to_be_bytes()); // lease_seconds = 30
    payload.push(0); // has_batch_size = false
    payload.push(0); // has_wait = false

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(202, &payload);
    builder.build()
}

/// Parse QUEUE response
pub fn parse_queue_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][...response data...]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        // Return msg_type (as u8), status, and full payload for further parsing
        return ((msg_type & 0xFF) as u8, status, payload);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

/// Extract message bodies from Queue Reserve response
/// Wire format: [u8 status][u32 count][for each: u64 id, u64 token, u32 body_len, bytes body]
pub fn extract_queue_messages(data: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    if data.len() < 5 {
        return Err("Queue response data too short".to_string());
    }

    // Byte 0: status (already checked by caller)
    // Bytes 1-4: message count
    let count = u32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;

    let mut messages = Vec::new();
    let mut offset = 5;

    for _ in 0..count {
        if offset + 8 > data.len() {
            return Err("Incomplete message ID".to_string());
        }
        // Skip message ID (8 bytes)
        offset += 8;

        if offset + 8 > data.len() {
            return Err("Incomplete token".to_string());
        }
        // Skip token (8 bytes)
        offset += 8;

        if offset + 4 > data.len() {
            return Err("Incomplete body length".to_string());
        }
        // Read body length
        let body_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        if offset + body_len > data.len() {
            return Err(format!(
                "Incomplete message body: expected {} bytes, got {}",
                body_len,
                data.len() - offset
            ));
        }

        messages.push(data[offset..offset + body_len].to_vec());
        offset += body_len;
    }

    Ok(messages)
}

// ============================================================================
// RPC DOMAIN - CONNECTOR TRAIT
// ============================================================================

pub struct TcpRpcConnector(TestClient);
pub struct WsRpcConnector(TestWebSocketClient);

impl TcpRpcConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

impl WsRpcConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
pub trait RpcConnector: Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
impl RpcConnector for TcpRpcConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        TestClient::new(server.tcp_addr)
            .await
            .map(TcpRpcConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl RpcConnector for WsRpcConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        let url = format!("ws://{}", server.ws_addr);
        TestWebSocketClient::connect(&url)
            .await
            .map(WsRpcConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Build RPC SUBSCRIBE frame (msg_type 300) to register a worker
pub fn build_rpc_subscribe(worker_addr: &str) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string worker_addr]
    let mut buf = Vec::new();
    buf.put_u32(worker_addr.len() as u32);
    buf.put_slice(worker_addr.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(300, &buf);
    builder.build()
}

/// Build RPC REQUEST frame (msg_type 302)
pub fn build_rpc_request(route: &str, _method: &str, payload: &[u8]) -> Vec<u8> {
    use bytes::BufMut;
    use uuid::Uuid;

    // Wire format: [bytes correlation_id(16)][string route][string reply_route][bytes body]
    let mut buf = Vec::new();

    // Correlation ID (UUID as length-prefixed bytes: u32 len + 16 bytes)
    let uuid = Uuid::new_v4();
    buf.put_u32(16); // Length prefix
    buf.put_slice(uuid.as_bytes());

    // Route (length-prefixed string)
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    // Reply route (use inbox pattern)
    let reply_route = format!("inbox://session/1/{}", uuid);
    buf.put_u32(reply_route.len() as u32);
    buf.put_slice(reply_route.as_bytes());

    // Body (length-prefixed bytes)
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(302, &buf);
    builder.build()
}

/// Build RPC RESPONSE frame for workers (msg_type 303)
pub fn build_rpc_response_delivery(
    correlation_id: uuid::Uuid,
    seq: u64,
    stream_end: bool,
    body: &[u8],
) -> Vec<u8> {
    use fitz::protocol::payload_codec::PayloadEncoder;

    let mut enc = PayloadEncoder::new();
    enc.put_bytes(correlation_id.as_bytes());
    enc.put_u64(seq);
    enc.put_bytes(body);
    enc.put_u8(stream_end as u8);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(303, &enc.finish());
    builder.build()
}

/// Parse RPC response
pub fn parse_rpc_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][...response data...]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        // Return msg_type (as u8), status, and full payload for further parsing
        return ((msg_type & 0xFF) as u8, status, payload);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

pub struct RpcRequestDelivery {
    pub msg_type: u16,
    pub correlation_id: uuid::Uuid,
    pub route: String,
    pub reply_route: String,
    pub body: Vec<u8>,
}

pub struct RpcResponseDelivery {
    pub msg_type: u16,
    pub correlation_id: uuid::Uuid,
    pub seq: u64,
    pub body: Vec<u8>,
    pub stream_end: bool,
}

/// Parse RPC REQUEST delivery (msg_type 302) sent to workers
pub fn parse_rpc_request_delivery(frame: &[u8]) -> Result<RpcRequestDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame.to_vec());
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing RPC request delivery frame".to_string())?;
    if msg_type != 302 {
        return Err(format!("Unexpected RPC request msg_type: {}", msg_type));
    }

    let mut dec = PayloadDecoder::new(&payload);
    let correlation_id_bytes = dec.get_bytes()?;
    if correlation_id_bytes.len() != 16 {
        return Err("Correlation ID must be 16 bytes".to_string());
    }
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&correlation_id_bytes);
    let correlation_id = uuid::Uuid::from_bytes(uuid_bytes);

    let route = dec.get_string()?;
    let reply_route = dec.get_string()?;
    let body = dec.get_bytes()?.to_vec();
    if !dec.is_complete() {
        return Err("Trailing data in RPC request delivery".to_string());
    }

    Ok(RpcRequestDelivery {
        msg_type,
        correlation_id,
        route,
        reply_route,
        body,
    })
}

/// Parse RPC RESPONSE delivery (msg_type 303) received by callers
pub fn parse_rpc_response_delivery(frame: &[u8]) -> Result<RpcResponseDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame.to_vec());
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing RPC response delivery frame".to_string())?;
    if msg_type != 303 {
        return Err(format!("Unexpected RPC response msg_type: {}", msg_type));
    }

    let mut dec = PayloadDecoder::new(&payload);
    let correlation_id_bytes = dec.get_bytes()?;
    if correlation_id_bytes.len() != 16 {
        return Err("Correlation ID must be 16 bytes".to_string());
    }
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&correlation_id_bytes);
    let correlation_id = uuid::Uuid::from_bytes(uuid_bytes);

    let seq = dec.get_u64()?;
    let body = dec.get_bytes()?.to_vec();
    let stream_end = dec.get_u8()? != 0;
    if !dec.is_complete() {
        return Err("Trailing data in RPC response delivery".to_string());
    }

    Ok(RpcResponseDelivery {
        msg_type,
        correlation_id,
        seq,
        body,
        stream_end,
    })
}

// ============================================================================
// STREAM DOMAIN - CONNECTOR TRAIT
// ============================================================================

pub struct TcpStreamConnector(TestClient);
pub struct WsStreamConnector(TestWebSocketClient);

impl TcpStreamConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

impl WsStreamConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
pub trait StreamConnector: Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
impl StreamConnector for TcpStreamConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        TestClient::new(server.tcp_addr)
            .await
            .map(TcpStreamConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl StreamConnector for WsStreamConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        let url = format!("ws://{}", server.ws_addr);
        TestWebSocketClient::connect(&url)
            .await
            .map(WsStreamConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Build STREAM BEGIN frame (msg_type 600)
/// Wire format: [string route][u64 expected_offset][optional bytes ingest_metadata]
pub fn build_stream_begin(route: &str, expected_offset: u64) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    // Expected offset (u64)
    buf.put_u64(expected_offset);

    // Optional ingest metadata (flag = 0 for none)
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(600, &buf);
    builder.build()
}

/// Build STREAM APPEND frame (msg_type 601)
pub fn build_stream_append(session_id: u64, data: &[u8]) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [u64 session_id][bytes body][optional metadata]
    let mut buf = Vec::new();

    // Session ID
    buf.put_u64(session_id);

    // Body (length-prefixed bytes)
    buf.put_u32(data.len() as u32);
    buf.put_slice(data);

    // Optional metadata (flag = 0 for none)
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(601, &buf);
    builder.build()
}

/// Build STREAM APPEND frame with default session ID (for simple tests)
/// Uses session_id = 1 by default - tests should call BEGIN first if they need a real session
pub fn build_stream_append_simple(_route: &str, data: &[u8]) -> Vec<u8> {
    build_stream_append(1, data)
}

/// Build STREAM READ frame (msg_type 604)
pub fn build_stream_read(route: &str, start_offset: u64) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string route][u64 from_offset][u64 limit][optional max_bytes]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    // From offset
    buf.put_u64(start_offset);

    // Limit (read up to 1000 entries)
    buf.put_u64(1000);

    // Optional max_bytes (flag = 0 for none)
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

/// Parse STREAM response
pub fn parse_stream_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][optional u64 session_id][...response data...]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        // Return msg_type (as u8), status, and full payload for further parsing
        return ((msg_type & 0xFF) as u8, status, payload);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

/// Parse session_id from STREAM BEGIN response data
/// Wire format: [u8 status][u8 has_session_id][u64 session_id][bytes data]
pub fn parse_stream_session_id(data: &[u8]) -> Result<u64, String> {
    if data.len() < 2 {
        return Err("Stream response data too short".to_string());
    }

    // Byte 0: status (0 = success, 1 = error)
    let status = data[0];
    if status != 0 {
        return Err("Stream BEGIN operation failed".to_string());
    }

    // Byte 1: has_session_id flag (1 = Some, 0 = None)
    let has_session_id = data[1];
    if has_session_id == 0 {
        return Err("No session_id in response".to_string());
    }

    // Bytes 2-9: session_id value (u64 big-endian)
    if data.len() < 10 {
        return Err("Session ID data incomplete".to_string());
    }

    let bytes = [
        data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
    ];
    Ok(u64::from_be_bytes(bytes))
}

// ============================================================================
// SCHEDULE DOMAIN - CONNECTOR TRAIT
// ============================================================================

pub struct TcpScheduleConnector(TestClient);
pub struct WsScheduleConnector(TestWebSocketClient);

impl TcpScheduleConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

impl WsScheduleConnector {
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
pub trait ScheduleConnector: Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
}

#[async_trait::async_trait]
impl ScheduleConnector for TcpScheduleConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        TestClient::new(server.tcp_addr)
            .await
            .map(TcpScheduleConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl ScheduleConnector for WsScheduleConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        let url = format!("ws://{}", server.ws_addr);
        TestWebSocketClient::connect(&url)
            .await
            .map(WsScheduleConnector)
            .map_err(|e| e.to_string())
    }

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Build SCHEDULE CREATE frame (msg_type 700)
pub fn build_schedule_create(route: &str, cron: &str, payload: &[u8]) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string route][string cron][bytes payload]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    // Cron expression (length-prefixed string)
    buf.put_u32(cron.len() as u32);
    buf.put_slice(cron.as_bytes());

    // Payload (length-prefixed bytes)
    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(700, &buf);
    builder.build()
}

/// Build SCHEDULE CANCEL frame (msg_type 701)
pub fn build_schedule_cancel(route: &str) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [string route]
    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(701, &buf);
    builder.build()
}

/// Build SCHEDULE LIST frame (msg_type 702)
pub fn build_schedule_list() -> Vec<u8> {
    // Wire format: empty payload
    let builder = TlvFrameBuilder::new();
    let mut frame_builder = builder;
    frame_builder.encode_field(702, &[]);
    frame_builder.build()
}

/// Parse SCHEDULE response
pub fn parse_schedule_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][...response data...]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        // Return msg_type (as u8), status, and full payload for further parsing
        return ((msg_type & 0xFF) as u8, status, payload);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

// ============================================================================
// KV DOMAIN - CONNECTOR TRAIT
// ============================================================================

#[async_trait::async_trait]
pub trait KvConnector: TestConnectorClient + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl KvConnector for TcpClient {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        TestClient::new(server.tcp_addr)
            .await
            .map(TcpClient)
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl KvConnector for WsClient {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        let url = format!("ws://{}", server.ws_addr);
        TestWebSocketClient::connect(&url)
            .await
            .map(WsClient)
            .map_err(|e| e.to_string())
    }
}

// Type aliases for backwards compatibility with test code
pub type TcpConnector = TcpClient;
pub type WsConnector = WsClient;

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

/// Build KV GET frame (msg_type 103)
pub fn build_kv_get(tx_id: u64, route: &str, key: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route][u32 BE key_len][key]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
    payload.extend_from_slice(key);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(103, &payload);
    builder.build()
}

/// Build KV COMMIT frame (msg_type 101)
pub fn build_kv_commit(tx_id: u64, route: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    // [u64 BE tx_id][u32 BE route_len][route]
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(101, &payload);
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
    let mut parser = TlvFrameParser::new(response.to_vec());

    // Server sends single TLV record: [msg_type][len][payload]
    // Payload format: [u8 status][...response data...]
    if let Some((msg_type, payload)) = parser.next_field() {
        let status = if !payload.is_empty() { payload[0] } else { 1 };
        // Return msg_type (as u8), status, and full payload (including status byte)
        // Helper functions expect the full payload and will skip the status byte themselves
        return ((msg_type & 0xFF) as u8, status, payload);
    }

    // Fallback if no data
    (0, 1, Vec::new())
}

/// Parse KV transaction ID from response (big-endian u64)
pub fn parse_kv_tx_id(data: &[u8]) -> Result<u64, String> {
    // BeginOk format: [u8 status][u64 tx_id]
    // Skip status byte at data[0], read tx_id from data[1..9]
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

/// Extract value from KV GET response
///
/// GetResult format: [u8 status][u8 found][u32 length_be][...value_bytes]
/// Returns the actual value bytes if found, empty vec if not found
pub fn extract_kv_value(data: &[u8]) -> Result<Vec<u8>, String> {
    if data.len() < 6 {
        return Err("GetResult data too short".to_string());
    }

    let found = data[1];
    if found == 0 {
        return Ok(Vec::new()); // Not found
    }

    // Read length from bytes 2-5 (big-endian u32)
    let length = u32::from_be_bytes([data[2], data[3], data[4], data[5]]) as usize;

    // Extract value from bytes 6 onwards
    if data.len() < 6 + length {
        return Err(format!(
            "GetResult value incomplete: expected {} bytes, got {}",
            length,
            data.len() - 6
        ));
    }

    Ok(data[6..6 + length].to_vec())
}

/// Parse KV value from response
pub fn parse_kv_value(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}
