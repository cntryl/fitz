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

#[async_trait::async_trait]
pub trait FrameReceivingConnector: TestConnectorClient {
    async fn recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String>;
}

// ============================================================================
// TCP AND WEBSOCKET CONNECTOR WRAPPER STRUCTS
// ============================================================================

pub struct TcpClient(TestClient);
pub struct WsClient(TestWebSocketClient);

#[async_trait::async_trait]
trait FixtureTransportClient: Send {
    async fn fixture_request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String>;
    async fn fixture_send_frame(&mut self, frame: &[u8]) -> Result<(), String>;
    async fn fixture_recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String>;
}

trait HasFixtureClient {
    type Client: FixtureTransportClient;

    fn client_mut(&mut self) -> &mut Self::Client;
}

async fn connect_tcp_raw(server: &TestServer) -> Result<TestClient, String> {
    TestClient::new(server.tcp_addr)
        .await
        .map_err(|e| e.to_string())
}

async fn connect_ws_raw(server: &TestServer) -> Result<TestWebSocketClient, String> {
    let url = format!("ws://{}", server.ws_addr);
    TestWebSocketClient::connect(&url)
        .await
        .map_err(|e| e.to_string())
}

#[async_trait::async_trait]
impl FixtureTransportClient for TestClient {
    async fn fixture_request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }

    async fn fixture_send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.send_frame(frame).await.map_err(|e| e.to_string())
    }

    async fn fixture_recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.recv_frame(timeout_ms).await.map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl FixtureTransportClient for TestWebSocketClient {
    async fn fixture_request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }

    async fn fixture_send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.send_frame(frame).await.map_err(|e| e.to_string())
    }

    async fn fixture_recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.recv_frame(timeout_ms).await.map_err(|e| e.to_string())
    }
}

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
impl FrameReceivingConnector for TcpClient {
    async fn recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .recv_frame(timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl<T> TestConnectorClient for T
where
    T: HasFixtureClient + Send,
{
    async fn request(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.client_mut().fixture_request(frame, timeout_ms).await
    }

    async fn send_frame(&mut self, frame: &[u8]) -> Result<(), String> {
        self.client_mut().fixture_send_frame(frame).await
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

#[async_trait::async_trait]
impl FrameReceivingConnector for WsClient {
    async fn recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.0
            .recv_frame(timeout_ms)
            .await
            .map_err(|e| e.to_string())
    }
}

#[async_trait::async_trait]
impl<T> FrameReceivingConnector for T
where
    T: HasFixtureClient + Send,
{
    async fn recv_frame(&mut self, timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.client_mut().fixture_recv_frame(timeout_ms).await
    }
}

// ============================================================================
// LEASE DOMAIN - CONNECTOR IMPLEMENTATIONS
// ============================================================================

pub struct TcpLeaseConnector(TestClient);
pub struct WsLeaseConnector(TestWebSocketClient);

impl HasFixtureClient for TcpLeaseConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsLeaseConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait LeaseConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl LeaseConnector for TcpLeaseConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpLeaseConnector)
    }
}

#[async_trait::async_trait]
impl LeaseConnector for WsLeaseConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsLeaseConnector)
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

/// Build LEASE ACQUIRE frame (msg_type 400) with waiting.
pub fn build_lease_acquire_with_wait(
    route: &str,
    owner_id: &str,
    ttl_secs: i32,
    wait_seconds: u32,
) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    buf.put_u32(owner_id.len() as u32);
    buf.put_slice(owner_id.as_bytes());

    buf.put_u64(ttl_secs as u64);
    buf.put_u32(wait_seconds);

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

/// Build LEASE QUERY frame (msg_type 403)
pub fn build_lease_query(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(403, &buf);
    builder.build()
}

/// Build LEASE SUBSCRIBE frame (msg_type 407)
pub fn build_lease_subscribe(pattern: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(pattern.len() as u32);
    buf.put_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(407, &buf);
    builder.build()
}

/// Build LEASE UNSUBSCRIBE frame (msg_type 408)
pub fn build_lease_unsubscribe(pattern: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(pattern.len() as u32);
    buf.put_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(408, &buf);
    builder.build()
}

pub fn extract_lease_subscription_id(data: &[u8]) -> Result<u64, String> {
    if data.len() < 9 {
        return Err("Lease subscribe response too short".to_string());
    }

    Ok(u64::from_be_bytes(data[1..9].try_into().map_err(|_| {
        "Lease subscribe response missing subscription id".to_string()
    })?))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseWatchDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub payload: Vec<u8>,
}

pub fn parse_lease_watch_delivery(frame: &[u8]) -> Result<LeaseWatchDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing lease watch delivery frame".to_string())?;
    if msg_type != 409 {
        return Err(format!(
            "Unexpected lease watch delivery msg_type: {}",
            msg_type
        ));
    }

    let mut decoder = PayloadDecoder::new(&payload);
    let subscription_id = decoder.get_u64()?;
    let route = decoder.get_string()?;
    let payload = decoder.get_bytes()?.to_vec();
    if !decoder.is_complete() {
        return Err("Trailing data in lease watch delivery".to_string());
    }

    Ok(LeaseWatchDelivery {
        msg_type,
        subscription_id,
        route,
        payload,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseStatusPayload {
    pub has_holder: bool,
    pub owner_id: Option<String>,
    pub expires_in_secs: Option<u64>,
    pub pending_waiters: u32,
}

/// Parse LEASE response: (msg_type: u8, status: u8, data: Vec<u8>)
pub fn parse_lease_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

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

pub fn parse_lease_acquire_response_type(data: &[u8]) -> Result<u8, String> {
    if data.len() < 2 {
        return Err("Acquire response too short".to_string());
    }

    if data[0] != 0 {
        return Err("Lease operation failed".to_string());
    }

    Ok(data[1])
}

pub fn parse_lease_error_message(data: &[u8]) -> Result<String, String> {
    let mut decoder = fitz::protocol::payload_codec::PayloadDecoder::new(data);
    let status = decoder.get_u8()?;
    if status == 0 {
        return Err("Lease operation succeeded".to_string());
    }

    decoder.get_string()
}

pub fn parse_lease_status_payload(data: &[u8]) -> Result<LeaseStatusPayload, String> {
    let mut decoder = fitz::protocol::payload_codec::PayloadDecoder::new(data);
    let status = decoder.get_u8()?;
    if status != 0 {
        return Err("Lease operation failed".to_string());
    }

    let has_holder = decoder.get_u8()? != 0;
    if !has_holder {
        let pending_waiters = decoder.get_u32()?;
        return Ok(LeaseStatusPayload {
            has_holder: false,
            owner_id: None,
            expires_in_secs: None,
            pending_waiters,
        });
    }

    let owner_id = decoder.get_string()?;
    let expires_in_secs = decoder.get_u64()?;
    let pending_waiters = decoder.get_u32()?;
    Ok(LeaseStatusPayload {
        has_holder: true,
        owner_id: Some(owner_id),
        expires_in_secs: Some(expires_in_secs),
        pending_waiters,
    })
}

// ============================================================================
// NOTICE DOMAIN - CONNECTOR IMPLEMENTATIONS
// ============================================================================

pub struct TcpNoticeConnector(TestClient);
pub struct WsNoticeConnector(TestWebSocketClient);

impl HasFixtureClient for TcpNoticeConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsNoticeConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait NoticeConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl NoticeConnector for TcpNoticeConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpNoticeConnector)
    }
}

#[async_trait::async_trait]
impl NoticeConnector for WsNoticeConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsNoticeConnector)
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

/// Build NOTICE UNSUBSCRIBE frame (msg_type 502)
pub fn build_notice_unsubscribe(subscription_id: u64) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u64(subscription_id);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(502, &buf);
    builder.build()
}

/// Parse NOTICE response
pub fn parse_notice_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

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

pub fn parse_notice_subscription_id(data: &[u8]) -> Result<Option<u64>, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

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

pub fn parse_notice_delivery(frame: &[u8]) -> Result<NoticeDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing notice delivery frame".to_string())?;
    if msg_type != 504 {
        return Err(format!("Unexpected notice delivery msg_type: {}", msg_type));
    }

    let mut dec = PayloadDecoder::new(&payload);
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

// ============================================================================
// QUEUE DOMAIN - CONNECTOR TRAIT
// ============================================================================

pub struct TcpQueueConnector(TestClient);
pub struct WsQueueConnector(TestWebSocketClient);

impl HasFixtureClient for TcpQueueConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsQueueConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait QueueConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl QueueConnector for TcpQueueConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpQueueConnector)
    }
}

#[async_trait::async_trait]
impl QueueConnector for WsQueueConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsQueueConnector)
    }
}

/// Build QUEUE ENQUEUE frame (msg_type 200)
fn normalize_queue_route(queue_name: &str) -> String {
    if queue_name.contains("://") {
        queue_name.to_string()
    } else {
        format!("queue://test/app/{queue_name}")
    }
}

fn normalize_queue_watch_pattern(pattern: &str) -> String {
    let normalized = normalize_queue_route(pattern);
    if normalized.contains('*') || normalized.ends_with("/ready") {
        normalized
    } else {
        format!("{normalized}/ready")
    }
}

pub fn build_queue_enqueue(queue_name: &str, data: &[u8]) -> Vec<u8> {
    // Wire format: [u32 route_len][route][u32 body_len][body][u8 has_delay=0]
    let route = normalize_queue_route(queue_name);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&(data.len() as u32).to_be_bytes());
    payload.extend_from_slice(data);
    payload.push(0); // has_delay = false

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(200, &payload);
    builder.build()
}

/// Build QUEUE RESERVE frame (msg_type 202)
pub fn build_queue_dequeue(queue_name: &str) -> Vec<u8> {
    // Wire format: [u32 route_len][route][u64 lease_seconds][u8 has_batch=0]
    let route = normalize_queue_route(queue_name);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&30_u64.to_be_bytes()); // lease_seconds = 30
    payload.push(0); // has_batch_size = false

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(202, &payload);
    builder.build()
}

/// Build QUEUE WATCH frame (msg_type 207).
pub fn build_queue_watch(pattern: &str) -> Vec<u8> {
    let pattern = normalize_queue_watch_pattern(pattern);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(pattern.len() as u32).to_be_bytes());
    payload.extend_from_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(207, &payload);
    builder.build()
}

/// Build QUEUE UNWATCH frame (msg_type 208).
pub fn build_queue_unwatch(pattern: &str) -> Vec<u8> {
    let pattern = normalize_queue_watch_pattern(pattern);
    let mut payload = Vec::new();
    payload.extend_from_slice(&(pattern.len() as u32).to_be_bytes());
    payload.extend_from_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(208, &payload);
    builder.build()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueWatchDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub ready_messages: u64,
    pub delayed_messages: u64,
    pub inflight_messages: u64,
}

pub fn extract_queue_subscription_id(data: &[u8]) -> Result<u64, String> {
    if data.len() < 9 {
        return Err("Queue watch response too short".to_string());
    }

    Ok(u64::from_be_bytes(data[1..9].try_into().map_err(|_| {
        "Queue watch response missing subscription id".to_string()
    })?))
}

pub fn parse_queue_watch_delivery(frame: &[u8]) -> Result<QueueWatchDelivery, String> {
    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field_ref()
        .ok_or_else(|| "Missing queue watch delivery frame".to_string())?;
    if msg_type != 209 {
        return Err(format!("Unexpected queue watch msg_type: {}", msg_type));
    }

    if payload.len() < 36 {
        return Err("Queue watch payload too short".to_string());
    }

    let subscription_id = u64::from_be_bytes(payload[0..8].try_into().unwrap());
    let route_len = u32::from_be_bytes(payload[8..12].try_into().unwrap()) as usize;
    if payload.len() < 12 + route_len + 24 {
        return Err("Queue watch payload truncated".to_string());
    }

    let route = String::from_utf8(payload[12..12 + route_len].to_vec())
        .map_err(|_| "Queue watch route is not valid UTF-8".to_string())?;
    let offset = 12 + route_len;
    let ready_messages = u64::from_be_bytes(payload[offset..offset + 8].try_into().unwrap());
    let delayed_messages = u64::from_be_bytes(payload[offset + 8..offset + 16].try_into().unwrap());
    let inflight_messages =
        u64::from_be_bytes(payload[offset + 16..offset + 24].try_into().unwrap());

    Ok(QueueWatchDelivery {
        msg_type,
        subscription_id,
        route,
        ready_messages,
        delayed_messages,
        inflight_messages,
    })
}

/// Parse QUEUE response
pub fn parse_queue_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

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

impl HasFixtureClient for TcpRpcConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsRpcConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait RpcConnector: TestConnectorClient + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl RpcConnector for TcpRpcConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpRpcConnector)
    }
}

#[async_trait::async_trait]
impl RpcConnector for WsRpcConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsRpcConnector)
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

/// Build RPC UNSUBSCRIBE frame (msg_type 301) to unregister a worker
pub fn build_rpc_unsubscribe(worker_addr: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(worker_addr.len() as u32);
    buf.put_slice(worker_addr.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(301, &buf);
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
    let mut parser = TlvFrameParser::new(response);

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

    let mut parser = TlvFrameParser::new(frame);
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

    let mut parser = TlvFrameParser::new(frame);
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

impl HasFixtureClient for TcpStreamConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsStreamConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait StreamConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl StreamConnector for TcpStreamConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpStreamConnector)
    }
}

#[async_trait::async_trait]
impl StreamConnector for WsStreamConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsStreamConnector)
    }
}

/// Build STREAM BEGIN frame (msg_type 600)
/// Wire format: [string route][optional bytes ingest_metadata]
pub fn build_stream_begin(route: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();

    // Route (length-prefixed string)
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    // Optional ingest metadata (flag = 0 for none)
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(600, &buf);
    builder.build()
}

/// Build STREAM APPEND frame (msg_type 601)
pub fn build_stream_append(session_id: u64, expected_offset: u64, data: &[u8]) -> Vec<u8> {
    build_stream_append_with_metadata(session_id, expected_offset, data, None)
}

/// Build STREAM APPEND frame (msg_type 601) with optional metadata.
pub fn build_stream_append_with_metadata(
    session_id: u64,
    expected_offset: u64,
    data: &[u8],
    metadata: Option<&[u8]>,
) -> Vec<u8> {
    use bytes::BufMut;

    // Wire format: [u64 session_id][u64 expected_offset][bytes body][optional metadata]
    let mut buf = Vec::new();

    // Session ID
    buf.put_u64(session_id);

    // Expected offset
    buf.put_u64(expected_offset);

    // Body (length-prefixed bytes)
    buf.put_u32(data.len() as u32);
    buf.put_slice(data);

    match metadata {
        Some(metadata) => {
            buf.put_u8(1);
            buf.put_u32(metadata.len() as u32);
            buf.put_slice(metadata);
        }
        None => buf.put_u8(0),
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(601, &buf);
    builder.build()
}

/// Build STREAM COMMIT frame (msg_type 602)
pub fn build_stream_commit(session_id: u64) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u64(session_id);
    buf.put_u8(0);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(602, &buf);
    builder.build()
}

/// Build STREAM SUBSCRIBE frame (msg_type 607)
pub fn build_stream_subscribe(route_pattern: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(route_pattern.len() as u32);
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(607, &buf);
    builder.build()
}

/// Build STREAM UNSUBSCRIBE frame (msg_type 608)
pub fn build_stream_unsubscribe(route_pattern: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(route_pattern.len() as u32);
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(608, &buf);
    builder.build()
}

/// Build STREAM APPEND frame with default session ID (for simple tests)
/// Uses session_id = 1 by default - tests should call BEGIN first if they need a real session
pub fn build_stream_append_simple(_route: &str, data: &[u8]) -> Vec<u8> {
    build_stream_append(1, 0, data)
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

/// Build STREAM LAST frame (msg_type 605)
pub fn build_stream_last(route: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(605, &buf);
    builder.build()
}

/// Build STREAM GET_METADATA frame (msg_type 606)
pub fn build_stream_get_metadata(route: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(606, &buf);
    builder.build()
}

/// Parse STREAM response
pub fn parse_stream_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

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

pub struct StreamDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub body: Vec<u8>,
}

pub fn parse_stream_delivery(frame: &[u8]) -> Result<StreamDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing stream delivery frame".to_string())?;
    if msg_type != 609 {
        return Err(format!("Unexpected stream delivery msg_type: {}", msg_type));
    }

    let mut dec = PayloadDecoder::new(&payload);
    let subscription_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let body = dec.get_bytes()?.to_vec();
    if !dec.is_complete() {
        return Err("Trailing data in stream delivery".to_string());
    }

    Ok(StreamDelivery {
        msg_type,
        subscription_id,
        route,
        body,
    })
}

// ============================================================================
// SCHEDULE DOMAIN - CONNECTOR TRAIT
// ============================================================================

pub struct TcpScheduleConnector(TestClient);
pub struct WsScheduleConnector(TestWebSocketClient);

impl HasFixtureClient for TcpScheduleConnector {
    type Client = TestClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

impl HasFixtureClient for WsScheduleConnector {
    type Client = TestWebSocketClient;

    fn client_mut(&mut self) -> &mut Self::Client {
        &mut self.0
    }
}

#[async_trait::async_trait]
pub trait ScheduleConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;

    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl ScheduleConnector for TcpScheduleConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpScheduleConnector)
    }
}

#[async_trait::async_trait]
impl ScheduleConnector for WsScheduleConnector {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsScheduleConnector)
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

/// Build SCHEDULE CREATE BATCH frame (msg_type 706)
pub fn build_schedule_create_batch(entries: &[(&str, &str, &[u8])]) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(entries.len() as u32);

    for (route, cron, payload) in entries {
        buf.put_u32(route.len() as u32);
        buf.put_slice(route.as_bytes());

        buf.put_u32(cron.len() as u32);
        buf.put_slice(cron.as_bytes());

        buf.put_u32(payload.len() as u32);
        buf.put_slice(payload);
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(706, &buf);
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

/// Build SCHEDULE SUBSCRIBE frame (msg_type 703)
pub fn build_schedule_subscribe(route_pattern: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(route_pattern.len() as u32);
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(703, &buf);
    builder.build()
}

/// Build SCHEDULE UNSUBSCRIBE frame (msg_type 704)
pub fn build_schedule_unsubscribe(route_pattern: &str) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u32(route_pattern.len() as u32);
    buf.put_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(704, &buf);
    builder.build()
}

/// Parse SCHEDULE response
pub fn parse_schedule_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response);

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

pub struct ScheduleDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub body: Vec<u8>,
}

pub fn parse_schedule_delivery(frame: &[u8]) -> Result<ScheduleDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing schedule delivery frame".to_string())?;
    if msg_type != 705 {
        return Err(format!(
            "Unexpected schedule delivery msg_type: {}",
            msg_type
        ));
    }

    let mut dec = PayloadDecoder::new(&payload);
    let subscription_id = dec.get_u64()?;
    let body = dec.get_bytes()?.to_vec();
    if !dec.is_complete() {
        return Err("Trailing data in schedule delivery".to_string());
    }

    Ok(ScheduleDelivery {
        msg_type,
        subscription_id,
        body,
    })
}

// ============================================================================
// KV DOMAIN - CONNECTOR TRAIT
// ============================================================================

#[async_trait::async_trait]
pub trait KvConnector: FrameReceivingConnector + Sized {
    async fn connect(server: &TestServer) -> Result<Self, String>;
    async fn send_and_receive(&mut self, frame: &[u8], timeout_ms: u64) -> Result<Vec<u8>, String> {
        self.request(frame, timeout_ms).await
    }
}

#[async_trait::async_trait]
impl KvConnector for TcpClient {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_tcp_raw(server).await.map(TcpClient)
    }
}

#[async_trait::async_trait]
impl KvConnector for WsClient {
    async fn connect(server: &TestServer) -> Result<Self, String> {
        connect_ws_raw(server).await.map(WsClient)
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

/// Build KV SUBSCRIBE frame (msg_type 109)
pub fn build_kv_subscribe(route_pattern: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(route_pattern.len() as u32).to_be_bytes());
    payload.extend_from_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(109, &payload);
    builder.build()
}

/// Build KV UNSUBSCRIBE frame (msg_type 110)
pub fn build_kv_unsubscribe(route_pattern: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(route_pattern.len() as u32).to_be_bytes());
    payload.extend_from_slice(route_pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(110, &payload);
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

pub fn extract_kv_subscription_id(data: &[u8]) -> Result<u64, String> {
    if data.len() < 9 {
        return Err("KV subscribe response too short".to_string());
    }

    Ok(u64::from_be_bytes(data[1..9].try_into().map_err(|_| {
        "KV subscribe response missing subscription id".to_string()
    })?))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvWatchDelivery {
    pub msg_type: u16,
    pub subscription_id: u64,
    pub route: String,
    pub mutation_count: u64,
}

pub fn parse_kv_watch_delivery(frame: &[u8]) -> Result<KvWatchDelivery, String> {
    use fitz::protocol::payload_codec::PayloadDecoder;

    let mut parser = TlvFrameParser::new(frame);
    let (msg_type, payload) = parser
        .next_field()
        .ok_or_else(|| "Missing KV watch delivery frame".to_string())?;
    if msg_type != 111 {
        return Err(format!(
            "Unexpected KV watch delivery msg_type: {}",
            msg_type
        ));
    }

    let mut decoder = PayloadDecoder::new(&payload);
    let subscription_id = decoder.get_u64()?;
    let route = decoder.get_string()?;
    let mutation_count = decoder.get_u64()?;
    if !decoder.is_complete() {
        return Err("Trailing data in KV watch delivery".to_string());
    }

    Ok(KvWatchDelivery {
        msg_type,
        subscription_id,
        route,
        mutation_count,
    })
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
