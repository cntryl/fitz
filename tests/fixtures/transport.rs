//! Transport helpers for end-to-end integration tests
//!
//! Provides connector traits and frame builders for each domain.
//! Tests use generic async functions parameterized by connector type.

// Re-export testkit types for test files
pub use fitz::testkit::{
    TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder, TlvFrameParser,
};
use std::net::SocketAddr;

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
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(400, route.as_bytes()); // Route
    builder.encode_field(401, owner_id.as_bytes()); // Owner ID
    builder.encode_field(402, &ttl_secs.to_le_bytes()); // TTL
    builder.encode_field(403, &0u32.to_le_bytes()); // Wait seconds = 0 (immediate)
    builder.build()
}

/// Build LEASE RENEW frame (msg_type 410)
pub fn build_lease_renew(route: &str, owner_id: &str, token: u64, ttl_secs: i32) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(410, route.as_bytes()); // Route
    builder.encode_field(411, owner_id.as_bytes()); // Owner ID
    builder.encode_field(412, &token.to_le_bytes()); // Token
    builder.encode_field(413, &ttl_secs.to_le_bytes()); // TTL
    builder.build()
}

/// Build LEASE RELEASE frame (msg_type 420)
pub fn build_lease_release(route: &str, owner_id: &str, token: u64) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(420, route.as_bytes()); // Route
    builder.encode_field(421, owner_id.as_bytes()); // Owner ID
    builder.encode_field(422, &token.to_le_bytes()); // Token
    builder.build()
}

/// Parse LEASE response: (msg_type: u8, status: u8, data: Vec<u8>)
pub fn parse_lease_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());

    let mut msg_type = 0u8;
    let mut status = 0u8;
    let mut data = Vec::new();

    while let Some((field_type, field_data)) = parser.next_field() {
        match field_type {
            1 => msg_type = field_data.get(0).copied().unwrap_or(0),
            2 => status = field_data.get(0).copied().unwrap_or(0),
            3 => data = field_data,
            _ => {}
        }
    }

    (msg_type, status, data)
}

/// Parse lease token from response data
pub fn parse_lease_token_response(data: &[u8]) -> Result<u64, String> {
    if data.len() < 8 {
        return Err("Token data too short".to_string());
    }
    let bytes = [
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ];
    Ok(u64::from_le_bytes(bytes))
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
pub fn build_notice_publish(route: &str, realm: &str, data: &[u8]) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(500, route.as_bytes()); // Route
    builder.encode_field(501, realm.as_bytes()); // Realm
    builder.encode_field(502, data); // Payload
    builder.build()
}

/// Build NOTICE SUBSCRIBE frame (msg_type 510)
pub fn build_notice_subscribe(route_pattern: &str) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(510, route_pattern.as_bytes()); // Route pattern
    builder.build()
}

/// Parse NOTICE response
pub fn parse_notice_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());
    let mut msg_type = 0u8;
    let mut status = 0u8;
    let mut data = Vec::new();

    while let Some((field_type, field_data)) = parser.next_field() {
        match field_type {
            1 => msg_type = field_data.get(0).copied().unwrap_or(0),
            2 => status = field_data.get(0).copied().unwrap_or(0),
            3 => data = field_data,
            _ => {}
        }
    }

    (msg_type, status, data)
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

/// Build QUEUE ENQUEUE frame (msg_type 600)
pub fn build_queue_enqueue(queue_name: &str, data: &[u8]) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(600, queue_name.as_bytes()); // Queue name
    builder.encode_field(601, data); // Payload
    builder.build()
}

/// Build QUEUE DEQUEUE frame (msg_type 610)
pub fn build_queue_dequeue(queue_name: &str) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(610, queue_name.as_bytes()); // Queue name
    builder.build()
}

/// Parse QUEUE response
pub fn parse_queue_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());
    let mut msg_type = 0u8;
    let mut status = 0u8;
    let mut data = Vec::new();

    while let Some((field_type, field_data)) = parser.next_field() {
        match field_type {
            1 => msg_type = field_data.get(0).copied().unwrap_or(0),
            2 => status = field_data.get(0).copied().unwrap_or(0),
            3 => data = field_data,
            _ => {}
        }
    }

    (msg_type, status, data)
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

/// Build RPC REQUEST frame (msg_type 700)
pub fn build_rpc_request(route: &str, method: &str, payload: &[u8]) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(700, route.as_bytes()); // Route
    builder.encode_field(701, method.as_bytes()); // Method
    builder.encode_field(702, payload); // Payload
    builder.build()
}

/// Parse RPC response
pub fn parse_rpc_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());
    let mut msg_type = 0u8;
    let mut status = 0u8;
    let mut data = Vec::new();

    while let Some((field_type, field_data)) = parser.next_field() {
        match field_type {
            1 => msg_type = field_data.get(0).copied().unwrap_or(0),
            2 => status = field_data.get(0).copied().unwrap_or(0),
            3 => data = field_data,
            _ => {}
        }
    }

    (msg_type, status, data)
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

/// Build STREAM APPEND frame (msg_type 800)
pub fn build_stream_append(route: &str, data: &[u8]) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(800, route.as_bytes()); // Route
    builder.encode_field(801, data); // Data
    builder.build()
}

/// Build STREAM READ frame (msg_type 810)
pub fn build_stream_read(route: &str, start_offset: u64) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(810, route.as_bytes()); // Route
    builder.encode_field(811, &start_offset.to_le_bytes()); // Start offset
    builder.build()
}

/// Parse STREAM response
pub fn parse_stream_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());
    let mut msg_type = 0u8;
    let mut status = 0u8;
    let mut data = Vec::new();

    while let Some((field_type, field_data)) = parser.next_field() {
        match field_type {
            1 => msg_type = field_data.get(0).copied().unwrap_or(0),
            2 => status = field_data.get(0).copied().unwrap_or(0),
            3 => data = field_data,
            _ => {}
        }
    }

    (msg_type, status, data)
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

/// Build SCHEDULE CREATE frame (msg_type 900)
pub fn build_schedule_create(route: &str, cron: &str, payload: &[u8]) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(900, route.as_bytes()); // Route
    builder.encode_field(901, cron.as_bytes()); // Cron expression
    builder.encode_field(902, payload); // Payload
    builder.build()
}

/// Build SCHEDULE CANCEL frame (msg_type 910)
pub fn build_schedule_cancel(route: &str) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(910, route.as_bytes()); // Route
    builder.build()
}

/// Build SCHEDULE LIST frame (msg_type 920)
pub fn build_schedule_list() -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(920, &[]); // No payload for LIST
    builder.build()
}

/// Parse SCHEDULE response
pub fn parse_schedule_response(response: &[u8]) -> (u8, u8, Vec<u8>) {
    let mut parser = TlvFrameParser::new(response.to_vec());
    let mut msg_type = 0u8;
    let mut status = 0u8;
    let mut data = Vec::new();

    while let Some((field_type, field_data)) = parser.next_field() {
        match field_type {
            1 => msg_type = field_data.get(0).copied().unwrap_or(0),
            2 => status = field_data.get(0).copied().unwrap_or(0),
            3 => data = field_data,
            _ => {}
        }
    }

    (msg_type, status, data)
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
    // TLV format: [u8 msg_type][u16 be length][payload]
    // Payload format: [u8 status][data...]
    if response.len() < 4 {
        return (0, 0, Vec::new());
    }

    let msg_type = response[0];
    // Skip length (2 bytes at positions 1-2)
    let status = response[3]; // Position 3 = after type + length
    let data = if response.len() > 4 {
        response[4..].to_vec()
    } else {
        Vec::new()
    };

    // Return (msg_type, status, data)
    (msg_type, status, data)
}

/// Parse KV transaction ID from response (big-endian u64)
pub fn parse_kv_tx_id(data: &[u8]) -> Result<u64, String> {
    if data.len() < 8 {
        return Err("TX ID too short".to_string());
    }
    let bytes = [
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ];
    Ok(u64::from_be_bytes(bytes))
}

/// Parse KV value from response
pub fn parse_kv_value(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}
