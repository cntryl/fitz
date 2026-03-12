//! Transport-layer test utilities for end-to-end integration tests
//!
//! Provides helpers for testing the complete request-response cycle:
//! Client → TCP/WebSocket → Session → Routing → Domain → Response → Client

use bytes::{BufMut, BytesMut};
use futures_util::{SinkExt, StreamExt};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Once, OnceLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};

static AUTH_ENV_INIT: Once = Once::new();
static TEST_SERVER_SEMAPHORE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

fn init_auth_env() {
    AUTH_ENV_INIT.call_once(|| {
        std::env::set_var("FITZ_JWT_HMAC_SECRET", "test-secret-key");
    });
}

fn test_server_semaphore() -> &'static Arc<tokio::sync::Semaphore> {
    TEST_SERVER_SEMAPHORE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(1)))
}

/// Wait for auth processing to settle before sending authenticated requests.
pub async fn wait_for_auth_ready() {
    tokio::time::sleep(Duration::from_millis(1000)).await;
}

/// Wait for session cleanup after a client disconnect.
pub async fn wait_for_disconnect_cleanup() {
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

/// Test server that starts Fitz on random available ports (TCP + WebSocket)
pub struct TestServer {
    pub tcp_addr: SocketAddr,
    pub ws_addr: SocketAddr,
    pub runtime: Arc<crate::boot::Runtime>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl TestServer {
    /// Start a test server with auth disabled (backward compatible)
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_with_auth(false).await
    }

    /// Start a test server with configurable auth mode
    pub async fn start_with_auth(auth_required: bool) -> Result<Self, Box<dyn std::error::Error>> {
        let permit = test_server_semaphore()
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        // Initialize observability (metrics + tracing) once for tests
        // Safe to call multiple times - will only initialize once
        let _ = crate::boot::observability::try_init_observability();

        if auth_required {
            init_auth_env();
        }

        // Find available ports and keep listeners bound to prevent reallocation race
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await?;
        let tcp_addr = tcp_listener.local_addr()?;

        let ws_listener = TcpListener::bind("127.0.0.1:0").await?;
        let ws_addr = ws_listener.local_addr()?;

        // Keep listeners alive - will be passed to spawn functions
        // This prevents the port reallocation race condition where parallel tests
        // could grab the same port between bind() and the spawn functions

        // Boot runtime with test configuration
        let boot_config = crate::boot::BootConfig {
            bind_addr: "127.0.0.1".to_string(),
            tcp_port: tcp_addr.port(),
            http_port: ws_addr.port(), // Use discovered WS port
            storage_mode: crate::boot::runtime::StorageMode::Memory,
            auth_required,
            max_connections: 1000,
            max_frame_size: 16_777_216, // 16 MB (test config allows larger frames than production 1 MB default)
            channel_capacity: 10_000,
        };

        // Step 1: Initialize storage
        let store = crate::boot::storage::init(&boot_config).await?;

        // Step 2: Initialize runtime
        let (router, ingress, ingress_config, _scheduler, runtime) =
            crate::boot::runtime::init(&boot_config, &store)?;

        // Mark storage ready
        runtime.mark_storage_ready();

        // Step 3: Register domain actors
        let domains = crate::boot::domains::setup(&router, &store)?;
        runtime.attach_domains(Arc::new(domains));

        // Mark domains ready
        runtime.mark_domains_ready();

        // Step 4: Start TCP listener with pre-bound socket (eliminates port race)
        let tcp_ready_rx = crate::boot::handlers::spawn_tcp_listener_with_bound_socket(
            tcp_listener,
            ingress.clone(),
            ingress_config.clone(),
            runtime.clone(),
        )?;

        // Step 5: Start HTTP/WebSocket listener with pre-bound socket
        let ws_ready_rx = crate::boot::handlers::spawn_http_listener_with_bound_socket(
            ws_listener,
            ingress.clone(),
            ingress_config.clone(),
            runtime.clone(),
        )?;

        // Wait for both listeners to be ready before returning
        // This ensures tests don't connect before accept loops are ready
        tcp_ready_rx.await.map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "TCP readiness wait failed: {}",
                e
            ))) as Box<dyn std::error::Error>
        })?;
        ws_ready_rx.await.map_err(|e| {
            Box::new(std::io::Error::other(format!(
                "WebSocket readiness wait failed: {}",
                e
            ))) as Box<dyn std::error::Error>
        })?;

        // Mark startup complete
        runtime.mark_startup_complete();

        // Return the runtime wrapped in Arc
        let runtime_arc = Arc::new(runtime);
        let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();

        Ok(TestServer {
            tcp_addr,
            ws_addr,
            runtime: runtime_arc,
            _shutdown: _shutdown_tx,
            _permit: permit,
        })
    }

    /// Connect to the test server via TCP
    pub async fn connect(&self) -> Result<TestClient, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(self.tcp_addr).await?;
        Ok(TestClient { stream })
    }

    /// Connect to the test server via WebSocket
    pub async fn connect_ws(&self) -> Result<TestWebSocketClient, Box<dyn std::error::Error>> {
        let url = format!("ws://{}/", self.ws_addr);
        TestWebSocketClient::connect(&url).await
    }
}

/// Test client for sending raw protocol frames
pub struct TestClient {
    stream: TcpStream,
}

impl TestClient {
    /// Create a client by connecting to an address
    pub async fn new(addr: SocketAddr) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }

    /// Send a length-prefixed frame (TCP protocol)
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        // Write length prefix (u32 BE)
        let len = frame.len() as u32;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(frame).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Receive a length-prefixed frame with timeout
    pub async fn recv_frame(
        &mut self,
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let recv_future = async {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            self.stream.read_exact(&mut len_buf).await?;
            let len = u32::from_be_bytes(len_buf) as usize;

            // Read frame
            let mut frame = vec![0u8; len];
            self.stream.read_exact(&mut frame).await?;
            Ok::<Vec<u8>, std::io::Error>(frame)
        };

        timeout(Duration::from_millis(timeout_ms), recv_future)
            .await
            .map_err(|_| "timeout waiting for response".to_string())?
            .map_err(|e| e.into())
    }

    /// Send a frame and wait for response
    pub async fn request(
        &mut self,
        frame: &[u8],
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.send_frame(frame).await?;
        self.recv_frame(timeout_ms).await
    }
}

/// Test client for WebSocket connections
pub struct TestWebSocketClient {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pending_frames: VecDeque<Message>,
}

impl TestWebSocketClient {
    /// Connect to a WebSocket server
    pub async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let (ws_stream, _response) = connect_async(url).await?;
        Ok(Self {
            ws: ws_stream,
            pending_frames: VecDeque::new(),
        })
    }

    /// Send a WebSocket binary frame (no length prefix - handled by WebSocket protocol)
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.ws.send(Message::Binary(frame.to_vec())).await?;
        Ok(())
    }

    /// Receive a WebSocket binary frame with timeout
    pub async fn recv_frame(
        &mut self,
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let recv_future = async {
            loop {
                // Check if we have pending frames from previous recv() calls
                // This is a fast synchronous check that avoids async overhead
                while let Some(msg) = self.pending_frames.pop_front() {
                    match msg {
                        Message::Binary(data) => return Ok(data),
                        Message::Ping(_) | Message::Pong(_) => continue,
                        Message::Close(_) => return Err("WebSocket closed".into()),
                        Message::Text(_) => continue,
                        Message::Frame(_) => continue,
                    }
                }

                // Pending buffer empty, await next message from WebSocket stream
                match self.ws.next().await {
                    Some(Ok(msg)) => {
                        match msg {
                            Message::Binary(data) => return Ok(data),
                            Message::Ping(_) | Message::Pong(_) => {
                                // Filter out control frames, try next message
                                continue;
                            }
                            Message::Close(_) => {
                                return Err("WebSocket closed".into());
                            }
                            Message::Text(_) => {
                                // Filter out text frames, try next message
                                continue;
                            }
                            Message::Frame(_) => {
                                // Filter out raw frames, try next message
                                continue;
                            }
                        }
                    }
                    Some(Err(e)) => return Err(e.into()),
                    None => return Err("WebSocket stream ended".into()),
                }
            }
        };

        timeout(Duration::from_millis(timeout_ms), recv_future)
            .await
            .map_err(|_| "timeout waiting for response".to_string())?
            .map_err(|e: Box<dyn std::error::Error>| e)
    }

    /// Send a frame and wait for response
    pub async fn request(
        &mut self,
        frame: &[u8],
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.send_frame(frame).await?;
        self.recv_frame(timeout_ms).await
    }
}

/// TLV encoder for building protocol frames
pub struct TlvFrameBuilder {
    buf: BytesMut,
}

impl TlvFrameBuilder {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::new(),
        }
    }

    /// Encode a TLV field: [message_type: u8 or ESCAPE+u16 BE][length: u16 BE][value: bytes]
    pub fn encode_field(&mut self, msg_type: u16, value: &[u8]) {
        // MessageType encoding:
        // - If msg_type <= 254: single byte
        // - If msg_type > 254: [0xFF escape][msg_type as u16 BE]
        const ESCAPE_MARKER: u8 = 0xFF;
        const MAX_SINGLE_BYTE: u16 = 254;

        if msg_type <= MAX_SINGLE_BYTE {
            self.buf.put_u8(msg_type as u8);
        } else {
            self.buf.put_u8(ESCAPE_MARKER);
            self.buf.put_slice(&msg_type.to_be_bytes());
        }

        // Length is u16 BE (max 65535 bytes)
        if value.len() > 65535 {
            panic!("TLV value too large: {} bytes", value.len());
        }
        self.buf.put_slice(&(value.len() as u16).to_be_bytes());

        // Value
        self.buf.put_slice(value);
    }

    /// Build the final frame
    pub fn build(self) -> Vec<u8> {
        self.buf.to_vec()
    }
}

/// TLV decoder for parsing protocol frames
pub struct TlvFrameParser {
    buf: Vec<u8>,
    offset: usize,
}

impl TlvFrameParser {
    pub fn new(buf: Vec<u8>) -> Self {
        Self { buf, offset: 0 }
    }

    /// Parse next TLV field
    pub fn next_field(&mut self) -> Option<(u16, Vec<u8>)> {
        const ESCAPE_MARKER: u8 = 0xFF;

        if self.offset >= self.buf.len() {
            return None;
        }

        // Parse msg_type
        let msg_type = if self.buf[self.offset] == ESCAPE_MARKER {
            if self.offset + 3 > self.buf.len() {
                return None;
            }
            let mt = u16::from_be_bytes([self.buf[self.offset + 1], self.buf[self.offset + 2]]);
            self.offset += 3;
            mt
        } else {
            let mt = self.buf[self.offset] as u16;
            self.offset += 1;
            mt
        };

        // Parse length (u16 BE)
        if self.offset + 2 > self.buf.len() {
            return None;
        }
        let len = u16::from_be_bytes([self.buf[self.offset], self.buf[self.offset + 1]]) as usize;
        self.offset += 2;

        // Parse value
        if self.offset + len > self.buf.len() {
            return None;
        }
        let value = self.buf[self.offset..self.offset + len].to_vec();
        self.offset += len;

        Some((msg_type, value))
    }

    /// Parse all fields
    pub fn parse_all(&mut self) -> Vec<(u16, Vec<u8>)> {
        let mut fields = Vec::new();
        while let Some(field) = self.next_field() {
            fields.push(field);
        }
        fields
    }
}

impl Default for TlvFrameBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Build CONNECT message (msg_type 1)
/// Wire format: [u32 realm_len][realm][u32 token_len][jwt_token]
pub fn build_connect_frame(_realm: &str, jwt_token: &str) -> Vec<u8> {
    // CONNECT frame: [msg_type: 1][length: u16 BE][JWT string bytes]
    // Server expects JWT as plain UTF-8 string, no additional structure
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(1, jwt_token.as_bytes()); // msg_type 1 = CONNECT
    builder.build()
}

/// Generate test JWT token for given realm
/// Uses HS256 with test secret "test-secret-key"
/// Token is valid for 1 hour from now
/// Includes full permissions for the realm
pub fn generate_test_jwt(realm: &str) -> String {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        iss: String,        // Issuer (empty for test, triggers no-verify path)
        aud: String,        // Audience
        tid: String,        // Realm identifier for auth routing
        sub: String,        // Subject: realm
        exp: i64,           // Expiration time
        iat: i64,           // Issued at
        roles: Vec<String>, // Permissions as role strings
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let claims = Claims {
        iss: "".to_string(),     // Empty issuer = no signature verification
        aud: "fitz".to_string(), // Standard audience
        tid: realm.to_string(),
        sub: realm.to_string(),
        exp: now + 3600, // Valid for 1 hour
        iat: now,
        roles: vec![
            format!("kv://{}/**#*", realm), // Full KV access for this realm
            format!("queue://{}/**#*", realm),
            format!("notice://{}/**#*", realm),
            format!("stream://{}/**#*", realm),
            format!("rpc://{}/**#*", realm),
            format!("lease://{}/**#*", realm),
            format!("schedule://{}/**#*", realm),
        ],
    };

    let mut header = Header::new(Algorithm::HS256);
    header.typ = Some("JWT".to_string());

    encode(
        &header,
        &claims,
        &EncodingKey::from_secret("test-secret-key".as_bytes()),
    )
    .unwrap()
}

/// Generate expired JWT (for testing rejection)
pub fn generate_expired_jwt(realm: &str) -> String {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        iss: String,
        aud: String,
        tid: String,
        sub: String,
        exp: i64,
        iat: i64,
        roles: Vec<String>,
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let claims = Claims {
        iss: "".to_string(),
        aud: "fitz".to_string(),
        tid: realm.to_string(),
        sub: realm.to_string(),
        exp: now - 3600, // Expired 1 hour ago
        iat: now - 7200,
        roles: vec![format!("kv://{}/**#*", realm)],
    };

    let header = Header::new(Algorithm::HS256);

    encode(
        &header,
        &claims,
        &EncodingKey::from_secret("test-secret-key".as_bytes()),
    )
    .unwrap()
}

/// Generate JWT with invalid signature (for testing rejection)
pub fn generate_invalid_signature_jwt(realm: &str) -> String {
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Claims {
        iss: String,
        aud: String,
        tid: String,
        sub: String,
        exp: i64,
        iat: i64,
        roles: Vec<String>,
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let claims = Claims {
        iss: "".to_string(),
        aud: "fitz".to_string(),
        tid: realm.to_string(),
        sub: realm.to_string(),
        exp: now + 3600,
        iat: now,
        roles: vec![format!("kv://{}/**#*", realm)],
    };

    let header = Header::new(Algorithm::HS256);

    // Use wrong secret to create invalid signature
    encode(
        &header,
        &claims,
        &EncodingKey::from_secret("wrong-secret-key".as_bytes()),
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_tlv_frame() {
        // Arrange
        let mut builder = TlvFrameBuilder::new();
        builder.encode_field(100, b"test_value");

        // Act
        let frame = builder.build();

        // Assert
        assert!(frame.len() >= 3 + b"test_value".len());
        assert_eq!(frame[0], 100); // msg_type
        assert_eq!(frame[1], 0);
        assert_eq!(frame[2], b"test_value".len() as u8);
        assert_eq!(&frame[3..], b"test_value");
    }

    #[test]
    fn should_decode_tlv_frame() {
        // Arrange
        let mut builder = TlvFrameBuilder::new();
        builder.encode_field(100, b"test_value");
        builder.encode_field(200, b"another_value");
        let frame = builder.build();

        // Act
        let mut parser = TlvFrameParser::new(frame);
        let fields = parser.parse_all();

        // Assert
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, 100);
        assert_eq!(fields[0].1, b"test_value");
        assert_eq!(fields[1].0, 200);
        assert_eq!(fields[1].1, b"another_value");
    }

    #[test]
    fn should_build_connect_frame() {
        // Arrange
        let realm = "test-realm";
        let jwt = "fake-jwt-token";

        // Act
        let frame = build_connect_frame(realm, jwt);

        // Assert
        assert!(!frame.is_empty());
        assert_eq!(frame[0], 1); // msg_type 1 (CONNECT)
    }

    #[test]
    fn should_generate_valid_jwt() {
        // Arrange
        let realm = "test-realm";

        // Act
        let jwt = generate_test_jwt(realm);

        // Assert
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(
            parts.len(),
            3,
            "JWT should have header.payload.signature format"
        );
    }
}
