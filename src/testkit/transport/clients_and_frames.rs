use bytes::{BufMut, BytesMut};
use futures_util::{SinkExt, StreamExt};
use std::collections::VecDeque;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::{
    client_async,
    tungstenite::{
        client::IntoClientRequest, handshake::client::Request as WebSocketRequest,
        protocol::Message,
    },
    MaybeTlsStream, WebSocketStream,
};

use super::{
    server::init_test_runtime_jwks_cache, TEST_AUDIENCE, TEST_ISSUER, TEST_RUNTIME_AUTH_SECRET,
};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    iss: String,
    aud: String,
    tid: String,
    sub: String,
    exp: i64,
    iat: i64,
    permissions: Vec<String>,
}

#[inline]
fn unix_time_now_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .cast_signed()
}

/// Test client for sending raw protocol frames
pub struct TestClient {
    pub(super) stream: TcpStream,
}

impl TestClient {
    /// Create a client by connecting to an address
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP connection cannot be established or
    /// configured.
    pub async fn new(addr: SocketAddr) -> Result<Self, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    /// Send a length-prefixed frame (TCP protocol)
    ///
    /// # Errors
    ///
    /// Returns an error if the frame length cannot be encoded or the socket
    /// write fails.
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        // Write length prefix (u32 BE)
        let len = u32::try_from(frame.len())?;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(frame).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Receive a length-prefixed frame with timeout
    ///
    /// # Errors
    ///
    /// Returns an error if the read times out, the socket read fails, or the
    /// frame length is invalid for the current platform.
    pub async fn recv_frame(
        &mut self,
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let recv_future = async {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            self.stream.read_exact(&mut len_buf).await?;
            let len = usize::try_from(u32::from_be_bytes(len_buf))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

            // Read frame
            let mut frame = vec![0u8; len];
            self.stream.read_exact(&mut frame).await?;
            Ok::<Vec<u8>, std::io::Error>(frame)
        };

        timeout(Duration::from_millis(timeout_ms), recv_future)
            .await
            .map_err(|_| "timeout waiting for response".to_string())?
            .map_err(Into::into)
    }

    /// Send a frame and wait for response
    ///
    /// # Errors
    ///
    /// Returns an error if sending fails, receiving fails, or the response
    /// times out.
    pub async fn request(
        &mut self,
        frame: &[u8],
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.send_frame(frame).await?;
        self.recv_frame(timeout_ms).await
    }

    /// Gracefully close the TCP client connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket shutdown fails.
    pub async fn close(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.stream.shutdown().await?;
        Ok(())
    }
}

/// Test client for WebSocket connections
pub struct TestWebSocketClient {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pending_frames: VecDeque<Message>,
}

impl TestWebSocketClient {
    /// Connect to a WebSocket server
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid, the TCP connection fails, or
    /// the WebSocket handshake is rejected.
    pub async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let request = url.into_client_request()?;
        Self::connect_request(request).await
    }

    /// Connect to a WebSocket server with an explicit `Origin` header.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL or header is invalid, the TCP connection
    /// fails, or the WebSocket handshake is rejected.
    pub async fn connect_with_origin(
        url: &str,
        origin: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut request = url.into_client_request()?;
        request.headers_mut().insert("Origin", origin.parse()?);
        Self::connect_request(request).await
    }

    async fn connect_request(
        request: WebSocketRequest,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let uri = request.uri();
        let host = uri.host().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "websocket url missing host",
            )
        })?;
        let port = uri.port_u16().unwrap_or(80);
        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        let (ws_stream, _response) = client_async(request, MaybeTlsStream::Plain(stream)).await?;
        Ok(Self {
            ws: ws_stream,
            pending_frames: VecDeque::new(),
        })
    }

    /// Send a WebSocket binary frame (no length prefix - handled by WebSocket protocol)
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket send fails.
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.ws.send(Message::Binary(frame.to_vec().into())).await?;
        Ok(())
    }

    /// Receive a WebSocket binary frame with timeout
    ///
    /// # Errors
    ///
    /// Returns an error if the read times out, the socket closes, or the
    /// WebSocket stream yields an error.
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
                        Message::Binary(data) => return Ok(data.to_vec()),
                        Message::Ping(_)
                        | Message::Pong(_)
                        | Message::Text(_)
                        | Message::Frame(_) => {}
                        Message::Close(_) => return Err("WebSocket closed".into()),
                    }
                }

                // Pending buffer empty, await next message from WebSocket stream
                match self.ws.next().await {
                    Some(Ok(msg)) => match msg {
                        Message::Binary(data) => return Ok(data.to_vec()),
                        Message::Ping(_)
                        | Message::Pong(_)
                        | Message::Text(_)
                        | Message::Frame(_) => {}
                        Message::Close(_) => return Err("WebSocket closed".into()),
                    },
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
    ///
    /// # Errors
    ///
    /// Returns an error if sending fails, receiving fails, or the response
    /// times out.
    pub async fn request(
        &mut self,
        frame: &[u8],
        timeout_ms: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.send_frame(frame).await?;
        self.recv_frame(timeout_ms).await
    }

    /// Gracefully close the websocket client connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket close frame cannot be sent.
    pub async fn close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.ws.close(None).await?;
        Ok(())
    }
}

/// TLV encoder for building protocol frames
pub struct TlvFrameBuilder {
    buf: BytesMut,
}

impl TlvFrameBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: BytesMut::new(),
        }
    }

    /// Encode a TLV field: [`message_type`: u8 or ESCAPE+u16 BE][length: u16 BE][value: bytes]
    ///
    /// # Panics
    ///
    /// Panics if the TLV value exceeds 65535 bytes.
    pub fn encode_field(&mut self, msg_type: u16, value: &[u8]) {
        // MessageType encoding:
        // - If msg_type <= 254: single byte
        // - If msg_type > 254: [0xFF escape][msg_type as u16 BE]
        const ESCAPE_MARKER: u8 = 0xFF;
        const MAX_SINGLE_BYTE: u16 = 254;

        if msg_type <= MAX_SINGLE_BYTE {
            self.buf
                .put_u8(u8::try_from(msg_type).expect("single-byte TLV type validated above"));
        } else {
            self.buf.put_u8(ESCAPE_MARKER);
            self.buf.put_slice(&msg_type.to_be_bytes());
        }

        // Length is u16 BE (max 65535 bytes)
        assert!(
            value.len() <= 65535,
            "TLV value too large: {} bytes",
            value.len()
        );
        self.buf.put_slice(
            &u16::try_from(value.len())
                .expect("TLV value length checked above")
                .to_be_bytes(),
        );

        // Value
        self.buf.put_slice(value);
    }

    /// Build the final frame
    #[must_use]
    pub fn build(self) -> Vec<u8> {
        self.buf.to_vec()
    }
}

/// TLV decoder for parsing protocol frames
pub struct TlvFrameParser<'a> {
    buf: &'a [u8],
    offset: usize,
}

impl<'a> TlvFrameParser<'a> {
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, offset: 0 }
    }

    /// Parse next TLV field without copying the payload.
    pub fn next_field_ref(&mut self) -> Option<(u16, &'a [u8])> {
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
            let mt = u16::from(self.buf[self.offset]);
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
        let value = &self.buf[self.offset..self.offset + len];
        self.offset += len;

        Some((msg_type, value))
    }

    /// Parse next TLV field into an owned buffer.
    pub fn next_field(&mut self) -> Option<(u16, Vec<u8>)> {
        self.next_field_ref()
            .map(|(msg_type, value)| (msg_type, value.to_vec()))
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

/// Build CONNECT message (`msg_type` 1).
/// The legacy route argument is ignored; CONNECT carries only the JWT payload.
#[must_use]
pub fn build_connect_frame(_realm: &str, jwt_token: &str) -> Vec<u8> {
    // CONNECT frame: [msg_type: 1][length: u16 BE][JWT string bytes]
    // Server expects JWT as plain UTF-8 string, no additional structure
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(1, jwt_token.as_bytes()); // msg_type 1 = CONNECT
    builder.build()
}

/// Generate a provider-shaped test JWT for a single partition string.
/// Uses JWKS-mode token shape signed with the shared test issuer secret.
/// Token is valid for 1 hour from now
/// Emits `tid` plus top-level `permissions`
#[must_use]
pub fn generate_test_jwt(realm: &str) -> String {
    generate_test_jwt_for_family(realm, 1)
}

/// Generate a provider-shaped test JWT for a specific route family slot.
///
/// # Panics
///
/// Panics if system time is earlier than the Unix epoch or JWT encoding fails.
#[must_use]
pub fn generate_test_jwt_for_family(realm: &str, _route_family: u32) -> String {
    init_test_runtime_jwks_cache();
    let now = unix_time_now_i64();

    let claims = JwtClaims {
        iss: TEST_ISSUER.to_string(),
        aud: TEST_AUDIENCE.to_string(),
        tid: realm.to_string(),
        sub: realm.to_string(),
        exp: now + 3600, // Valid for 1 hour
        iat: now,
        permissions: vec![
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
        &EncodingKey::from_secret(TEST_RUNTIME_AUTH_SECRET.as_bytes()),
    )
    .unwrap()
}

/// Generate expired JWT (for testing rejection)
///
/// # Panics
///
/// Panics if system time is earlier than the Unix epoch or JWT encoding fails.
#[must_use]
pub fn generate_expired_jwt(realm: &str) -> String {
    init_test_runtime_jwks_cache();
    let now = unix_time_now_i64();

    let claims = JwtClaims {
        iss: TEST_ISSUER.to_string(),
        aud: TEST_AUDIENCE.to_string(),
        tid: realm.to_string(),
        sub: realm.to_string(),
        exp: now - 3600, // Expired 1 hour ago
        iat: now - 7200,
        permissions: vec![format!("kv://{}/**#*", realm)],
    };

    let header = Header::new(Algorithm::HS256);

    encode(
        &header,
        &claims,
        &EncodingKey::from_secret(TEST_RUNTIME_AUTH_SECRET.as_bytes()),
    )
    .unwrap()
}

/// Generate JWT with invalid signature (for testing rejection)
///
/// # Panics
///
/// Panics if system time is earlier than the Unix epoch or JWT encoding fails.
#[must_use]
pub fn generate_invalid_signature_jwt(realm: &str) -> String {
    init_test_runtime_jwks_cache();
    let now = unix_time_now_i64();

    let claims = JwtClaims {
        iss: TEST_ISSUER.to_string(),
        aud: TEST_AUDIENCE.to_string(),
        tid: realm.to_string(),
        sub: realm.to_string(),
        exp: now + 3600,
        iat: now,
        permissions: vec![format!("kv://{}/**#*", realm)],
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
