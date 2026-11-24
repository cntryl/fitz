use crate::testkit::scaled_test_timeout;
use bytes::{Bytes, BytesMut};
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

/// True for a broker frame that answers no request.
///
/// `HELLO` answers CONNECT on every new session. A request/response helper
/// must skip it rather than mistake it for the response it is waiting on.
fn is_unsolicited_control_frame(frame: &[u8]) -> bool {
    crate::protocol::wire3::header::MessageHeader::decode_wire3(frame)
        .is_ok_and(|header| header.message_type == crate::protocol::wire3::control::HELLO)
}

fn is_connect_frame(frame: &[u8]) -> bool {
    crate::protocol::wire3::header::MessageHeader::decode_wire3(frame)
        .is_ok_and(|header| header.message_type == 1)
}

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
    pub(super) started: bool,
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
        Ok(Self {
            stream,
            started: false,
        })
    }

    /// Send a length-prefixed frame (TCP protocol)
    ///
    /// # Errors
    ///
    /// Returns an error if the frame length cannot be encoded or the socket
    /// write fails.
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.send_frame_bytes(Bytes::copy_from_slice(frame)).await
    }

    /// Send an owned length-prefixed frame (TCP protocol).
    ///
    /// # Errors
    ///
    /// Returns an error if the frame length cannot be encoded or the socket
    /// write fails.
    pub async fn send_frame_bytes(
        &mut self,
        frame: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !self.started {
            self.started = true;
            if !is_connect_frame(&frame) {
                let connect = build_connect_frame("", "");
                self.stream.write_u32(u32::try_from(connect.len())?).await?;
                self.stream.write_all(&connect).await?;
            }
        }
        // Write length prefix (u32 BE)
        let len = u32::try_from(frame.len())?;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(&frame).await?;
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
        self.recv_frame_bytes(timeout_ms)
            .await
            .map(|frame| frame.to_vec())
    }

    /// Receive a length-prefixed frame as `Bytes` with timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the read times out, the socket read fails, or the
    /// frame length is invalid for the current platform.
    pub async fn recv_frame_bytes(
        &mut self,
        timeout_ms: u64,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        timeout(
            scaled_test_timeout(Duration::from_millis(timeout_ms)),
            self.recv_frame_bytes_without_timeout(),
        )
        .await
        .map_err(|_| "timeout waiting for response".to_string())?
    }

    /// Receive a length-prefixed frame as `Bytes` without installing a timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket read fails or the frame length is invalid
    /// for the current platform.
    pub async fn recv_frame_bytes_without_timeout(
        &mut self,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        loop {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            self.stream.read_exact(&mut len_buf).await?;
            let len = usize::try_from(u32::from_be_bytes(len_buf))
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;

            let mut frame = BytesMut::with_capacity(len);
            frame.resize(len, 0);
            self.stream.read_exact(&mut frame).await?;
            if is_unsolicited_control_frame(&frame) {
                continue;
            }
            return Ok(frame.freeze());
        }
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

    /// Send a `Bytes` frame and wait for a `Bytes` response.
    ///
    /// # Errors
    ///
    /// Returns an error if sending fails, receiving fails, or the response
    /// times out.
    pub async fn request_bytes(
        &mut self,
        frame: Bytes,
        timeout_ms: u64,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        self.send_frame_bytes(frame).await?;
        self.recv_frame_bytes(timeout_ms).await
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
    started: bool,
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
            started: false,
        })
    }

    /// Send a WebSocket binary frame (no length prefix - handled by WebSocket protocol)
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket send fails.
    pub async fn send_frame(&mut self, frame: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        self.send_frame_bytes(Bytes::copy_from_slice(frame)).await?;
        Ok(())
    }

    /// Send a WebSocket binary frame from an owned `Bytes` buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the WebSocket send fails.
    pub async fn send_frame_bytes(
        &mut self,
        frame: Bytes,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !self.started {
            self.started = true;
            if !is_connect_frame(&frame) {
                self.ws
                    .send(Message::Binary(build_connect_frame("", "").into()))
                    .await?;
            }
        }
        self.ws.send(Message::Binary(frame)).await?;
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
        self.recv_frame_bytes(timeout_ms)
            .await
            .map(|frame| frame.to_vec())
    }

    /// Receive a WebSocket binary frame as `Bytes` with timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the read times out, the socket closes, or the
    /// WebSocket stream yields an error.
    pub async fn recv_frame_bytes(
        &mut self,
        timeout_ms: u64,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        timeout(
            scaled_test_timeout(Duration::from_millis(timeout_ms)),
            self.recv_frame_bytes_without_timeout(),
        )
        .await
        .map_err(|_| "timeout waiting for response".to_string())?
    }

    /// Receive the next WebSocket binary frame without installing a per-frame timeout.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket closes or the WebSocket stream yields an error.
    pub async fn recv_frame_bytes_without_timeout(
        &mut self,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        loop {
            // Check if we have pending frames from previous recv() calls.
            while let Some(msg) = self.pending_frames.pop_front() {
                match msg {
                    Message::Binary(data) => {
                        if is_unsolicited_control_frame(&data) {
                            continue;
                        }
                        return Ok(data);
                    }
                    Message::Ping(_) | Message::Pong(_) | Message::Text(_) | Message::Frame(_) => {}
                    Message::Close(_) => return Err("WebSocket closed".into()),
                }
            }

            match self.ws.next().await {
                Some(Ok(msg)) => match msg {
                    Message::Binary(data) => {
                        if is_unsolicited_control_frame(&data) {
                            continue;
                        }
                        return Ok(data);
                    }
                    Message::Ping(_) | Message::Pong(_) | Message::Text(_) | Message::Frame(_) => {}
                    Message::Close(_) => return Err("WebSocket closed".into()),
                },
                Some(Err(e)) => return Err(e.into()),
                None => return Err("WebSocket stream ended".into()),
            }
        }
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

    /// Send a `Bytes` frame and wait for a `Bytes` response.
    ///
    /// # Errors
    ///
    /// Returns an error if sending fails, receiving fails, or the response
    /// times out.
    pub async fn request_bytes(
        &mut self,
        frame: Bytes,
        timeout_ms: u64,
    ) -> Result<Bytes, Box<dyn std::error::Error>> {
        self.send_frame_bytes(frame).await?;
        self.recv_frame_bytes(timeout_ms).await
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

pub use fitz_core::testkit::frames::{FrameBuilder, FrameParser};

/// Build the Wire 3 CONNECT message carrying `jwt_token` and an empty capability set.
/// The legacy realm argument is ignored.
///
/// # Panics
///
/// Panics only if the empty capability set or generated CONNECT extension violates
/// the encoder's invariant.
#[must_use]
pub fn build_connect_frame(_realm: &str, jwt_token: &str) -> Vec<u8> {
    let core = connect_body(jwt_token, "");
    let capabilities = crate::protocol::wire3::extensions::encode_capability_ids(&[])
        .expect("empty capability set is valid");
    let fields = [crate::protocol::wire3::extensions::ExtensionField {
        field_id: crate::protocol::wire3::extensions::CAPABILITY_IDS_FIELD_ID,
        value: &capabilities,
    }];
    let payload = crate::protocol::wire3::extensions::encode_payload(
        &core,
        crate::protocol::wire3::extensions::CONNECT_MESSAGE_TYPE,
        crate::protocol::wire3::extensions::ExtensionDirection::ClientToBroker,
        &fields,
    )
    .expect("CONNECT capability extension is valid");
    crate::protocol::wire3::header::encode_message(
        crate::protocol::wire3::control::CONNECT,
        crate::protocol::wire3::header::HeaderFlags::extensions_present(),
        None,
        payload.value.as_ref(),
    )
}

/// Encode the Wire 3 CONNECT core body for direct ingress tests.
#[must_use]
pub fn connect_body(jwt_token: &str, service_name: &str) -> Vec<u8> {
    crate::protocol::wire3::handshake::Connect {
        min_version: crate::protocol::wire3::WIRE_VERSION,
        max_version: crate::protocol::wire3::WIRE_VERSION,
        client_caps: 0,
        jwt: jwt_token.to_owned(),
        service_name: service_name.to_owned(),
        client_build: "fitz-testkit".to_owned(),
    }
    .encode()
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
        &EncodingKey::from_secret(TEST_RUNTIME_AUTH_SECRET.as_bytes())
            .expect("nonempty test secret"),
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
        &EncodingKey::from_secret(TEST_RUNTIME_AUTH_SECRET.as_bytes())
            .expect("nonempty test secret"),
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
        &EncodingKey::from_secret("wrong-secret-key".as_bytes()).expect("nonempty test secret"),
    )
    .unwrap()
}
