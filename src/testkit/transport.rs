//! Transport-layer test utilities for end-to-end integration tests
//!
//! Provides helpers for testing the complete request-response cycle:
//! Client → TCP/WebSocket → Session → Routing → Domain → Response → Client

use bytes::{BufMut, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

/// Test server that starts Fitz on a random available port
pub struct TestServer {
    pub addr: SocketAddr,
    pub runtime: Arc<crate::boot::Runtime>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

impl TestServer {
    /// Start a test server on random available port with in-memory storage
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        // Initialize tracing once for tests
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                std::env::var("RUST_LOG").unwrap_or_else(|_| "info,fitz=debug".to_string()),
            )
            .try_init();

        // Disable auth for tests via environment variable
        std::env::set_var("FITZ_AUTH_REQUIRED", "false");

        // Find an available port
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        drop(listener); // Release the port so spawn_tcp_listener can bind to it

        // Boot runtime with test configuration
        let boot_config = crate::boot::BootConfig {
            bind_addr: "127.0.0.1".to_string(),
            tcp_port: addr.port(),
            http_port: addr.port().saturating_sub(1000), // Offset to avoid conflicts
            storage_mode: crate::boot::runtime::StorageMode::Memory,
            auth_required: false,            // Disable auth for tests
            max_connections: 1000,
            max_frame_size: 16_777_216, // 16 MB
            channel_capacity: 10_000,
        };

        // Step 1: Initialize storage
        let store = crate::boot::storage::init(&boot_config).await?;

        // Step 2: Initialize runtime
        let (router, ingress, ingress_config, _scheduler, runtime) =
            crate::boot::runtime::init(&store)?;

        // Mark storage ready
        runtime.mark_storage_ready();

        // Step 3: Register domain actors
        crate::boot::domains::setup(&router, &store)?;

        // Mark domains ready
        runtime.mark_domains_ready();

        // Step 4: Start TCP listener (spawns its own listener task)
        crate::boot::handlers::spawn_tcp_listener(
            &boot_config,
            ingress.clone(),
            ingress_config.clone(),
            runtime.clone(),
        )
        .await?;

        // Mark startup complete
        runtime.mark_startup_complete();

        // Return the runtime wrapped in Arc
        let runtime_arc = Arc::new(runtime);
        let (_shutdown_tx, _shutdown_rx) = tokio::sync::oneshot::channel();

        Ok(TestServer {
            addr,
            runtime: runtime_arc,
            _shutdown: _shutdown_tx,
        })
    }

    /// Connect to the test server
    pub async fn connect(&self) -> Result<TestClient, Box<dyn std::error::Error>> {
        let stream = TcpStream::connect(self.addr).await?;
        Ok(TestClient { stream })
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
        if self.offset + 6 > self.buf.len() {
            return None;
        }

        let msg_type = u16::from_be_bytes([self.buf[self.offset], self.buf[self.offset + 1]]);
        let len =
            u32::from_be_bytes([
                self.buf[self.offset + 2],
                self.buf[self.offset + 3],
                self.buf[self.offset + 4],
                self.buf[self.offset + 5],
            ]) as usize;

        self.offset += 6;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_and_decode_tlv_frame() {
        // Arrange
        let mut builder = TlvFrameBuilder::new();
        builder.encode_field(100, b"test_value");
        builder.encode_field(200, b"another_value");

        // Act
        let frame = builder.build();
        let mut parser = TlvFrameParser::new(frame);
        let fields = parser.parse_all();

        // Assert
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, 100);
        assert_eq!(fields[0].1, b"test_value");
        assert_eq!(fields[1].0, 200);
        assert_eq!(fields[1].1, b"another_value");
    }
}
