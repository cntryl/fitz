//! Connection management
//!
//! Provides a unified connection abstraction over TCP and WebSocket transports,
//! plus a thread-safe `SharedConnection` wrapper that eliminates the
//! encode→send→recv→decode boilerplate from every domain method.

use crate::codec::{decode_message_frame, encode_message_frame};
use crate::error::{FitzError, Result};
use crate::transport::{AnyTransport, Transport};
use std::sync::{Arc, Mutex};

pub struct FitzConnection {
    transport: AnyTransport,
}

impl FitzConnection {
    /// Connect via TCP
    pub fn connect_tcp(host: &str, port: u16) -> Result<Self> {
        let transport = AnyTransport::Tcp(
            crate::transport::tcp::TcpTransport::connect(host, port)
                .map_err(|e| FitzError::Connection(e.to_string()))?,
        );
        Ok(Self { transport })
    }

    /// Connect via WebSocket
    pub fn connect_ws(url: &str) -> Result<Self> {
        let transport = AnyTransport::WebSocket(Box::new(
            crate::transport::websocket::WebSocketTransport::connect(url)
                .map_err(|e| FitzError::Connection(e.to_string()))?,
        ));
        Ok(Self { transport })
    }

    pub fn send_frame(&mut self, frame: &[u8]) -> Result<()> {
        self.transport
            .send_frame(frame)
            .map_err(|e| FitzError::Transport(e.to_string()))
    }

    pub fn recv_frame(&mut self) -> Result<Vec<u8>> {
        self.transport
            .recv_frame()
            .map_err(|e| FitzError::Transport(e.to_string()))
    }

    pub fn close(&mut self) -> Result<()> {
        self.transport
            .close()
            .map_err(|e| FitzError::Transport(e.to_string()))
    }
}

/// Thread-safe connection handle shared across domain clients.
///
/// Provides `send_request` which encapsulates the entire
/// encode→send→recv→strip-header cycle in one call, so domain
/// methods never touch framing.
#[derive(Clone)]
pub struct SharedConnection {
    inner: Arc<Mutex<FitzConnection>>,
}

impl SharedConnection {
    pub fn new(conn: FitzConnection) -> Self {
        Self {
            inner: Arc::new(Mutex::new(conn)),
        }
    }

    /// Send a typed request and return the raw response payload (TLV header stripped).
    ///
    /// This is the single point of encode→send→recv→decode for the entire client.
    /// Domain methods call this instead of manually locking, framing, and stripping.
    pub fn send_request(&self, msg_type: u16, payload: &[u8]) -> Result<Vec<u8>> {
        let frame = encode_message_frame(msg_type, payload);
        let mut conn = self.lock()?;
        conn.send_frame(&frame)?;
        let resp_frame = conn.recv_frame()?;
        drop(conn); // release lock before decoding
        strip_tlv_header(&resp_frame)
    }

    /// Send a frame with no response expected (fire-and-forget, e.g. CONNECT).
    pub fn send_only(&self, msg_type: u16, payload: &[u8]) -> Result<()> {
        let frame = encode_message_frame(msg_type, payload);
        let mut conn = self.lock()?;
        conn.send_frame(&frame)
    }

    pub fn close(&self) -> Result<()> {
        let mut conn = self.lock()?;
        conn.close()
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, FitzConnection>> {
        self.inner
            .lock()
            .map_err(|_| FitzError::Connection("Connection lock poisoned".into()))
    }
}

/// Strip the TLV header from a response frame and return just the payload bytes.
fn strip_tlv_header(frame: &[u8]) -> Result<Vec<u8>> {
    if frame.is_empty() {
        return Ok(Vec::new());
    }
    let (_msg_type, payload_start) = decode_message_frame(frame)?;
    Ok(frame[payload_start..].to_vec())
}
