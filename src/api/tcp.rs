// LAYER: API
//! TCP protocol adapter with length-prefixed framing
//!
//! This module provides TCP transport handling that:
//! 1. Uses length-prefixed frames (u32 BE + payload)
//! 2. Converts frames to `bytes::Bytes`
//! 3. Forwards frames through the `Ingress` boundary trait
//! 4. Handles session lifecycle and backpressure

use crate::api::ingress::IngressConfig;
use crate::session::manager::Ingress;
use crate::session::{CloseReason, Session, SessionMetadata, SessionPermissions, TransportKind};
use bytes::{Buf, Bytes, BytesMut};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};

/// TCP connection handler
///
/// Manages a single TCP connection with length-prefixed frame protocol:
/// - Frame format: [u32 BE length][payload]
/// - Length field includes only the payload (4 bytes)
/// - Normalizes frames and forwards them through the runtime ingress boundary.
///
/// The handler owns the read half of the TCP stream. The write half is
/// returned separately from `create_session` so an independent outbound
/// writer task can send response frames without contending on a shared mutex.
pub struct TcpHandler {
    /// Ingress trait implementation (runtime boundary)
    ingress: Arc<dyn Ingress>,
    /// Configuration for ingress
    config: IngressConfig,
    /// Session ID assigned by ingress
    pub session_id: u64,
    /// Channel for sending frames to runtime
    tx: mpsc::Sender<(u64, Bytes)>,
    /// Read half of the TCP stream (writer half is owned by the outbound task)
    read_half: OwnedReadHalf,
}

impl TcpHandler {
    /// Create a new TCP handler
    ///
    /// # Arguments
    ///
    /// * `ingress` - Runtime boundary implementation
    /// * `config` - Ingress configuration
    /// * `session_id` - Session ID from ingress
    /// * `tx` - Channel for forwarding frames to runtime
    /// * `read_half` - Read half of the TCP stream
    pub fn new(
        ingress: Arc<dyn Ingress>,
        config: IngressConfig,
        session_id: u64,
        tx: mpsc::Sender<(u64, Bytes)>,
        read_half: OwnedReadHalf,
    ) -> Self {
        Self {
            ingress,
            config,
            session_id,
            tx,
            read_half,
        }
    }

    /// Process incoming TCP frames
    ///
    /// Reads length-prefixed frames from the TCP stream and forwards them
    /// to the runtime. Handles backpressure gracefully.
    ///
    /// Returns `Ok` on clean close, `Err` on error
    pub async fn run(mut self) -> Result<(), String> {
        let mut buffer = BytesMut::with_capacity(4096);
        info!(session_id = self.session_id, "TCP handler run loop started");

        loop {
            // Read more data from the owned read half (no mutex needed)
            let n = self.read_half.read_buf(&mut buffer).await.map_err(|e| {
                error!(session_id = self.session_id, error = %e, "TCP read error");
                format!("read error: {}", e)
            })?;

            // 0 bytes means EOF
            if n == 0 {
                info!(
                    session_id = self.session_id,
                    "TCP connection EOF (client closed)"
                );
                self.ingress
                    .on_close(self.session_id, CloseReason::ClientClose)
                    .await;
                return Ok(());
            }
            trace!(
                session_id = self.session_id,
                bytes_read = n,
                buffer_len = buffer.len(),
                "TCP read data"
            );

            // Try to extract complete frames from buffer
            while buffer.len() >= 4 {
                // Read length field (u32 BE)
                let len_bytes = &buffer[0..4];
                let len =
                    u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]])
                        as usize;

                // Check frame size
                if len > self.config.max_frame_size {
                    let reason =
                        format!("frame too large: {} > {}", len, self.config.max_frame_size);
                    warn!(
                        session_id = self.session_id,
                        frame_len = len,
                        max = self.config.max_frame_size,
                        "TCP frame too large, closing"
                    );
                    self.ingress
                        .on_close(self.session_id, CloseReason::Error(reason.clone()))
                        .await;
                    return Err(reason);
                }

                // Check if we have the complete frame
                if buffer.len() < 4 + len {
                    break;
                }

                // Extract frame payload (skip 4-byte length field)
                let frame = Bytes::copy_from_slice(&buffer[4..4 + len]);
                buffer.advance(4 + len);

                debug!(
                    session_id = self.session_id,
                    frame_len = len,
                    "TCP frame extracted, forwarding to runtime"
                );
                trace!(
                    session_id = self.session_id,
                    frame_hex = %hex_preview(&frame),
                    "TCP frame payload preview"
                );

                // Forward to runtime with backpressure handling
                match self.tx.try_send((self.session_id, frame.clone())) {
                    Ok(()) => {
                        trace!(
                            session_id = self.session_id,
                            "TCP frame forwarded to channel successfully"
                        );
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        warn!(
                            session_id = self.session_id,
                            "TCP channel full, backpressure - retrying after timeout"
                        );
                        tokio::time::sleep(self.config.backpressure_timeout).await;
                        match self.tx.try_send((self.session_id, frame)) {
                            Ok(()) => {
                                debug!(
                                    session_id = self.session_id,
                                    "TCP frame forwarded after backpressure retry"
                                );
                            }
                            Err(_) => {
                                let reason = "channel full: backpressure exceeded".to_string();
                                error!(
                                    session_id = self.session_id,
                                    "TCP backpressure exceeded, closing session"
                                );
                                self.ingress
                                    .on_close(self.session_id, CloseReason::Error(reason.clone()))
                                    .await;
                                return Err(reason);
                            }
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        error!(session_id = self.session_id, "TCP runtime channel closed");
                        return Err("runtime channel closed".to_string());
                    }
                }
            }
        }
    }
}

/// Create a new TCP session and handler
///
/// Splits the TCP stream into independent read and write halves to avoid
/// mutex contention between the inbound reader and outbound writer.
///
/// # Arguments
///
/// * `ingress` - Runtime boundary implementation
/// * `config` - Ingress configuration
/// * `stream` - TCP stream (will be split)
/// * `tx` - Channel for forwarding frames
///
/// # Returns
///
/// `(handler, write_half)` if session accepted, error message if rejected.
/// The caller should pass `write_half` to the outbound writer task.
pub async fn create_session(
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    stream: TcpStream,
    tx: mpsc::Sender<(u64, Bytes)>,
) -> Result<(TcpHandler, OwnedWriteHalf), String> {
    // Extract peer address before splitting
    let peer_addr = stream.peer_addr().ok();
    debug!(peer_addr = ?peer_addr, "Creating TCP session");

    // Split stream into independent read/write halves (no shared mutex)
    let (read_half, write_half) = stream.into_split();

    // Create transport-level session
    let session_config = crate::session::NewSessionConfig::unauthenticated(
        TransportKind::Tcp,
        peer_addr,
        SessionPermissions::empty(),
        SessionMetadata::new(),
        config.channel_capacity,
        None,
        crate::runtime::routing::RouteFamily::new(1), // Default dev family = 1
    );
    let session = Session::new(generate_session_id(), session_config);

    // Let ingress validate and accept the session
    let session_id = ingress.on_open(session.info()).await?;
    info!(session_id = session_id, peer_addr = ?peer_addr, "TCP session created and accepted by ingress");

    // Create handler with the read half
    Ok((
        TcpHandler::new(ingress, config, session_id, tx, read_half),
        write_half,
    ))
}

/// Generate a unique session ID
fn generate_session_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);
    SESSION_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Helper: preview first N bytes as hex string for trace logging
fn hex_preview(data: &[u8]) -> String {
    let limit = data.len().min(32);
    let hex: Vec<String> = data[..limit].iter().map(|b| format!("{:02x}", b)).collect();
    if data.len() > limit {
        format!("{}... ({} bytes total)", hex.join(" "), data.len())
    } else {
        hex.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_generate_unique_session_ids() {
        // Arrange

        // Act
        let id1 = generate_session_id();
        let id2 = generate_session_id();
        let id3 = generate_session_id();

        // Assert
        assert!(id1 < id2);
        assert!(id2 < id3);
    }

    #[test]
    fn should_encode_length_prefix() {
        // Arrange
        let data = [1, 2, 3, 4, 5];
        let len = data.len() as u32;

        // Act
        let len_bytes = len.to_be_bytes();
        let reconstructed = u32::from_be_bytes(len_bytes) as usize;

        // Assert
        assert_eq!(reconstructed, 5);
    }

    #[test]
    fn should_handle_large_frames() {
        // Arrange
        let large_len = 1024 * 1024; // 1 MB

        // Act
        let len_bytes = (large_len as u32).to_be_bytes();
        let reconstructed = u32::from_be_bytes(len_bytes) as usize;

        // Assert
        assert_eq!(reconstructed, 1024 * 1024);
    }
}
