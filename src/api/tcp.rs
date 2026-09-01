// LAYER: API
//! TCP protocol adapter with length-prefixed framing
//!
//! This module provides TCP transport handling that:
//! 1. Uses length-prefixed frames (u32 BE + payload)
//! 2. Converts frames to `bytes::Bytes`
//! 3. Forwards frames through the `Ingress` boundary trait
//! 4. Handles session lifecycle and backpressure

use crate::api::ingress::IngressConfig;
use crate::api::runtime_ingress::Ingress;
use crate::observability as obs;
use crate::session::{
    generate_session_id, Session, SessionMetadata, SessionPermissions, TransportKind,
};
use bytes::{Bytes, BytesMut};
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
    /// * `config` - Ingress configuration
    /// * `session_id` - Session ID from ingress
    /// * `tx` - Channel for forwarding frames to runtime
    /// * `read_half` - Read half of the TCP stream
    #[must_use]
    pub fn new(
        config: IngressConfig,
        session_id: u64,
        tx: mpsc::Sender<(u64, Bytes)>,
        read_half: OwnedReadHalf,
    ) -> Self {
        Self {
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
    /// # Errors
    ///
    /// Returns an error when reading from the socket fails, the peer sends a
    /// frame larger than the configured maximum, the transport channel remains
    /// backpressured through the configured timeout, or the runtime channel closes.
    pub async fn run(mut self) -> Result<(), String> {
        let mut buffer = BytesMut::with_capacity(4096);
        info!(session_id = self.session_id, "TCP handler run loop started");

        loop {
            let n = self.read_into_buffer(&mut buffer).await?;

            // 0 bytes means EOF
            if n == 0 {
                info!(
                    session_id = self.session_id,
                    "TCP connection EOF (client closed)"
                );
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
                let len = frame_len(&buffer);

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
                    return Err(reason);
                }

                // Check if we have the complete frame
                if buffer.len() < 4 + len {
                    break;
                }

                // Split the complete length-prefixed record once and slice the
                // payload directly so the hot path does not clone frame bytes.
                let record = buffer.split_to(4 + len).freeze();
                let frame = record.slice(4..);

                debug!(
                    session_id = self.session_id,
                    frame_len = len,
                    "TCP frame extracted, forwarding to runtime"
                );

                // Forward to runtime with backpressure handling.
                // Pass ownership directly — recover from TrySendError::Full to avoid
                // an atomic ref-count increment (Bytes::clone) on every successful send.
                let _handoff_latency = crate::observability::ScopedHistogramUs::new(
                    obs::METRIC_TCP_CHANNEL_HANDOFF_LATENCY,
                );
                if let Err(send_error) = self.tx.try_send((self.session_id, frame)) {
                    self.handle_send_error(send_error).await?;
                } else {
                    trace!(
                        session_id = self.session_id,
                        "TCP frame forwarded to channel successfully"
                    );
                }
            }
        }
    }

    async fn read_into_buffer(&mut self, buffer: &mut BytesMut) -> Result<usize, String> {
        match self.read_half.read_buf(buffer).await {
            Ok(bytes_read) => Ok(bytes_read),
            Err(error) => {
                error!(session_id = self.session_id, error = %error, "TCP read error");
                let reason = format!("read error: {error}");
                Err(reason)
            }
        }
    }

    async fn handle_send_error(
        &self,
        error: mpsc::error::TrySendError<(u64, Bytes)>,
    ) -> Result<(), String> {
        match error {
            mpsc::error::TrySendError::Full((_, frame)) => self.retry_send(frame).await,
            mpsc::error::TrySendError::Closed(_) => {
                error!(session_id = self.session_id, "TCP runtime channel closed");
                Self::close_with_error("runtime channel closed".to_string())
            }
        }
    }

    async fn retry_send(&self, frame: Bytes) -> Result<(), String> {
        crate::observability::counter_inc(obs::METRIC_TCP_BACKPRESSURE);
        warn!(
            session_id = self.session_id,
            "TCP channel full, waiting for handoff capacity"
        );
        match tokio::time::timeout(
            self.config.backpressure_timeout,
            self.tx.send((self.session_id, frame)),
        )
        .await
        {
            Ok(Ok(())) => {
                debug!(
                    session_id = self.session_id,
                    "TCP frame forwarded after backpressure"
                );
                Ok(())
            }
            Ok(Err(_)) => {
                error!(session_id = self.session_id, "TCP runtime channel closed");
                Self::close_with_error("runtime channel closed".to_string())
            }
            Err(_) => {
                error!(
                    session_id = self.session_id,
                    "TCP backpressure exceeded, closing session"
                );
                Self::close_with_error("channel full: backpressure exceeded".to_string())
            }
        }
    }

    fn close_with_error(reason: String) -> Result<(), String> {
        Err(reason)
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
///
/// # Errors
///
/// Returns an error when enabling `TCP_NODELAY` fails or ingress rejects the
/// newly opened transport session.
pub async fn create_session(
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    stream: TcpStream,
    tx: mpsc::Sender<(u64, Bytes)>,
) -> Result<(TcpHandler, OwnedWriteHalf), String> {
    stream
        .set_nodelay(true)
        .map_err(|e| format!("failed to enable TCP_NODELAY: {e}"))?;
    let socket = socket2::SockRef::from(&stream);
    let keepalive =
        socket2::TcpKeepalive::new().with_time(crate::api::ingress::TCP_KEEPALIVE_INTERVAL);
    socket
        .set_tcp_keepalive(&keepalive)
        .map_err(|e| format!("failed to enable TCP keepalive: {e}"))?;

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
        TcpHandler::new(config, session_id, tx, read_half),
        write_half,
    ))
}

fn frame_len(buffer: &BytesMut) -> usize {
    let len_bytes = &buffer[..4];
    u32::from_be_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

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
        // Arrange: a real length-prefixed frame buffer as `frame_len` expects
        // to receive it off the wire (4-byte big-endian length + payload).
        let data = [1u8, 2, 3, 4, 5];
        let mut buffer = BytesMut::new();
        let encoded_len = u32::try_from(data.len()).expect("test frame length should fit in u32");
        buffer.extend_from_slice(&encoded_len.to_be_bytes());
        buffer.extend_from_slice(&data);

        // Act
        let decoded_len = frame_len(&buffer);

        // Assert
        assert_eq!(decoded_len, 5);
    }

    #[test]
    fn should_handle_large_frames() {
        // Arrange
        let large_len: u32 = 1024 * 1024; // 1 MB
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&large_len.to_be_bytes());
        buffer.extend_from_slice(&[0u8; 4]); // frame_len only reads the prefix

        // Act
        let decoded_len = frame_len(&buffer);

        // Assert
        assert_eq!(decoded_len, 1024 * 1024);
    }

    #[tokio::test]
    async fn should_resume_tcp_handoff_when_capacity_returns_before_timeout() {
        // Arrange
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test listener");
        let address = listener.local_addr().expect("test listener address");
        let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let _client = client.expect("connect test client");
        let (server, _) = accepted.expect("accept test client");
        let (read_half, _write_half) = server.into_split();
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send((7, Bytes::from_static(b"occupied")))
            .expect("fill handoff channel");
        let handler = TcpHandler::new(
            IngressConfig::default().with_backpressure_timeout(Duration::from_millis(200)),
            7,
            tx,
            read_half,
        );

        // Act
        let mut retry =
            tokio::spawn(async move { handler.retry_send(Bytes::from_static(b"next")).await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = rx.recv().await.expect("drain occupied frame");
        let resumed = tokio::time::timeout(Duration::from_millis(50), &mut retry).await;
        if resumed.is_err() {
            retry.abort();
            let _ = retry.await;
        }

        // Assert
        let retry_result = resumed
            .expect("handoff should resume promptly")
            .expect("join retry task");
        assert_eq!(retry_result, Ok(()));
        assert_eq!(
            rx.recv().await.expect("retried frame"),
            (7, Bytes::from_static(b"next"))
        );
    }
}
