//! WebSocket API endpoints with ingress integration
//!
//! This module provides WebSocket transport handling that:
//! 1. Accepts binary frames only
//! 2. Converts WebSocket frames to `bytes::Bytes`
//! 3. Forwards frames through the `Ingress` boundary trait
//! 4. Handles session lifecycle and backpressure

use crate::api::ingress::IngressConfig;
use crate::runtime::ingress::{CloseReason, Ingress, IngressDecision, Session, TransportKind};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

/// WebSocket connection handler
///
/// Manages a single WebSocket connection, normalizing frames
/// and forwarding them through the runtime ingress boundary.
pub struct WebSocketHandler {
    /// Ingress trait implementation (runtime boundary)
    ingress: Arc<dyn Ingress>,
    /// Configuration for ingress
    config: IngressConfig,
    /// Session ID assigned by ingress
    session_id: u64,
    /// Channel for sending frames to runtime
    tx: mpsc::Sender<(u64, Bytes)>,
}

impl WebSocketHandler {
    /// Create a new WebSocket handler
    ///
    /// # Arguments
    ///
    /// * `ingress` - Runtime boundary implementation
    /// * `config` - Ingress configuration
    /// * `tx` - Channel for forwarding frames to runtime
    pub fn new(
        ingress: Arc<dyn Ingress>,
        config: IngressConfig,
        tx: mpsc::Sender<(u64, Bytes)>,
        session_id: u64,
    ) -> Self {
        Self {
            ingress,
            config,
            session_id,
            tx,
        }
    }

    /// Handle an incoming WebSocket message
    ///
    /// Binary frames are converted to `Bytes` and forwarded.
    /// Text frames are logged and dropped.
    /// Control frames (ping, close) are handled appropriately.
    ///
    /// Returns `Ok(true)` if connection should continue,
    /// `Ok(false)` if connection should close.
    pub async fn handle_message(&self, msg: Message) -> Result<bool, String> {
        match msg {
            // Binary frames: convert to Bytes and forward
            Message::Binary(data) => {
                let frame = Bytes::from(data);

                // Check frame size
                if frame.len() > self.config.max_frame_size {
                    let reason = format!(
                        "frame too large: {} > {}",
                        frame.len(),
                        self.config.max_frame_size
                    );
                    self.ingress
                        .on_close(self.session_id, CloseReason::Error(reason.clone()))
                        .await;
                    return Err(reason);
                }

                // Forward to runtime with backpressure handling
                match self.tx.try_send((self.session_id, frame)) {
                    Ok(()) => Ok(true),
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Backpressure: wait briefly and retry
                        tokio::time::sleep(self.config.backpressure_timeout).await;
                        match self.tx.try_send((self.session_id, Bytes::new())) {
                            Ok(()) => Ok(true),
                            Err(_) => {
                                let reason = "channel full: backpressure exceeded".to_string();
                                self.ingress
                                    .on_close(self.session_id, CloseReason::Error(reason.clone()))
                                    .await;
                                Err(reason)
                            }
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        Err("runtime channel closed".to_string())
                    }
                }
            }
            // Text frames: not accepted, log and drop
            Message::Text(_) => {
                eprintln!("WebSocket: dropping text frame, binary only");
                Ok(true)
            }
            // Control frames
            Message::Ping(ping) => {
                // Pong will be sent automatically by tungstenite
                // Just acknowledge
                Ok(true)
            }
            Message::Pong(_) => {
                // Ignore pong frames
                Ok(true)
            }
            Message::Close(frame) => {
                let reason = frame
                    .map(|f| f.reason.to_string())
                    .unwrap_or_else(|| "client close".to_string());
                self.ingress
                    .on_close(self.session_id, CloseReason::ClientClose)
                    .await;
                Ok(false)
            }
            // Future frame types
            _ => {
                eprintln!("WebSocket: unsupported frame type");
                Ok(true)
            }
        }
    }
}

/// Create a new WebSocket session and handler
///
/// # Arguments
///
/// * `ingress` - Runtime boundary implementation
/// * `config` - Ingress configuration
/// * `peer_addr` - Peer address if available
/// * `tx` - Channel for forwarding frames
///
/// # Returns
///
/// Session ID if accepted, error message if rejected
pub async fn create_session(
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    peer_addr: Option<std::net::SocketAddr>,
    tx: mpsc::Sender<(u64, Bytes)>,
) -> Result<u64, String> {
    // Create transport-level session
    let session = Session::new(
        generate_session_id(),
        TransportKind::WebSocket,
        peer_addr,
    );

    // Let ingress validate and accept the session
    ingress.on_open(session).await
}

/// Generate a unique session ID
fn generate_session_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);
    SESSION_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock ingress for testing
    struct MockIngress;

    #[async_trait::async_trait]
    impl Ingress for MockIngress {
        async fn on_open(&self, _session: Session) -> Result<u64, String> {
            Ok(1)
        }

        async fn on_frame(&self, _session_id: u64, _frame: Bytes) -> IngressDecision {
            IngressDecision::Accept
        }

        async fn on_close(&self, _session_id: u64, _reason: CloseReason) {}
    }

    #[test]
    fn should_generate_unique_session_ids() {
        // Arrange & Act
        let id1 = generate_session_id();
        let id2 = generate_session_id();
        let id3 = generate_session_id();

        // Assert
        assert!(id1 < id2);
        assert!(id2 < id3);
    }

    #[tokio::test]
    async fn should_create_websocket_session() {
        // Arrange
        let ingress = Arc::new(MockIngress);
        let config = IngressConfig::default();
        let (tx, _rx) = mpsc::channel(100);

        // Act
        let result = create_session(ingress, config, None, tx).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }
}
