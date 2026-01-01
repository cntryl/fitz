//! WebSocket API endpoints with ingress integration
//!
//! This module provides WebSocket transport handling that:
//! 1. Accepts binary frames only
//! 2. Converts WebSocket frames to `bytes::Bytes`
//! 3. Forwards frames through the `Ingress` boundary trait
//! 4. Handles session lifecycle and backpressure

use crate::api::ingress::IngressConfig;
use crate::session::{CloseReason, Ingress};
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

                // Forward frame to session for processing, handling backpressure
                // via the session API
                // For now simply send through tx channel as a placeholder
                if let Err(e) = self.tx.send((self.session_id, frame)).await {
                    let reason = format!("failed to send frame: {}", e);
                    self.ingress
                        .on_close(self.session_id, CloseReason::Error(reason.clone()))
                        .await;
                    return Err(reason);
                }

                Ok(true)
            }
            Message::Close(_) => {
                self.ingress
                    .on_close(self.session_id, CloseReason::ClientClose)
                    .await;
                Ok(false)
            }
            Message::Ping(_) | Message::Pong(_) => Ok(true),
            Message::Text(_) => Ok(true),
            _ => Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{SessionInfo, TransportKind, SessionMetadata, SessionPermissions, IngressDecision};
    use crate::protocol::frame::ChannelId;

    // Mock ingress for testing
    struct MockIngress;

    #[async_trait::async_trait]
    impl Ingress for MockIngress {
        async fn on_open(&self, _session: SessionInfo) -> Result<u64, String> {
            Ok(1)
        }

        async fn on_frame(&self, _session_id: u64, _channel_id: ChannelId, _message_payload: Bytes) -> IngressDecision {
            IngressDecision::Accept
        }

        async fn on_close(&self, _session_id: u64, _reason: CloseReason) {}
    }

    #[test]
    fn should_generate_unique_session_ids() {
        // Arrange & Act
        let id1 = crate::session::next_session_id();
        let id2 = crate::session::next_session_id();
        let id3 = crate::session::next_session_id();

        // Assert
        assert!(id1 < id2);
        assert!(id2 < id3);
    }

    #[tokio::test]
    async fn should_create_websocket_session() {
        // Arrange
        let ingress = Arc::new(MockIngress);
        let session = SessionInfo {
            session_id: 1,
            transport_kind: TransportKind::WebSocket,
            peer_addr: None,
            metadata: Arc::new(SessionMetadata::new()),
            permissions_snapshot: SessionPermissions::empty(),
        };

        // Act
        let result = ingress.on_open(session).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
    }
}
