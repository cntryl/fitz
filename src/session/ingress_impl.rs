// LAYER: SESSION
//! Reference implementation of the Ingress trait
//!
//! This module provides a working implementation of the `Ingress` trait
//! that integrates with the runtime. It handles:
//! 1. Session lifecycle (open, frame, close)
//! 2. Frame routing to session actors
//! 3. Backpressure and error handling

use crate::runtime::ingress::{Ingress, IngressDecision};
use crate::protocol::frame::ChannelId;
use crate::session::{CloseReason, SessionInfo};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

/// Session frame message for dispatching to domain handlers
#[derive(Debug, Clone)]
pub struct SessionFrame {
    pub session_id: u64,
    pub channel_id: ChannelId,
    pub payload: Bytes,
}

/// Session lifecycle event
#[derive(Debug, Clone)]
pub enum SessionEvent {
    Open(u64, SessionInfo),
    Frame(SessionFrame),
    Close(u64, CloseReason),
}

/// Ingress implementation with session tracking
///
/// This reference implementation tracks active sessions and can route
/// frame events to domain handlers. It's designed to be embedded in
/// a runtime dispatcher or session manager.
pub struct RuntimeIngress {
    sessions: Arc<DashMap<u64, SessionInfo>>,
    /// Optional callback for session events (for routing to handlers)
    event_handler: Option<Arc<dyn Fn(SessionEvent) + Send + Sync>>,
}

impl RuntimeIngress {
    /// Create a new ingress implementation
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            event_handler: None,
        }
    }

    /// Set the event handler for session events
    ///
    /// The handler is called for each session lifecycle event (open, frame, close).
    pub fn with_event_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(SessionEvent) + Send + Sync + 'static,
    {
        self.event_handler = Some(Arc::new(handler));
        self
    }

    /// Get a session by ID
    pub fn get_session(&self, session_id: u64) -> Option<SessionInfo> {
        self.sessions
            .get(&session_id)
            .map(|entry| entry.value().clone())
    }

    /// Get all active sessions
    pub fn active_sessions(&self) -> Vec<SessionInfo> {
        self.sessions
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get session count
    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }
}

impl Default for RuntimeIngress {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Ingress for RuntimeIngress {
    async fn on_open(&self, session: SessionInfo) -> Result<u64, String> {
        let session_id = session.session_id;

        self.sessions.insert(session_id, session.clone());

        if let Some(handler) = &self.event_handler {
            handler(SessionEvent::Open(session_id, session));
        }

        Ok(session_id)
    }

    async fn on_frame(&self, session_id: u64, channel_id: ChannelId, message_payload: Bytes) -> IngressDecision {
        // Verify session exists
        if !self.sessions.contains_key(&session_id) {
            eprintln!("Frame for unknown session: {}", session_id);
            return IngressDecision::Close(format!("unknown session: {}", session_id));
        }

        // Notify handler if present
        if let Some(handler) = &self.event_handler {
            handler(SessionEvent::Frame(SessionFrame {
                session_id,
                channel_id,
                payload: message_payload.clone(),
            }));
        }

        IngressDecision::Accept
    }

    async fn on_close(&self, session_id: u64, reason: CloseReason) {
        // Remove session
        self.sessions.remove(&session_id);

        // Notify handler if present
        if let Some(handler) = &self.event_handler {
            handler(SessionEvent::Close(session_id, reason));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::ChannelId;
    use crate::session::{SessionInfo, SessionMetadata, SessionPermissions, TransportKind};
    use bytes::Bytes;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn make_session_info(id: u64, kind: TransportKind) -> SessionInfo {
        SessionInfo {
            session_id: id,
            transport_kind: kind,
            peer_addr: None,
            metadata: Arc::new(SessionMetadata::new()),
            permissions_snapshot: SessionPermissions::empty(),
        }
    }

    #[tokio::test]
    async fn should_open_session() {
        let ingress = RuntimeIngress::new();
        let session = make_session_info(1, TransportKind::WebSocket);

        let result = ingress.on_open(session).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);
        assert_eq!(ingress.session_count(), 1);
    }

    #[tokio::test]
    async fn should_process_frame() {
        let ingress = RuntimeIngress::new();
        let session = make_session_info(2, TransportKind::WebSocket);
        ingress.on_open(session).await.unwrap();

        let decision = ingress
            .on_frame(2, ChannelId::Control, Bytes::from("test"))
            .await;

        assert_eq!(decision, IngressDecision::Accept);
    }

    #[tokio::test]
    async fn should_reject_unknown_session() {
        let ingress = RuntimeIngress::new();

        let decision = ingress
            .on_frame(999, ChannelId::Control, Bytes::from("test"))
            .await;

        assert!(matches!(decision, IngressDecision::Close(_)));
    }

    #[tokio::test]
    async fn should_call_event_handler() {
        let event_count = Arc::new(AtomicUsize::new(0));
        let count_clone = event_count.clone();
        let ingress = RuntimeIngress::new().with_event_handler(move |_event| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });
        let session = make_session_info(3, TransportKind::WebSocket);

        ingress.on_open(session).await.unwrap();
        ingress
            .on_frame(3, ChannelId::Control, Bytes::from("hello"))
            .await;
        ingress.on_close(3, CloseReason::ClientClose).await;

        assert_eq!(event_count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn should_retrieve_session_info() {
        let ingress = RuntimeIngress::new();
        let session = make_session_info(42, TransportKind::Tcp);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
        });
        let retrieved = ingress.get_session(42);

        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().session_id, 42);
    }

    #[test]
    fn should_list_sessions() {
        let ingress = RuntimeIngress::new();
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async {
            for i in 1..=3 {
                let session = make_session_info(i, TransportKind::WebSocket);
                ingress.on_open(session).await.unwrap();
            }
        });

        assert_eq!(ingress.session_count(), 3);
    }
}
