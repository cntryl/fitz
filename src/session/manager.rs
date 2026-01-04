// LAYER: SESSION (Async → Sync Bridge)
//! Ingress trait and reference implementation for the async → sync boundary
//!
//! # Purpose
//!
//! This module defines the async `Ingress` trait (the single async/sync boundary)
//! and provides a reference implementation `RuntimeIngress` for session lifecycle
//! management and event dispatching.
//!
//! # Design
//!
//! - **Trait definition** and **reference impl** live together to make the boundary
//!   explicit and easy to review.
//! - **API** (`api/tcp.rs`, `api/ws/mod.rs`) consumes this trait.
//! - **Other session helpers** remain in their respective modules.

use crate::protocol::frame::ChannelId;
use crate::session::{CloseReason, SessionInfo};
use bytes::Bytes;
use dashmap::DashMap;
use std::sync::Arc;

/// Outcome from the runtime for a single protocol message
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressDecision {
    Accept,
    Close(String),
    Backpressure,
}

/// Trait implemented by the session layer to consume transport frames
#[async_trait::async_trait]
pub trait Ingress: Send + Sync {
    /// Called when transport opens a new session
    async fn on_open(&self, session: SessionInfo) -> Result<u64, String>;

    /// Called for every demultiplexed channel message
    async fn on_frame(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        message_payload: Bytes,
    ) -> IngressDecision;

    /// Called when the transport closes the connection
    async fn on_close(&self, session_id: u64, reason: CloseReason);
}

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
/// frame events to event handlers. It's designed to be embedded in
/// a runtime dispatcher or session manager.
pub struct RuntimeIngress {
    sessions: Arc<DashMap<u64, SessionInfo>>,
    /// Per-session SessionActor instances for authorization checks
    session_actors: Arc<DashMap<u64, crate::session::actor::SessionActor>>,
    /// Optional callback for session events (for routing to handlers)
    event_handler: Option<Arc<dyn Fn(SessionEvent) + Send + Sync>>,
}

impl RuntimeIngress {
    /// Create a new ingress implementation
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(DashMap::new()),
            session_actors: Arc::new(DashMap::new()),
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

        // Create a per-session SessionActor with the session's initial permissions
        self.session_actors.insert(
            session_id,
            crate::session::actor::SessionActor::new(
                crate::session::session::SessionId(session_id),
                session.permissions_snapshot.clone(),
            ),
        );

        if let Some(handler) = &self.event_handler {
            handler(SessionEvent::Open(session_id, session));
        }

        Ok(session_id)
    }

    async fn on_frame(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        msg_type: crate::protocol::tlv::MessageType,
        message_payload: Bytes,
    ) -> IngressDecision {
        eprintln!("on_frame enter: session_id={} channel={} msg_type={}", session_id, channel_id, msg_type.as_u16());
        // Verify session exists
        if !self.sessions.contains_key(&session_id) {
            eprintln!("Frame for unknown session: {}", session_id);
            return IngressDecision::Close(format!("unknown session: {}", session_id));
        }

        // Auth gating: if session is not authenticated, only allow CONNECT control messages
        // We'll set authenticated=true while holding the map write guard, but
        // perform handler notification after dropping the guard to avoid lock reentrancy.
        let mut notify_frame: Option<SessionFrame> = None;
        {
            let mut entry = self.sessions.get_mut(&session_id).unwrap();
            if !entry.authenticated {
                if channel_id != ChannelId::Control || msg_type != crate::protocol::tlv::MessageType::CONNECT {
                    eprintln!("forcing close for unauthenticated session {}", session_id);
                    return IngressDecision::Close("unauthenticated: connect required".to_string());
                }

                // Attempt to extract permissions from the JWT (no signature verification yet)
                match crate::auth::permissions_from_compact_jwt(std::str::from_utf8(&message_payload).unwrap_or("")) {
                    Ok(snapshot) => {
                        entry.permissions_snapshot = snapshot.clone();
                        entry.authenticated = true;

                        // Update the per-session actor with the authenticated permissions
                        self.session_actors.insert(
                            session_id,
                            crate::session::actor::SessionActor::new(
                                crate::session::session::SessionId(session_id),
                                snapshot,
                            ),
                        );

                        notify_frame = Some(SessionFrame {
                            session_id,
                            channel_id,
                            payload: message_payload.clone(),
                        });
                    }
                    Err(e) => {
                        eprintln!("connect failed: {}", e);
                        return IngressDecision::Close(format!("connect failed: {}", e));
                    }
                }
            }
        }

        if let Some(frame) = notify_frame {
            eprintln!("notifying frame for session {}", session_id);
            if let Some(handler) = &self.event_handler {
                handler(SessionEvent::Frame(frame));
            }
            eprintln!("returning Accept for connect");
            return IngressDecision::Accept;
        }

        // Notify handler if present
        if let Some(handler) = &self.event_handler {
            handler(SessionEvent::Frame(SessionFrame {
                session_id,
                channel_id,
                payload: message_payload.clone(),
            }));
        }

        eprintln!("returning Accept for regular frame");
        IngressDecision::Accept
    }

    async fn on_close(&self, session_id: u64, reason: CloseReason) {
        // Remove session and associated actor
        self.sessions.remove(&session_id);
        self.session_actors.remove(&session_id);

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
    use base64::Engine;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn make_session_info(id: u64, kind: TransportKind) -> SessionInfo {
        SessionInfo {
            session_id: id,
            transport_kind: kind,
            peer_addr: None,
            metadata: Arc::new(SessionMetadata::new()),
            permissions_snapshot: SessionPermissions::empty(),
            authenticated: false,
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

    #[test]
    fn should_process_frame() {
        let ingress = RuntimeIngress::new();
        let session = make_session_info(2, TransportKind::WebSocket);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();

            // First, perform a connect to authenticate the session
            let payload = serde_json::json!({
                "iss": "https://idp.example/",
                "aud": "fitz-broker",
                "sub": "user:2",
                "exp": 9999999999u64,
                "tid": "acme-prod",
                "fitz": { "permissions": ["notice://prod/orders/**#read"] }
            });
            let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
            let jwt = format!("{}.{}.{}", "{}", b64, "sig");

            let decision = ingress
                .on_frame(2, ChannelId::Control, crate::protocol::tlv::MessageType::CONNECT, Bytes::from(jwt))
                .await;

            assert_eq!(decision, IngressDecision::Accept);
        });
    }

    #[tokio::test]
    async fn should_reject_unknown_session() {
        let ingress = RuntimeIngress::new();

        let decision = ingress
            .on_frame(999, ChannelId::Control, crate::protocol::tlv::MessageType::new(42), Bytes::from("test"))
            .await;

        assert!(matches!(decision, IngressDecision::Close(_)));
    }

    #[test]
    fn should_call_event_handler() {
        let event_count = Arc::new(AtomicUsize::new(0));
        let count_clone = event_count.clone();
        let ingress = RuntimeIngress::new().with_event_handler(move |_event| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });
        let session = make_session_info(3, TransportKind::WebSocket);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session).await.unwrap();
            // Authenticate session with a connect
            let payload = serde_json::json!({
                "iss": "https://idp.example/",
                "aud": "fitz-broker",
                "sub": "user:3",
                "exp": 9999999999u64,
                "tid": "acme-prod",
                "fitz": { "permissions": ["notice://prod/orders/**#read"] }
            });
            let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
            let jwt = format!("{}.{}.{}", "{}", b64, "sig");

            ingress
                .on_frame(3, ChannelId::Control, crate::protocol::tlv::MessageType::CONNECT, Bytes::from(jwt))
                .await;
            ingress.on_close(3, CloseReason::ClientClose).await;
        });

        assert_eq!(event_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn should_reject_non_connect_before_auth() {
        let ingress = RuntimeIngress::new();
        let session = make_session_info(4, TransportKind::WebSocket);
        ingress.on_open(session).await.unwrap();

        let decision = ingress
            .on_frame(4, ChannelId::Pub, crate::protocol::tlv::MessageType::new(100), Bytes::from("payload"))
            .await;

        assert!(matches!(decision, IngressDecision::Close(_)));
    }

    #[tokio::test]
    async fn should_reject_control_non_connect_before_auth() {
        let ingress = RuntimeIngress::new();
        let session = make_session_info(5, TransportKind::WebSocket);
        ingress.on_open(session).await.unwrap();

        // Control message with wrong type
        let decision = ingress
            .on_frame(5, ChannelId::Control, crate::protocol::tlv::MessageType::new(2), Bytes::from("payload"))
            .await;

        assert!(matches!(decision, IngressDecision::Close(_)));
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
    fn connect_with_valid_token_sets_permissions() {
        use base64::Engine;
        let ingress = RuntimeIngress::new();
        let session = make_session_info(50, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "https://idp.example/",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["notice://prod/orders/**#read"] }
        });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        let jwt = format!("{}.{}.{}", "{}", b64, "sig");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
            let decision = ingress
                .on_frame(50, ChannelId::Control, crate::protocol::tlv::MessageType::CONNECT, Bytes::from(jwt.clone()))
                .await;
            assert_eq!(decision, IngressDecision::Accept);
        });

        let retrieved = ingress.get_session(50).unwrap();
        assert!(retrieved.permissions_snapshot.allows(&crate::runtime::routing::Route::new("notice://prod/orders/create"), crate::auth::Access::Read));
    }

    #[test]
    fn connect_with_malformed_permissions_rejected() {
        let ingress = RuntimeIngress::new();
        let session = make_session_info(51, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "https://idp.example/",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["badperm#oops"] }
        });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        let jwt = format!("{}.{}.{}", "{}", b64, "sig");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
            let decision = ingress
                .on_frame(51, ChannelId::Control, crate::protocol::tlv::MessageType::CONNECT, Bytes::from(jwt.clone()))
                .await;
            assert!(matches!(decision, IngressDecision::Close(_)));
        });
    }

    #[test]
    fn session_actor_created_on_open() {
        let ingress = RuntimeIngress::new();
        let session = make_session_info(60, TransportKind::Tcp);

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
        });

        // Actor should exist but have no permissions
        assert!(ingress.session_actors.contains_key(&60));
        let actor_ref = ingress.session_actors.get(&60).unwrap();
        let actor = actor_ref.value();
        assert!(!actor.authorize(&crate::runtime::routing::Route::new("notice://prod/orders/create"), crate::auth::Access::Write));
    }

    #[test]
    fn session_actor_updated_on_connect() {
        use base64::Engine;
        let ingress = RuntimeIngress::new();
        let session = make_session_info(61, TransportKind::Tcp);

        let payload = serde_json::json!({
            "iss": "https://idp.example/",
            "aud": "fitz-broker",
            "sub": "user:42",
            "exp": 9999999999u64,
            "tid": "acme-prod",
            "fitz": { "permissions": ["notice://prod/orders/**#write"] }
        });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
        let jwt = format!("{}.{}.{}", "{}", b64, "sig");

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            ingress.on_open(session.clone()).await.unwrap();
            let decision = ingress
                .on_frame(61, ChannelId::Control, crate::protocol::tlv::MessageType::CONNECT, Bytes::from(jwt.clone()))
                .await;
            assert_eq!(decision, IngressDecision::Accept);
        });

        // Actor should now allow write on the route
        let actor_ref = ingress.session_actors.get(&61).unwrap();
        let actor = actor_ref.value();
        assert!(actor.authorize(&crate::runtime::routing::Route::new("notice://prod/orders/create"), crate::auth::Access::Write));
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
