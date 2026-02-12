// LAYER: SESSION
//! Session lifecycle and multiplexed frame handling
//!
//! Each session owns a TLV decoder, a mux, and permission metadata. The session
//! pipeline decodes frames, demuxes them into logical channels, and dispatches
//! them through the runtime ingress boundary.

use crate::protocol::frame::ChannelId;
use crate::protocol::mux::{Mux, MuxError, TypeMapping};
use crate::protocol::tlv::{TlvDecoder, TlvError};
use crate::runtime::routing::RouteFamily;
use crate::session::manager::{Ingress, IngressDecision};
use crate::session::permissions::SessionPermissions;
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, error, trace, warn};

/// Unique session identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

/// Transport kind identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransportKind {
    WebSocket,
    Tcp,
}

impl fmt::Display for TransportKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportKind::WebSocket => write!(f, "websocket"),
            TransportKind::Tcp => write!(f, "tcp"),
        }
    }
}

/// Why the session was closed
#[derive(Debug, Clone)]
pub enum CloseReason {
    ClientClose,
    ServerClose(String),
    Error(String),
    Timeout,
}

impl fmt::Display for CloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloseReason::ClientClose => write!(f, "client_close"),
            CloseReason::ServerClose(msg) => write!(f, "server_close: {}", msg),
            CloseReason::Error(err) => write!(f, "error: {}", err),
            CloseReason::Timeout => write!(f, "timeout"),
        }
    }
}

/// Session metadata stored alongside permissions
#[derive(Debug, Clone, Default)]
pub struct SessionMetadata {
    properties: HashMap<String, String>,
}

impl SessionMetadata {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(|s| s.as_str())
    }
}

/// Summary of session metadata exposed to runtime ingress
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: u64,
    pub transport_kind: TransportKind,
    pub peer_addr: Option<SocketAddr>,
    pub metadata: Arc<SessionMetadata>,
    pub permissions_snapshot: SessionPermissions,
    /// Immutable authentication claims (set at auth time, never modified)
    pub claims: Option<Arc<crate::auth::Claims>>,
    /// Whether the session has completed the connect/auth handshake
    pub authenticated: bool,
    /// Route family for this session (tenant isolation boundary)
    /// Resolved from tenant_id at authentication time
    pub route_family: RouteFamily,
}

/// Errors that can occur while handling a frame
#[derive(Debug, Clone)]
pub enum SessionError {
    Decode(TlvError),
    Mux(MuxError),
    /// Ingress requested close with reason
    IngressClose(String),
    /// Ingress asked for backpressure
    Backpressure(ChannelId),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(err) => write!(f, "TLV decode error: {}", err),
            Self::Mux(err) => write!(f, "Mux error: {}", err),
            Self::IngressClose(reason) => write!(f, "ingress requested close: {}", reason),
            Self::Backpressure(channel) => write!(f, "backpressure on channel {}", channel),
        }
    }
}

impl std::error::Error for SessionError {}

/// Session object owning decoder + mux
pub struct Session {
    info: SessionInfo,
    decoder: TlvDecoder,
    mux: Mux,
    /// Buffer for streaming frames across transport messages
    buffer: bytes::BytesMut,
}

/// Configuration used to create sessions. Grouped to avoid long parameter lists.
pub struct NewSessionConfig {
    pub transport_kind: TransportKind,
    pub peer_addr: Option<SocketAddr>,
    pub permissions: SessionPermissions,
    pub claims: Option<crate::auth::Claims>,
    pub metadata: SessionMetadata,
    pub channel_capacity: usize,
    pub type_mapping: Option<TypeMapping>,
    pub route_family: RouteFamily,
}

impl NewSessionConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn authenticated(
        transport_kind: TransportKind,
        peer_addr: Option<SocketAddr>,
        permissions: SessionPermissions,
        claims: crate::auth::Claims,
        metadata: SessionMetadata,
        channel_capacity: usize,
        type_mapping: Option<TypeMapping>,
        route_family: RouteFamily,
    ) -> Self {
        Self {
            transport_kind,
            peer_addr,
            permissions,
            claims: Some(claims),
            metadata,
            channel_capacity,
            type_mapping,
            route_family,
        }
    }

    pub fn unauthenticated(
        transport_kind: TransportKind,
        peer_addr: Option<SocketAddr>,
        permissions: SessionPermissions,
        metadata: SessionMetadata,
        channel_capacity: usize,
        type_mapping: Option<TypeMapping>,
        route_family: RouteFamily,
    ) -> Self {
        Self {
            transport_kind,
            peer_addr,
            permissions,
            claims: None,
            metadata,
            channel_capacity,
            type_mapping,
            route_family,
        }
    }
}

impl Session {
    /// Create a new session with authentication claims
    pub fn new_authenticated(session_id: u64, cfg: NewSessionConfig) -> Self {
        let mux = if let Some(mapping) = cfg.type_mapping {
            Mux::with_mapping(cfg.channel_capacity, mapping)
        } else {
            Mux::new(cfg.channel_capacity)
        };

        let authenticated = cfg.claims.is_some();
        let claims = cfg.claims.map(Arc::new);

        let info = SessionInfo {
            session_id,
            transport_kind: cfg.transport_kind,
            peer_addr: cfg.peer_addr,
            metadata: Arc::new(cfg.metadata),
            permissions_snapshot: cfg.permissions.clone(),
            claims,
            authenticated,
            route_family: cfg.route_family,
        };

        Self {
            info,
            decoder: TlvDecoder::new(),
            mux,
            buffer: bytes::BytesMut::with_capacity(4096),
        }
    }

    /// Create a new unauthenticated session (pre-auth)
    pub fn new(session_id: u64, config: NewSessionConfig) -> Self {
        let mux = if let Some(mapping) = config.type_mapping {
            Mux::with_mapping(config.channel_capacity, mapping)
        } else {
            Mux::new(config.channel_capacity)
        };

        let authenticated = config.claims.is_some();
        let info = SessionInfo {
            session_id,
            transport_kind: config.transport_kind,
            peer_addr: config.peer_addr,
            metadata: Arc::new(config.metadata),
            permissions_snapshot: config.permissions.clone(),
            claims: config.claims.map(Arc::new),
            authenticated,
            route_family: config.route_family,
        };

        Self {
            info,
            decoder: TlvDecoder::new(),
            mux,
            buffer: bytes::BytesMut::with_capacity(4096),
        }
    }

    /// Get session metadata for runtime ingress
    pub fn info(&self) -> SessionInfo {
        self.info.clone()
    }

    /// Handle a raw frame
    pub async fn on_frame(
        &mut self,
        frame: Bytes,
        ingress: &dyn Ingress,
    ) -> Result<(), SessionError> {
        debug!(
            session_id = self.info.session_id,
            frame_len = frame.len(),
            buffer_before = self.buffer.len(),
            "Session on_frame: received raw frame"
        );
        // Append incoming bytes to per-session buffer and decode as many records
        self.buffer.extend_from_slice(&frame);

        loop {
            // Attempt to decode one record from buffer
            match self.decoder.decode_one_ref(&self.buffer) {
                Ok((msg_type, slice, consumed)) => {
                    debug!(
                        session_id = self.info.session_id,
                        msg_type = msg_type.as_u16(),
                        payload_len = slice.len(),
                        consumed = consumed,
                        "Session decoded TLV record"
                    );
                    // Build owned record and route it
                    let record = crate::protocol::tlv::TlvRecord::new(
                        msg_type,
                        Bytes::copy_from_slice(slice),
                    );
                    let message = self.mux.route(record).map_err(|e| {
                        warn!(session_id = self.info.session_id, error = ?e, "Mux routing error");
                        SessionError::Mux(e)
                    })?;

                    debug!(
                        session_id = self.info.session_id,
                        channel = ?message.channel,
                        msg_type = message.msg_type.as_u16(),
                        payload_len = message.payload.len(),
                        "Session dispatching to ingress.on_frame"
                    );

                    let decision = ingress
                        .on_frame(
                            self.info.session_id,
                            message.channel,
                            message.msg_type,
                            message.payload,
                        )
                        .await;

                    self.mux.release(message.channel);

                    match decision {
                        IngressDecision::Accept => {
                            trace!(session_id = self.info.session_id, "Ingress accepted frame");
                            // consume bytes and continue
                            let _ = self.buffer.split_to(consumed);
                            continue;
                        }
                        IngressDecision::Backpressure => {
                            warn!(session_id = self.info.session_id, channel = ?message.channel, "Ingress backpressure");
                            // leave buffer intact (message held by mux) and propagate backpressure
                            return Err(SessionError::Backpressure(message.channel));
                        }
                        IngressDecision::Close(reason) => {
                            warn!(session_id = self.info.session_id, reason = %reason, "Ingress requested close");
                            return Err(SessionError::IngressClose(reason));
                        }
                    }
                }
                Err(crate::protocol::tlv::TlvError::IncompleteType)
                | Err(crate::protocol::tlv::TlvError::IncompleteLength)
                | Err(crate::protocol::tlv::TlvError::IncompleteValue { .. })
                | Err(crate::protocol::tlv::TlvError::EmptyFrame) => {
                    trace!(
                        session_id = self.info.session_id,
                        buffer_remaining = self.buffer.len(),
                        "Session: incomplete TLV frame, waiting for more bytes"
                    );
                    // Incomplete frame or empty buffer: wait for more bytes
                    break;
                }
                Err(e) => {
                    error!(session_id = self.info.session_id, error = ?e, "Session TLV decode error");
                    return Err(SessionError::Decode(e));
                }
            }
        }

        Ok(())
    }

    /// Access permissions snapshot
    pub fn permissions(&self) -> &SessionPermissions {
        &self.info.permissions_snapshot
    }

    /// Get the immutable claims for this session (if authenticated)
    pub fn claims(&self) -> Option<&crate::auth::Claims> {
        self.info.claims.as_ref().map(|c| c.as_ref())
    }

    /// Authenticate this session with new claims and update permissions
    ///
    /// **Important:** This replaces both claims AND permissions atomically.
    /// Used for initial auth and re-auth flows.
    pub fn authenticate(
        &mut self,
        claims: crate::auth::Claims,
        permissions: SessionPermissions,
    ) -> Result<(), String> {
        // Update both atomically
        self.info.claims = Some(Arc::new(claims));
        self.info.permissions_snapshot = permissions;
        self.info.authenticated = true;
        Ok(())
    }

    /// Check expiration of claims (if authenticated)
    pub fn check_expiration(&self, now: u64) -> Result<(), String> {
        if let Some(claims) = self.claims() {
            if now >= claims.exp {
                return Err("token expired".to_string());
            }
            Ok(())
        } else {
            Err("not authenticated".to_string())
        }
    }
}

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Generate a unique session ID across transports
pub fn next_session_id() -> u64 {
    SESSION_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::tlv::{MessageType, TlvEncoder};
    use crate::session::manager::{Ingress, IngressDecision, SessionFrame};
    use std::sync::{Arc, Mutex};
    use tokio::runtime::Runtime;

    struct DummyIngress {
        frames: Arc<Mutex<Vec<SessionFrame>>>,
    }

    #[async_trait::async_trait]
    impl Ingress for DummyIngress {
        async fn on_open(&self, _session: SessionInfo) -> Result<u64, String> {
            Ok(1)
        }

        async fn on_frame(
            &self,
            session_id: u64,
            channel_id: crate::protocol::frame::ChannelId,
            _msg_type: crate::protocol::tlv::MessageType,
            message_payload: bytes::Bytes,
        ) -> IngressDecision {
            let mut vec = self.frames.lock().unwrap();
            vec.push(SessionFrame {
                session_id,
                channel_id,
                payload: message_payload,
            });
            IngressDecision::Accept
        }

        async fn on_close(&self, _session_id: u64, _reason: CloseReason) {}
    }

    #[test]
    fn should_buffer_partial_frames() {
        // Arrange
        let rt = Runtime::new().unwrap();
        let frames = Arc::new(Mutex::new(Vec::new()));
        let ingress = DummyIngress {
            frames: frames.clone(),
        };

        let config = NewSessionConfig::unauthenticated(
            TransportKind::Tcp,
            None,
            SessionPermissions::empty(),
            SessionMetadata::new(),
            10,
            None,
            crate::runtime::routing::RouteFamily::new(0),
        );

        let mut session = Session::new(42, config);

        // Build a TLV message and split it into two parts
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"abcdefgh");
        let data = encoder.finish();
        let split = data.len() / 2;
        let part1 = data.slice(0..split);
        let part2 = data.slice(split..);

        // Act
        let ingress_ref: &dyn Ingress = &ingress;
        rt.block_on(async {
            session.on_frame(part1, ingress_ref).await.unwrap();
        });

        // Verify: no frames processed yet
        assert_eq!(frames.lock().unwrap().len(), 0);

        // Step 2: send second part (completes message)
        rt.block_on(async {
            session.on_frame(part2, ingress_ref).await.unwrap();
        });

        // Assert
        assert_eq!(frames.lock().unwrap().len(), 1);
        let f = &frames.lock().unwrap()[0];
        assert_eq!(f.session_id, 42);
        assert_eq!(f.payload, b"abcdefgh".as_ref());
    }
}
