// LAYER: SESSION
//! Session lifecycle and multiplexed frame handling
//!
//! Each session owns a TLV decoder, a mux, and permission metadata. The session
//! pipeline decodes frames, demuxes them into logical channels, and dispatches
//! them through the runtime ingress boundary.

use crate::observability as obs;
use crate::protocol::frame::ChannelId;
use crate::protocol::mux::{Mux, MuxError, TypeMapping};
use crate::protocol::tlv::{TlvDecoder, TlvError};
use crate::runtime::routing::RouteFamily;
use crate::session::permissions::SessionPermissions;
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
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
    /// Route family for this session (tenant isolation boundary).
    /// Resolved from the verified `fitz.route_family` claim at authentication time.
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

    /// Append bytes received from a transport to the session decoder buffer.
    pub fn push_frame(&mut self, frame: Bytes) {
        let _frame_latency =
            crate::observability::ScopedHistogramUs::new(obs::METRIC_SESSION_FRAME_PROCESS_LATENCY);
        debug!(
            session_id = self.info.session_id,
            frame_len = frame.len(),
            buffer_before = self.buffer.len(),
            "Session on_frame: received raw frame"
        );
        self.buffer.extend_from_slice(&frame);
    }

    /// Decode the next complete message, if one is buffered.
    pub fn next_message(
        &mut self,
    ) -> Result<Option<crate::protocol::mux::ChannelMessage>, SessionError> {
        let decode_start = Instant::now();
        let decode_result = self.decoder.decode_one_ref(&self.buffer);
        crate::observability::hot_path_histogram_observe_us(
            obs::METRIC_SESSION_TLV_DECODE_LATENCY,
            decode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
        );

        match decode_result {
            Ok((msg_type, slice, consumed)) => {
                let payload_offset = consumed.saturating_sub(slice.len());
                debug!(
                    session_id = self.info.session_id,
                    msg_type = msg_type.as_u16(),
                    payload_len = slice.len(),
                    consumed = consumed,
                    "Session decoded TLV record"
                );
                let record_bytes = self.buffer.split_to(consumed).freeze();
                let record = crate::protocol::tlv::TlvRecord::new(
                    msg_type,
                    record_bytes.slice(payload_offset..consumed),
                );
                let mux_start = Instant::now();
                let message = self.mux.route(record);
                crate::observability::hot_path_histogram_observe_us(
                    obs::METRIC_SESSION_MUX_ROUTE_LATENCY,
                    mux_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
                );
                message.map(Some).map_err(|error| {
                    warn!(session_id = self.info.session_id, error = ?error, "Mux routing error");
                    SessionError::Mux(error)
                })
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
                Ok(None)
            }
            Err(error) => {
                crate::observability::counter_inc(obs::METRIC_TLV_DECODE_ERRORS);
                crate::observability::counter_inc(obs::METRIC_FRAMES_MALFORMED);
                error!(session_id = self.info.session_id, error = ?error, "Session TLV decode error");
                Err(SessionError::Decode(error))
            }
        }
    }

    /// Release mux capacity after the transport edge has handed off a message.
    pub fn release_channel(&mut self, channel: ChannelId) {
        self.mux.release(channel);
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
        route_family: RouteFamily,
    ) -> Result<(), String> {
        // Update both atomically
        self.info.claims = Some(Arc::new(claims));
        self.info.permissions_snapshot = permissions;
        self.info.authenticated = true;
        self.info.route_family = route_family;
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

    #[test]
    fn should_release_channel_capacity_after_handoff() {
        // Arrange
        let config = NewSessionConfig::unauthenticated(
            TransportKind::Tcp,
            None,
            SessionPermissions::empty(),
            SessionMetadata::new(),
            1,
            None,
            crate::runtime::routing::RouteFamily::new(0),
        );
        let mut session = Session::new(42, config);
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"first");
        encoder.encode(MessageType::new(1), b"second");
        let data = encoder.finish();

        // Act
        session.push_frame(data);
        let first = session.next_message().unwrap().unwrap();
        session.release_channel(first.channel);
        let second = session.next_message().unwrap().unwrap();

        // Assert
        assert_eq!(first.payload, b"first".as_ref());
        assert_eq!(second.payload, b"second".as_ref());
    }

    #[test]
    fn should_buffer_partial_frames() {
        // Arrange
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
        session.push_frame(part1);
        let incomplete = session.next_message().unwrap();
        session.push_frame(part2);
        let complete = session.next_message().unwrap().unwrap();

        // Assert
        assert!(incomplete.is_none());
        assert_eq!(complete.payload, b"abcdefgh".as_ref());
    }
}
