// LAYER: SESSION
//! Session lifecycle and multiplexed frame handling
//!
//! Each session owns a TLV decoder, a mux, and permission metadata. The session
//! pipeline decodes frames, demuxes them into logical channels, and dispatches
//! them through the runtime ingress boundary.

use crate::observability as obs;
use crate::protocol::frame::ChannelId;
use crate::protocol::mux::{Mux, MuxError, TypeMapping};
use crate::protocol::wire2::header::{decode_message, MessageError, HEADER_LEN};
use crate::runtime::routing::RouteFamily;
use crate::session::permissions::SessionPermissions;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tracing::{debug, error, warn};

#[inline]
fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

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
            CloseReason::ServerClose(msg) => write!(f, "server_close: {msg}"),
            CloseReason::Error(err) => write!(f, "error: {err}"),
            CloseReason::Timeout => write!(f, "timeout"),
        }
    }
}

#[derive(Debug)]
struct SessionLiveStats {
    connected_at: SystemTime,
    last_activity: Mutex<Instant>,
    service_name: Mutex<Option<String>>,
    messages_received: AtomicU64,
    messages_sent: AtomicU64,
}

impl Default for SessionLiveStats {
    fn default() -> Self {
        Self {
            connected_at: SystemTime::now(),
            last_activity: Mutex::new(Instant::now()),
            service_name: Mutex::new(None),
            messages_received: AtomicU64::new(0),
            messages_sent: AtomicU64::new(0),
        }
    }
}

/// Session metadata stored alongside permissions and live transport stats.
#[derive(Debug, Clone)]
pub struct SessionMetadata {
    properties: HashMap<String, String>,
    live: Arc<SessionLiveStats>,
}

impl Default for SessionMetadata {
    fn default() -> Self {
        Self {
            properties: HashMap::new(),
            live: Arc::new(SessionLiveStats::default()),
        }
    }
}

impl SessionMetadata {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.properties.get(key).map(String::as_str)
    }

    #[must_use]
    pub fn connected_at(&self) -> SystemTime {
        self.live.connected_at
    }

    #[must_use]
    pub fn idle_seconds(&self) -> u64 {
        self.live.last_activity.lock().elapsed().as_secs()
    }

    #[must_use]
    pub fn messages_received(&self) -> u64 {
        self.live.messages_received.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn messages_sent(&self) -> u64 {
        self.live.messages_sent.load(Ordering::Relaxed)
    }

    /// Return the optional name reported by the connected client.
    #[must_use]
    pub fn service_name(&self) -> Option<String> {
        self.live.service_name.lock().clone()
    }

    /// Store the optional name reported by the connected client.
    pub fn set_service_name(&self, service_name: String) {
        *self.live.service_name.lock() = Some(service_name);
    }

    pub fn record_frame_received(&self) {
        self.live.messages_received.fetch_add(1, Ordering::Relaxed);
        *self.live.last_activity.lock() = Instant::now();
    }

    pub fn record_frame_sent(&self) {
        self.live.messages_sent.fetch_add(1, Ordering::Relaxed);
        *self.live.last_activity.lock() = Instant::now();
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
    /// Route family for this session. Resolved server-side from verified identity context.
    pub route_family: RouteFamily,
}

impl SessionInfo {
    #[must_use]
    pub fn connected_at(&self) -> SystemTime {
        self.metadata.connected_at()
    }

    #[must_use]
    pub fn idle_seconds(&self) -> u64 {
        self.metadata.idle_seconds()
    }

    #[must_use]
    pub fn messages_received(&self) -> u64 {
        self.metadata.messages_received()
    }

    #[must_use]
    pub fn messages_sent(&self) -> u64 {
        self.metadata.messages_sent()
    }

    pub fn record_frame_received(&self) {
        self.metadata.record_frame_received();
    }

    pub fn record_frame_sent(&self) {
        self.metadata.record_frame_sent();
    }
}

/// Errors that can occur while handling a frame
#[derive(Debug, Clone)]
pub enum SessionError {
    Decode(MessageError),
    /// The header named a request the broker will not serve (a reserved flag
    /// bit); the transport answers `UNSUPPORTED` and keeps the session.
    Unsupported {
        message_type: u16,
        correlation: Option<std::num::NonZeroU64>,
        reason: &'static str,
    },
    Mux(MuxError),
    /// Ingress requested close with reason
    IngressClose(String),
    /// Ingress asked for backpressure
    Backpressure(ChannelId),
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Decode(err) => write!(f, "message decode error: {err}"),
            Self::Unsupported {
                message_type,
                reason,
                ..
            } => write!(f, "unsupported message {message_type}: {reason}"),
            Self::Mux(err) => write!(f, "Mux error: {err}"),
            Self::IngressClose(reason) => write!(f, "ingress requested close: {reason}"),
            Self::Backpressure(channel) => write!(f, "backpressure on channel {channel}"),
        }
    }
}

impl std::error::Error for SessionError {}

/// Session object owning the channel multiplexer.
pub struct Session {
    info: SessionInfo,
    mux: Mux,
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
    #[must_use]
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

    #[must_use]
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

        Self { info, mux }
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

        Self { info, mux }
    }

    /// Get session metadata for runtime ingress
    #[must_use]
    pub fn info(&self) -> SessionInfo {
        self.info.clone()
    }

    /// Decode the one message a transport frame carries and admit it to its
    /// channel. Wire format 2 puts exactly one message in every frame, so a
    /// frame that is short, long, or carries reserved flags is malformed.
    ///
    /// # Errors
    ///
    /// Returns `SessionError::Decode` when the frame is not exactly one
    /// well-formed message, or `SessionError::Mux` when its channel rejects it.
    pub fn decode_frame(
        &mut self,
        frame: &Bytes,
    ) -> Result<crate::protocol::mux::ChannelMessage, SessionError> {
        let _frame_latency =
            crate::observability::ScopedHistogramUs::new(obs::METRIC_SESSION_FRAME_PROCESS_LATENCY);
        let decode_start = Instant::now();
        let decoded = decode_message(frame, u32::MAX);
        crate::observability::hot_path_histogram_observe_us(
            obs::METRIC_SESSION_TLV_DECODE_LATENCY,
            u128_to_u64_saturating(decode_start.elapsed().as_micros()),
        );
        let (header, _) = decoded.map_err(|error| {
            crate::observability::counter_inc(obs::METRIC_TLV_DECODE_ERRORS);
            crate::observability::counter_inc(obs::METRIC_FRAMES_MALFORMED);
            error!(session_id = self.info.session_id, error = %error, "Session message decode error");
            match error {
                MessageError::ReservedFlags {
                    message_type,
                    correlation,
                    ..
                } => SessionError::Unsupported {
                    message_type,
                    correlation,
                    reason: "reserved header flag bit set",
                },
                other => SessionError::Decode(other),
            }
        })?;
        debug!(
            session_id = self.info.session_id,
            msg_type = header.message_type,
            payload_len = header.length,
            "Session decoded message"
        );
        let payload = frame.slice(HEADER_LEN..);
        let record = crate::protocol::message::WireMessage::new(
            crate::protocol::message::MessageType::new(header.message_type),
            payload,
        );
        let mux_start = Instant::now();
        let message = self.mux.route(record);
        crate::observability::hot_path_histogram_observe_us(
            obs::METRIC_SESSION_MUX_ROUTE_LATENCY,
            u128_to_u64_saturating(mux_start.elapsed().as_micros()),
        );
        message
            .map(|message| {
                message
                    .with_correlation(header.correlation)
                    .with_flags(header.flags)
            })
            .map_err(|error| {
                warn!(session_id = self.info.session_id, error = ?error, "Mux routing error");
                SessionError::Mux(error)
            })
    }

    /// Release mux capacity after the transport edge has handed off a message.
    pub fn release_channel(&mut self, channel: ChannelId) {
        self.mux.release(channel);
    }

    /// Access permissions snapshot
    #[must_use]
    pub fn permissions(&self) -> &SessionPermissions {
        &self.info.permissions_snapshot
    }

    /// Get the immutable claims for this session (if authenticated)
    #[must_use]
    pub fn claims(&self) -> Option<&crate::auth::Claims> {
        self.info.claims.as_ref().map(AsRef::as_ref)
    }

    /// Authenticate this session with new claims and update permissions
    ///
    /// **Important:** This replaces both claims AND permissions atomically.
    /// Used for initial auth and re-auth flows.
    ///
    /// # Errors
    ///
    /// Returns an error string if a future authentication flow adds validation
    /// and rejects the supplied claims or permissions.
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
    ///
    /// # Errors
    ///
    /// Returns an error string when the session is unauthenticated or the token
    /// has expired.
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
    use crate::protocol::wire2::header::{encode_message, HeaderFlags};

    fn session_with_capacity(channel_capacity: usize) -> Session {
        let config = NewSessionConfig::unauthenticated(
            TransportKind::Tcp,
            None,
            SessionPermissions::empty(),
            SessionMetadata::new(),
            channel_capacity,
            None,
            crate::runtime::routing::RouteFamily::new(0),
        );
        Session::new(42, config)
    }

    #[test]
    fn should_release_channel_capacity_after_handoff() {
        // Arrange
        let mut session = session_with_capacity(1);
        let first = Bytes::from(encode_message(1, HeaderFlags::none(), None, b"first"));
        let second = Bytes::from(encode_message(1, HeaderFlags::none(), None, b"second"));

        // Act
        let first_message = session.decode_frame(&first).unwrap();
        session.release_channel(first_message.channel);
        let second_message = session.decode_frame(&second).unwrap();

        // Assert
        assert_eq!(first_message.payload, b"first".as_ref());
        assert_eq!(second_message.payload, b"second".as_ref());
    }

    #[test]
    fn should_carry_the_header_correlation_onto_the_message() {
        // Arrange
        let mut session = session_with_capacity(10);
        let correlation = std::num::NonZeroU64::new(77);
        let frame = Bytes::from(encode_message(1, HeaderFlags::none(), correlation, b"x"));

        // Act
        let message = session.decode_frame(&frame).unwrap();

        // Assert
        assert_eq!(message.correlation, correlation);
    }

    #[test]
    fn should_reject_a_frame_that_is_not_exactly_one_message() {
        // Arrange
        let mut session = session_with_capacity(10);
        let mut frame = encode_message(1, HeaderFlags::none(), None, b"abcdefgh");
        frame.extend_from_slice(&encode_message(1, HeaderFlags::none(), None, b"second"));

        // Act
        let error = session.decode_frame(&Bytes::from(frame)).unwrap_err();

        // Assert
        assert!(matches!(
            error,
            SessionError::Decode(MessageError::LengthMismatch { .. })
        ));
    }
}
