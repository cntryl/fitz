//! LAYER: API
//! Unified transport driver for TCP and WebSocket
//!
//! # Design Invariants
//!
//! 1. **Single Session Lifecycle**: Each transport creates exactly one session
//!    and calls on_close exactly once, regardless of error path.
//!
//! 2. **Deterministic Shutdown**: Any error, close, or violation results in:
//!    - Exactly one on_close call
//!    - Clean task termination
//!    - No leaked resources
//!
//! 3. **Explicit Backpressure**: All channels are bounded. Saturation behavior
//!    is explicit: sleep + retry once, then close session.
//!
//! 4. **Outbound Wiring**: Every session has a functional outbound path.
//!    Responses flow: ingress → outbound_tx → sink → wire.
//!
//! 5. **Protocol Validation**: Frame size limits enforced. Invalid frames rejected early.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐
//! │   Socket    │ (TCP or WebSocket)
//! └──────┬──────┘
//!        │
//!   ┌────▼────┐
//!   │  Source │ (read frames from wire)
//!   └────┬────┘
//!        │
//!        ▼
//!   Transport Driver ──────┐
//!        │                 │
//!        │            ┌────▼────┐
//!        │            │  Sink   │ (write frames to wire)
//!        │            └────▲────┘
//!        │                 │
//!        ▼                 │
//!   ┌─────────┐       ┌───┴───┐
//!   │ Session │◄──────┤ Channel│ (outbound_tx/rx)
//!   └────┬────┘       └───────┘
//!        │
//!        ▼
//!   ┌─────────┐
//!   │ Ingress │
//!   └─────────┘
//! ```

use crate::api::ingress::IngressConfig;
use crate::api::runtime_ingress::Ingress;
use crate::session::{
    CloseReason, Session, SessionInfo, SessionMetadata, SessionPermissions, TransportKind,
    generate_session_id,
};
use bytes::Bytes;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// Outcome of session lifecycle
#[derive(Debug)]
enum SessionOutcome {
    /// Client closed cleanly
    ClientClose,
    /// Error occurred
    Error(String),
}

/// Transport driver that manages session lifecycle
///
/// This is the single source of truth for:
/// - Opening a session
/// - Processing inbound frames
/// - Forwarding outbound frames
/// - Closing a session (exactly once)
pub struct TransportDriver {
    /// Session information
    session: Session,
    /// Session ID assigned by ingress
    session_id: u64,
    /// Ingress for frame processing
    ingress: Arc<dyn Ingress>,
    /// Configuration
    config: IngressConfig,
    /// Outbound frame sender (to sink task)
    outbound_tx: mpsc::Sender<Bytes>,
    /// Has on_close been called?
    closed: bool,
}

impl TransportDriver {
    /// Create and open a new transport session
    ///
    /// # Lifecycle Guarantee
    /// If this function succeeds, the caller MUST ensure close() is called exactly once.
    ///
    /// # Arguments
    /// * `transport_kind` - TCP or WebSocket
    /// * `ingress` - Runtime ingress
    /// * `config` - Transport configuration
    /// * `outbound_tx` - Channel to sink task for sending frames
    ///
    /// # Returns
    /// TransportDriver if session accepted, error if rejected
    pub async fn open(
        transport_kind: TransportKind,
        peer_addr: Option<std::net::SocketAddr>,
        ingress: Arc<dyn Ingress>,
        config: IngressConfig,
        outbound_tx: mpsc::Sender<Bytes>,
    ) -> Result<Self, String> {
        // Generate session ID
        let temp_session_id = generate_session_id();

        // Create session
        let session_config = crate::session::NewSessionConfig::unauthenticated(
            transport_kind,
            peer_addr,
            SessionPermissions::empty(),
            SessionMetadata::new(),
            config.channel_capacity,
            None,
            crate::runtime::routing::RouteFamily::new(1), // Default dev family = 1
        );
        let session = Session::new(temp_session_id, session_config);

        // Register with ingress (may reject)
        let session_id = ingress.on_open(session.info()).await?;

        info!(
            session_id = session_id,
            transport = ?transport_kind,
            "Transport session opened"
        );

        Ok(Self {
            session,
            session_id,
            ingress,
            config,
            outbound_tx,
            closed: false,
        })
    }

    /// Process an inbound frame
    ///
    /// # Errors
    /// Returns error if:
    /// - Frame exceeds max size
    /// - Frame parsing fails
    /// - Session processing fails
    ///
    /// Caller MUST call close() on error.
    pub async fn process_frame(&mut self, frame: Bytes) -> Result<(), String> {
        self.ingress.record_frame_received(self.session_id);

        // Validate frame size
        if frame.len() > self.config.max_frame_size {
            return Err(format!(
                "frame too large: {} > {}",
                frame.len(),
                self.config.max_frame_size
            ));
        }

        // Forward to session for TLV decoding and routing
        crate::api::session::process_session_frame(&mut self.session, frame, self.ingress.as_ref())
            .await
            .map_err(|e| format!("session error: {:?}", e))
    }

    /// Close the session
    ///
    /// # Lifecycle Guarantee
    /// This MUST be called exactly once per TransportDriver instance.
    /// Multiple calls are prevented by the `closed` flag.
    ///
    /// # Arguments
    /// * `outcome` - Why the session is closing
    pub async fn close(mut self, outcome: SessionOutcome) {
        if self.closed {
            warn!(
                session_id = self.session_id,
                "Attempted duplicate close (prevented)"
            );
            return;
        }

        self.closed = true;

        let reason = match outcome {
            SessionOutcome::ClientClose => {
                info!(session_id = self.session_id, "Session closed by client");
                CloseReason::ClientClose
            }
            SessionOutcome::Error(msg) => {
                error!(session_id = self.session_id, error = %msg, "Session closed due to error");
                CloseReason::Error(msg)
            }
        };

        // Notify ingress (exactly once)
        self.ingress.on_close(self.session_id, reason).await;
    }

    /// Get the outbound sender for wiring to sink
    pub fn outbound_sender(&self) -> mpsc::Sender<Bytes> {
        self.outbound_tx.clone()
    }

    /// Get session ID
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
}

impl Drop for TransportDriver {
    fn drop(&mut self) {
        if !self.closed {
            error!(
                session_id = self.session_id,
                "TransportDriver dropped without calling close() - session lifecycle violated!"
            );
        }
    }
}
