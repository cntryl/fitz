// LAYER: SESSION
//! Session management helpers
//!
//! This layer provides:
//! - Per-connection session state (metadata, permissions, ID)
//! - Synchronous TLV decoding and multiplexing
//! - SessionId generation
//! - Session lifecycle (open, close, reason tracking)
//!
//! Async transport adapters live under `src/api/`.

#![allow(clippy::module_inception)]

pub mod actor;
pub mod id_generator;
pub mod manager;
pub mod outbound;
pub mod permissions;
pub mod session;

pub use actor::SessionActor;

pub use id_generator::generate as generate_session_id;
pub use manager::{Ingress, IngressDecision, RuntimeIngress, SessionEvent, SessionFrame};
pub use outbound::SessionOutboundSink;
pub use permissions::SessionPermissions;
pub use session::{
    next_session_id, CloseReason, NewSessionConfig, Session, SessionError, SessionId, SessionInfo,
    SessionMetadata, TransportKind,
};
