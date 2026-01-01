// LAYER: SESSION (Async → Sync Bridge)
//! Session management helpers
//!
//! This layer provides:
//! - Per-connection session state (metadata, permissions, ID)
//! - Async → Sync boundary (owns `Mux`, hands frames to runtime)
//! - SessionId generation
//! - Session lifecycle (open, close, reason tracking)
//!
//! Session is the critical bridge between async transport and synchronous runtime.

#![allow(clippy::module_inception)]

pub mod permissions;
pub mod session;

pub use permissions::SessionPermissions;
pub use session::{next_session_id, CloseReason, Session, SessionError, SessionId, SessionInfo, SessionMetadata, TransportKind};

