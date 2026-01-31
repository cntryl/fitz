// LAYER: API (Edge/Transport)
//! Edge API surfaces
//!
//! This layer owns:
//! - Tokio async I/O
//! - Socket accept loops
//! - Protocol framing (TCP length-prefix, WebSocket)
//! - Session creation and lifecycle
//! - Forwarding of bytes to `Session`
//!
//! This layer MUST NOT:
//! - Route messages
//! - Create envelopes
//! - Inspect permissions
//! - Contain domain logic

pub mod admin;
pub mod ingress;
pub mod tcp;
pub mod ws;
