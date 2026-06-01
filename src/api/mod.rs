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
pub mod background;
pub mod handlers;
pub mod ingress;
pub mod mcp;
pub mod outbound;
pub mod runtime_ingress;
pub mod session;
pub mod tcp;
pub mod ws;
