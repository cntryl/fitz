// LAYER: API (Edge/Transport)
//! Edge API surfaces
//!
//! This layer owns:
//! - Tokio async I/O
//! - Socket accept loops
//! - Protocol framing (TCP length-prefix, WebSocket)
//! - Runtime ingress edge coordination
//! - Forwarding framed bytes into the synchronous core
//!
//! Transport handlers in this layer MUST NOT:
//! - Route messages
//! - Create envelopes
//! - Inspect permissions
//! - Contain domain logic
//!
//! `runtime_ingress` is the explicit async edge boundary that performs
//! authentication, authorization, cleanup dispatch, and bounded delivery into
//! the synchronous runtime/domain core.

pub mod admin;
pub mod background;
pub(crate) mod frame_adapter;
pub mod handlers;
pub mod http;
pub mod ingress;
pub mod mcp;
pub mod origin;
pub mod outbound;
pub mod runtime_ingress;
pub mod session;
pub mod tcp;
