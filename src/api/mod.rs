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
pub(crate) mod authentication;
pub mod background;
mod broker;
mod broker_shutdown;
pub mod handlers;
pub mod http;
pub mod ingress;
pub mod jwks;
pub mod mcp;
pub mod origin;
pub mod outbound;
pub mod runtime_ingress;
pub mod session;
pub mod tcp;

/// Run the broker transport runtime to completion.
///
/// # Errors
/// Returns startup, listener, storage, or shutdown failures.
pub fn run(config: crate::boot::BootConfig) -> crate::boot::BootResult<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(broker::boot(config))
}

pub(crate) mod storage_runtime;
