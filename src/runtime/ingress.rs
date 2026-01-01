//! Runtime boundary trait for transports
//!
//! # Purpose
//!
//! This module defines the async trait interface that connects the async transport
//! layer (API) to the synchronous runtime layer. It is the ONE async/sync boundary
//! in Fitz's architecture.
//!
//! # Design
//!
//! - **Trait definition only** (this file)
//! - **Implementation in SESSION layer** (`session/ingress_impl.rs`)
//! - **Consumer in API layer** (`api/tcp.rs`, `api/ws/mod.rs`)
//!
//! This separation ensures:
//! - Async code lives in the transport and session layers
//! - Core runtime code remains 100% synchronous
//! - The boundary is explicit and testable
//!
//! # Why Async Here?
//!
//! The transport layer is inherently async (Tokio, WebSocket, TCP).
//! The Ingress trait must be async to handle frame arrival asynchronously.
//! All async work is done BEFORE calling into the synchronous runtime;
//! the runtime receives pre-parsed, synchronous results.

use bytes::Bytes;
use crate::protocol::frame::ChannelId;
use crate::session::{CloseReason, SessionInfo};

/// Outcome from the runtime for a single protocol message
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressDecision {
    Accept,
    Close(String),
    Backpressure,
}

/// Trait implemented by the runtime to consume transport frames
#[async_trait::async_trait]
pub trait Ingress: Send + Sync {
    /// Called when transport opens a new session
    async fn on_open(&self, session: SessionInfo) -> Result<u64, String>;

    /// Called for every demultiplexed channel message
    async fn on_frame(
        &self,
        session_id: u64,
        channel_id: ChannelId,
        message_payload: Bytes,
    ) -> IngressDecision;

    /// Called when the transport closes the connection
    async fn on_close(&self, session_id: u64, reason: CloseReason);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decisions_are_distinct() {
        assert_eq!(IngressDecision::Accept, IngressDecision::Accept);
        assert_ne!(IngressDecision::Accept, IngressDecision::Backpressure);
    }
}
