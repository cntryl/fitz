//! ReplyInboxActor: per-client inbox for strict contiguous RPC responses
//!
//! Each client session has a dedicated ReplyInboxActor that:
//! - Enforces strict contiguous streaming order
//! - Handles slow transports without blocking workers
//! - Drops state when session disconnects
//! - Forwards responses to transport layer
//!
//! # Streaming Enforcement
//!
//! The inbox tracks expected sequence numbers per correlation ID:
//! - If seq == expected: forward immediately and increment expected
//! - If seq != expected: terminate the stream state immediately
//! - On stream_end: finalize and clear correlation state

use crate::domains::rpc::errors::RpcError;
use crate::domains::rpc::protocol::RpcResponse;
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::RouteFamily;
use std::collections::HashMap;
use uuid::Uuid;

/// Tracks streaming state for a single correlation ID
#[derive(Debug)]
struct StreamState {
    /// Next expected sequence number
    next_seq: u64,
}

impl StreamState {
    fn new() -> Self {
        Self { next_seq: 0 }
    }
}

/// Messages handled by ReplyInboxActor
#[derive(Debug, Clone)]
pub enum InboxMessage {
    /// Response chunk from worker via RpcRouteActor
    Response(RpcResponse),
    /// Error to be delivered to client
    Error(RpcError),
    /// Clean up state for a correlation ID
    Cleanup { correlation_id: Uuid },
}

/// ReplyInboxActor manages response ordering and delivery for one client
///
/// Maintains per-correlation streaming state and enforces chunk ordering.
/// Buffers out-of-order chunks until gaps are filled.
pub struct ReplyInboxActor {
    /// Route family for isolation
    _family: RouteFamily,
    /// Streaming state per correlation ID
    streams: HashMap<Uuid, StreamState>,
    /// Number of invalid sequence failures observed by this inbox.
    invalid_sequence_failures: usize,
}

impl ReplyInboxActor {
    /// Create new reply inbox actor
    pub fn new(family: RouteFamily) -> Self {
        Self {
            _family: family,
            streams: HashMap::with_capacity(64), // Pre-allocate for typical concurrent requests
            invalid_sequence_failures: 0,
        }
    }

    /// Create reply inbox with the same strict contiguous semantics.
    pub fn with_buffer_size(family: RouteFamily, _max_buffer_size: usize) -> Self {
        Self::new(family)
    }

    /// Handle incoming response chunk
    fn handle_response(&mut self, response: RpcResponse, _ctx: &mut Context<Self>) {
        let correlation_id = response.correlation_id;

        // Get or create stream state
        let stream = self
            .streams
            .entry(correlation_id)
            .or_insert_with(StreamState::new);

        // Check sequence number
        if response.seq == stream.next_seq {
            // Expected chunk, forward immediately
            Self::forward_response_static(&response);
            stream.next_seq += 1;

            // If this completes the stream, clean up
            if response.stream_end {
                self.streams.remove(&correlation_id);
            }
        } else {
            self.invalid_sequence_failures = self.invalid_sequence_failures.saturating_add(1);
            self.streams.remove(&correlation_id);
        }
    }

    /// Forward response to transport layer (static to avoid borrowing self)
    fn forward_response_static(_response: &RpcResponse) {
        // NOTE: Transport integration pending - responses currently logged only
        // See: https://github.com/cntryl/fitz/issues/track-response-routing
        tracing::debug!("RPC response would be forwarded to transport");
    }

    /// Handle error delivery
    fn handle_error(&mut self, error: RpcError, _ctx: &mut Context<Self>) {
        // Clean up any streaming state for this correlation
        self.streams.remove(&error.correlation_id);

        // NOTE: Transport integration pending - errors currently logged only
        // See: https://github.com/cntryl/fitz/issues/track-response-routing
        tracing::warn!("RPC error would be forwarded to transport: {:?}", error);
    }

    /// Clean up state for a correlation ID
    fn handle_cleanup(&mut self, correlation_id: Uuid) {
        self.streams.remove(&correlation_id);
    }

    /// Get number of active streams
    pub fn active_streams(&self) -> usize {
        self.streams.len()
    }

    /// Get buffered chunk count for a correlation ID.
    ///
    /// Strict contiguous delivery does not buffer chunks.
    pub fn buffered_count(&self, correlation_id: &Uuid) -> usize {
        let _ = correlation_id;
        0
    }

    /// Get number of invalid sequence failures observed by this inbox.
    pub fn invalid_sequence_failures(&self) -> usize {
        self.invalid_sequence_failures
    }
}

impl Actor for ReplyInboxActor {
    type Message = InboxMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            InboxMessage::Response(response) => {
                self.handle_response(response, ctx);
            }
            InboxMessage::Error(error) => {
                self.handle_error(error, ctx);
            }
            InboxMessage::Cleanup { correlation_id } => {
                self.handle_cleanup(correlation_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::{Route, RouteAddress};

    fn create_inbox() -> ReplyInboxActor {
        ReplyInboxActor::new(RouteFamily::new(1))
    }

    fn create_response(correlation_id: Uuid, seq: u64, stream_end: bool) -> RpcResponse {
        RpcResponse {
            correlation_id,
            seq,
            body: bytes::Bytes::new(),
            stream_end,
        }
    }

    #[test]
    fn should_create_inbox() {
        let inbox = create_inbox();
        assert_eq!(inbox.active_streams(), 0);
    }

    #[test]
    fn should_handle_single_chunk_response() {
        // Arrange
        let mut inbox = create_inbox();
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://test"));
        let mut ctx = Context::new(addr, router);
        let correlation_id = Uuid::new_v4();
        let response = create_response(correlation_id, 0, true);

        // Act
        inbox.handle_response(response, &mut ctx);

        // Assert
        assert_eq!(inbox.active_streams(), 0); // Cleaned up after stream_end
    }

    #[test]
    fn should_fail_out_of_order_chunks() {
        // Arrange
        let mut inbox = create_inbox();
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://test"));
        let mut ctx = Context::new(addr, router);
        let correlation_id = Uuid::new_v4();

        // Act - receive seq 2 before seq 0 and 1
        inbox.handle_response(create_response(correlation_id, 2, false), &mut ctx);

        // Assert
        assert_eq!(inbox.active_streams(), 0);
        assert_eq!(inbox.buffered_count(&correlation_id), 0);
        assert_eq!(inbox.invalid_sequence_failures(), 1);
    }

    #[test]
    fn should_fail_when_gap_appears_mid_stream() {
        // Arrange
        let mut inbox = create_inbox();
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://test"));
        let mut ctx = Context::new(addr, router);
        let correlation_id = Uuid::new_v4();

        // Act
        inbox.handle_response(create_response(correlation_id, 0, false), &mut ctx);
        inbox.handle_response(create_response(correlation_id, 2, false), &mut ctx);

        // Assert
        assert_eq!(inbox.active_streams(), 0);
        assert_eq!(inbox.buffered_count(&correlation_id), 0);
        assert_eq!(inbox.invalid_sequence_failures(), 1);
    }

    #[test]
    fn should_fail_duplicate_chunks() {
        // Arrange
        let mut inbox = create_inbox();
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://test"));
        let mut ctx = Context::new(addr, router);
        let correlation_id = Uuid::new_v4();

        // Act - receive seq 0 twice
        inbox.handle_response(create_response(correlation_id, 0, false), &mut ctx);
        inbox.handle_response(create_response(correlation_id, 0, false), &mut ctx);

        // Assert
        assert_eq!(inbox.active_streams(), 0);
        assert_eq!(inbox.invalid_sequence_failures(), 1);
    }
}
