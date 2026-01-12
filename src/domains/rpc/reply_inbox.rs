//! ReplyInboxActor: per-client inbox for ordering and buffering RPC responses
//!
//! Each client session has a dedicated ReplyInboxActor that:
//! - Enforces streaming chunk ordering (buffers out-of-order chunks)
//! - Handles slow transports without blocking workers
//! - Drops state when session disconnects
//! - Forwards responses to transport layer
//!
//! # Streaming Enforcement
//!
//! The inbox tracks expected sequence numbers per correlation ID:
//! - If seq == expected: forward immediately and increment expected
//! - If seq > expected: buffer until gap is filled
//! - If seq < expected: drop as duplicate
//! - On stream_end: finalize and clear correlation state

use crate::domains::rpc::errors::RpcError;
use crate::domains::rpc::protocol::RpcResponse;
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::RouteFamily;
use std::collections::{BTreeMap, HashMap};
use uuid::Uuid;

/// Tracks streaming state for a single correlation ID
#[derive(Debug)]
struct StreamState {
    /// Next expected sequence number
    next_seq: u64,
    /// Buffered chunks received ahead of time
    buffer: BTreeMap<u64, RpcResponse>,
}

impl StreamState {
    fn new() -> Self {
        Self {
            next_seq: 0,
            buffer: BTreeMap::new(),
        }
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
    /// Maximum number of buffered chunks per stream
    max_buffer_size: usize,
}

impl ReplyInboxActor {
    /// Create new reply inbox actor
    pub fn new(family: RouteFamily) -> Self {
        Self {
            _family: family,
            streams: HashMap::with_capacity(64), // Pre-allocate for typical concurrent requests
            max_buffer_size: 100,                // Default: buffer up to 100 out-of-order chunks
        }
    }

    /// Create reply inbox with custom buffer size
    pub fn with_buffer_size(family: RouteFamily, max_buffer_size: usize) -> Self {
        Self {
            _family: family,
            streams: HashMap::with_capacity(64),
            max_buffer_size,
        }
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
        if response.seq < stream.next_seq {
            // Duplicate chunk, drop it
            return;
        }

        if response.seq == stream.next_seq {
            // Expected chunk, forward immediately
            Self::forward_response_static(&response);
            stream.next_seq += 1;

            // If this completes the stream, clean up
            if response.stream_end {
                self.streams.remove(&correlation_id);
                return;
            }

            // Try to flush buffered chunks (no need to drop stream, borrow ends here)
            self.flush_buffer(&correlation_id);
        } else {
            // Ahead-of-time chunk, buffer it
            if stream.buffer.len() >= self.max_buffer_size {
                // Buffer overflow, disconnect session
                // TODO: Send disconnect signal to transport
                self.streams.remove(&correlation_id);
                return;
            }

            stream.buffer.insert(response.seq, response);
        }
    }

    /// Flush buffered chunks that are now in order
    fn flush_buffer(&mut self, correlation_id: &Uuid) {
        loop {
            let should_remove = {
                let Some(stream) = self.streams.get_mut(correlation_id) else {
                    break;
                };

                // Check if next expected chunk is in buffer
                let Some(response) = stream.buffer.remove(&stream.next_seq) else {
                    break;
                };

                Self::forward_response_static(&response);
                stream.next_seq += 1;

                // Signal if this was the final chunk
                response.stream_end
            };

            // Clean up if stream ended
            if should_remove {
                self.streams.remove(correlation_id);
                break;
            }
        }
    }

    /// Forward response to transport layer (static to avoid borrowing self)
    fn forward_response_static(_response: &RpcResponse) {
        // TODO: Send to transport actor
        // For now, this is a placeholder for transport integration
    }

    /// Handle error delivery
    fn handle_error(&mut self, error: RpcError, _ctx: &mut Context<Self>) {
        // Clean up any streaming state for this correlation
        self.streams.remove(&error.correlation_id);

        // Forward error to client
        // TODO: Send error to transport actor
    }

    /// Clean up state for a correlation ID
    fn handle_cleanup(&mut self, correlation_id: Uuid) {
        self.streams.remove(&correlation_id);
    }

    /// Get number of active streams
    pub fn active_streams(&self) -> usize {
        self.streams.len()
    }

    /// Get buffered chunk count for a correlation ID
    pub fn buffered_count(&self, correlation_id: &Uuid) -> usize {
        self.streams
            .get(correlation_id)
            .map(|s| s.buffer.len())
            .unwrap_or(0)
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
    fn should_buffer_out_of_order_chunks() {
        // Arrange
        let mut inbox = create_inbox();
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://test"));
        let mut ctx = Context::new(addr, router);
        let correlation_id = Uuid::new_v4();

        // Act - receive seq 2 before seq 0 and 1
        inbox.handle_response(create_response(correlation_id, 2, false), &mut ctx);

        // Assert
        assert_eq!(inbox.active_streams(), 1);
        assert_eq!(inbox.buffered_count(&correlation_id), 1);
    }

    #[test]
    fn should_flush_buffer_when_gap_filled() {
        // Arrange
        let mut inbox = create_inbox();
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://test"));
        let mut ctx = Context::new(addr, router);
        let correlation_id = Uuid::new_v4();

        // Act - receive out of order, then fill gap
        inbox.handle_response(create_response(correlation_id, 2, false), &mut ctx);
        inbox.handle_response(create_response(correlation_id, 0, false), &mut ctx);
        inbox.handle_response(create_response(correlation_id, 1, false), &mut ctx);

        // Assert - seq 2 should have been flushed when we filled the gap
        assert_eq!(inbox.buffered_count(&correlation_id), 0);
    }

    #[test]
    fn should_drop_duplicate_chunks() {
        // Arrange
        let mut inbox = create_inbox();
        let router = std::sync::Arc::new(crate::runtime::router::Router::new());
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://test"));
        let mut ctx = Context::new(addr, router);
        let correlation_id = Uuid::new_v4();

        // Act - receive seq 0 twice
        inbox.handle_response(create_response(correlation_id, 0, false), &mut ctx);
        inbox.handle_response(create_response(correlation_id, 0, false), &mut ctx);

        // Assert - duplicate dropped, only expecting seq 1 now
        assert_eq!(inbox.active_streams(), 1);
    }
}
