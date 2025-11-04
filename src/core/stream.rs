use crate::core::engine::EngineHandle;

// ============================================================================
// STREAM DOMAIN TYPES
// ============================================================================
// These types define the stream subsystem's domain model.
// They live in core because they represent business logic, not storage primitives.

/// A stream event with client-controlled sequence and server-assigned area sequence.
#[derive(Debug, Clone)]
pub struct StreamEvent {
    pub resource_seq: u64,     // Client-controlled, 0-indexed monotonic
    pub area_seq: Option<u64>, // Server-assigned at finalization
    pub body: Vec<u8>,
    pub metadata: Option<Vec<u8>>,
    pub created_at: u64,
    pub is_end: bool, // Stream finalization marker
}

/// Result of appending to a stream (resource sequence and optional area sequence range).
#[derive(Debug, Clone)]
pub struct AppendResult {
    pub resource_seq: u64,
    pub area_seq_range: Option<std::ops::Range<u64>>,
}

/// Response from reading an area (interleaved events with watermark).
#[derive(Debug, Clone)]
pub struct AreaReadResponse {
    pub events: Vec<StreamEvent>,
    pub watermark: u64,
}

/// Stream operation errors.
#[derive(Debug, Clone)]
pub enum StreamError {
    SequenceGap { expected: u64, received: u64 },
    SequenceConflict { seq: u64 },
    StreamClosed,
    WrongExpectedVersion(u64), // Carries current head (legacy)
    Other(String),
}

/// Expected revision for optimistic concurrency control.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRevision {
    Any,
    NoStream,
    StreamExists,
    Exact(u64),
}

// Re-export for backward compatibility
pub use ExpectedRevision as StreamExpectedRevision;

// ============================================================================
// STREAM API
// ============================================================================

/// Stream API: append-only ordered logs with peek and live notifications (via Notice subscribe route).
#[derive(Clone, Debug)]
pub struct Stream {
    engine: EngineHandle,
}

impl Stream {
    pub fn new(engine: EngineHandle) -> Self {
        Self { engine }
    }

    /// Append an event with optimistic concurrency check; returns assigned seq.
    pub async fn append(
        &self,
        route: String,
        id: Option<String>,
        body: Vec<u8>,
        metadata: Option<Vec<u8>>,
        expected: ExpectedRevision,
    ) -> Result<u64, String> {
        self.engine
            .stream_append_old(route, id, body, metadata, expected)
            .await
    }

    /// Peek N events starting from a given sequence (inclusive). Returns (seq, body).
    pub async fn peek(
        &self,
        route: String,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(u64, Vec<u8>)>, String> {
        self.engine.stream_peek_old(route, from_seq, limit).await
    }

    /// Consume hierarchically over a prefix route; returns (route, seq, body) records.
    pub async fn consume_prefix(
        &self,
        prefix: String,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<(String, u64, Vec<u8>)>, String> {
        self.engine
            .stream_consume_prefix(prefix, from_seq, limit)
            .await
    }
}
