//! Stream domain types

/// A stream event with monotonic sequencing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamEvent {
    pub resource_seq: u64,     // Client-controlled, 0-indexed monotonic
    pub area_seq: Option<u64>, // Server-assigned at finalization
    pub body: Vec<u8>,
    pub metadata: Option<Vec<u8>>,
    pub created_at: u64,
    pub is_end: bool, // Stream finalization marker
}

/// Result of appending events to a stream.
#[derive(Debug, Clone)]
pub struct AppendResult {
    pub resource_seq: u64,
    pub area_seq_range: Option<std::ops::Range<u64>>,
}

/// Response from reading a stream area.
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
    WrongExpectedVersion(u64), // carries current head (legacy)
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
