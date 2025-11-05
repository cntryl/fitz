//! Stream domain types

use crate::protocol::route::Route;

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

/// Stream operation types determined by route operation segment
#[derive(Debug, Clone)]
pub enum StreamOperation {
    /// Append - add event to stream (TAG_SEQ = resource_seq, TAG_BODY = event body)
    Append,
    /// Read - read events from resource stream (TAG_SEQ = from_seq, limit in route)
    Read,
    /// ReadArea - read from area with watermark (TAG_SEQ = from_seq, limit in route)
    ReadArea,
    /// Peek - peek events without advancing (TAG_SEQ = from_seq, limit in route)
    Peek,
    /// Subscribe - subscribe to stream for live updates
    Subscribe,
}

impl StreamOperation {
    /// Determine operation from route
    pub fn from_route(route: &Route) -> Result<Self, String> {
        match route.operation.as_deref() {
            Some("append") => Ok(StreamOperation::Append),
            Some("read") => Ok(StreamOperation::Read),
            Some("read-area") => Ok(StreamOperation::ReadArea),
            Some("peek") => Ok(StreamOperation::Peek),
            Some("subscribe") => Ok(StreamOperation::Subscribe),
            None => {
                // Default based on route structure
                // If route has resource, default to Read
                // If route has only area, default to ReadArea
                if route.resource.is_some() {
                    Ok(StreamOperation::Read)
                } else if route.area.is_some() {
                    Ok(StreamOperation::ReadArea)
                } else {
                    Err("Stream operation requires operation or resource/area".to_string())
                }
            }
            Some(op) => Err(format!("Unknown stream operation: {}", op)),
        }
    }
}
