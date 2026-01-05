//! Stream protocol messages and types

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::runtime::routing::{Route, RouteFamily};

/// A durable event record in a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    /// Strict order within resource stream (caller-assigned)
    pub resource_offset: u64,
    
    /// Global order within area (system-assigned)
    pub area_offset: Option<u64>,
    
    /// Global order within realm (system-assigned)
    pub realm_offset: Option<u64>,
    
    /// Event payload
    pub body: Bytes,
    
    /// Optional metadata
    pub metadata: Option<Bytes>,
    
    /// Server timestamp (milliseconds since epoch)
    pub created_at: u64,
}

/// Optional metadata attached to an ingest batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestMetadata {
    pub opaque: Bytes,
}

// ═══════════════════════════════════════════════════════════════════════════
// EXTERNAL API (Client-facing messages)
// ═══════════════════════════════════════════════════════════════════════════

/// Messages for stream operations
#[derive(Debug, Clone)]
pub enum StreamMessage {
    /// Begin streaming append session
    /// Client provides: resource path, expected_offset, optional metadata
    /// NO client-supplied area/realm offsets
    BeginSession {
        family_id: RouteFamily,
        route: Route,
        expected_offset: u64,
        ingest_metadata: Option<IngestMetadata>,
    },
    
    /// Append event to active session
    AppendToSession {
        session_id: String,
        body: Bytes,
        metadata: Option<Bytes>,
    },
    
    /// Commit session (atomic write)
    CommitSession {
        session_id: String,
    },
    
    /// Abort session (discard)
    AbortSession {
        session_id: String,
    },
    
    /// Read events from stream
    Read {
        family_id: RouteFamily,
        route: Route,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    },
    
    /// Peek at the last committed record in stream (tail operation)
    Peek {
        family_id: RouteFamily,
        route: Route,
    },
    
    // Internal actor messages
    /// Request more offsets from AreaActor (StreamActor -> AreaActor)
    RequestLease {
        realm: String,
        area: String,
        count: u64,
        reply_to: String,
    },
    
    /// Request area offsets (alias for RequestLease)
    RequestAreaLease {
        count: u64,
    },
    
    /// Request realm offsets from RealmActor
    RequestRealmLease {
        count: u64,
    },
    
    /// Lease granted from AreaActor to StreamActor
    LeaseGranted {
        grant: LeaseGranted,
    },
    
    /// Batch committed notification from StreamActor to AreaActor
    BatchCommitted {
        first_area_offset: u64,
        last_area_offset: u64,
        first_realm_offset: u64,
        last_realm_offset: u64,
    },
    
    /// Area watermark advanced from AreaActor to RealmActor
    AreaWatermarkAdvanced {
        realm: String,
        area: String,
        watermark: u64,
    },
}

// ═══════════════════════════════════════════════════════════════════════════
// INTERNAL MESSAGES (Actor-to-actor communication)
// ═══════════════════════════════════════════════════════════════════════════

/// Request lease from AreaActor
#[derive(Debug, Clone)]
pub struct RequestLease {
    pub realm: String,
    pub area: String,
    pub count: u64,
    pub reply_to: String, // StreamActor ID
}

/// Lease granted by AreaActor
#[derive(Debug, Clone)]
pub struct LeaseGranted {
    pub area_start: u64,
    pub area_end: u64,      // inclusive
    pub realm_start: u64,
    pub realm_end: u64,     // inclusive
}

// Backward compatibility alias (for gradual migration)
pub type LeaseGrant = LeaseGranted;

/// Batch committed notification from StreamActor to AreaActor
#[derive(Debug, Clone)]
pub struct BatchCommitted {
    pub first_area_offset: u64,
    pub last_area_offset: u64,
    pub first_realm_offset: u64,
    pub last_realm_offset: u64,
}

/// Area watermark advanced notification from AreaActor to RealmActor
#[derive(Debug, Clone)]
pub struct AreaWatermarkAdvanced {
    pub area: String,
    pub watermark: u64,
}

// ═══════════════════════════════════════════════════════════════════════════
// RESPONSES
// ═══════════════════════════════════════════════════════════════════════════

/// Response for begin session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeginSessionResponse {
    pub session_id: String,
}

/// Response for append to session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendResponse {
    pub success: bool,
}

/// Response for commit session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitSessionResponse {
    pub first_resource_offset: u64,
    pub last_resource_offset: u64,
    pub first_area_offset: u64,
    pub last_area_offset: u64,
    pub first_realm_offset: u64,
    pub last_realm_offset: u64,
    pub batch_size: usize,
    pub ingest_metadata: Option<IngestMetadata>,
}

/// Cursor for resumable streaming reads
#[derive(Debug, Clone)]
pub struct ReadCursor {
    pub last_resource_offset: u64,
    pub last_area_offset: Option<u64>,
    pub last_realm_offset: Option<u64>,
    pub has_more: bool,
}

/// Response for read operation (streaming batch)
#[derive(Debug, Clone)]
pub struct ReadResponse {
    pub records: Vec<StreamRecord>,
    pub cursor: ReadCursor,
}

/// Response for peek operation (last committed record)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeekResponse {
    /// The last committed record, or None if stream is empty
    pub record: Option<StreamRecord>,
}

// ═══════════════════════════════════════════════════════════════════════════
// ERRORS
// ═══════════════════════════════════════════════════════════════════════════

/// Stream errors
#[derive(Debug, Clone, Copy)]
pub enum StreamError {
    /// Optimistic concurrency conflict - expected_offset mismatch (2001)
    ConcurrencyConflict,
    
    /// Session already active for this resource (2002)
    SessionAlreadyActive,
    
    /// Session not found (2003)
    SessionNotFound,
    
    /// Invalid read bounds (2004)
    InvalidReadBound,
    
    /// Read beyond watermark (2005)
    ReadBeyondWatermark,
    
    /// Event too large (2006)
    EventTooLarge,
    
    /// Session full - lease capacity reached (2007)
    SessionFull,
    
    /// Batch too large (2008)
    BatchTooLarge,
}

impl StreamError {
    pub fn code(&self) -> u16 {
        match self {
            StreamError::ConcurrencyConflict => 2001,
            StreamError::SessionAlreadyActive => 2002,
            StreamError::SessionNotFound => 2003,
            StreamError::InvalidReadBound => 2004,
            StreamError::ReadBeyondWatermark => 2005,
            StreamError::EventTooLarge => 2006,
            StreamError::SessionFull => 2007,
            StreamError::BatchTooLarge => 2008,
        }
    }
}
