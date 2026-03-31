//! Stream protocol messages and types

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::runtime::routing::{route_exact_quad, Route, RouteFamily};

/// Parse a stream route into (realm, area, resource, operation).
///
/// Expected format: `{scheme}://{realm}/{area}/{resource}/{operation}`
/// or `/{realm}/{area}/{resource}/{operation}`
pub fn parse_stream_route(route: &Route) -> Result<(String, String, String, String), String> {
    route_exact_quad(route.as_str())
        .map(|parts| {
            (
                parts.realm.to_string(),
                parts.area.to_string(),
                parts.resource.to_string(),
                parts.operation.to_string(),
            )
        })
        .ok_or_else(|| "Stream routes require exactly 4 segments".to_string())
}

// ═══════════════════════════════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

/// Maximum size for a single event (body + metadata combined)
pub const MAX_EVENT_SIZE: usize = 1_048_576; // 1 MB

/// Default lease size when requesting offsets from AreaActor
/// Optimized for bulk workloads: 10K events amortizes coordination overhead
pub const DEFAULT_LEASE_SIZE: u64 = 10_000;

/// Default realm lease block size when AreaActor requests from RealmActor
pub const DEFAULT_REALM_LEASE_BLOCK: u64 = 10_000;

// ═══════════════════════════════════════════════════════════════════════════
// OFFSET LEASE MANAGEMENT
// ═══════════════════════════════════════════════════════════════════════════

/// Lease for area or realm offsets with end-exclusive semantics
///
/// **CRITICAL**: `end` is EXCLUSIVE (not inclusive)
/// Valid range: [next, end)
#[derive(Debug, Clone)]
pub struct OffsetLease {
    pub next: u64,
    pub end: u64, // exclusive
}

impl OffsetLease {
    /// Create empty lease (no offsets available)
    pub fn new() -> Self {
        Self { next: 0, end: 0 }
    }

    /// Check if lease has no remaining offsets
    pub fn is_empty(&self) -> bool {
        self.next >= self.end
    }

    /// Get number of remaining offsets
    pub fn remaining(&self) -> u64 {
        self.end.saturating_sub(self.next)
    }

    /// Consume N offsets and return the starting offset
    ///
    /// Returns None if insufficient offsets available
    pub fn consume(&mut self, count: u64) -> Option<u64> {
        if self.remaining() < count {
            return None;
        }
        let start = self.next;
        self.next += count;
        Some(start)
    }

    /// Update lease from area-level grant
    pub fn update_from_area_lease(&mut self, grant: &LeaseGrant) {
        self.next = grant.area_start;
        self.end = grant.area_end_exclusive;
    }

    /// Update lease from realm-level grant
    pub fn update_from_realm_lease(&mut self, grant: &LeaseGrant) {
        self.next = grant.realm_start;
        self.end = grant.realm_end_exclusive;
    }
}

impl Default for OffsetLease {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// CORE DATA TYPES
// ═══════════════════════════════════════════════════════════════════════════

/// A durable event record in a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecord {
    /// Strict order within resource stream (server-assigned by StreamActor, strictly increasing)
    pub resource_offset: u64,

    /// Global order within area (server-assigned via leased offsets on commit)
    pub area_offset: Option<u64>,

    /// Global order within realm (server-assigned via leased offsets on commit)
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
    Begin {
        family_id: RouteFamily,
        route: Route,
        expected_offset: u64,
        ingest_metadata: Option<IngestMetadata>,
    },

    /// Append event to active session
    Append {
        session_id: u64,
        body: Bytes,
        metadata: Option<Bytes>,
    },

    /// Commit session (atomic write)
    /// Requires the caller to specify a write mode: Buffered or Sync.
    Commit {
        session_id: u64,
        mode: StreamWriteMode,
    }, // caller must specify StreamWriteMode (Buffered|Sync)

    /// Rollback session (discard)
    Rollback { session_id: u64 },

    /// Read events from stream
    Read {
        family_id: RouteFamily,
        route: Route,
        from_offset: u64,
        limit: u64,
        max_bytes: Option<usize>,
    },

    /// Get the last visible entry in the stream (tail operation)
    Last {
        family_id: RouteFamily,
        route: Route,
    },

    /// Get stream metadata and current state
    GetMetadata {
        family_id: RouteFamily,
        route: Route,
    },

    /// Subscribe to stream change notifications (client -> server)
    Subscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    },

    /// Unsubscribe from stream change notifications (client -> server)
    Unsubscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    },

    /// Unsubscribe all stream subscriptions for a session (called on disconnect)
    UnsubscribeAll {
        session_id: u64,
        subscriber: crate::runtime::routing::RouteAddress,
    },

    // Internal actor messages
    /// Request paired area+realm offsets from AreaActor (StreamActor -> AreaActor)
    RequestLease {
        realm: String,
        area: String,
        count: u64,
        reply_to: String,
    },

    /// Lease granted from AreaActor to StreamActor
    LeaseGranted { grant: LeaseGranted },

    /// Request realm offsets from RealmActor (AreaActor -> RealmActor, internal only)
    RequestRealmLease { count: u64 },

    /// Batch committed notification from StreamActor to AreaActor
    BatchCommitted(BatchCommitted),

    /// Area watermark advanced from AreaActor to RealmActor
    AreaWatermarkAdvanced(AreaWatermarkAdvanced),
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

/// Lease granted by AreaActor (paired area+realm ranges)
///
/// **CRITICAL: All ranges are END-EXCLUSIVE**
/// Valid range: [start, end_exclusive)
#[derive(Debug, Clone)]
pub struct LeaseGranted {
    pub area_start: u64,
    pub area_end_exclusive: u64,
    pub realm_start: u64,
    pub realm_end_exclusive: u64,
}

/// Write mode for stream commits
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamWriteMode {
    /// Buffered: throughput-first, may lose recent events on crash
    Buffered,
    /// Sync: correctness-first, writes are committed synchronously
    Sync,
}

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
    pub session_id: u64,
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

/// Stream metadata and current state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetadata {
    /// Maximum events per batch
    pub max_batch_events: usize,

    /// Maximum bytes per batch
    pub max_batch_bytes: usize,

    /// TTL in seconds (None = no expiration)
    pub ttl_seconds: Option<u64>,

    /// Last committed resource offset (None if stream empty)
    pub last_resource_offset: Option<u64>,

    /// Area watermark (highest contiguous area offset)
    pub area_watermark: u64,

    /// Realm watermark (minimum of all area watermarks in realm)
    pub realm_watermark: u64,
}

/// Response for get metadata operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetMetadataResponse {
    pub metadata: StreamMetadata,
}

/// Unified response type for all stream operations
#[derive(Debug, Clone)]
pub enum StreamResponse {
    /// Response to Begin operation
    BeginOk(BeginSessionResponse),
    /// Response to Append operation
    AppendOk(AppendResponse),
    /// Response to Commit operation
    CommitOk(CommitSessionResponse),
    /// Response to Read operation
    ReadOk(ReadResponse),
    /// Response to Last operation
    LastOk(PeekResponse),
    /// Response to GetMetadata operation
    MetadataOk(GetMetadataResponse),
    /// Error response for any operation
    Error(StreamError),
}

// ═══════════════════════════════════════════════════════════════════════════
// ERRORS
// ═══════════════════════════════════════════════════════════════════════════

/// Stream errors
#[derive(Debug, Clone, Copy)]
pub enum StreamError {
    /// Invalid realm format (3000)
    InvalidRealm,

    /// Realm mismatch - operation targets different realm than active session (3001)
    RealmMismatch,

    /// Optimistic concurrency conflict - expected_offset mismatch (2001)
    ConcurrencyConflict,

    /// Session already active for this resource (2002)
    SessionAlreadyActive,

    /// Session not found (2003)
    SessionNotFound,

    /// Invalid read bounds (2004)
    InvalidReadBound,

    /// Event too large (2006)
    EventTooLarge,

    /// Session full - lease capacity reached (2007)
    SessionFull,

    /// Batch too large (2008)
    BatchTooLarge,

    /// Lease requested - commit queued pending lease grant (2009)
    LeaseRequested,
}

impl StreamError {
    pub fn code(&self) -> u16 {
        match self {
            StreamError::InvalidRealm => 3000,
            StreamError::RealmMismatch => 3001,
            StreamError::ConcurrencyConflict => 2001,
            StreamError::SessionAlreadyActive => 2002,
            StreamError::SessionNotFound => 2003,
            StreamError::InvalidReadBound => 2004,
            StreamError::EventTooLarge => 2006,
            StreamError::SessionFull => 2007,
            StreamError::BatchTooLarge => 2008,
            StreamError::LeaseRequested => 2009,
        }
    }
}

/// Backward compatibility alias
pub type LeaseGrant = LeaseGranted;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_stream_route_with_operation() {
        // Arrange
        let route = Route::new("stream://acme/orders/checkout/append");

        // Act
        let result = parse_stream_route(&route).unwrap();

        // Assert
        assert_eq!(result.0, "acme");
        assert_eq!(result.1, "orders");
        assert_eq!(result.2, "checkout");
        assert_eq!(result.3, "append");
    }

    #[test]
    fn should_reject_stream_route_missing_operation() {
        // Arrange
        let route = Route::new("stream://acme/orders/checkout");

        // Act
        let result = parse_stream_route(&route);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_reject_stream_route_given_extra_segment() {
        // Arrange
        let route = Route::new("stream://acme/orders/checkout/append/extra");

        // Act
        let result = parse_stream_route(&route);

        // Assert
        assert!(result.is_err());
    }
}
