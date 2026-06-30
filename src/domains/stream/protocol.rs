//! Stream protocol messages and types

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::runtime::routing::{route_exact_quad, Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;

/// Parse a stream route into (realm, area, resource, operation).
///
/// Expected format: `{scheme}://{realm}/{area}/{resource}/{operation}`
/// or `/{realm}/{area}/{resource}/{operation}`
///
/// # Errors
///
/// Returns an error when `route` does not contain exactly four path segments.
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

/// Default lease size when requesting offsets from `AreaActor`
/// Optimized for bulk workloads: 10K events amortizes coordination overhead
pub const DEFAULT_LEASE_SIZE: u64 = 10_000;

/// Default realm lease block size when `AreaActor` requests from `RealmActor`
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
    #[must_use]
    pub fn new() -> Self {
        Self { next: 0, end: 0 }
    }

    /// Check if lease has no remaining offsets
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.next >= self.end
    }

    /// Get number of remaining offsets
    #[must_use]
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
    pub fn update_from_area_lease(&mut self, grant: &LeaseGranted) {
        self.next = grant.area_start;
        self.end = grant.area_end_exclusive;
    }

    /// Update lease from realm-level grant
    pub fn update_from_realm_lease(&mut self, grant: &LeaseGranted) {
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
    /// Strict order within resource stream (server-assigned by `StreamActor`, strictly increasing)
    pub resource_offset: u64,

    /// Global order within area (server-assigned via leased offsets on commit).
    ///
    /// `Some` when the record was read via the resource index (i.e., area/realm offsets
    /// were committed alongside the resource offset). `None` for records read via the
    /// area index when the area offset wasn't stored on the individual resource record.
    pub area_offset: Option<u64>,

    /// Global order within realm (server-assigned via leased offsets on commit).
    ///
    /// `Some` only for realm-scope reads where the realm offset is available in the
    /// realm index. `None` for resource-scope and area-scope reads where the realm
    /// index is not consulted.
    ///
    /// Note: changing this to `u64` would require a storage migration because the
    /// on-disk `ResourceValue` encodes this as `Option<u64>`.
    pub realm_offset: Option<u64>,

    /// Event payload
    pub body: Bytes,

    /// Optional metadata
    pub metadata: Option<Bytes>,

    /// Server timestamp (milliseconds since epoch)
    pub created_at: u64,
}

/// Server-visible immutable discriminator attached to a committed event.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct StreamDiscriminator(pub String);

impl StreamDiscriminator {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for StreamDiscriminator {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for StreamDiscriminator {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Simple discriminator predicate supported by the stream broker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StreamFilterClause {
    Equals(String),
    NotEquals(String),
    StartsWith(String),
    AnyOf(Vec<String>),
}

impl StreamFilterClause {
    fn encode_into(&self, enc: &mut PayloadEncoder) {
        match self {
            Self::Equals(value) => {
                enc.put_u8(0);
                enc.put_string(value);
            }
            Self::NotEquals(value) => {
                enc.put_u8(1);
                enc.put_string(value);
            }
            Self::StartsWith(prefix) => {
                enc.put_u8(2);
                enc.put_string(prefix);
            }
            Self::AnyOf(values) => {
                enc.put_u8(3);
                enc.put_u32(u32::try_from(values.len()).unwrap_or(u32::MAX));
                for value in values {
                    enc.put_string(value);
                }
            }
        }
    }

    fn try_decode(dec: &mut PayloadDecoder) -> Result<Self, String> {
        match dec.get_u8()? {
            0 => Ok(Self::Equals(dec.get_string()?)),
            1 => Ok(Self::NotEquals(dec.get_string()?)),
            2 => Ok(Self::StartsWith(dec.get_string()?)),
            3 => {
                let count = dec.get_u32()? as usize;
                let mut values = Vec::with_capacity(count);
                for _ in 0..count {
                    values.push(dec.get_string()?);
                }
                Ok(Self::AnyOf(values))
            }
            other => Err(format!("Unknown stream filter clause {other}")),
        }
    }

    fn matches(&self, discriminator: &str) -> bool {
        match self {
            Self::Equals(value) => discriminator == value,
            Self::NotEquals(value) => discriminator != value,
            Self::StartsWith(prefix) => discriminator.starts_with(prefix),
            Self::AnyOf(values) => values.iter().any(|value| value == discriminator),
        }
    }
}

/// Conjunctive set of discriminator clauses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct StreamFilterSet {
    pub clauses: Vec<StreamFilterClause>,
}

impl StreamFilterSet {
    const VERSION_MARKER: [u8; 2] = [0, 0xF1];

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut enc = PayloadEncoder::with_capacity(2 + 4 + self.clauses.len() * 8);
        enc.put_u8(Self::VERSION_MARKER[0]);
        enc.put_u8(Self::VERSION_MARKER[1]);
        enc.put_u32(u32::try_from(self.clauses.len()).unwrap_or(u32::MAX));
        for clause in &self.clauses {
            clause.encode_into(&mut enc);
        }
        enc.finish()
    }

    /// # Errors
    ///
    /// Returns an error when the byte payload is missing the version marker,
    /// contains malformed clauses, or has trailing bytes.
    pub fn try_decode(bytes: &[u8]) -> Result<Self, String> {
        let mut dec = PayloadDecoder::new(bytes);
        let marker0 = dec.get_u8()?;
        let marker1 = dec.get_u8()?;
        if [marker0, marker1] != Self::VERSION_MARKER {
            return Err("decode stream filter set: missing marker".to_string());
        }

        let clause_count = dec.get_u32()? as usize;
        let mut clauses = Vec::with_capacity(clause_count);
        for _ in 0..clause_count {
            clauses.push(StreamFilterClause::try_decode(&mut dec)?);
        }

        if !dec.is_complete() {
            return Err("decode stream filter set: trailing bytes".to_string());
        }

        Ok(Self { clauses })
    }

    #[must_use]
    pub fn matches(&self, discriminator: Option<&str>) -> bool {
        let discriminator = discriminator.unwrap_or("");
        self.clauses
            .iter()
            .all(|clause| clause.matches(discriminator))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty()
    }
}

/// Optional metadata attached to an ingest batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestMetadata {
    pub opaque: Bytes,
}

/// Reason a committed offset was emitted as a synthetic delivery marker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamFilteredReason {
    ServerFilter,
    Permission,
    Projection,
}

/// A delivery item returned from a stream read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StreamReadItem {
    Event(StreamRecord),
    Filtered {
        offset: u64,
        reason: Option<StreamFilteredReason>,
    },
    FilteredRange {
        from_offset: u64,
        to_offset: u64,
        reason: Option<StreamFilteredReason>,
    },
}

// ═══════════════════════════════════════════════════════════════════════════
// EXTERNAL API (Client-facing messages)
// ═══════════════════════════════════════════════════════════════════════════

/// Messages for stream operations
#[derive(Debug, Clone)]
pub enum StreamMessage {
    /// Begin streaming append session
    /// Client provides: resource path, optional metadata
    /// NO client-supplied area/realm offsets
    Begin {
        family_id: RouteFamily,
        route: Route,
        ingest_metadata: Option<IngestMetadata>,
    },

    /// Append event to active session
    /// Client provides: `session_id`, `expected_offset`, body, optional metadata,
    /// and an optional immutable discriminator sidecar.
    Append {
        session_id: u64,
        expected_offset: u64,
        body: Bytes,
        metadata: Option<Bytes>,
        discriminator: Option<StreamDiscriminator>,
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
        filter: Option<StreamFilterSet>,
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
}

/// Subscription management messages handled by the sink before actor dispatch.
///
/// These are never forwarded to `StreamActor`. The sink owns subscription state
/// and handles these directly.
#[derive(Debug, Clone)]
pub enum StreamSubscriptionMessage {
    /// Subscribe to stream change notifications (client -> server)
    Subscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Unsubscribe from a specific stream pattern (client -> server)
    Unsubscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
}

/// Parsed client request delivered to the Stream domain sink.
#[derive(Debug, Clone)]
pub struct StreamClientRequest {
    pub meta: ClientFrameMeta,
    pub frame: Result<StreamClientFrame, String>,
}

impl StreamClientRequest {
    pub fn new(meta: ClientFrameMeta, frame: Result<StreamClientFrame, String>) -> Self {
        Self { meta, frame }
    }
}

/// Stream request classified after wire parsing.
#[derive(Debug, Clone)]
pub enum StreamClientFrame {
    Op(StreamMessage),
    Sub(StreamSubscriptionMessage),
}

/// Client-facing stream response body before transport encoding.
#[derive(Debug, Clone)]
pub enum StreamClientResponseBody {
    Ok {
        session_id: Option<u64>,
        data: Vec<u8>,
    },
    Error(String),
}

/// Stream response routed from the domain sink back to a client session.
#[derive(Debug, Clone)]
pub struct StreamClientResponse {
    pub meta: ClientFrameMeta,
    pub response: StreamClientResponseBody,
}

impl StreamClientResponse {
    #[must_use]
    pub fn new(meta: ClientFrameMeta, response: StreamClientResponseBody) -> Self {
        Self { meta, response }
    }
}

/// Stream notification routed from the domain sink back to a subscribed session.
#[derive(Debug, Clone)]
pub struct StreamClientNotification {
    pub session_id: u64,
    pub route_family: RouteFamily,
    pub subscription_id: u64,
    pub route: Route,
    pub payload: Bytes,
}

impl StreamClientNotification {
    pub fn new(
        session_id: u64,
        route_family: RouteFamily,
        subscription_id: u64,
        route: Route,
        payload: Bytes,
    ) -> Self {
        Self {
            session_id,
            route_family,
            subscription_id,
            route,
            payload,
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// INTERNAL MESSAGES (Actor-to-actor communication)
// ═══════════════════════════════════════════════════════════════════════════

/// Messages exchanged between `AreaActor`, `RealmActor`, and `StreamActor` for
/// offset lease coordination and watermark propagation.
///
/// These are internal to the stream module and must never appear on the
/// client-facing `StreamMessage` enum or be routed through the public sink.
#[derive(Debug, Clone)]
pub enum StreamCoordinationMessage {
    /// Request paired area+realm offsets from `AreaActor` (`StreamActor` -> `AreaActor`)
    RequestLease {
        realm: String,
        area: String,
        count: u64,
        reply_to: String,
    },
    /// Lease granted from `AreaActor` to `StreamActor`
    LeaseGranted { grant: LeaseGranted },
    /// Request realm offsets from `RealmActor` (`AreaActor` -> `RealmActor`)
    RequestRealmLease { count: u64 },
    /// Batch committed notification from `StreamActor` to `AreaActor`
    BatchCommitted(BatchCommitted),
    /// Area watermark advanced from `AreaActor` to `RealmActor`
    AreaWatermarkAdvanced(AreaWatermarkAdvanced),
}

/// Request lease from `AreaActor`
#[derive(Debug, Clone)]
pub struct RequestLease {
    pub realm: String,
    pub area: String,
    pub count: u64,
    pub reply_to: String, // StreamActor ID
}

/// Lease granted by `AreaActor` (paired area+realm ranges)
///
/// **CRITICAL: All ranges are END-EXCLUSIVE**
/// Valid range: [start, `end_exclusive`)
#[derive(Debug, Clone, Copy)]
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
    /// `CloudStrict`: internal broker mode for cloud provider-ack commits
    CloudStrict,
}

/// Batch committed notification from `StreamActor` to `AreaActor`
#[derive(Debug, Clone)]
pub struct BatchCommitted {
    pub first_area_offset: u64,
    pub last_area_offset: u64,
    pub first_realm_offset: u64,
    pub last_realm_offset: u64,
}

/// Area watermark advanced notification from `AreaActor` to `RealmActor`
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
    pub items: Vec<StreamReadItem>,
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

    /// First currently readable resource offset (None if stream empty)
    pub first_resource_offset: Option<u64>,

    /// Last committed resource offset (None if stream empty)
    pub last_resource_offset: Option<u64>,

    /// Number of currently readable resource records
    pub resource_count: u64,

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
    /// Response to Rollback operation
    RollbackOk,
    /// Response to Read operation
    ReadOk(ReadResponse),
    /// Response to Last operation
    LastOk(PeekResponse),
    /// Response to `GetMetadata` operation
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

    /// Optimistic concurrency conflict - `expected_offset` mismatch (2001)
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
}

impl StreamError {
    #[must_use]
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
        }
    }
}

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

    #[test]
    fn should_match_discriminator_when_all_clauses_match() {
        // Arrange
        let filter = StreamFilterSet {
            clauses: vec![
                StreamFilterClause::StartsWith("proj.".to_string()),
                StreamFilterClause::NotEquals("proj.skip".to_string()),
                StreamFilterClause::AnyOf(vec!["proj.alpha".to_string(), "proj.beta".to_string()]),
            ],
        };

        // Act
        let result = filter.matches(Some("proj.alpha"));

        // Assert
        assert!(result);
    }

    #[test]
    fn should_reject_discriminator_when_any_clause_fails() {
        // Arrange
        let filter = StreamFilterSet {
            clauses: vec![StreamFilterClause::StartsWith("proj.".to_string())],
        };

        // Act
        let result = filter.matches(Some("audit.alpha"));

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_roundtrip_stream_filter_set_codec() {
        // Arrange
        let filter = StreamFilterSet {
            clauses: vec![
                StreamFilterClause::Equals("alpha".to_string()),
                StreamFilterClause::NotEquals("beta".to_string()),
                StreamFilterClause::StartsWith("proj.".to_string()),
                StreamFilterClause::AnyOf(vec!["one".to_string(), "two".to_string()]),
            ],
        };

        // Act
        let encoded = filter.encode();
        let decoded = StreamFilterSet::try_decode(&encoded).expect("decode stream filter");

        // Assert
        assert_eq!(decoded, filter);
    }

    #[test]
    fn should_reject_stream_filter_set_with_wrong_marker() {
        // Arrange
        let encoded = vec![0, 0, 0, 0];

        // Act
        let result = StreamFilterSet::try_decode(&encoded);

        // Assert
        assert!(result.is_err());
    }
}
