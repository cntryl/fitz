//! Idempotency classification and deduplication support

use std::fmt;
use std::sync::Arc;

/// Idempotency classification of an operation
///
/// Defined per CLIENT.md lines 892–950.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Idempotency {
    /// Safe to retry unconditionally (read-only or state-neutral).
    /// Examples: GET, SCAN, READ, LAST, QUERY.
    Idempotent,
    /// Unsafe to retry (modifies state, has side effects).
    /// Examples: PUT, INSERT, DELETE, APPEND, BEGIN, COMMIT, PUBLISH, ENQUEUE.
    NonIdempotent,
    /// Safe to retry only with server-side deduplication.
    /// Examples: COMPLETE, REQUEST.
    ContextDependent { dedup_key: &'static str },
}

impl Idempotency {
    /// Returns true if the operation is safe to retry (either idempotent or context-dependent)
    #[must_use]
    pub fn is_safe_to_retry(&self) -> bool {
        matches!(self, Self::Idempotent | Self::ContextDependent { .. })
    }

    /// Returns the deduplication key description if context-dependent
    #[must_use]
    pub fn dedup_key(&self) -> Option<&'static str> {
        if let Self::ContextDependent { dedup_key } = self {
            Some(dedup_key)
        } else {
            None
        }
    }
}

impl fmt::Display for Idempotency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Idempotent => write!(f, "Idempotent"),
            Self::NonIdempotent => write!(f, "Non-Idempotent"),
            Self::ContextDependent { dedup_key } => write!(f, "Context-Dependent ({dedup_key})"),
        }
    }
}

/// Fitz domains for classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Domain {
    Kv,
    Stream,
    Notice,
    Queue,
    Lease,
    Rpc,
    Schedule,
}

use dashmap::DashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Unique identifier for a context-dependent operation that requires deduplication
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DedupIdentifier {
    /// Queue COMPLETE: (family, area, resource, owner_session_id, message_id, token)
    QueueComplete {
        family: u64,
        area: String,
        resource: String,
        owner_session_id: u64,
        message_id: u64,
        token: u64,
    },
    /// RPC REQUEST: correlation_id (Uuid)
    RpcRequest(Uuid),
}

/// Composite key for deduplication ensuring realm and domain isolation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DedupKey {
    pub realm: String,
    pub domain: Domain,
    pub identifier: DedupIdentifier,
}

/// Cached result of a previously processed context-dependent operation
#[derive(Debug, Clone)]
pub struct DedupRecord {
    pub response_payload: Vec<u8>,
    pub expires_at: Instant,
}

/// Store for tracking and deduplicating context-dependent operations
///
/// Implements expiration with realm-isolated tracking for context-dependent operations.
pub struct DedupStore {
    records: DashMap<DedupKey, DedupRecord>,
    default_ttl: Duration,
}

impl DedupStore {
    /// Create a new deduplication store with the specified default TTL
    #[must_use]
    pub fn new(default_ttl: Duration) -> Self {
        Self {
            records: DashMap::new(),
            default_ttl,
        }
    }

    /// Check if an operation has already been processed and return its cached result
    #[must_use]
    pub fn get(&self, key: &DedupKey) -> Option<Vec<u8>> {
        if let Some(record) = self.records.get(key) {
            if record.expires_at > Instant::now() {
                return Some(record.response_payload.clone());
            }
            // Expired - remove it (lazy cleanup)
            drop(record);
            self.records.remove(key);
        }
        None
    }

    /// Record the result of a context-dependent operation
    pub fn record(&self, key: DedupKey, response_payload: Vec<u8>) {
        let record = DedupRecord {
            response_payload,
            expires_at: Instant::now() + self.default_ttl,
        };
        self.records.insert(key, record);
    }

    /// Perform maintenance cleanup of all expired records
    pub fn cleanup(&self) {
        let now = Instant::now();
        self.records.retain(|_, record| record.expires_at > now);
    }

    /// Get current count of active deduplication records
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Check if deduplication store is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// Create a fresh deduplication store using the default queue/RPC retry TTL.
#[must_use]
pub fn default_dedup_store() -> Arc<DedupStore> {
    Arc::new(DedupStore::new(Duration::from_secs(300)))
}

/// Classify an operation by domain and message type ID
#[must_use]
pub fn classify(domain: Domain, msg_type: u16) -> Idempotency {
    match domain {
        Domain::Kv => classify_kv(msg_type),
        Domain::Stream => classify_stream(msg_type),
        Domain::Notice => classify_notice(msg_type),
        Domain::Queue => classify_queue(msg_type),
        Domain::Lease => classify_lease(msg_type),
        Domain::Rpc => classify_rpc(msg_type),
        Domain::Schedule => classify_schedule(msg_type),
    }
}

fn classify_kv(msg_type: u16) -> Idempotency {
    match msg_type {
        // GET, SCAN are idempotent
        103 | 108 => Idempotency::Idempotent,
        // BEGIN, COMMIT, ROLLBACK, PUT, INSERT, DELETE are non-idempotent
        100 | 101 | 102 | 104 | 105 | 106 | 107 => Idempotency::NonIdempotent,
        _ => Idempotency::NonIdempotent, // Default to safe
    }
}

fn classify_stream(msg_type: u16) -> Idempotency {
    match msg_type {
        // READ, LAST, GET_METADATA are idempotent
        604..=606 => Idempotency::Idempotent,
        // BEGIN, APPEND, COMMIT, ROLLBACK are non-idempotent
        600..=603 => Idempotency::NonIdempotent,
        _ => Idempotency::NonIdempotent,
    }
}

fn classify_notice(msg_type: u16) -> Idempotency {
    match msg_type {
        // PUBLISH, SUBSCRIBE, UNSUBSCRIBE, UNSUBSCRIBE_ALL, NOTIFY are all non-idempotent
        500..=504 => Idempotency::NonIdempotent,
        _ => Idempotency::NonIdempotent,
    }
}

fn classify_queue(msg_type: u16) -> Idempotency {
    match msg_type {
        // COMPLETE is context-dependent
        204 => Idempotency::ContextDependent {
            dedup_key: "message_id+token",
        },
        // ENQUEUE, RESERVE, EXTEND are non-idempotent
        200..=203 => Idempotency::NonIdempotent,
        _ => Idempotency::NonIdempotent,
    }
}

fn classify_lease(msg_type: u16) -> Idempotency {
    match msg_type {
        // QUERY is idempotent
        403 => Idempotency::Idempotent,
        // ACQUIRE, RENEW, RELEASE are non-idempotent
        400..=402 => Idempotency::NonIdempotent,
        _ => Idempotency::NonIdempotent,
    }
}

fn classify_rpc(msg_type: u16) -> Idempotency {
    match msg_type {
        // RPC operations are process-local and must not imply durable replay or deduplication.
        300..=304 => Idempotency::NonIdempotent,
        _ => Idempotency::NonIdempotent,
    }
}

fn classify_schedule(msg_type: u16) -> Idempotency {
    match msg_type {
        // LIST is idempotent
        702 => Idempotency::Idempotent,
        // CREATE, CANCEL are non-idempotent
        700 | 701 => Idempotency::NonIdempotent,
        _ => Idempotency::NonIdempotent,
    }
}
