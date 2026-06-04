//! QueueActor: manages a single durable message queue
//!
//! Each queue has:
//! - Identity: (realm, area, resource) from route
//! - Durable storage: Message headers and bodies persisted separately in Midge
//! - Ephemeral inflight tracking: In-memory visibility tracking
//!
//! # Invariants
//!
//! 1. **Crash-safe ID reservation**: persisted ID reservations prevent collisions across restarts
//! 2. **At-least-once delivery**: Messages may be delivered multiple times
//! 3. **Inflight isolation**: Reserved messages invisible to other consumers
//! 4. **Automatic redelivery**: Expired inflight entries or crashes return messages to ready queue
//! 5. **Full recovery**: All persisted state restored on restart (V-003 Fix)
//! 6. **Correct time semantics**: Delays use absolute SystemTime epochs (V-002 Fix)
//! 7. **Fair distribution**: Competing consumers get best-effort ready-queue order
//!
//! # Intent vs Events
//!
//! Queues represent **intent** (work to be done), not events of record.
//! Minimal data loss is acceptable (producers can regenerate work items).
//! Messages commit together with any required ID-reservation extension.
//! Crashes may create ID gaps, but never ID reuse or collisions.
//!
//! # State Model
//!
//! ```text
//! ENQUEUE Ã¢â€ â€™ [READY QUEUE]
//!             Ã¢â€ â€œ reserve
//!           [INFLIGHT] Ã¢â€â‚¬Ã¢â€â‚¬completeÃ¢â€ â€™ DELETED
//!             Ã¢â€ â€œ expire
//!           [READY QUEUE] (redelivery with attempts++)
//! ```
//!
//! # Performance Model
//!
//! - ready shards: VecDeque of compressed ranges - O(1) push_back, O(1) pop_front
//! - inflight: HashMap - O(1) lookup, O(1) insert, O(1) remove
//! - timers: BinaryHeap - O(log n) push, O(1) peek, O(log n) pop
//!
//! # Storage Model
//!
//! Midge keys:
//! - `queue:{realm}:{area}:{resource}:msg:{id}` Ã¢â€ â€™ QueueRecord (body, attempts)
//! - `queue:{realm}:{area}:{resource}:meta` Ã¢â€ â€™ [next_id:8]

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fxhash::FxBuildHasher;

use crate::api::admin::QueueAgeBuckets;
use crate::observability as obs;
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::clock::{Clock, SystemClock};
use crate::runtime::routing::RouteFamily;
use crate::utils::storage_key::{self, DomainKeyspace};

use super::{
    MessageId, QueueAdminSnapshot, QueueDeadLetterSnapshot, QueueInflightSnapshot, QueueKey,
    QueueMessage, QueueResponse, ReservedMessage,
};

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_ACK_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_REDELIVERY_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[path = "actor_recovery.rs"]
pub(crate) mod recovery;
#[path = "actor_state.rs"]
pub(crate) mod state;
#[path = "actor_storage.rs"]
pub(crate) mod storage;
#[path = "actor_timers.rs"]
pub(crate) mod timers;

type FastMap<K, V> = HashMap<K, V, FxBuildHasher>;

/// Durable queue record (persisted to Midge)
///
/// All time values use SystemTime::UNIX_EPOCH (milliseconds).
/// This ensures delays survive process restarts correctly.
#[derive(Debug, Clone)]
struct QueueRecord {
    /// Message body, hydrated lazily for recovered records.
    body: Option<Bytes>,
    /// Durable queue state for this message.
    state: QueueState,
    /// Monotonic enqueue order for the original message create.
    enqueue_seq: u64,
    /// Current ready ordering sequence when the message is in the ready state.
    ready_seq: Option<u64>,
    /// Number of delivery attempts (starts at 0, increments on successful inflight assignment).
    attempts: u32,
    /// Visibility timestamp (milliseconds since UNIX epoch).
    ///
    /// For delayed rows this is the time the message becomes ready. For inflight
    /// rows this is the inflight expiry. For ready rows it is 0.
    visible_at_ms: u64,
    /// When the message was first committed to the queue.
    first_enqueued_at_ms: u64,
    /// Last successful inflight assignment timestamp.
    last_inflight_at_ms: Option<u64>,
    /// Monotonic inflight epoch; increments on every successful inflight assignment.
    inflight_epoch: u64,
    /// Current durable inflight token, if any.
    inflight_token: Option<u64>,
    /// Durable inflight expiry in UNIX epoch milliseconds, if inflight.
    inflight_expires_at_ms: Option<u64>,
    /// Durable DLQ timestamp.
    dead_lettered_at_ms: Option<u64>,
    /// Durable DLQ reason code.
    dlq_reason: Option<DlqReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueState {
    Ready = 0,
    Delayed = 1,
    Inflight = 2,
    Dlq = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DlqReason {
    MaxAttemptsExceeded = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoredRecordLayout {
    EmbeddedHeader,
    SplitHeaderBody,
    LegacyKey,
}

impl QueueRecord {
    #[inline]
    #[allow(dead_code)]
    fn ready(body: Bytes, enqueue_seq: u64, ready_seq: u64, now_epoch_ms: u64) -> Self {
        Self {
            body: Some(body),
            state: QueueState::Ready,
            enqueue_seq,
            ready_seq: Some(ready_seq),
            attempts: 0,
            visible_at_ms: 0,
            first_enqueued_at_ms: now_epoch_ms,
            last_inflight_at_ms: None,
            inflight_epoch: 0,
            inflight_token: None,
            inflight_expires_at_ms: None,
            dead_lettered_at_ms: None,
            dlq_reason: None,
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn delayed(body: Bytes, enqueue_seq: u64, visible_at_ms: u64, now_epoch_ms: u64) -> Self {
        Self {
            body: Some(body),
            state: QueueState::Delayed,
            enqueue_seq,
            ready_seq: None,
            attempts: 0,
            visible_at_ms,
            first_enqueued_at_ms: now_epoch_ms,
            last_inflight_at_ms: None,
            inflight_epoch: 0,
            inflight_token: None,
            inflight_expires_at_ms: None,
            dead_lettered_at_ms: None,
            dlq_reason: None,
        }
    }

    #[inline]
    fn loaded_legacy(body: Bytes, attempts: u32, visible_at_ms: u64) -> Self {
        Self {
            body: Some(body),
            state: QueueState::Ready,
            enqueue_seq: 0,
            ready_seq: None,
            attempts,
            visible_at_ms,
            first_enqueued_at_ms: 0,
            last_inflight_at_ms: None,
            inflight_epoch: 0,
            inflight_token: None,
            inflight_expires_at_ms: None,
            dead_lettered_at_ms: None,
            dlq_reason: None,
        }
    }

    #[inline]
    fn loaded(body: Bytes, attempts: u32, visible_at_ms: u64) -> Self {
        Self::loaded_legacy(body, attempts, visible_at_ms)
    }

    #[inline]
    fn metadata_only(attempts: u32, visible_at_ms: u64) -> Self {
        let mut record = Self::loaded_legacy(Bytes::new(), attempts, visible_at_ms);
        record.body = None;
        record
    }

    #[inline]
    fn metadata_only_from(&self) -> Self {
        Self {
            body: None,
            state: self.state,
            enqueue_seq: self.enqueue_seq,
            ready_seq: self.ready_seq,
            attempts: self.attempts,
            visible_at_ms: self.visible_at_ms,
            first_enqueued_at_ms: self.first_enqueued_at_ms,
            last_inflight_at_ms: self.last_inflight_at_ms,
            inflight_epoch: self.inflight_epoch,
            inflight_token: self.inflight_token,
            inflight_expires_at_ms: self.inflight_expires_at_ms,
            dead_lettered_at_ms: self.dead_lettered_at_ms,
            dlq_reason: self.dlq_reason,
        }
    }
}

/// In-flight message state (ephemeral, actor-owned)
#[derive(Debug, Clone)]
pub struct Inflight {
    /// Random token for operation validation
    token: u64,
    /// Absolute expiration time
    expires_at: Instant,
    /// Durable inflight expiry in UNIX epoch milliseconds.
    expires_at_epoch_ms: u64,
    /// Owning live session, if the inflight entry was created through the broker session layer.
    owner_session_id: Option<u64>,
    /// Delivery attempt count presented with the current inflight assignment.
    attempts: u32,
    /// Durable inflight epoch for stale-event suppression.
    inflight_epoch: u64,
}

/// Timer event for inflight expiration
#[derive(Debug, Clone, PartialEq, Eq)]
struct InflightExpiry {
    /// Message ID to re-enqueue
    id: MessageId,
    /// Inflight epoch to detect stale expiry events after extend/reassign.
    inflight_epoch: u64,
    /// Expiration time (for ordering in heap)
    expires_at: Instant,
    /// Expiration time in UNIX epoch milliseconds.
    expires_at_ms: u64,
}

impl Ord for InflightExpiry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Earlier expiration = higher priority (min-heap via Reverse wrapper)
        other.expires_at.cmp(&self.expires_at)
    }
}

impl PartialOrd for InflightExpiry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Delayed message visibility event
#[derive(Debug, Clone, PartialEq, Eq)]
struct DelayedMessage {
    /// Message ID to make visible
    id: MessageId,
    /// Original enqueue sequence for deterministic promotion ordering.
    enqueue_seq: u64,
    /// Visibility time (for ordering in heap)
    visible_at: Instant,
    /// Visibility time in UNIX epoch milliseconds.
    visible_at_ms: u64,
}

impl Ord for DelayedMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Earlier visibility = higher priority (min-heap via Reverse wrapper)
        other
            .visible_at
            .cmp(&self.visible_at)
            .then_with(|| other.enqueue_seq.cmp(&self.enqueue_seq))
            .then_with(|| other.id.as_u64().cmp(&self.id.as_u64()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadyEntry {
    ready_seq: u64,
    id: MessageId,
    ready_enqueued_at_ms: u64,
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct QueueMetaSnapshot {
    next_id: u64,
    next_ready_seq: u64,
    ready_count: u64,
    delayed_count: u64,
    inflight_count: u64,
    dlq_count: u64,
    oldest_ready_enqueued_at_ms: Option<u64>,
}

impl PartialOrd for DelayedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Compressed ready-queue segment for one shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReadyRange {
    next: u64,
    end: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistedReadyMutation {
    Delete {
        removed: ReadyRange,
    },
    Replace {
        removed: ReadyRange,
        inserted: ReadyRange,
    },
    Split {
        removed: ReadyRange,
        left: ReadyRange,
        right: ReadyRange,
    },
}

#[derive(Debug, Clone, Copy)]
struct PersistedIndexMutationPlan {
    ready_mutation: Option<(usize, PersistedReadyMutation)>,
    delayed_index_delete: Option<u64>,
    staged_ready_count: usize,
    staged_delayed_count: usize,
    staged_next_delayed_visibility: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryPath {
    Empty,
    IndexHit,
    IndexMissingFallback,
    IndexInvalidFallback,
    IndexErrorFallback,
}

enum IndexRecoveryAttempt {
    Hit { next_id: u64, max_id: Option<u64> },
    Missing { next_id: u64 },
    Invalid { next_id: u64, reason: String },
    Error { next_id: u64, reason: String },
}

#[derive(Debug, Clone, Copy)]
struct IndexMetaSnapshot {
    next_id: u64,
    ready_count: u64,
    delayed_count: u64,
    next_delayed_visibility_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
enum DecodedIndexMeta {
    LegacyV1,
    V2(IndexMetaSnapshot),
}

/// Queue actor managing a single message queue
///
/// # State
///
/// - `family`: RouteFamily this actor serves (for validation)
/// - `queue_key`: Queue identity (realm/area/resource)
/// - `store`: Midge storage handle (for persistence)
/// - `next_id`: Message ID counter (monotonic)
/// - `ready`: Sharded FIFO queues of ready message IDs
/// - `inflight`: Map of inflight messages (id -> Inflight)
/// - `timers`: Min-heap of expiration events (earliest first)
/// - `clock`: Time source for expiration checks
///
/// # Actor Responsibilities
///
/// - Maintain best-effort ordering via sharded ready queues
/// - Track inflight entries with expiration
/// - Re-enqueue expired messages
/// - Increment attempts on redelivery
/// - Persist deletes on successful completion
/// - Never persist inflight state
pub struct QueueActor {
    /// Route family this actor serves (for validation)
    #[allow(dead_code)]
    family: RouteFamily,

    /// Queue identity
    queue_key: QueueKey,

    /// Cached Midge metadata key for this queue.
    meta_key: Vec<u8>,

    /// Legacy recovery-index metadata key.
    #[allow(dead_code)]
    index_meta_key: Vec<u8>,

    /// Cached Midge header-key prefix for this queue.
    header_key_prefix: Vec<u8>,

    /// Cached Midge body-key prefix for this queue.
    body_key_prefix: Vec<u8>,

    /// Cached Midge legacy record-key prefix for this queue.
    legacy_message_key_prefix: Vec<u8>,

    /// Cached Midge ready-index prefix for this queue.
    ready_index_prefix: Vec<u8>,

    /// Cached Midge delayed-index prefix for this queue.
    delayed_index_prefix: Vec<u8>,

    /// Cached Midge inflight-index prefix for this queue.
    #[allow(dead_code)]
    inflight_index_prefix: Vec<u8>,

    /// Cached Midge dead-letter index prefix for this queue.
    #[allow(dead_code)]
    dlq_index_prefix: Vec<u8>,

    /// Cached Midge durable COMPLETE deduplication prefix for this queue.
    #[allow(dead_code)]
    ack_dedup_prefix: Vec<u8>,

    /// Midge storage handle (for durable persistence)
    store: Arc<cntryl_midge::MidgeEngine>,

    /// Commit policy for queue mutations.
    /// Durable stores use buffered commits; explicitly ephemeral stores can use
    /// best-effort commits to avoid WAL work that cannot survive process exit.
    commit_write_options: cntryl_midge::WriteOptions,

    /// Next message ID to allocate (monotonic counter)
    next_id: u64,

    /// Next durable ready ordering sequence.
    next_ready_seq: u64,

    /// Legacy reserved ID upper bound retained while the actor migrates away
    /// from the old range-index path.
    #[allow(dead_code)]
    next_id_limit: u64,

    /// Ready queue ordered by durable ready sequence.
    ready: VecDeque<ReadyEntry>,

    /// Legacy sharded ready queue retained for compile-time compatibility while
    /// the durable ready-sequence path replaces it.
    #[allow(dead_code)]
    ready_shards: Vec<VecDeque<ReadyRange>>,

    /// Legacy persisted ready range index retained for compatibility.
    #[allow(dead_code)]
    persisted_ready_shards: Vec<VecDeque<ReadyRange>>,

    /// Legacy persisted ready count retained for compatibility.
    #[allow(dead_code)]
    persisted_ready_count: usize,

    /// Cached total number of ready messages across all shards.
    ready_count: usize,

    /// Durable dead-letter count.
    #[allow(dead_code)]
    dlq_count: usize,

    /// Oldest ready-message enqueue timestamp.
    oldest_ready_enqueued_at_ms: Option<u64>,

    /// Legacy round-robin shard cursor retained for compatibility.
    #[allow(dead_code)]
    next_ready_shard: usize,

    /// Bounded in-memory message metadata cache (durable backing is Midge).
    records: FastMap<MessageId, QueueRecord>,

    /// Storage layout cache aligned with `records`.
    record_layouts: FastMap<MessageId, StoredRecordLayout>,

    /// FIFO eviction order for the bounded metadata cache.
    record_cache_fifo: VecDeque<MessageId>,

    /// Small hot-body cache to keep recent enqueue/receive paths fast without
    /// pinning the full queue payload set in memory.
    body_cache: FastMap<MessageId, Bytes>,

    /// FIFO eviction order for the bounded hot-body cache.
    body_cache_fifo: VecDeque<MessageId>,

    /// Approximate total bytes pinned by the hot-body cache.
    body_cache_bytes: usize,

    /// Inflight map: inflight messages (id -> Inflight)
    pub inflight: FastMap<MessageId, Inflight>,

    /// Timer heap: inflight expiration events (earliest first, min-heap)
    timers: BinaryHeap<Reverse<InflightExpiry>>,

    /// Delayed visibility heap: messages not yet visible (earliest first, min-heap)
    delayed: BinaryHeap<Reverse<DelayedMessage>>,

    /// Authoritative delayed index entries keyed by message ID.
    persisted_delayed: FastMap<MessageId, u64>,

    /// Authoritative dead-letter index entries keyed by message ID.
    persisted_dlq: FastMap<MessageId, u64>,

    /// Cached minimum delayed visibility across `persisted_delayed`.
    persisted_next_delayed_visibility_ms: Option<u64>,

    /// Legacy persisted-index marker retained for compatibility.
    #[allow(dead_code)]
    index_meta_written: bool,

    /// Legacy recovery mode retained for compatibility with existing tests.
    #[allow(dead_code)]
    recovery_path: RecoveryPath,

    /// Flag indicating that queue-local waiters should be re-checked.
    needs_wake_waiters: bool,

    /// Clock for time-based operations
    clock: Box<dyn Clock>,

    /// Maximum delivery attempts before DLQ (None = unlimited retries)
    max_attempts: Option<u32>,

    /// Deduplication store for context-dependent operations (e.g., COMPLETE)
    dedup_store: Arc<crate::utils::idempotency::DedupStore>,

    /// Cached next expiration deadline (deferred timer processing)
    /// Only process timers if current time >= this deadline
    next_expiration_deadline: Instant,

    /// Cached next delayed message deadline (deferred delayed processing)
    /// Only process delayed messages if current time >= this deadline
    next_delayed_deadline: Instant,
}

impl QueueActor {
    const READY_SHARDS: usize = 8;
    const ID_RESERVATION_BLOCK: u64 = 256;
    #[allow(dead_code)]
    const META_VERSION_V2: u8 = 2;
    const HEADER_VERSION_V2: u8 = 2;
    #[allow(dead_code)]
    const ACK_DEDUP_TTL_MS: u64 = 5 * 60 * 1_000;
    const INDEX_VERSION_V1: u8 = 1;
    const INDEX_VERSION_V2: u8 = 2;
    const INDEX_META_VALID_MARKER: u8 = 1;
    const INDEX_META_NEXT_DELAY_NONE: u64 = u64::MAX;
    const PADDED_U64_WIDTH: usize = 20;
    const RECORD_CACHE_LIMIT: usize = 16 * 1024;
    const RECORD_CACHE_FIFO_SLACK_MULTIPLIER: usize = 2;
    const BODY_CACHE_LIMIT: usize = 1024;
    const BODY_CACHE_LIMIT_BYTES: usize = 16 * 1024 * 1024;
    const BODY_CACHE_FIFO_SLACK_MULTIPLIER: usize = 2;

    /// Create a new queue actor using buffered commits for durable stores.
    pub fn new(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        Self::new_with_write_options(
            family,
            queue_key,
            store,
            max_attempts,
            dedup_store,
            cntryl_midge::WriteOptions::buffered(),
        )
    }

    /// Create a new queue actor with explicit commit policy selection.
    pub fn new_with_write_options(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        commit_write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        Self::try_new_with_write_options(
            family,
            queue_key,
            store,
            max_attempts,
            dedup_store,
            commit_write_options,
        )
        .expect("recover queue actor from store")
    }

    pub fn try_new_with_write_options(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        commit_write_options: cntryl_midge::WriteOptions,
    ) -> Result<Self, String> {
        Self::try_with_clock_and_write_options(
            family,
            queue_key,
            store,
            Box::new(SystemClock),
            max_attempts,
            dedup_store,
            commit_write_options,
        )
    }

    /// Create a new queue actor with a custom clock (for testing) using buffered commits.
    pub fn with_clock(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        clock: Box<dyn Clock>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        Self::with_clock_and_write_options(
            family,
            queue_key,
            store,
            clock,
            max_attempts,
            dedup_store,
            cntryl_midge::WriteOptions::buffered(),
        )
    }

    /// Create a new queue actor with a custom clock and explicit commit policy.
    pub fn with_clock_and_write_options(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        clock: Box<dyn Clock>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        commit_write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        Self::try_with_clock_and_write_options(
            family,
            queue_key,
            store,
            clock,
            max_attempts,
            dedup_store,
            commit_write_options,
        )
        .expect("recover queue actor from store")
    }

    pub fn try_with_clock_and_write_options(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        clock: Box<dyn Clock>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        commit_write_options: cntryl_midge::WriteOptions,
    ) -> Result<Self, String> {
        let now = Instant::now();

        let mut actor = Self {
            family,
            meta_key: Self::meta_key(&queue_key),
            index_meta_key: Self::index_meta_key(&queue_key),
            header_key_prefix: Self::header_key_prefix(&queue_key),
            body_key_prefix: Self::body_key_prefix(&queue_key),
            legacy_message_key_prefix: Self::legacy_message_key_prefix(&queue_key),
            ready_index_prefix: Self::ready_index_prefix(&queue_key),
            delayed_index_prefix: Self::delayed_index_prefix(&queue_key),
            inflight_index_prefix: Self::inflight_index_prefix(&queue_key),
            dlq_index_prefix: Self::dlq_index_prefix(&queue_key),
            ack_dedup_prefix: Self::ack_dedup_prefix(&queue_key),
            queue_key,
            store,
            commit_write_options,
            next_id: 1,
            next_ready_seq: 1,
            next_id_limit: 1,
            ready: VecDeque::new(),
            ready_shards: (0..Self::READY_SHARDS).map(|_| VecDeque::new()).collect(),
            persisted_ready_shards: (0..Self::READY_SHARDS).map(|_| VecDeque::new()).collect(),
            persisted_ready_count: 0,
            ready_count: 0,
            dlq_count: 0,
            oldest_ready_enqueued_at_ms: None,
            next_ready_shard: 0,
            records: HashMap::with_capacity_and_hasher(128, FxBuildHasher::default()),
            record_layouts: HashMap::with_capacity_and_hasher(128, FxBuildHasher::default()),
            record_cache_fifo: VecDeque::with_capacity(128),
            body_cache: HashMap::with_capacity_and_hasher(128, FxBuildHasher::default()),
            body_cache_fifo: VecDeque::with_capacity(Self::BODY_CACHE_LIMIT),
            body_cache_bytes: 0,
            inflight: HashMap::with_capacity_and_hasher(64, FxBuildHasher::default()),
            timers: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
            persisted_delayed: HashMap::with_capacity_and_hasher(64, FxBuildHasher::default()),
            persisted_dlq: HashMap::with_capacity_and_hasher(32, FxBuildHasher::default()),
            persisted_next_delayed_visibility_ms: None,
            index_meta_written: false,
            recovery_path: RecoveryPath::Empty,
            needs_wake_waiters: false,
            clock,
            max_attempts,
            dedup_store,
            // Initialize deadlines to now (will process on first receive if queues are not empty)
            next_expiration_deadline: now,
            next_delayed_deadline: now,
        };

        actor.recover_from_store()?;
        Ok(actor)
    }

    pub fn validate_persisted_state_for_existing_families(
        store: &cntryl_midge::Engine,
    ) -> Result<(), String> {
        let families = store
            .list_column_families()
            .map_err(|error| format!("list queue column families failed: {error}"))?;

        for family in families {
            if family.id() == 0 {
                continue;
            }
            Self::validate_persisted_state_for_family(store, family.id())?;
        }

        Ok(())
    }

    fn validate_persisted_state_for_family(
        store: &cntryl_midge::Engine,
        family: u32,
    ) -> Result<(), String> {
        let txn = store
            .begin_tx(family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| {
                format!(
                    "queue validation failed: family={} key_category=transaction error={:?}",
                    family, error
                )
            })?;
        let mut iter = txn.scan(&cntryl_midge::Query::new()).map_err(|error| {
            format!(
                "queue validation failed: family={} key_category=scan error={:?}",
                family, error
            )
        })?;
        let mut required_bodies = HashSet::<Vec<u8>>::new();
        let mut body_rows = HashSet::<Vec<u8>>::new();

        for (key, value) in iter.collect_all() {
            let Some(suffix) = storage_key::strip_domain_prefix(&key, DomainKeyspace::Queue) else {
                continue;
            };

            if suffix.ends_with(b":meta") && !suffix.ends_with(b":idx:meta") {
                let Some(meta) = Self::decode_meta(&value) else {
                    return Err(format!(
                        "queue validation failed: family={} key_category=meta error=invalid encoding",
                        family
                    ));
                };
                if meta.next_id == 0 {
                    return Err(format!(
                        "queue validation failed: family={} key_category=meta error=next_id is zero",
                        family
                    ));
                }
                continue;
            }

            if let Some((queue_prefix, id_bytes)) = Self::split_authoritative_key(suffix, b":hdr:")
            {
                Self::validate_authoritative_message_id(family, "header", id_bytes)?;
                let is_split = Self::is_versioned_header(&value) || value.len() == 12;
                if Self::decode_record_header(&value).is_err()
                    && Self::decode_legacy_record(value.clone()).is_err()
                {
                    return Err(format!(
                        "queue validation failed: family={} key_category=header error=invalid encoding",
                        family
                    ));
                }
                if is_split {
                    required_bodies.insert(Self::body_suffix(queue_prefix, id_bytes));
                }
                continue;
            }

            if let Some((_queue_prefix, id_bytes)) = Self::split_authoritative_key(suffix, b":msg:")
            {
                Self::validate_authoritative_message_id(family, "legacy_message", id_bytes)?;
                Self::decode_legacy_record(value).map_err(|error| {
                    format!(
                        "queue validation failed: family={} key_category=legacy_message error={}",
                        family, error
                    )
                })?;
                continue;
            }

            if let Some((queue_prefix, id_bytes)) = Self::split_authoritative_key(suffix, b":body:")
            {
                Self::validate_authoritative_message_id(family, "body", id_bytes)?;
                body_rows.insert(Self::body_suffix(queue_prefix, id_bytes));
            }
        }

        for required_body in required_bodies {
            if !body_rows.remove(&required_body) {
                return Err(format!(
                    "queue validation failed: family={} key_category=body error=missing body for split header",
                    family
                ));
            }
        }
        if !body_rows.is_empty() {
            return Err(format!(
                "queue validation failed: family={} key_category=body error=orphan body row",
                family
            ));
        }

        Ok(())
    }

    fn split_authoritative_key<'a>(
        suffix: &'a [u8],
        marker: &[u8],
    ) -> Option<(&'a [u8], &'a [u8])> {
        let marker_start = suffix
            .windows(marker.len())
            .rposition(|window| window == marker)?;
        Some((
            &suffix[..marker_start],
            &suffix[marker_start + marker.len()..],
        ))
    }

    fn validate_authoritative_message_id(
        family: u32,
        category: &str,
        bytes: &[u8],
    ) -> Result<(), String> {
        if bytes.is_empty() || bytes.iter().any(|byte| !byte.is_ascii_digit()) {
            return Err(format!(
                "queue validation failed: family={} key_category={} error=invalid message id",
                family, category
            ));
        }
        let id = std::str::from_utf8(bytes)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        if id == 0 {
            return Err(format!(
                "queue validation failed: family={} key_category={} error=invalid message id",
                family, category
            ));
        }
        Ok(())
    }

    fn body_suffix(queue_prefix: &[u8], id_bytes: &[u8]) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(queue_prefix.len() + 6 + id_bytes.len());
        suffix.extend_from_slice(queue_prefix);
        suffix.extend_from_slice(b":body:");
        suffix.extend_from_slice(id_bytes);
        suffix
    }

    #[inline]
    fn reserved_id_limit_for(&self, additional_ids: u64) -> Option<u64> {
        let required_limit = self.next_id.saturating_add(additional_ids);
        if required_limit <= self.next_id_limit {
            return None;
        }

        let deficit = required_limit.saturating_sub(self.next_id_limit);
        let blocks = deficit.div_ceil(Self::ID_RESERVATION_BLOCK);
        Some(
            self.next_id_limit
                .saturating_add(blocks.saturating_mul(Self::ID_RESERVATION_BLOCK)),
        )
    }

    /// Generate a random inflight token
    fn generate_token() -> u64 {
        static TOKEN_COUNTER: AtomicU64 = AtomicU64::new(1);
        static TOKEN_SEED: OnceLock<u64> = OnceLock::new();

        let seed = *TOKEN_SEED.get_or_init(|| {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_nanos() as u64;
            now ^ ((std::process::id() as u64) << 32) ^ 0x9E37_79B9_7F4A_7C15
        });

        let mixed = TOKEN_COUNTER
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(seed);

        Self::mix_u64(mixed)
    }

    #[inline]
    fn mix_u64(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn prefixed_queue_key(queue_key: &QueueKey, suffix: String) -> Vec<u8> {
        storage_key::prefixed_key(&queue_key.realm, DomainKeyspace::Queue, suffix.as_bytes())
    }

    /// Midge key for queue metadata
    fn meta_key(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(
            queue_key,
            format!("{}:{}:meta", queue_key.area, queue_key.resource),
        )
    }

    fn index_meta_key(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(
            queue_key,
            format!("{}:{}:idx:meta", queue_key.area, queue_key.resource),
        )
    }

    /// Midge key for legacy combined message record
    #[cfg(test)]
    fn legacy_message_key(queue_key: &QueueKey, id: MessageId) -> Vec<u8> {
        let mut key = Self::legacy_message_key_prefix(queue_key);
        key.extend_from_slice(id.as_u64().to_string().as_bytes());
        key
    }

    /// Midge key for persisted message header
    #[cfg(test)]
    fn header_key(queue_key: &QueueKey, id: MessageId) -> Vec<u8> {
        let mut key = Self::header_key_prefix(queue_key);
        key.extend_from_slice(id.as_u64().to_string().as_bytes());
        key
    }

    /// Midge key for persisted message body
    #[cfg(test)]
    fn body_key(queue_key: &QueueKey, id: MessageId) -> Vec<u8> {
        let mut key = Self::body_key_prefix(queue_key);
        key.extend_from_slice(id.as_u64().to_string().as_bytes());
        key
    }

    fn header_key_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(
            queue_key,
            format!("{}:{}:hdr:", queue_key.area, queue_key.resource),
        )
    }

    fn body_key_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(
            queue_key,
            format!("{}:{}:body:", queue_key.area, queue_key.resource),
        )
    }

    fn ready_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(
            queue_key,
            format!("{}:{}:idx:ready:", queue_key.area, queue_key.resource),
        )
    }

    fn delayed_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(
            queue_key,
            format!("{}:{}:idx:delay:", queue_key.area, queue_key.resource),
        )
    }

    fn inflight_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(
            queue_key,
            format!("{}:{}:idx:inflight:", queue_key.area, queue_key.resource),
        )
    }

    fn dlq_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(
            queue_key,
            format!("{}:{}:idx:dlq:", queue_key.area, queue_key.resource),
        )
    }

    fn ack_dedup_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(
            queue_key,
            format!("{}:{}:ack:", queue_key.area, queue_key.resource),
        )
    }

    fn legacy_message_key_prefix(queue_key: &QueueKey) -> Vec<u8> {
        Self::prefixed_queue_key(
            queue_key,
            format!("{}:{}:msg:", queue_key.area, queue_key.resource),
        )
    }

    fn cached_id_key(prefix: &[u8], id: MessageId) -> Vec<u8> {
        let mut digits = [0_u8; 20];
        let mut value = id.as_u64();
        let mut start = digits.len();

        loop {
            start -= 1;
            digits[start] = b'0' + (value % 10) as u8;
            value /= 10;
            if value == 0 {
                break;
            }
        }

        let mut key = Vec::with_capacity(prefix.len() + digits.len() - start);
        key.extend_from_slice(prefix);
        key.extend_from_slice(&digits[start..]);
        key
    }

    #[inline]
    fn cached_header_key(&self, id: MessageId) -> Vec<u8> {
        Self::cached_id_key(&self.header_key_prefix, id)
    }

    #[inline]
    fn cached_body_key(&self, id: MessageId) -> Vec<u8> {
        Self::cached_id_key(&self.body_key_prefix, id)
    }

    #[inline]
    fn cached_legacy_message_key(&self, id: MessageId) -> Vec<u8> {
        Self::cached_id_key(&self.legacy_message_key_prefix, id)
    }

    #[allow(dead_code)]
    fn ready_entry_index_key_with_prefix(prefix: &[u8], ready_seq: u64, id: MessageId) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + (Self::PADDED_U64_WIDTH * 2) + 1);
        key.extend_from_slice(prefix);
        Self::append_padded_u64(&mut key, ready_seq);
        key.push(b':');
        Self::append_padded_u64(&mut key, id.as_u64());
        key
    }

    #[allow(dead_code)]
    fn ready_entry_index_key(&self, ready_seq: u64, id: MessageId) -> Vec<u8> {
        Self::ready_entry_index_key_with_prefix(&self.ready_index_prefix, ready_seq, id)
    }

    #[allow(dead_code)]
    fn parse_ready_entry_index_key(key: &[u8], prefix: &[u8]) -> Option<(u64, MessageId)> {
        let rest = key.strip_prefix(prefix)?;
        if rest.len() != (Self::PADDED_U64_WIDTH * 2) + 1 {
            return None;
        }
        if rest[Self::PADDED_U64_WIDTH] != b':' {
            return None;
        }
        let ready_seq = Self::parse_padded_u64(&rest[..Self::PADDED_U64_WIDTH])?;
        let id = Self::parse_padded_u64(&rest[(Self::PADDED_U64_WIDTH + 1)..])?;
        Some((ready_seq, MessageId::new(id)))
    }

    fn append_padded_u64(buf: &mut Vec<u8>, value: u64) {
        let mut digits = [b'0'; Self::PADDED_U64_WIDTH];
        let mut cursor = Self::PADDED_U64_WIDTH;
        let mut remaining = value;

        loop {
            cursor -= 1;
            digits[cursor] = b'0' + (remaining % 10) as u8;
            remaining /= 10;
            if remaining == 0 {
                break;
            }
        }

        buf.extend_from_slice(&digits);
    }

    fn parse_padded_u64(bytes: &[u8]) -> Option<u64> {
        if bytes.len() != Self::PADDED_U64_WIDTH {
            return None;
        }

        let mut value = 0_u64;
        for &byte in bytes {
            if !byte.is_ascii_digit() {
                return None;
            }
            value = value.checked_mul(10)?.checked_add((byte - b'0') as u64)?;
        }
        Some(value)
    }

    #[allow(dead_code)]
    fn encode_meta(meta: QueueMetaSnapshot) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + (8 * 7));
        out.push(Self::META_VERSION_V2);
        out.extend_from_slice(&meta.next_id.to_le_bytes());
        out.extend_from_slice(&meta.next_ready_seq.to_le_bytes());
        out.extend_from_slice(&meta.ready_count.to_le_bytes());
        out.extend_from_slice(&meta.delayed_count.to_le_bytes());
        out.extend_from_slice(&meta.inflight_count.to_le_bytes());
        out.extend_from_slice(&meta.dlq_count.to_le_bytes());
        out.extend_from_slice(&meta.oldest_ready_enqueued_at_ms.unwrap_or(0).to_le_bytes());
        out
    }

    fn decode_meta(bytes: &[u8]) -> Option<QueueMetaSnapshot> {
        if bytes.len() == 8 {
            let next_id = u64::from_le_bytes(bytes.try_into().ok()?);
            return Some(QueueMetaSnapshot {
                next_id,
                next_ready_seq: next_id,
                ready_count: 0,
                delayed_count: 0,
                inflight_count: 0,
                dlq_count: 0,
                oldest_ready_enqueued_at_ms: None,
            });
        }

        if bytes.first().copied()? != Self::META_VERSION_V2 || bytes.len() != 57 {
            return None;
        }

        let next_id = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
        let next_ready_seq = u64::from_le_bytes(bytes[9..17].try_into().ok()?);
        let ready_count = u64::from_le_bytes(bytes[17..25].try_into().ok()?);
        let delayed_count = u64::from_le_bytes(bytes[25..33].try_into().ok()?);
        let inflight_count = u64::from_le_bytes(bytes[33..41].try_into().ok()?);
        let dlq_count = u64::from_le_bytes(bytes[41..49].try_into().ok()?);
        let oldest_ready = u64::from_le_bytes(bytes[49..57].try_into().ok()?);
        Some(QueueMetaSnapshot {
            next_id,
            next_ready_seq,
            ready_count,
            delayed_count,
            inflight_count,
            dlq_count,
            oldest_ready_enqueued_at_ms: (oldest_ready != 0).then_some(oldest_ready),
        })
    }

    fn encode_index_meta(
        next_id: u64,
        ready_count: u64,
        delayed_count: u64,
        next_delayed_visibility_ms: Option<u64>,
    ) -> Vec<u8> {
        let mut out = Vec::with_capacity(34);
        out.push(Self::INDEX_VERSION_V2);
        out.push(Self::INDEX_META_VALID_MARKER);
        out.extend_from_slice(&next_id.to_le_bytes());
        out.extend_from_slice(&ready_count.to_le_bytes());
        out.extend_from_slice(&delayed_count.to_le_bytes());
        out.extend_from_slice(
            &next_delayed_visibility_ms
                .unwrap_or(Self::INDEX_META_NEXT_DELAY_NONE)
                .to_le_bytes(),
        );
        out
    }

    fn index_meta_is_valid(bytes: &[u8]) -> bool {
        Self::decode_index_meta(bytes).is_ok()
    }

    fn decode_index_meta(bytes: &[u8]) -> Result<DecodedIndexMeta, String> {
        if bytes.len() < 2 {
            return Err("Queue index meta too short".to_string());
        }
        if bytes[1] != Self::INDEX_META_VALID_MARKER {
            return Err("Queue index meta missing validity marker".to_string());
        }

        match bytes[0] {
            Self::INDEX_VERSION_V1 => Ok(DecodedIndexMeta::LegacyV1),
            Self::INDEX_VERSION_V2 => {
                if bytes.len() < 34 {
                    return Err("Queue index meta v2 payload too short".to_string());
                }

                let next_id = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
                let ready_count = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
                let delayed_count = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
                let raw_next_delayed = u64::from_le_bytes(bytes[26..34].try_into().unwrap());
                let next_delayed_visibility_ms =
                    if raw_next_delayed == Self::INDEX_META_NEXT_DELAY_NONE {
                        None
                    } else {
                        Some(raw_next_delayed)
                    };

                if next_id == 0 {
                    return Err("Queue index meta v2 has invalid next_id=0".to_string());
                }
                if delayed_count == 0 && next_delayed_visibility_ms.is_some() {
                    return Err(
                        "Queue index meta v2 delayed_count=0 with non-empty next delayed"
                            .to_string(),
                    );
                }
                if delayed_count > 0 && next_delayed_visibility_ms.is_none() {
                    return Err(
                        "Queue index meta v2 delayed_count>0 without next delayed".to_string()
                    );
                }

                Ok(DecodedIndexMeta::V2(IndexMetaSnapshot {
                    next_id,
                    ready_count,
                    delayed_count,
                    next_delayed_visibility_ms,
                }))
            }
            other => Err(format!("Unsupported queue index meta version {}", other)),
        }
    }

    fn decode_next_id(bytes: Option<&[u8]>) -> u64 {
        bytes
            .and_then(Self::decode_meta)
            .map(|meta| meta.next_id)
            .unwrap_or(1)
    }

    #[allow(dead_code)]
    fn load_meta_from_store(&self) -> Option<QueueMetaSnapshot> {
        let cf_id = self.queue_key.family.id();
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .ok()?;
        let bytes = txn.get(&self.meta_key).ok()??;
        Self::decode_meta(bytes.as_ref())
    }

    fn load_next_id_from_meta_key(&self) -> u64 {
        let cf_id = self.queue_key.family.id();
        let txn = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        {
            Ok(txn) => txn,
            Err(e) => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    error = ?e,
                    "Failed to begin queue meta recovery transaction; starting from 1"
                );
                return 1;
            }
        };

        match txn.get(&self.meta_key) {
            Ok(Some(bytes)) => Self::decode_next_id(Some(bytes.as_ref())),
            Ok(None) => 1,
            Err(e) if Self::is_missing_read_snapshot_error(&e) => 1,
            Err(e) => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    error = ?e,
                    "Failed to recover queue next_id; starting from 1"
                );
                1
            }
        }
    }

    fn min_persisted_delayed_visibility_ms(&self) -> Option<u64> {
        self.persisted_next_delayed_visibility_ms
    }

    fn min_persisted_delayed_visibility_ms_excluding(&self, excluded: MessageId) -> Option<u64> {
        if self.persisted_delayed.get(&excluded).copied()
            != self.persisted_next_delayed_visibility_ms
        {
            return self.persisted_next_delayed_visibility_ms;
        }

        self.persisted_delayed
            .iter()
            .filter_map(|(&id, &visible_at_ms)| (id != excluded).then_some(visible_at_ms))
            .min()
    }

    fn recompute_persisted_delayed_visibility_ms(&mut self) {
        self.persisted_next_delayed_visibility_ms = self.persisted_delayed.values().copied().min();
    }

    fn insert_persisted_delayed(&mut self, id: MessageId, visible_at_ms: u64) {
        self.persisted_delayed.insert(id, visible_at_ms);
        self.persisted_next_delayed_visibility_ms = Some(
            self.persisted_next_delayed_visibility_ms
                .map(|current| current.min(visible_at_ms))
                .unwrap_or(visible_at_ms),
        );
    }

    fn remove_persisted_delayed(&mut self, id: MessageId) -> Option<u64> {
        let removed = self.persisted_delayed.remove(&id);
        if removed == self.persisted_next_delayed_visibility_ms {
            self.recompute_persisted_delayed_visibility_ms();
        }
        removed
    }

    fn clear_persisted_delayed(&mut self) {
        self.persisted_delayed.clear();
        self.persisted_next_delayed_visibility_ms = None;
    }

    fn insert_persisted_dlq(&mut self, id: MessageId, dead_lettered_at_ms: u64) {
        self.persisted_dlq.insert(id, dead_lettered_at_ms);
        self.dlq_count = self.persisted_dlq.len();
    }

    fn remove_persisted_dlq(&mut self, id: MessageId) -> Option<u64> {
        let removed = self.persisted_dlq.remove(&id);
        self.dlq_count = self.persisted_dlq.len();
        removed
    }

    fn clear_persisted_dlq(&mut self) {
        self.persisted_dlq.clear();
        self.dlq_count = 0;
    }

    fn delete_record_for_layout(
        txn: &mut cntryl_midge::Transaction,
        layout: StoredRecordLayout,
        header_key: Vec<u8>,
        body_key: Vec<u8>,
        legacy_key: Vec<u8>,
    ) -> cntryl_midge::MidgeResult<()> {
        match layout {
            StoredRecordLayout::EmbeddedHeader => txn.delete(header_key),
            StoredRecordLayout::SplitHeaderBody => {
                txn.delete(header_key).and_then(|_| txn.delete(body_key))
            }
            StoredRecordLayout::LegacyKey => txn.delete(legacy_key),
        }
    }

    fn staged_ready_count_after_mutation(
        &self,
        mutation: Option<(usize, PersistedReadyMutation)>,
    ) -> usize {
        let mut count = self.persisted_ready_count;
        if let Some((_shard, mutation)) = mutation {
            let removed_len = match mutation {
                PersistedReadyMutation::Delete { removed }
                | PersistedReadyMutation::Replace { removed, .. }
                | PersistedReadyMutation::Split { removed, .. } => Self::range_len(removed),
            };
            count = count.saturating_sub(removed_len);
            count += match mutation {
                PersistedReadyMutation::Delete { .. } => 0,
                PersistedReadyMutation::Replace { inserted, .. } => Self::range_len(inserted),
                PersistedReadyMutation::Split { left, right, .. } => {
                    Self::range_len(left) + Self::range_len(right)
                }
            };
        }
        count
    }

    fn plan_index_mutation_for_unavailable_message(
        &self,
        id: MessageId,
    ) -> PersistedIndexMutationPlan {
        let ready_mutation = Self::plan_ready_index_mutation(&self.persisted_ready_shards, id);
        let delayed_index_delete = self.persisted_delayed.get(&id).copied();

        PersistedIndexMutationPlan {
            ready_mutation,
            delayed_index_delete,
            staged_ready_count: self.staged_ready_count_after_mutation(ready_mutation),
            staged_delayed_count: self
                .persisted_delayed
                .len()
                .saturating_sub(usize::from(delayed_index_delete.is_some())),
            staged_next_delayed_visibility: if delayed_index_delete.is_some() {
                self.min_persisted_delayed_visibility_ms_excluding(id)
            } else {
                self.min_persisted_delayed_visibility_ms()
            },
        }
    }

    fn write_persisted_ready_mutation(
        &self,
        txn: &mut cntryl_midge::Transaction,
        shard: usize,
        mutation: PersistedReadyMutation,
    ) -> Result<(), String> {
        match mutation {
            PersistedReadyMutation::Delete { removed } => txn
                .delete(self.ready_range_key(shard, removed.next))
                .map_err(|e| format!("Failed to delete queue ready index: {:?}", e)),
            PersistedReadyMutation::Replace { removed, inserted } => txn
                .delete(self.ready_range_key(shard, removed.next))
                .and_then(|_| {
                    txn.put(
                        self.ready_range_key(shard, inserted.next),
                        Self::encode_ready_range_value(inserted),
                        None,
                    )
                })
                .map_err(|e| format!("Failed to replace queue ready index: {:?}", e)),
            PersistedReadyMutation::Split {
                removed,
                left,
                right,
            } => txn
                .delete(self.ready_range_key(shard, removed.next))
                .and_then(|_| {
                    txn.put(
                        self.ready_range_key(shard, left.next),
                        Self::encode_ready_range_value(left),
                        None,
                    )
                })
                .and_then(|_| {
                    txn.put(
                        self.ready_range_key(shard, right.next),
                        Self::encode_ready_range_value(right),
                        None,
                    )
                })
                .map_err(|e| format!("Failed to split queue ready index: {:?}", e)),
        }
    }

    fn write_index_mutation_plan(
        &self,
        txn: &mut cntryl_midge::Transaction,
        id: MessageId,
        plan: PersistedIndexMutationPlan,
        dead_lettered_at_ms: Option<u64>,
    ) -> Result<(), String> {
        if let Some((shard, mutation)) = plan.ready_mutation {
            self.write_persisted_ready_mutation(txn, shard, mutation)
                .map_err(|error| {
                    format!("Failed to update ready index for message {}: {}", id, error)
                })?;
        }

        if let Some(visible_at_ms) = plan.delayed_index_delete {
            txn.delete(self.delayed_index_key(visible_at_ms, id))
                .map_err(|e| {
                    format!("Failed to update delayed index for message {}: {:?}", id, e)
                })?;
        }

        if let Some(dead_lettered_at_ms) = dead_lettered_at_ms {
            txn.put(
                self.dlq_index_key(dead_lettered_at_ms, id),
                Vec::new(),
                None,
            )
            .map_err(|e| format!("Failed to write DLQ index for message {}: {:?}", id, e))?;
        }

        txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                self.next_id_limit,
                plan.staged_ready_count as u64,
                plan.staged_delayed_count as u64,
                plan.staged_next_delayed_visibility,
            ),
            None,
        )
        .map_err(|e| {
            format!(
                "Failed to update queue index meta for message {}: {:?}",
                id, e
            )
        })
    }

    fn apply_index_mutation_plan(
        &mut self,
        id: MessageId,
        plan: PersistedIndexMutationPlan,
        dead_lettered_at_ms: Option<u64>,
    ) {
        if let Some((shard, mutation)) = plan.ready_mutation {
            self.apply_ready_index_mutation(shard, mutation);
        }

        if plan.delayed_index_delete.is_some() {
            self.remove_persisted_delayed(id);
        }

        if let Some(dead_lettered_at_ms) = dead_lettered_at_ms {
            self.insert_persisted_dlq(id, dead_lettered_at_ms);
        }
    }

    fn ready_range_key_with_prefix(prefix: &[u8], shard: usize, start: u64) -> Vec<u8> {
        let tens = (shard / 10) as u8;
        let ones = (shard % 10) as u8;
        let mut key = Vec::with_capacity(prefix.len() + 3 + Self::PADDED_U64_WIDTH);
        key.extend_from_slice(prefix);
        key.push(b'0' + tens);
        key.push(b'0' + ones);
        key.push(b':');
        Self::append_padded_u64(&mut key, start);
        key
    }

    fn ready_range_key(&self, shard: usize, start: u64) -> Vec<u8> {
        Self::ready_range_key_with_prefix(&self.ready_index_prefix, shard, start)
    }

    fn parse_ready_range_key(key: &[u8], prefix: &[u8]) -> Option<(usize, u64)> {
        let rest = key.strip_prefix(prefix)?;
        if rest.len() != 3 + Self::PADDED_U64_WIDTH {
            return None;
        }
        if !rest[0].is_ascii_digit() || !rest[1].is_ascii_digit() || rest[2] != b':' {
            return None;
        }

        let shard = ((rest[0] - b'0') as usize) * 10 + (rest[1] - b'0') as usize;
        if shard >= Self::READY_SHARDS {
            return None;
        }

        let start = Self::parse_padded_u64(&rest[3..])?;
        Some((shard, start))
    }

    fn encode_ready_range_value(range: ReadyRange) -> Vec<u8> {
        range.end.to_le_bytes().to_vec()
    }

    fn decode_ready_range(start: u64, value: &[u8]) -> Option<ReadyRange> {
        let end = u64::from_le_bytes(value.get(0..8)?.try_into().ok()?);
        let step = Self::READY_SHARDS as u64;
        if end < start || !(end - start).is_multiple_of(step) {
            return None;
        }
        Some(ReadyRange { next: start, end })
    }

    fn delayed_index_key_with_prefix(prefix: &[u8], visible_at_ms: u64, id: MessageId) -> Vec<u8> {
        let mut key = Vec::with_capacity(prefix.len() + (Self::PADDED_U64_WIDTH * 2) + 1);
        key.extend_from_slice(prefix);
        Self::append_padded_u64(&mut key, visible_at_ms);
        key.push(b':');
        Self::append_padded_u64(&mut key, id.as_u64());
        key
    }

    fn delayed_index_key(&self, visible_at_ms: u64, id: MessageId) -> Vec<u8> {
        Self::delayed_index_key_with_prefix(&self.delayed_index_prefix, visible_at_ms, id)
    }

    #[allow(dead_code)]
    fn delayed_entry_index_key(
        &self,
        visible_at_ms: u64,
        enqueue_seq: u64,
        id: MessageId,
    ) -> Vec<u8> {
        let mut key =
            Vec::with_capacity(self.delayed_index_prefix.len() + (Self::PADDED_U64_WIDTH * 3) + 2);
        key.extend_from_slice(&self.delayed_index_prefix);
        Self::append_padded_u64(&mut key, visible_at_ms);
        key.push(b':');
        Self::append_padded_u64(&mut key, enqueue_seq);
        key.push(b':');
        Self::append_padded_u64(&mut key, id.as_u64());
        key
    }

    #[allow(dead_code)]
    fn parse_delayed_entry_index_key(key: &[u8], prefix: &[u8]) -> Option<(u64, u64, MessageId)> {
        let rest = key.strip_prefix(prefix)?;
        if rest.len() != (Self::PADDED_U64_WIDTH * 3) + 2 {
            return None;
        }
        if rest[Self::PADDED_U64_WIDTH] != b':' || rest[(Self::PADDED_U64_WIDTH * 2) + 1] != b':' {
            return None;
        }
        let visible_at_ms = Self::parse_padded_u64(&rest[..Self::PADDED_U64_WIDTH])?;
        let enqueue_seq = Self::parse_padded_u64(
            &rest[(Self::PADDED_U64_WIDTH + 1)..((Self::PADDED_U64_WIDTH * 2) + 1)],
        )?;
        let id = Self::parse_padded_u64(&rest[((Self::PADDED_U64_WIDTH * 2) + 2)..])?;
        Some((visible_at_ms, enqueue_seq, MessageId::new(id)))
    }

    #[allow(dead_code)]
    fn inflight_index_key(
        &self,
        expires_at_ms: u64,
        inflight_epoch: u64,
        id: MessageId,
    ) -> Vec<u8> {
        let mut key =
            Vec::with_capacity(self.inflight_index_prefix.len() + (Self::PADDED_U64_WIDTH * 3) + 2);
        key.extend_from_slice(&self.inflight_index_prefix);
        Self::append_padded_u64(&mut key, expires_at_ms);
        key.push(b':');
        Self::append_padded_u64(&mut key, inflight_epoch);
        key.push(b':');
        Self::append_padded_u64(&mut key, id.as_u64());
        key
    }

    #[allow(dead_code)]
    fn dlq_index_key(&self, dead_lettered_at_ms: u64, id: MessageId) -> Vec<u8> {
        let mut key =
            Vec::with_capacity(self.dlq_index_prefix.len() + (Self::PADDED_U64_WIDTH * 2) + 1);
        key.extend_from_slice(&self.dlq_index_prefix);
        Self::append_padded_u64(&mut key, dead_lettered_at_ms);
        key.push(b':');
        Self::append_padded_u64(&mut key, id.as_u64());
        key
    }

    #[allow(dead_code)]
    fn ack_dedup_key(&self, id: MessageId, token: u64) -> Vec<u8> {
        let mut key =
            Vec::with_capacity(self.ack_dedup_prefix.len() + (Self::PADDED_U64_WIDTH * 2) + 1);
        key.extend_from_slice(&self.ack_dedup_prefix);
        Self::append_padded_u64(&mut key, id.as_u64());
        key.push(b':');
        Self::append_padded_u64(&mut key, token);
        key
    }

    fn parse_delayed_index_key(key: &[u8], prefix: &[u8]) -> Option<(u64, MessageId)> {
        let rest = key.strip_prefix(prefix)?;
        if rest.len() != (Self::PADDED_U64_WIDTH * 2) + 1 {
            return None;
        }
        if rest[Self::PADDED_U64_WIDTH] != b':' {
            return None;
        }

        let visible_at_ms = Self::parse_padded_u64(&rest[..Self::PADDED_U64_WIDTH])?;
        let id = Self::parse_padded_u64(&rest[(Self::PADDED_U64_WIDTH + 1)..])?;
        Some((visible_at_ms, MessageId::new(id)))
    }

    fn parse_dlq_index_key(key: &[u8], prefix: &[u8]) -> Option<(u64, MessageId)> {
        let rest = key.strip_prefix(prefix)?;
        if rest.len() != (Self::PADDED_U64_WIDTH * 2) + 1 {
            return None;
        }
        if rest[Self::PADDED_U64_WIDTH] != b':' {
            return None;
        }

        let dead_lettered_at_ms = Self::parse_padded_u64(&rest[..Self::PADDED_U64_WIDTH])?;
        let id = Self::parse_padded_u64(&rest[(Self::PADDED_U64_WIDTH + 1)..])?;
        Some((dead_lettered_at_ms, MessageId::new(id)))
    }

    #[inline]
    fn parse_message_id_from_key(key: &[u8], prefix: &[u8]) -> Option<MessageId> {
        if !key.starts_with(prefix) || key.len() <= prefix.len() {
            return None;
        }

        let mut value = 0_u64;
        for &byte in &key[prefix.len()..] {
            if !byte.is_ascii_digit() {
                return None;
            }
            value = value.checked_mul(10)?.checked_add((byte - b'0') as u64)?;
        }

        Some(MessageId::new(value))
    }

    /// Serialize QueueRecord header to bytes.
    fn encode_record_header(record: &QueueRecord) -> Vec<u8> {
        let mut buf = Vec::with_capacity(79);
        buf.push(Self::HEADER_VERSION_V2);
        buf.push(record.state as u8);
        buf.extend_from_slice(&record.enqueue_seq.to_le_bytes());
        buf.extend_from_slice(&record.ready_seq.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&record.attempts.to_le_bytes());
        buf.extend_from_slice(&record.visible_at_ms.to_le_bytes());
        buf.extend_from_slice(&record.first_enqueued_at_ms.to_le_bytes());
        buf.extend_from_slice(&record.last_inflight_at_ms.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&record.inflight_epoch.to_le_bytes());
        buf.extend_from_slice(&record.inflight_token.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&record.inflight_expires_at_ms.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&record.dead_lettered_at_ms.unwrap_or(0).to_le_bytes());
        buf.push(record.dlq_reason.map(|value| value as u8).unwrap_or(0));
        buf
    }

    /// Serialize a legacy combined QueueRecord for compatibility writes.
    fn encode_legacy_record(record: &QueueRecord) -> Vec<u8> {
        let body = record
            .body
            .as_ref()
            .expect("legacy queue record must have a body before persistence");
        let mut buf = Vec::with_capacity(16 + body.len());
        buf.extend_from_slice(&record.attempts.to_le_bytes());
        buf.extend_from_slice(&record.visible_at_ms.to_le_bytes());
        buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
        buf.extend_from_slice(body);
        buf
    }

    pub fn ready_len(&self) -> usize {
        self.ready.len()
    }

    fn backlog_age_metrics(&self, now_epoch_ms: u64) -> (QueueAgeBuckets, u64) {
        let mut buckets = QueueAgeBuckets::default();
        let mut oldest_backlog_age_seconds = 0u64;

        for record in self.records.values() {
            if matches!(record.state, QueueState::Ready | QueueState::Delayed) {
                let age_seconds = now_epoch_ms.saturating_sub(record.first_enqueued_at_ms) / 1_000;
                buckets.record_age_seconds(age_seconds);
                oldest_backlog_age_seconds = oldest_backlog_age_seconds.max(age_seconds);
            }
        }

        (buckets, oldest_backlog_age_seconds)
    }

    fn delay_age_metrics(&self, now_epoch_ms: u64) -> QueueAgeBuckets {
        let mut buckets = QueueAgeBuckets::default();

        for record in self.records.values() {
            if matches!(record.state, QueueState::Delayed) {
                let age_seconds = now_epoch_ms.saturating_sub(record.first_enqueued_at_ms) / 1_000;
                buckets.record_age_seconds(age_seconds);
            }
        }

        buckets
    }

    pub fn admin_snapshot(&self) -> QueueAdminSnapshot {
        let now_epoch_ms = self.clock.now_epoch_ms();
        let (backlog_age_buckets, oldest_backlog_age_seconds) =
            self.backlog_age_metrics(now_epoch_ms);
        let delay_age_buckets = self.delay_age_metrics(now_epoch_ms);
        QueueAdminSnapshot {
            messages_ready: self.ready.len(),
            messages_delayed: self.persisted_delayed.len(),
            messages_inflight: self.inflight.len(),
            messages_dead_lettered: self.persisted_dlq.len(),
            messages_total: self.ready.len()
                + self.inflight.len()
                + self.persisted_delayed.len()
                + self.persisted_dlq.len(),
            oldest_message_age_seconds: self
                .oldest_ready_enqueued_at_ms
                .map(|timestamp| now_epoch_ms.saturating_sub(timestamp) / 1_000)
                .unwrap_or(0),
            oldest_backlog_age_seconds,
            backlog_age_buckets,
            delay_age_buckets,
        }
    }

    pub fn admin_inflight(&self) -> Vec<QueueInflightSnapshot> {
        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();

        self.inflight
            .iter()
            .map(|(id, inflight)| QueueInflightSnapshot {
                message_id: id.as_u64(),
                inflight_token: inflight.token,
                session_id: inflight.owner_session_id,
                expires_at_epoch_ms: now_epoch_ms.saturating_add(
                    inflight
                        .expires_at
                        .saturating_duration_since(now_instant)
                        .as_millis() as u64,
                ),
                attempts: inflight.attempts as usize,
            })
            .collect()
    }

    pub fn admin_dead_letters(&self) -> Vec<QueueDeadLetterSnapshot> {
        let mut dead_letters: Vec<_> = self
            .persisted_dlq
            .iter()
            .map(|(id, &dead_lettered_at_epoch_ms)| {
                let record = self
                    .records
                    .get(id)
                    .cloned()
                    .filter(|record| matches!(record.state, QueueState::Dlq))
                    .or_else(|| {
                        self.load_record_metadata_from_store(*id)
                            .ok()
                            .map(|(record, _)| record)
                            .filter(|record| matches!(record.state, QueueState::Dlq))
                    });

                let attempts = record
                    .as_ref()
                    .map(|record| record.attempts as usize)
                    .unwrap_or_default();
                let dead_lettered_at_epoch_ms = record
                    .as_ref()
                    .and_then(|record| record.dead_lettered_at_ms)
                    .unwrap_or(dead_lettered_at_epoch_ms);
                let reason = record
                    .as_ref()
                    .and_then(|record| record.dlq_reason)
                    .map(Self::dlq_reason_label)
                    .unwrap_or("unknown");

                QueueDeadLetterSnapshot {
                    message_id: id.as_u64(),
                    dead_lettered_at_epoch_ms,
                    attempts,
                    reason,
                }
            })
            .collect();

        dead_letters.sort_by(|left, right| {
            (left.dead_lettered_at_epoch_ms, left.message_id)
                .cmp(&(right.dead_lettered_at_epoch_ms, right.message_id))
        });
        dead_letters
    }

    fn dlq_reason_label(reason: DlqReason) -> &'static str {
        match reason {
            DlqReason::MaxAttemptsExceeded => "max_attempts_exceeded",
        }
    }

    /// Drop any live inflight entries owned by a disconnected session and return the
    /// committed messages to the ready queue. The inflight ownership itself is
    /// ephemeral and is not durably recovered.
    pub fn cleanup_session_inflight(&mut self, session_id: u64) -> usize {
        let released: Vec<_> = self
            .inflight
            .iter()
            .filter_map(|(id, inflight)| {
                (inflight.owner_session_id == Some(session_id)).then_some(*id)
            })
            .collect();

        for id in released.iter().copied() {
            self.inflight.remove(&id);
            if let Some(record) = self.records.get_mut(&id) {
                record.state = QueueState::Ready;
                record.visible_at_ms = 0;
                record.inflight_token = None;
                record.inflight_expires_at_ms = None;
            }
            self.push_ready(id);
        }

        released.len()
    }

    pub fn ready_contains(&self, id: MessageId) -> bool {
        self.ready.iter().any(|entry| entry.id == id)
    }

    /// Returns true if queue-local waiters should be re-checked.
    /// Clears the flag. Used by the domain sink after state-changing operations.
    pub fn take_needs_wake_waiters(&mut self) -> bool {
        std::mem::take(&mut self.needs_wake_waiters)
    }

    /// Handle send operation
    pub fn handle_send(&mut self, body: Bytes, delay_seconds: Option<u64>) -> QueueResponse {
        // Track empty state before send for notification
        let was_empty = self.ready_count == 0;

        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let Some(delay_ms) = delay_seconds.unwrap_or(0).checked_mul(1_000) else {
            return QueueResponse::BadRequest {
                reason: "delay_seconds is too large".to_string(),
            };
        };

        // Start transaction
        let cf_id = self.queue_key.family.id();
        let mut txn = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(t) => t,
            Err(e) => {
                return QueueResponse::Error {
                    message: format!("Failed to begin transaction: {:?}", e),
                };
            }
        };

        // Allocate message ID
        let id = MessageId::new(self.next_id);
        let visible_at_ms = now_epoch_ms.saturating_add(delay_ms);
        let Some(visible_at) = now_instant.checked_add(Duration::from_millis(delay_ms)) else {
            return QueueResponse::BadRequest {
                reason: "delay_seconds is too large".to_string(),
            };
        };

        let record = QueueRecord::metadata_only(0, visible_at_ms);
        let cached_body = body.clone();
        let reserved_limit = self.reserved_id_limit_for(1);
        let staged_next_id = reserved_limit.unwrap_or(self.next_id_limit);
        let staged_ready_count =
            self.persisted_ready_count + usize::from(visible_at <= now_instant);
        let staged_delayed_count =
            self.persisted_delayed.len() + usize::from(visible_at > now_instant);
        let staged_next_delayed_visibility = if visible_at > now_instant {
            Some(
                self.min_persisted_delayed_visibility_ms()
                    .map(|current| current.min(visible_at_ms))
                    .unwrap_or(visible_at_ms),
            )
        } else {
            self.min_persisted_delayed_visibility_ms()
        };
        let ready_index_write = if visible_at <= now_instant {
            let tail = self.persisted_ready_shards[Self::shard_for_id(id)]
                .back()
                .copied();
            Some(Self::prepare_persisted_ready_append(tail, id))
        } else {
            None
        };

        // Write message header + body to one durable transaction.
        let header_key = self.cached_header_key(id);
        let header_value = Self::encode_legacy_record(&QueueRecord::loaded(
            body.clone(),
            record.attempts,
            record.visible_at_ms,
        ));
        if let Err(e) = txn.put(header_key, header_value, None) {
            return QueueResponse::Error {
                message: format!("Failed to add message header to transaction: {:?}", e),
            };
        }

        if let Some((shard, range)) = ready_index_write {
            if let Err(e) = txn.put(
                self.ready_range_key(shard, range.next),
                Self::encode_ready_range_value(range),
                None,
            ) {
                return QueueResponse::Error {
                    message: format!("Failed to update queue ready index: {:?}", e),
                };
            }
        } else if let Err(e) = txn.put(self.delayed_index_key(visible_at_ms, id), Vec::new(), None)
        {
            return QueueResponse::Error {
                message: format!("Failed to update queue delayed index: {:?}", e),
            };
        }

        if let Some(limit) = reserved_limit {
            if let Err(e) = txn.put(self.meta_key.clone(), limit.to_le_bytes().to_vec(), None) {
                return QueueResponse::Error {
                    message: format!("Failed to update queue meta: {:?}", e),
                };
            }
        }

        if let Err(e) = txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                staged_next_id,
                staged_ready_count as u64,
                staged_delayed_count as u64,
                staged_next_delayed_visibility,
            ),
            None,
        ) {
            return QueueResponse::Error {
                message: format!("Failed to update queue index meta: {:?}", e),
            };
        }

        // Commit with buffered mode for high throughput
        // The store will sync periodically, maintaining durability without per-operation cost
        let commit_start = Instant::now();
        if let Err(e) = txn.commit(self.commit_write_options) {
            return QueueResponse::Error {
                message: format!("Failed to commit transaction: {:?}", e),
            };
        }
        Self::observe_elapsed_us(obs::METRIC_QUEUE_ENQUEUE_COMMIT_LATENCY, commit_start);

        // Commit succeeded; advance in-memory ID state.
        self.next_id = self.next_id.saturating_add(1);
        if let Some(limit) = reserved_limit {
            self.next_id_limit = limit;
        }

        // Cache record in memory for fast reserve path
        self.cache_record(id, record, StoredRecordLayout::EmbeddedHeader);
        self.cache_body(id, cached_body);

        // Update in-memory queues
        if visible_at <= now_instant {
            self.push_ready(id);
            self.push_persisted_ready(id);
        } else {
            self.delayed.push(Reverse(DelayedMessage {
                id,
                enqueue_seq: id.as_u64(),
                visible_at,
                visible_at_ms,
            }));
            self.insert_persisted_delayed(id, visible_at_ms);
        }
        if !self.index_meta_written {
            self.index_meta_written = true;
        }

        // Mark queue-local waiters for wakeup if the queue transitioned from empty to non-empty
        // (only for immediately visible messages, not delayed ones).
        if was_empty && visible_at <= now_instant && self.ready_count > 0 {
            self.needs_wake_waiters = true;
        }

        QueueResponse::Sent { id }
    }

    /// Send multiple messages in one transaction (batch).
    /// Same semantics as N×handle_send; use for throughput when the caller has many messages.
    pub fn handle_send_batch(&mut self, items: &[(Bytes, Option<u64>)]) -> QueueResponse {
        if items.is_empty() {
            return QueueResponse::SentBatch { ids: vec![] };
        }

        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let cf_id = self.queue_key.family.id();

        let mut txn = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(t) => t,
            Err(e) => {
                return QueueResponse::Error {
                    message: format!("Failed to begin transaction: {:?}", e),
                };
            }
        };

        let mut ids = Vec::with_capacity(items.len());
        let mut post_commit: Vec<(MessageId, QueueRecord, Bytes, std::time::Instant)> =
            Vec::with_capacity(items.len());
        let mut staged_ready_tails: Vec<Option<ReadyRange>> = self
            .persisted_ready_shards
            .iter()
            .map(|ranges| ranges.back().copied())
            .collect();
        let mut staged_ready_ids = Vec::new();
        let mut staged_delayed = Vec::new();
        let mut staged_ready_add = 0usize;
        let mut staged_delayed_add = 0usize;
        let mut staged_next_delayed_visibility = self.min_persisted_delayed_visibility_ms();
        let mut next_id = self.next_id;
        let reserved_limit = self.reserved_id_limit_for(items.len() as u64);

        for (body, delay_seconds) in items {
            let Some(delay_ms) = delay_seconds.unwrap_or(0).checked_mul(1_000) else {
                return QueueResponse::BadRequest {
                    reason: "delay_seconds is too large".to_string(),
                };
            };
            let id = MessageId::new(next_id);
            let visible_at_ms = now_epoch_ms.saturating_add(delay_ms);
            let Some(visible_at) = now_instant.checked_add(Duration::from_millis(delay_ms)) else {
                return QueueResponse::BadRequest {
                    reason: "delay_seconds is too large".to_string(),
                };
            };

            let record = QueueRecord::metadata_only(0, visible_at_ms);

            let header_key = self.cached_header_key(id);
            let header_value = Self::encode_legacy_record(&QueueRecord::loaded(
                body.clone(),
                record.attempts,
                record.visible_at_ms,
            ));
            if let Err(e) = txn.put(header_key, header_value, None) {
                return QueueResponse::Error {
                    message: format!("Failed to add message header to transaction: {:?}", e),
                };
            }

            if visible_at <= now_instant {
                let (shard, range) = Self::prepare_persisted_ready_append(
                    staged_ready_tails[Self::shard_for_id(id)],
                    id,
                );
                staged_ready_tails[shard] = Some(range);
                if let Err(e) = txn.put(
                    self.ready_range_key(shard, range.next),
                    Self::encode_ready_range_value(range),
                    None,
                ) {
                    return QueueResponse::Error {
                        message: format!("Failed to update queue ready index: {:?}", e),
                    };
                }
                staged_ready_ids.push(id);
                staged_ready_add += 1;
            } else if let Err(e) =
                txn.put(self.delayed_index_key(visible_at_ms, id), Vec::new(), None)
            {
                return QueueResponse::Error {
                    message: format!("Failed to update queue delayed index: {:?}", e),
                };
            }

            ids.push(id);
            post_commit.push((id, record, body.clone(), visible_at));
            if visible_at > now_instant {
                staged_delayed.push((id, visible_at_ms));
                staged_delayed_add += 1;
                staged_next_delayed_visibility = Some(
                    staged_next_delayed_visibility
                        .map(|current| current.min(visible_at_ms))
                        .unwrap_or(visible_at_ms),
                );
            }
            next_id += 1;
        }

        if let Some(limit) = reserved_limit {
            if let Err(e) = txn.put(self.meta_key.clone(), limit.to_le_bytes().to_vec(), None) {
                return QueueResponse::Error {
                    message: format!("Failed to update queue meta: {:?}", e),
                };
            }
        }

        let staged_next_id = reserved_limit.unwrap_or(self.next_id_limit);
        if let Err(e) = txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                staged_next_id,
                (self.persisted_ready_count + staged_ready_add) as u64,
                (self.persisted_delayed.len() + staged_delayed_add) as u64,
                staged_next_delayed_visibility,
            ),
            None,
        ) {
            return QueueResponse::Error {
                message: format!("Failed to update queue index meta: {:?}", e),
            };
        }

        let commit_start = Instant::now();
        if let Err(e) = txn.commit(self.commit_write_options) {
            return QueueResponse::Error {
                message: format!("Failed to commit transaction: {:?}", e),
            };
        }
        Self::observe_elapsed_us(obs::METRIC_QUEUE_ENQUEUE_COMMIT_LATENCY, commit_start);

        self.next_id = next_id;
        if let Some(limit) = reserved_limit {
            self.next_id_limit = limit;
        }
        for (id, record, cached_body, visible_at) in post_commit {
            let visible_at_ms = record.visible_at_ms;
            self.cache_record(id, record, StoredRecordLayout::EmbeddedHeader);
            self.cache_body(id, cached_body);
            if visible_at <= now_instant {
                self.push_ready(id);
            } else {
                self.delayed.push(Reverse(DelayedMessage {
                    id,
                    enqueue_seq: id.as_u64(),
                    visible_at,
                    visible_at_ms,
                }));
            }
        }
        for id in staged_ready_ids {
            self.push_persisted_ready(id);
        }
        for (id, visible_at_ms) in staged_delayed {
            self.insert_persisted_delayed(id, visible_at_ms);
        }
        if !self.index_meta_written {
            self.index_meta_written = true;
        }

        QueueResponse::SentBatch { ids }
    }

    /// Handle reserve operation
    ///
    /// IMPORTANT: This method ALWAYS returns immediately (never blocks).
    /// QueueDomainSink owns queue watch notifications outside the actor.
    /// QueueActor never stores subscriptions or blocks on empty queues.
    pub fn handle_receive(
        &mut self,
        inflight_seconds: u64,
        batch_size: Option<usize>,
    ) -> QueueResponse {
        self.handle_receive_internal(None, inflight_seconds, batch_size)
    }

    pub fn handle_receive_for_session(
        &mut self,
        session_id: u64,
        inflight_seconds: u64,
        batch_size: Option<usize>,
    ) -> QueueResponse {
        self.handle_receive_internal(Some(session_id), inflight_seconds, batch_size)
    }

    fn handle_receive_internal(
        &mut self,
        owner_session_id: Option<u64>,
        inflight_seconds: u64,
        batch_size: Option<usize>,
    ) -> QueueResponse {
        let batch_size = batch_size.unwrap_or(1);
        let now = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let inflight_duration = Duration::from_secs(inflight_seconds);
        let Some(expires_at) = now.checked_add(inflight_duration) else {
            return QueueResponse::BadRequest {
                reason: "inflight_seconds is too large".to_string(),
            };
        };

        let mut messages = Vec::with_capacity(self.ready.len().min(batch_size));

        for _ in 0..batch_size {
            let id = match self.ready.front().map(|entry| entry.id) {
                Some(id) => id,
                None => break, // No more messages
            };

            let (body, attempts) = match self.hydrate_record_for_receive(id) {
                Ok(record) => record,
                Err(e) => {
                    tracing::warn!(
                        queue = ?self.queue_key,
                        route_family = self.queue_key.family.as_u64(),
                        message_id = id.as_u64(),
                        error_reason = %e,
                        "Failed to hydrate queue record for receive"
                    );
                    break;
                }
            };

            let id = match self.pop_ready() {
                Some(id) => id,
                None => break,
            };
            self.evict_cached_body(id);

            // Generate inflight token
            let token = Self::generate_token();
            let inflight_epoch = self
                .records
                .get(&id)
                .map(|record| record.inflight_epoch.saturating_add(1))
                .unwrap_or(1);
            let expires_at_epoch_ms =
                now_epoch_ms.saturating_add(inflight_seconds.saturating_mul(1_000));

            // Create inflight entry
            self.inflight.insert(
                id,
                Inflight {
                    token,
                    expires_at,
                    expires_at_epoch_ms,
                    owner_session_id,
                    attempts: attempts + 1,
                    inflight_epoch,
                },
            );
            self.update_cached_inflight_metadata(
                id,
                inflight_epoch,
                Some(token),
                Some(expires_at_epoch_ms),
                Some(now_epoch_ms),
            );

            // Schedule expiration timer
            self.timers.push(Reverse(InflightExpiry {
                id,
                inflight_epoch,
                expires_at,
                expires_at_ms: expires_at_epoch_ms,
            }));

            // Update deadline cache if this expiration is sooner
            if expires_at < self.next_expiration_deadline {
                self.next_expiration_deadline = expires_at;
            }

            // Build response message
            messages.push(ReservedMessage {
                id,
                body,
                token,
                inflight_seconds,
                attempts: attempts + 1, // First attempt is 1 (not 0)
            });
        }

        // If no messages were reserved, return an empty response (avoid NotFound).
        // Clients expect an empty slice when the queue is empty rather than an error.
        if messages.is_empty() {
            return QueueResponse::Received { messages };
        }

        QueueResponse::Received { messages }
    }

    /// Handle extend operation
    pub fn handle_extend(
        &mut self,
        id: MessageId,
        token: u64,
        inflight_seconds: u64,
    ) -> QueueResponse {
        let now = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();

        // Check if message is inflight
        let inflight = match self.inflight.get_mut(&id) {
            Some(inflight) => inflight,
            None => return QueueResponse::NotFound,
        };

        // Validate token
        if inflight.token != token {
            return QueueResponse::InvalidToken;
        }

        // Check if already expired
        if inflight.expires_at <= now {
            self.inflight.remove(&id);
            return QueueResponse::InflightExpired;
        }

        // Extend expiration
        let Some(new_expires_at) = now.checked_add(Duration::from_secs(inflight_seconds)) else {
            return QueueResponse::BadRequest {
                reason: "inflight_seconds is too large".to_string(),
            };
        };
        inflight.inflight_epoch = inflight.inflight_epoch.saturating_add(1);
        inflight.expires_at = new_expires_at;
        inflight.expires_at_epoch_ms =
            now_epoch_ms.saturating_add(inflight_seconds.saturating_mul(1_000));
        let inflight_epoch = inflight.inflight_epoch;
        let inflight_expires_at_ms = inflight.expires_at_epoch_ms;

        // Schedule new timer (old timer will be ignored when it fires)
        self.timers.push(Reverse(InflightExpiry {
            id,
            inflight_epoch,
            expires_at: new_expires_at,
            expires_at_ms: inflight_expires_at_ms,
        }));
        self.update_cached_inflight_metadata(
            id,
            inflight_epoch,
            Some(token),
            Some(inflight_expires_at_ms),
            Some(now_epoch_ms),
        );

        // Update deadline cache if this expiration is sooner
        if new_expires_at < self.next_expiration_deadline {
            self.next_expiration_deadline = new_expires_at;
        }

        QueueResponse::Extended
    }

    /// Handle acknowledge operation
    pub fn handle_ack(&mut self, id: MessageId, token: u64) -> QueueResponse {
        use crate::utils::idempotency::{DedupIdentifier, DedupKey, Domain};

        let now = self.clock.now_instant();

        // Check deduplication store first (prevents re-processing completed operations)
        let dedup_key = DedupKey {
            realm: self.queue_key.realm.clone(),
            domain: Domain::Queue,
            identifier: DedupIdentifier::QueueComplete {
                family: self.queue_key.family.as_u64(),
                area: self.queue_key.area.clone(),
                resource: self.queue_key.resource.clone(),
                message_id: id.as_u64(),
                token,
            },
        };

        if let Some(cached_response) = self.dedup_store.get(&dedup_key) {
            tracing::debug!(
                realm = %self.queue_key.realm,
                area = %self.queue_key.area,
                resource = %self.queue_key.resource,
                message_id = id.as_u64(),
                token = token,
                "Queue COMPLETE deduplicated (returning cached response)"
            );
            // Deserialize cached response
            match bincode::deserialize(&cached_response) {
                Ok(resp) => return resp,
                Err(e) => {
                    tracing::warn!(
                        message_id = id.as_u64(),
                        token = token,
                        error = ?e,
                        "Failed to deserialize cached COMPLETE response, processing normally"
                    );
                }
            }
        }

        // Check if message is inflight
        let inflight = match self.inflight.get(&id) {
            Some(inflight) => inflight.clone(),
            None => {
                let response = QueueResponse::NotFound;
                // Cache negative response (prevents retries from hitting storage)
                if let Ok(bytes) = bincode::serialize(&response) {
                    self.dedup_store.record(dedup_key, bytes);
                }
                return response;
            }
        };

        // Validate token
        if inflight.token != token {
            Self::increment_counter(obs::METRIC_QUEUE_COMPLETE_REJECTED);
            let response = QueueResponse::InvalidToken;
            // Don't cache invalid token - security: wrong token should fail every time
            return response;
        }

        // Check if already expired
        if inflight.expires_at <= now {
            // Remove stale inflight entry
            self.inflight.remove(&id);
            Self::increment_counter(obs::METRIC_QUEUE_COMPLETE_REJECTED);
            let response = QueueResponse::InflightExpired;
            return response;
        }

        let (stored_layout, _record_visible_at_ms) = if let Some(record) = self.records.get(&id) {
            (
                self.record_layouts
                    .get(&id)
                    .copied()
                    .unwrap_or(StoredRecordLayout::EmbeddedHeader),
                record.visible_at_ms,
            )
        } else {
            match self.load_record_metadata_from_store(id) {
                Ok((record, layout)) => (layout, record.visible_at_ms),
                Err(_) => (StoredRecordLayout::EmbeddedHeader, 0),
            }
        };
        let index_plan = self.plan_index_mutation_for_unavailable_message(id);

        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);
        let legacy_key = self.cached_legacy_message_key(id);

        let commit_result = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(mut txn) => match Self::delete_record_for_layout(
                &mut txn,
                stored_layout,
                header_key,
                body_key,
                legacy_key,
            ) {
                Err(e) => {
                    tracing::warn!(
                        queue = ?self.queue_key,
                        route_family = self.queue_key.family.as_u64(),
                        message_id = id.as_u64(),
                        error = ?e,
                        "Failed to delete queue message in transaction"
                    );
                    Err(format!("Failed to delete message {} in txn: {:?}", id, e))
                }
                Ok(()) => {
                    if let Err(error) =
                        self.write_index_mutation_plan(&mut txn, id, index_plan, None)
                    {
                        return QueueResponse::Error { message: error };
                    }

                    match Self::commit_ack_transaction(txn, self.commit_write_options) {
                        Ok(()) => {
                            self.apply_index_mutation_plan(id, index_plan, None);
                            Ok(())
                        }
                        Err(e) => {
                            tracing::warn!(
                                queue = ?self.queue_key,
                                route_family = self.queue_key.family.as_u64(),
                                message_id = id.as_u64(),
                                error_reason = %e,
                                "Failed to commit queue delete transaction"
                            );
                            Err(format!(
                                "Failed to commit delete txn for message {}: {}",
                                id, e
                            ))
                        }
                    }
                }
            },
            Err(e) => Err(format!(
                "Failed to begin tx to delete message {}: {:?}",
                id, e
            )),
        };

        if let Err(message) = commit_result {
            return QueueResponse::Error { message };
        }

        self.inflight.remove(&id);
        self.evict_cached_record(id);
        self.evict_cached_body(id);

        let response = QueueResponse::Acked;

        // Cache successful completion response
        if let Ok(bytes) = bincode::serialize(&response) {
            self.dedup_store.record(dedup_key, bytes);
        }

        tracing::info!(
            realm = %self.queue_key.realm,
            area = %self.queue_key.area,
            resource = %self.queue_key.resource,
            message_id = id.as_u64(),
            "Queue COMPLETE processed successfully"
        );

        response
    }

    pub fn replay_dead_letter(&mut self, id: MessageId) -> Result<bool, String> {
        self.process_due_work();

        let (mut record, record_layout) = match self.load_full_record_for_admin_mutation(id) {
            Ok(record) => record,
            Err(_) => return Ok(false),
        };

        if !matches!(record.state, QueueState::Dlq) {
            return Ok(false);
        }

        let dead_lettered_at_ms = record
            .dead_lettered_at_ms
            .or_else(|| self.persisted_dlq.get(&id).copied())
            .unwrap_or(record.first_enqueued_at_ms);
        let ready_seq = self.next_ready_seq;
        let (ready_shard, ready_range) = Self::prepare_persisted_ready_append(
            self.persisted_ready_shards[Self::shard_for_id(id)]
                .back()
                .copied(),
            id,
        );

        record.state = QueueState::Ready;
        record.ready_seq = Some(ready_seq);
        record.attempts = 0;
        record.visible_at_ms = 0;
        record.inflight_token = None;
        record.inflight_expires_at_ms = None;
        record.dead_lettered_at_ms = None;
        record.dlq_reason = None;

        let cf_id = self.queue_key.family.id();
        let mut txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("Failed to begin replay tx for message {}: {:?}", id, e))?;

        self.write_record_as_split(&mut txn, id, &record, Some(record_layout))?;
        txn.delete(self.dlq_index_key(dead_lettered_at_ms, id))
            .map_err(|e| {
                format!(
                    "Failed to delete queue DLQ index for message {}: {:?}",
                    id, e
                )
            })?;
        txn.put(
            self.ready_range_key(ready_shard, ready_range.next),
            Self::encode_ready_range_value(ready_range),
            None,
        )
        .map_err(|e| {
            format!(
                "Failed to write queue ready index for message {}: {:?}",
                id, e
            )
        })?;
        txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                self.next_id_limit,
                (self.persisted_ready_count + 1) as u64,
                self.persisted_delayed.len() as u64,
                self.min_persisted_delayed_visibility_ms(),
            ),
            None,
        )
        .map_err(|e| {
            format!(
                "Failed to update queue index meta for message {}: {:?}",
                id, e
            )
        })?;
        txn.commit(self.commit_write_options)
            .map_err(|e| format!("Failed to commit replay tx for message {}: {:?}", id, e))?;

        self.remove_persisted_dlq(id);
        self.cache_record_state(id, &record, StoredRecordLayout::SplitHeaderBody);
        self.push_ready_entry(ready_seq, id);
        self.next_ready_seq = self.next_ready_seq.saturating_add(1);
        self.push_persisted_ready(id);

        Ok(true)
    }

    pub fn purge_dead_letter(&mut self, id: MessageId) -> Result<bool, String> {
        self.process_due_work();

        let (record, record_layout) = if let Some(record) = self.records.get(&id).cloned() {
            (
                record,
                self.record_layouts
                    .get(&id)
                    .copied()
                    .unwrap_or(StoredRecordLayout::EmbeddedHeader),
            )
        } else {
            match self.load_record_metadata_from_store(id) {
                Ok(record) => record,
                Err(_) => return Ok(false),
            }
        };

        if !matches!(record.state, QueueState::Dlq) {
            return Ok(false);
        }

        let dead_lettered_at_ms = record
            .dead_lettered_at_ms
            .or_else(|| self.persisted_dlq.get(&id).copied())
            .unwrap_or(record.first_enqueued_at_ms);
        let cf_id = self.queue_key.family.id();
        let mut txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("Failed to begin purge tx for message {}: {:?}", id, e))?;

        Self::delete_record_for_layout(
            &mut txn,
            record_layout,
            self.cached_header_key(id),
            self.cached_body_key(id),
            self.cached_legacy_message_key(id),
        )
        .map_err(|e| format!("Failed to delete message {} in purge tx: {:?}", id, e))?;
        txn.delete(self.dlq_index_key(dead_lettered_at_ms, id))
            .map_err(|e| {
                format!(
                    "Failed to delete queue DLQ index for message {}: {:?}",
                    id, e
                )
            })?;
        txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                self.next_id_limit,
                self.persisted_ready_count as u64,
                self.persisted_delayed.len() as u64,
                self.min_persisted_delayed_visibility_ms(),
            ),
            None,
        )
        .map_err(|e| {
            format!(
                "Failed to update queue index meta for message {}: {:?}",
                id, e
            )
        })?;
        txn.commit(self.commit_write_options)
            .map_err(|e| format!("Failed to commit purge tx for message {}: {:?}", id, e))?;

        self.remove_persisted_dlq(id);
        self.evict_cached_record(id);
        self.evict_cached_body(id);

        Ok(true)
    }

    /// Handle inflight expiration (internal timer event)
    /// Process delayed messages that are now visible
    /// Updates the cached deadline for the next delayed message check
    pub fn process_delayed_messages(&mut self) {
        let now = self.clock.now_instant();

        while let Some(Reverse(delayed)) = self.delayed.peek() {
            if delayed.visible_at > now {
                // Found next delayed message deadline, cache it
                self.next_delayed_deadline = delayed.visible_at;
                break;
            }

            // Pop now-visible message
            let delayed = self.delayed.pop().unwrap().0;

            // Add to ready queue
            self.push_ready(delayed.id);
        }

        // If no more delayed messages, set deadline to far future
        if self.delayed.is_empty() {
            self.next_delayed_deadline = now + Duration::from_secs(3600); // 1 hour
        }
    }

    /// Process only the timer and delayed-message work that is actually due.
    pub fn process_due_work(&mut self) {
        let now = self.clock.now_instant();

        if now >= self.next_expiration_deadline {
            self.process_expired_timers();
        }

        if now >= self.next_delayed_deadline {
            self.process_delayed_messages();
        }
    }
}

impl Actor for QueueActor {
    type Message = QueueMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        self.process_due_work();

        let response = match msg {
            QueueMessage::Send {
                body,
                delay_seconds,
                ..
            } => self.handle_send(body, delay_seconds),

            QueueMessage::Receive {
                inflight_seconds,
                batch_size,
                ..
            } => self.handle_receive(inflight_seconds, batch_size),

            QueueMessage::Extend {
                id,
                token,
                inflight_seconds,
                ..
            } => self.handle_extend(id, token, inflight_seconds),

            QueueMessage::Ack { id, token, .. } => self.handle_ack(id, token),

            QueueMessage::InflightExpired { id } => {
                self.handle_inflight_expired(id);
                return; // No response needed for internal timer message
            }
        };

        // Send response back to the client via reply
        let _ = ctx.reply(response).ok();
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // Recovery is handled during actor construction; started() is a no-op.
    }

    fn on_timer(&mut self, _timer_id: crate::runtime::context::TimerId, _ctx: &mut Context<Self>) {}
}

impl QueueActor {
    fn reset_recovery_state(&mut self) {
        self.reset_live_ready_state();
        self.reset_persisted_index_state();
        self.delayed.clear();
        let now = self.clock.now_instant();
        self.next_delayed_deadline = now + Duration::from_secs(3600);
    }

    fn populate_live_ready_from_persisted(&mut self, matured_delayed_ids: &[MessageId]) {
        self.reset_live_ready_state();

        let mut ready_ids =
            Vec::with_capacity(self.persisted_ready_count + matured_delayed_ids.len());
        for ranges in &self.persisted_ready_shards {
            for range in ranges {
                let mut id = range.next;
                while id <= range.end {
                    ready_ids.push(MessageId::new(id));
                    id = id.saturating_add(Self::READY_SHARDS as u64);
                }
            }
        }
        ready_ids.extend_from_slice(matured_delayed_ids);
        ready_ids.sort_unstable_by_key(|id| id.as_u64());
        ready_ids.dedup_by_key(|id| id.as_u64());

        for id in ready_ids {
            self.push_ready(id);
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::runtime::routing::RouteFamily;
    use crate::testkit::create_test_engine_with_cfs;
    use uuid::Uuid;

    /// Mock clock for deterministic testing
    #[derive(Clone)]
    pub struct MockClock {
        state: Arc<std::sync::Mutex<MockClockState>>,
    }

    #[derive(Clone, Copy)]
    struct MockClockState {
        instant: Instant,
        epoch_ms: u64,
    }

    impl Default for MockClock {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockClock {
        pub fn new() -> Self {
            Self {
                state: Arc::new(std::sync::Mutex::new(MockClockState {
                    instant: Instant::now(),
                    epoch_ms: 1_700_000_000_000, // deterministic-ish base
                })),
            }
        }

        pub fn advance(&self, duration: Duration) {
            let mut state = self.state.lock().unwrap();
            state.instant += duration;
            state.epoch_ms = state.epoch_ms.saturating_add(duration.as_millis() as u64);
        }
    }

    impl Clock for MockClock {
        fn now_instant(&self) -> Instant {
            self.state.lock().unwrap().instant
        }

        fn now_epoch_ms(&self) -> u64 {
            self.state.lock().unwrap().epoch_ms
        }
    }

    fn unique_queue_key(resource_prefix: &str) -> QueueKey {
        QueueKey {
            family: RouteFamily::new(0), /* CF=0 for Midge test limitation */
            realm: "test".to_string(),
            area: "queue".to_string(),
            resource: format!("{}-{}", resource_prefix, Uuid::new_v4()),
        }
    }

    fn send_and_reserve_single_message(actor: &mut QueueActor, body: &str) -> (MessageId, u64) {
        let send_response = actor.handle_send(Bytes::from(body.to_string()), None);
        let id = match send_response {
            QueueResponse::Sent { id } => id,
            _ => panic!("Expected Sent response"),
        };

        let receive_response = actor.handle_receive(30, Some(1));
        match receive_response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                (id, messages[0].token)
            }
            _ => panic!("Expected Received response"),
        }
    }

    fn read_index_meta(
        store: &Arc<cntryl_midge::MidgeEngine>,
        queue_key: &QueueKey,
    ) -> Option<Bytes> {
        let txn = store
            .begin_tx(
                queue_key.family.id(),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .expect("begin read tx");
        txn.get(&QueueActor::index_meta_key(queue_key))
            .expect("read index meta")
    }

    fn read_ready_index_ranges(
        store: &Arc<cntryl_midge::MidgeEngine>,
        queue_key: &QueueKey,
    ) -> Vec<(usize, ReadyRange)> {
        let txn = store
            .begin_tx(
                queue_key.family.id(),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .expect("begin read tx");
        let prefix = QueueActor::ready_index_prefix(queue_key);
        let query = cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&prefix));
        let mut iter = txn.scan(&query).expect("scan ready index");
        let mut ranges = Vec::new();

        while let Some((key, value)) = iter.next() {
            let (shard, start) =
                QueueActor::parse_ready_range_key(&key, &prefix).expect("parse ready key");
            let range = QueueActor::decode_ready_range(start, &value).expect("decode ready range");
            ranges.push((shard, range));
        }

        ranges.sort_unstable_by_key(|(shard, range)| (*shard, range.next));
        ranges
    }

    fn read_delayed_index_entries(
        store: &Arc<cntryl_midge::MidgeEngine>,
        queue_key: &QueueKey,
    ) -> Vec<(MessageId, u64)> {
        let txn = store
            .begin_tx(
                queue_key.family.id(),
                cntryl_midge::TransactionMode::ReadOnly,
            )
            .expect("begin read tx");
        let prefix = QueueActor::delayed_index_prefix(queue_key);
        let query = cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&prefix));
        let mut iter = txn.scan(&query).expect("scan delayed index");
        let mut entries = Vec::new();

        while let Some((key, _value)) = iter.next() {
            let (visible_at_ms, id) =
                QueueActor::parse_delayed_index_key(&key, &prefix).expect("parse delayed key");
            entries.push((id, visible_at_ms));
        }

        entries.sort_unstable_by_key(|(id, visible_at_ms)| (*visible_at_ms, id.as_u64()));
        entries
    }

    fn clear_queue_index(
        store: &Arc<cntryl_midge::MidgeEngine>,
        queue_key: &QueueKey,
        meta_override: Option<Vec<u8>>,
    ) {
        let cf_id = queue_key.family.id();
        let mut txn = store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write tx");
        let ready_prefix = QueueActor::ready_index_prefix(queue_key);
        let delayed_prefix = QueueActor::delayed_index_prefix(queue_key);
        let ready_query = cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&ready_prefix));
        let delayed_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&delayed_prefix));
        let mut ready_iter = txn.scan(&ready_query).expect("scan ready index");
        let mut delayed_iter = txn.scan(&delayed_query).expect("scan delayed index");
        let mut keys = Vec::new();

        while let Some((key, _)) = ready_iter.next() {
            keys.push(key.to_vec());
        }
        while let Some((key, _)) = delayed_iter.next() {
            keys.push(key.to_vec());
        }
        keys.push(QueueActor::index_meta_key(queue_key));

        for key in keys {
            txn.delete(key).expect("delete queue index key");
        }

        if let Some(meta) = meta_override {
            txn.put(QueueActor::index_meta_key(queue_key), meta, None)
                .expect("override index meta");
        }

        txn.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit index mutation");
    }

    fn put_queue_validation_row(store: &cntryl_midge::Engine, suffix: &[u8], value: Vec<u8>) {
        let mut txn = store
            .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin write tx");
        txn.put(
            storage_key::prefixed_key("test", DomainKeyspace::Queue, suffix),
            value,
            None,
        )
        .expect("write queue validation row");
        txn.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit queue validation row");
    }

    #[test]
    fn should_reject_malformed_authoritative_queue_rows_during_preflight() {
        // Arrange
        let cases = [
            (b"jobs:email:meta".as_slice(), b"broken".to_vec(), "meta"),
            (b"jobs:email:hdr:1".as_slice(), b"broken".to_vec(), "header"),
            (
                b"jobs:email:msg:1".as_slice(),
                b"broken".to_vec(),
                "legacy_message",
            ),
        ];

        // Act
        let errors = cases
            .into_iter()
            .map(|(suffix, value, category)| {
                let store = create_test_engine_with_cfs(vec![1]);
                put_queue_validation_row(store.as_ref(), suffix, value);
                (
                    QueueActor::validate_persisted_state_for_existing_families(store.as_ref())
                        .expect_err("malformed queue row should fail preflight"),
                    category,
                )
            })
            .collect::<Vec<_>>();

        // Assert
        assert!(errors
            .into_iter()
            .all(|(error, category)| error.contains(&format!("key_category={category}"))));
    }

    #[test]
    fn should_reject_missing_queue_body_for_split_header_during_preflight() {
        // Arrange
        let store = create_test_engine_with_cfs(vec![1]);
        put_queue_validation_row(
            store.as_ref(),
            b"jobs:email:hdr:1",
            QueueActor::encode_record_header(&QueueRecord::ready(
                Bytes::from_static(b"payload"),
                1,
                1,
                1_700_000_000_000,
            )),
        );

        // Act
        let result = QueueActor::validate_persisted_state_for_existing_families(store.as_ref());

        // Assert
        assert!(result
            .expect_err("missing queue body should fail preflight")
            .contains("missing body for split header"));
    }

    #[test]
    fn should_reject_orphan_queue_body_during_preflight() {
        // Arrange
        let store = create_test_engine_with_cfs(vec![1]);
        put_queue_validation_row(store.as_ref(), b"jobs:email:body:1", b"payload".to_vec());

        // Act
        let result = QueueActor::validate_persisted_state_for_existing_families(store.as_ref());

        // Assert
        assert!(result
            .expect_err("orphan queue body should fail preflight")
            .contains("orphan body row"));
    }

    #[test]
    fn should_reserve_enqueued_message() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs");
        // Use CF=0 here because the in-memory Midge test engine exposes only the default CF.
        // Production queues still use the normal RouteFamily -> CF mapping.
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Act
        let body = Bytes::from("test message");
        let enqueue_response = actor.handle_send(body.clone(), None);
        let msg_id = match enqueue_response {
            QueueResponse::Sent { id } => id,
            _ => panic!("Expected Enqueued response"),
        };

        // Assert
        assert_eq!(actor.ready_len(), 1);
        let reserve_response = actor.handle_receive(30, Some(1));
        match reserve_response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, msg_id);
                assert_eq!(messages[0].body, body);
                assert_eq!(messages[0].attempts, 1);
                assert_eq!(messages[0].inflight_seconds, 30);
            }
            _ => panic!("Expected Received response"),
        }
        assert_eq!(actor.ready_len(), 0);
        assert_eq!(actor.inflight.len(), 1);
    }

    #[test]
    fn should_bound_hot_body_cache_size() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-body-cache");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Act
        for i in 0..(QueueActor::BODY_CACHE_LIMIT + 32) {
            let body = Bytes::from(format!("message-{}", i));
            let response = actor.handle_send(body, None);
            assert!(matches!(response, QueueResponse::Sent { .. }));
        }

        // Assert
        assert!(actor.records.len() <= QueueActor::RECORD_CACHE_LIMIT);
        assert_eq!(actor.body_cache.len(), QueueActor::BODY_CACHE_LIMIT);
        assert!(actor.body_cache_bytes <= QueueActor::BODY_CACHE_LIMIT_BYTES);
    }

    #[test]
    fn should_bound_metadata_cache_size() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-record-cache");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Act
        for i in 0..(QueueActor::RECORD_CACHE_LIMIT + 32) {
            let body = Bytes::from(format!("message-{}", i));
            let response = actor.handle_send(body, None);
            assert!(matches!(response, QueueResponse::Sent { .. }));
        }

        // Assert
        assert_eq!(actor.records.len(), QueueActor::RECORD_CACHE_LIMIT);
        let max_fifo_len = QueueActor::RECORD_CACHE_LIMIT
            * QueueActor::RECORD_CACHE_FIFO_SLACK_MULTIPLIER
            + actor.records.len();
        assert!(actor.record_cache_fifo.len() <= max_fifo_len);
    }

    #[test]
    fn should_bound_hot_body_cache_total_bytes() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-body-cache-bytes");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Act
        let body_size = QueueActor::BODY_CACHE_LIMIT_BYTES / 4 + 1;
        for i in 0..5 {
            let body = Bytes::from(vec![i as u8; body_size]);
            let response = actor.handle_send(body, None);
            assert!(matches!(response, QueueResponse::Sent { .. }));
        }

        // Assert
        assert_eq!(actor.records.len(), 5);
        assert!(actor.body_cache.len() < 5);
        assert!(actor.body_cache_bytes <= QueueActor::BODY_CACHE_LIMIT_BYTES);
    }

    #[test]
    fn should_not_hydrate_metadata_cache_during_recovery() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-recovery-meta");

        {
            let mut actor = QueueActor::new(
                RouteFamily::new(0),
                queue_key.clone(),
                store.clone(),
                None,
                crate::utils::idempotency::default_dedup_store(),
            );

            for i in 0..64 {
                let body = Bytes::from(format!("recovered-{}", i));
                let response = actor.handle_send(body, None);
                assert!(matches!(response, QueueResponse::Sent { .. }));
            }
        }

        // Act
        let actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Assert
        assert_eq!(actor.ready_len(), 64);
        assert!(actor.records.is_empty());
        assert!(actor.record_cache_fifo.is_empty());
        assert!(actor.body_cache.is_empty());
        assert_eq!(actor.recovery_path, RecoveryPath::IndexHit);
        assert!(QueueActor::index_meta_is_valid(
            &read_index_meta(&store, &queue_key).expect("index meta should exist")
        ));
        assert!(!read_ready_index_ranges(&store, &queue_key).is_empty());
    }

    #[test]
    fn should_rewrite_missing_queue_index_via_fallback() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-missing-index");

        {
            let mut actor = QueueActor::new(
                RouteFamily::new(0),
                queue_key.clone(),
                store.clone(),
                None,
                crate::utils::idempotency::default_dedup_store(),
            );

            for i in 0..12 {
                let response = actor.handle_send(Bytes::from(format!("visible-{}", i)), None);
                assert!(matches!(response, QueueResponse::Sent { .. }));
            }
        }

        clear_queue_index(&store, &queue_key, None);

        // Act
        let actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Assert
        assert_eq!(actor.ready_len(), 12);
        assert_eq!(actor.recovery_path, RecoveryPath::IndexMissingFallback);
        assert!(QueueActor::index_meta_is_valid(
            &read_index_meta(&store, &queue_key).expect("rewritten index meta should exist")
        ));
        assert!(!read_ready_index_ranges(&store, &queue_key).is_empty());
    }

    #[test]
    fn should_rewrite_corrupted_queue_index_meta_via_fallback() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-corrupt-index");

        {
            let mut actor = QueueActor::new(
                RouteFamily::new(0),
                queue_key.clone(),
                store.clone(),
                None,
                crate::utils::idempotency::default_dedup_store(),
            );

            for i in 0..8 {
                let response = actor.handle_send(Bytes::from(format!("task-{}", i)), None);
                assert!(matches!(response, QueueResponse::Sent { .. }));
            }
        }

        clear_queue_index(&store, &queue_key, Some(vec![0, 0]));

        // Act
        let actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Assert
        assert_eq!(actor.ready_len(), 8);
        assert_eq!(actor.recovery_path, RecoveryPath::IndexInvalidFallback);
        assert!(QueueActor::index_meta_is_valid(
            &read_index_meta(&store, &queue_key).expect("rewritten index meta should exist")
        ));
    }

    #[test]
    fn should_plan_ready_index_mutations_for_persisted_ready_ranges() {
        // Arrange
        let mut shards: Vec<VecDeque<ReadyRange>> = (0..QueueActor::READY_SHARDS)
            .map(|_| VecDeque::new())
            .collect();
        for id in [1_u64, 9, 17, 25] {
            QueueActor::stage_persisted_ready_append(&mut shards, MessageId::new(id));
        }

        // Act
        let (shard, mutation) = QueueActor::plan_ready_index_mutation(&shards, MessageId::new(1))
            .expect("head mutation");
        // Assert
        assert_eq!(shard, 1);
        assert_eq!(
            mutation,
            PersistedReadyMutation::Replace {
                removed: ReadyRange { next: 1, end: 25 },
                inserted: ReadyRange { next: 9, end: 25 },
            }
        );

        let (shard, mutation) = QueueActor::plan_ready_index_mutation(&shards, MessageId::new(25))
            .expect("tail mutation");
        assert_eq!(shard, 1);
        assert_eq!(
            mutation,
            PersistedReadyMutation::Replace {
                removed: ReadyRange { next: 1, end: 25 },
                inserted: ReadyRange { next: 1, end: 17 },
            }
        );

        let mut split_shards = shards.clone();
        let (shard, mutation) =
            QueueActor::plan_ready_index_mutation(&split_shards, MessageId::new(17))
                .expect("middle mutation");
        assert_eq!(shard, 1);
        assert_eq!(
            mutation,
            PersistedReadyMutation::Split {
                removed: ReadyRange { next: 1, end: 25 },
                left: ReadyRange { next: 1, end: 9 },
                right: ReadyRange { next: 25, end: 25 },
            }
        );
        QueueActor::apply_ready_index_mutation_to_shards(&mut split_shards, shard, mutation);
        assert_eq!(split_shards[1].len(), 2);
    }

    #[test]
    fn should_remove_delayed_index_entry_after_ack_even_when_visibility_passed() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-delayed-index");
        let clock = MockClock::new();

        {
            let mut actor = QueueActor::with_clock(
                RouteFamily::new(0),
                queue_key.clone(),
                store.clone(),
                Box::new(clock.clone()),
                None,
                crate::utils::idempotency::default_dedup_store(),
            );
            let response = actor.handle_send(Bytes::from("delayed"), Some(1));
            assert!(matches!(response, QueueResponse::Sent { .. }));
        }

        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        assert_eq!(actor.ready_len(), 0);
        assert_eq!(read_delayed_index_entries(&store, &queue_key).len(), 1);

        // Act
        clock.advance(Duration::from_secs(2));
        actor.process_delayed_messages();

        let reserved = match actor.handle_receive(30, Some(1)) {
            QueueResponse::Received { messages } => messages,
            other => panic!("Expected Received response, got {:?}", other),
        };
        assert_eq!(reserved.len(), 1);

        let message = &reserved[0];
        assert!(matches!(
            actor.handle_ack(message.id, message.token),
            QueueResponse::Acked
        ));
        // Assert
        assert!(read_delayed_index_entries(&store, &queue_key).is_empty());
    }

    #[test]
    fn should_recover_legacy_combined_records_after_storage_split() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-legacy-storage");
        let msg_id = MessageId::new(1);
        let cf_id = queue_key.family.id();
        let mut txn = store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin tx");

        txn.put(
            QueueActor::meta_key(&queue_key),
            2_u64.to_le_bytes().to_vec(),
            None,
        )
        .expect("write queue meta");
        txn.put(
            QueueActor::legacy_message_key(&queue_key, msg_id),
            QueueActor::encode_legacy_record(&QueueRecord::loaded(
                Bytes::from_static(b"legacy-message"),
                0,
                1_700_000_000_000,
            )),
            None,
        )
        .expect("write legacy queue record");
        txn.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit legacy queue record");

        // Act
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Assert
        assert_eq!(actor.ready_len(), 1);

        match actor.handle_receive(30, Some(1)) {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, msg_id);
                assert_eq!(messages[0].body, Bytes::from_static(b"legacy-message"));
            }
            _ => panic!("Expected Received response"),
        }
    }

    #[test]
    fn should_hydrate_oversized_body_from_store_without_caching() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-oversized-body");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Act
        let oversized = Bytes::from(vec![b'x'; QueueActor::BODY_CACHE_LIMIT_BYTES + 1]);
        let response = actor.handle_send(oversized.clone(), None);
        assert!(matches!(response, QueueResponse::Sent { .. }));

        match actor.handle_receive(30, Some(1)) {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].body, oversized);
            }
            _ => panic!("Expected Received response"),
        }

        // Assert
        assert_eq!(actor.body_cache.len(), 0);
        assert_eq!(actor.body_cache_bytes, 0);
    }

    #[test]
    fn should_preserve_ready_body_cache_when_receiving_uncached_message() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-receive-cache-preserve");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );
        let mut ids = Vec::with_capacity(QueueActor::BODY_CACHE_LIMIT + 1);

        for i in 0..(QueueActor::BODY_CACHE_LIMIT + 1) {
            let body = Bytes::from(format!("message-{}", i));
            let response = actor.handle_send(body, None);
            let id = match response {
                QueueResponse::Sent { id } => id,
                _ => panic!("Expected Sent response"),
            };
            ids.push(id);
        }

        let first_id = ids[0];
        let second_id = ids[1];
        assert!(!actor.body_cache.contains_key(&first_id));
        assert!(actor.body_cache.contains_key(&second_id));
        assert_eq!(actor.body_cache.len(), QueueActor::BODY_CACHE_LIMIT);

        // Act
        let response = actor.handle_receive(30, Some(1));

        // Assert
        match response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, first_id);
            }
            _ => panic!("Expected Received response"),
        }
        assert!(actor.body_cache.contains_key(&second_id));
        assert_eq!(actor.body_cache.len(), QueueActor::BODY_CACHE_LIMIT);
    }

    #[test]
    fn should_evict_reserved_message_body_from_cache_on_receive() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-receive-cache-evict");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );
        let message_id = match actor.handle_send(Bytes::from("cached message"), None) {
            QueueResponse::Sent { id } => id,
            _ => panic!("Expected Sent response"),
        };
        assert!(actor.body_cache.contains_key(&message_id));

        // Act
        let response = actor.handle_receive(30, Some(1));

        // Assert
        match response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, message_id);
            }
            _ => panic!("Expected Received response"),
        }
        assert!(!actor.body_cache.contains_key(&message_id));
    }

    #[test]
    fn should_compact_hot_body_fifo_under_cache_churn() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-body-cache-churn");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Act
        for i in 0..(QueueActor::BODY_CACHE_LIMIT * 3) {
            let body = Bytes::from(format!("message-{}", i));
            let response = actor.handle_send(body, None);
            let id = match response {
                QueueResponse::Sent { id } => id,
                _ => panic!("Expected Sent response"),
            };
            let response = actor.handle_receive(30, Some(1));
            let token = match response {
                QueueResponse::Received { messages } => {
                    assert_eq!(messages.len(), 1);
                    messages[0].token
                }
                _ => panic!("Expected Received response"),
            };
            assert_eq!(actor.handle_ack(id, token), QueueResponse::Acked);
        }

        // Assert
        let max_fifo_len = QueueActor::BODY_CACHE_LIMIT
            * QueueActor::BODY_CACHE_FIFO_SLACK_MULTIPLIER
            + actor.body_cache.len();
        assert!(actor.body_cache.is_empty());
        assert!(actor.body_cache_bytes == 0);
        assert!(actor.body_cache_fifo.len() <= max_fifo_len);
    }

    #[test]
    fn should_return_empty_when_reserving_empty_queue() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-empty");
        let mut actor = QueueActor::new(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Act
        let response = actor.handle_receive(30, Some(10));

        // Assert - empty queue returns Received with no messages (not NotFound)
        match response {
            QueueResponse::NotFound => {}
            QueueResponse::Received { messages } if messages.is_empty() => {}
            _ => panic!("Expected NotFound or empty Received response for empty queue"),
        }
    }

    #[test]
    fn should_complete_message_with_valid_token() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-complete");
        let mut actor = QueueActor::new(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        let body = Bytes::from("test message");
        actor.handle_send(body, None);
        let reserve_response = actor.handle_receive(30, Some(1));

        let (msg_id, token) = match reserve_response {
            QueueResponse::Received { messages } => (messages[0].id, messages[0].token),
            _ => panic!("Expected Received response"),
        };

        // Act
        let response = actor.handle_ack(msg_id, token);

        // Assert
        assert_eq!(response, QueueResponse::Acked);
        assert_eq!(actor.inflight.len(), 0);
    }

    #[test]
    fn should_return_error_when_ack_commit_fails() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-ack-commit-failure");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        let sent = actor.handle_send(Bytes::from("test message"), None);
        let id = match sent {
            QueueResponse::Sent { id } => id,
            _ => panic!("Expected Sent response"),
        };
        let reserved = match actor.handle_receive(30, Some(1)) {
            QueueResponse::Received { messages } => messages,
            _ => panic!("Expected Received response"),
        };
        let token = reserved[0].token;
        QueueActor::fail_next_ack_commit_for_tests();

        // Act
        let response = actor.handle_ack(id, token);

        // Assert
        assert!(matches!(response, QueueResponse::Error { .. }));
    }

    #[test]
    fn should_keep_message_ready_when_receive_hydration_fails() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-hydrate-failure");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );
        let message_id = match actor.handle_send(Bytes::from("test message"), None) {
            QueueResponse::Sent { id } => id,
            other => panic!("Expected Sent response, found {other:?}"),
        };
        actor.evict_cached_record(message_id);
        actor.evict_cached_body(message_id);
        let mut txn = store
            .begin_tx(
                queue_key.family.id(),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .expect("begin write tx");
        txn.delete(QueueActor::header_key(&queue_key, message_id))
            .expect("delete queue header");
        txn.commit(cntryl_midge::WriteOptions::buffered())
            .expect("commit queue header delete");

        // Act
        let response = actor.handle_receive(30, Some(1));

        // Assert
        match response {
            QueueResponse::Received { messages } => assert!(messages.is_empty()),
            other => panic!("Expected empty Received response, found {other:?}"),
        }
        assert_eq!(actor.ready_len(), 1);
        assert_eq!(actor.inflight.len(), 0);
        assert!(actor.ready_contains(message_id));
    }

    #[test]
    fn should_complete_message_when_cached_complete_response_is_invalid() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let dedup_store = crate::utils::idempotency::default_dedup_store();
        let queue_key = unique_queue_key("jobs-invalid-complete-cache");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store,
            None,
            dedup_store.clone(),
        );
        let (message_id, token) = send_and_reserve_single_message(&mut actor, "test message");
        dedup_store.record(
            crate::utils::idempotency::DedupKey {
                realm: queue_key.realm.clone(),
                domain: crate::utils::idempotency::Domain::Queue,
                identifier: crate::utils::idempotency::DedupIdentifier::QueueComplete {
                    family: queue_key.family.as_u64(),
                    area: queue_key.area.clone(),
                    resource: queue_key.resource.clone(),
                    message_id: message_id.as_u64(),
                    token,
                },
            },
            vec![0xFF, 0xAA, 0x55],
        );

        // Act
        let response = actor.handle_ack(message_id, token);

        // Assert
        assert_eq!(response, QueueResponse::Acked);
        assert_eq!(actor.inflight.len(), 0);
    }

    #[test]
    fn should_keep_inflight_message_when_redelivery_commit_fails() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-redelivery-commit-fail");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );
        let (msg_id, _) = send_and_reserve_single_message(&mut actor, "test message");
        clock.advance(Duration::from_secs(31));
        QueueActor::fail_next_redelivery_commit_for_tests();

        // Act
        actor.process_expired_timers();

        // Assert
        assert_eq!(actor.ready_len(), 0);
        assert_eq!(actor.inflight.len(), 1);
        assert!(actor.inflight.contains_key(&msg_id));
        assert!(!actor.ready_contains(msg_id));
    }

    #[test]
    fn should_redeliver_message_on_retry_sweep_after_redelivery_commit_failure() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-redelivery-retry");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );
        let (msg_id, _) = send_and_reserve_single_message(&mut actor, "test message");
        clock.advance(Duration::from_secs(31));
        QueueActor::fail_next_redelivery_commit_for_tests();
        actor.process_expired_timers();
        clock.advance(Duration::from_secs(1));

        // Act
        actor.process_expired_timers();

        // Assert
        assert_eq!(actor.ready_len(), 1);
        assert_eq!(actor.inflight.len(), 0);
        assert!(actor.ready_contains(msg_id));
    }

    #[test]
    fn should_reject_complete_with_invalid_token() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-invalid-token");
        let mut actor = QueueActor::new(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        let body = Bytes::from("test message");
        actor.handle_send(body, None);
        let reserve_response = actor.handle_receive(30, Some(1));

        let msg_id = match reserve_response {
            QueueResponse::Received { messages } => messages[0].id,
            _ => panic!("Expected Received response"),
        };

        // Act
        let response = actor.handle_ack(msg_id, 99999);

        // Assert
        assert_eq!(response, QueueResponse::InvalidToken);
        assert_eq!(actor.inflight.len(), 1);
    }

    #[test]
    fn should_isolate_ack_dedup_given_different_queue_resources() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let shared_dedup_store = crate::utils::idempotency::default_dedup_store();
        let first_key = unique_queue_key("jobs-dedup-a");
        let second_key = unique_queue_key("jobs-dedup-b");
        let mut first_actor = QueueActor::new(
            RouteFamily::new(0),
            first_key,
            store.clone(),
            None,
            shared_dedup_store.clone(),
        );
        let mut second_actor = QueueActor::new(
            RouteFamily::new(0),
            second_key,
            store,
            None,
            shared_dedup_store,
        );
        let (first_id, first_token) = send_and_reserve_single_message(&mut first_actor, "first");
        let (second_id, second_token) =
            send_and_reserve_single_message(&mut second_actor, "second");
        if second_token == first_token {
            second_actor
                .inflight
                .get_mut(&second_id)
                .expect("second inflight message")
                .token = first_token.wrapping_add(1);
        }

        // Act
        let first_response = first_actor.handle_ack(first_id, first_token);
        let second_response = second_actor.handle_ack(second_id, first_token);

        // Assert
        assert_eq!(first_id, second_id);
        assert_eq!(first_response, QueueResponse::Acked);
        assert_eq!(second_response, QueueResponse::InvalidToken);
    }

    #[test]
    fn should_isolate_ack_dedup_given_different_route_families() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1, 2]);
        let shared_dedup_store = crate::utils::idempotency::default_dedup_store();
        let first_key = QueueKey {
            family: RouteFamily::new(1),
            realm: "test".to_string(),
            area: "queue".to_string(),
            resource: format!("jobs-family-{}", Uuid::new_v4()),
        };
        let second_key = QueueKey {
            family: RouteFamily::new(2),
            realm: first_key.realm.clone(),
            area: first_key.area.clone(),
            resource: first_key.resource.clone(),
        };
        let mut first_actor = QueueActor::new(
            RouteFamily::new(1),
            first_key,
            store.clone(),
            None,
            shared_dedup_store.clone(),
        );
        let mut second_actor = QueueActor::new(
            RouteFamily::new(2),
            second_key,
            store,
            None,
            shared_dedup_store,
        );
        let (first_id, first_token) = send_and_reserve_single_message(&mut first_actor, "first");
        let (second_id, second_token) =
            send_and_reserve_single_message(&mut second_actor, "second");
        if second_token == first_token {
            second_actor
                .inflight
                .get_mut(&second_id)
                .expect("second inflight message")
                .token = first_token.wrapping_add(1);
        }

        // Act
        let first_response = first_actor.handle_ack(first_id, first_token);
        let second_response = second_actor.handle_ack(second_id, first_token);

        // Assert
        assert_eq!(first_id, second_id);
        assert_eq!(first_response, QueueResponse::Acked);
        assert_eq!(second_response, QueueResponse::InvalidToken);
    }

    #[test]
    fn should_extend_inflight_with_valid_token() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-extend");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        let body = Bytes::from("test message");
        actor.handle_send(body, None);
        let reserve_response = actor.handle_receive(30, Some(1));

        let (msg_id, token) = match reserve_response {
            QueueResponse::Received { messages } => (messages[0].id, messages[0].token),
            _ => panic!("Expected Received response"),
        };

        let old_expiry = actor.inflight.get(&msg_id).unwrap().expires_at;

        // Act
        clock.advance(Duration::from_secs(15));
        let response = actor.handle_extend(msg_id, token, 60);

        // Assert
        assert_eq!(response, QueueResponse::Extended);
        let new_expiry = actor.inflight.get(&msg_id).unwrap().expires_at;
        assert!(new_expiry > old_expiry);
    }

    #[test]
    fn should_reject_extend_with_invalid_token() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-extend-invalid");
        let mut actor = QueueActor::new(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        let body = Bytes::from("test message");
        actor.handle_send(body, None);
        let reserve_response = actor.handle_receive(30, Some(1));

        let msg_id = match reserve_response {
            QueueResponse::Received { messages } => messages[0].id,
            _ => panic!("Expected Received response"),
        };

        // Act
        let response = actor.handle_extend(msg_id, 99999, 60);

        // Assert
        assert_eq!(response, QueueResponse::InvalidToken);
    }

    #[test]
    fn should_redeliver_message_when_inflight_expires() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-redelivery");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        let body = Bytes::from("test message");
        actor.handle_send(body.clone(), None);
        let reserve_response = actor.handle_receive(30, Some(1));

        let msg_id = match reserve_response {
            QueueResponse::Received { messages } => messages[0].id,
            _ => panic!("Expected Received response"),
        };

        assert_eq!(actor.ready_len(), 0);
        assert_eq!(actor.inflight.len(), 1);

        // Act
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();

        // Message should return to the ready queue before redelivery.
        assert_eq!(actor.ready_len(), 1);
        assert_eq!(actor.inflight.len(), 0);
        assert!(actor.ready_contains(msg_id));

        // Reserve again after expiration.
        let redelivery_response = actor.handle_receive(30, Some(1));

        // Assert
        match redelivery_response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, msg_id);
                assert_eq!(messages[0].attempts, 2); // Incremented from 1 to 2
            }
            _ => panic!("Expected Received response"),
        }
    }

    #[test]
    fn should_reserve_multiple_messages_in_batch() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-reserve-batch");
        let mut actor = QueueActor::new(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Enqueue 5 messages
        for i in 0..5 {
            let body = Bytes::from(format!("message {}", i));
            actor.handle_send(body, None);
        }

        // Act
        let response = actor.handle_receive(30, Some(3));

        // Assert
        match response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 3);
                assert_eq!(actor.ready_len(), 2); // 2 remaining
                assert_eq!(actor.inflight.len(), 3);
            }
            _ => panic!("Expected Received response"),
        }
    }

    #[test]
    fn should_dequeue_all_enqueued_messages() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("fifo-order");
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Seed the queue in a known order.
        for i in 0..5 {
            let body = Bytes::from(format!("msg-{}", i));
            let _ = actor.handle_send(body, None);
        }

        // Act
        let mut reserved_all = Vec::new();
        loop {
            match actor.handle_receive(30, Some(2)) {
                QueueResponse::Received { messages } => {
                    if messages.is_empty() {
                        // Queue is empty (actor returns empty Received, not NotFound)
                        break;
                    }
                    for m in messages {
                        reserved_all.push(m.body);
                        // Simulate immediate completion to prevent redelivery
                        let _ = actor.handle_ack(m.id, m.token);
                    }
                }
                QueueResponse::NotFound => {
                    // Queue is empty, we're done
                    break;
                }
                _ => panic!("Expected Received or NotFound response"),
            }
        }

        // Assert
        let mut expected: Vec<Bytes> = (0..5).map(|i| Bytes::from(format!("msg-{}", i))).collect();
        reserved_all.sort();
        expected.sort();
        assert_eq!(reserved_all, expected);
    }

    #[test]
    fn should_ignore_stale_timer_after_extend() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-stale-timer");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        let body = Bytes::from("test message");
        actor.handle_send(body, None);
        let reserve_response = actor.handle_receive(30, Some(1));

        let (msg_id, token) = match reserve_response {
            QueueResponse::Received { messages } => (messages[0].id, messages[0].token),
            _ => panic!("Expected Received response"),
        };
        let initial_epoch = actor.inflight.get(&msg_id).unwrap().inflight_epoch;

        // Act - Extend before first timer expires
        clock.advance(Duration::from_secs(15));
        actor.handle_extend(msg_id, token, 60);
        let extended_epoch = actor.inflight.get(&msg_id).unwrap().inflight_epoch;

        // Advance to first timer expiration (30s total)
        clock.advance(Duration::from_secs(15));
        actor.process_expired_timers();

        // Assert - Message still inflight (stale timer ignored)
        assert!(extended_epoch > initial_epoch);
        assert_eq!(actor.ready_len(), 0);
        assert_eq!(actor.inflight.len(), 1);
    }

    #[test]
    fn should_reject_operations_on_expired_inflight() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-expired-inflight");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        let body = Bytes::from("test message");
        actor.handle_send(body, None);
        let reserve_response = actor.handle_receive(30, Some(1));

        let (msg_id, token) = match reserve_response {
            QueueResponse::Received { messages } => (messages[0].id, messages[0].token),
            _ => panic!("Expected Received response"),
        };

        // Act - Advance past expiration
        clock.advance(Duration::from_secs(31));

        // Assert - Extend fails (entry would be cleaned up if timer processed)
        // Since we're calling directly without going through actor receive,
        // the entry still exists but is expired
        let _extend_response = actor.handle_extend(msg_id, token, 60);
        // However the logic checks expiration first, so it returns InflightExpired
        // before being removed, OR it could be NotFound if already cleaned up
        // Let's process timers explicitly to make this deterministic
        actor.process_expired_timers();

        // Now the entry is definitely gone
        let extend_response2 = actor.handle_extend(msg_id, token, 60);
        assert_eq!(extend_response2, QueueResponse::NotFound);

        let complete_response = actor.handle_ack(msg_id, token);
        assert_eq!(complete_response, QueueResponse::NotFound);
    }

    #[test]
    fn should_return_not_found_for_nonexistent_message() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-not-found");
        let mut actor = QueueActor::new(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            None,
            crate::utils::idempotency::default_dedup_store(),
        );
        let fake_id = MessageId::new(99999);

        // Act
        let extend_response = actor.handle_extend(fake_id, 12345, 60);
        let complete_response = actor.handle_ack(fake_id, 12345);

        // Assert
        assert_eq!(extend_response, QueueResponse::NotFound);
        assert_eq!(complete_response, QueueResponse::NotFound);
    }

    #[test]
    fn should_delay_message_visibility() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-delay");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        // Act
        let body = Bytes::from("delayed message");
        let response = actor.handle_send(body.clone(), Some(30));

        let msg_id = match response {
            QueueResponse::Sent { id } => id,
            _ => panic!("Expected Enqueued response"),
        };

        // Message should stay delayed until visibility expires.
        assert_eq!(actor.ready_len(), 0);
        assert_eq!(actor.delayed.len(), 1);

        // Immediate reserve should still be empty.
        let reserve_response = actor.handle_receive(30, Some(1));
        match reserve_response {
            QueueResponse::NotFound => {}
            QueueResponse::Received { messages } if messages.is_empty() => {}
            _ => panic!("Expected NotFound or empty Received response for delayed messages"),
        }

        // Advance time past the visibility delay.
        clock.advance(Duration::from_secs(31));
        actor.process_delayed_messages();

        // The message should now be ready.
        assert_eq!(actor.ready_len(), 1);
        assert_eq!(actor.delayed.len(), 0);

        // Final reserve should succeed.
        let reserve_response = actor.handle_receive(30, Some(1));
        // Assert
        match reserve_response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, msg_id);
                assert_eq!(messages[0].body, body);
            }
            _ => panic!("Expected Received response"),
        }
    }

    #[test]
    fn should_move_to_dlq_after_max_attempts() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-dlq");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key.clone(),
            store.clone(),
            Box::new(clock.clone()),
            Some(3), // max_attempts = 3
            crate::utils::idempotency::default_dedup_store(),
        );

        // Act - Enqueue message
        let body = Bytes::from("test message");
        let enqueue_response = actor.handle_send(body.clone(), None);
        let msg_id = match enqueue_response {
            QueueResponse::Sent { id } => id,
            _ => panic!("Expected Sent response"),
        };

        // Simulate 3 failed delivery attempts
        for attempt in 1..=3 {
            // Reserve
            let reserve_response = actor.handle_receive(30, Some(1));
            match reserve_response {
                QueueResponse::Received { messages } => {
                    assert_eq!(messages.len(), 1);
                    assert_eq!(messages[0].attempts, attempt);
                }
                _ => panic!("Expected Reserved response on attempt {}", attempt),
            }

            // Expire inflight entry (simulating failed processing)
            clock.advance(Duration::from_secs(31));
            actor.process_expired_timers();

            if attempt < 3 {
                // Should be back in ready queue
                assert_eq!(actor.ready_len(), 1);
                assert_eq!(actor.inflight.len(), 0);
            }
        }

        // Assert
        assert_eq!(actor.ready_len(), 0);
        assert_eq!(actor.inflight.len(), 0);
        assert_eq!(actor.dlq_count, 1);

        let (record, layout) = actor
            .load_record_metadata_from_store(msg_id)
            .expect("dlq record should remain in storage");
        assert_eq!(layout, StoredRecordLayout::SplitHeaderBody);
        assert_eq!(record.state, QueueState::Dlq);
        assert_eq!(record.attempts, 3);
        assert!(record.dead_lettered_at_ms.is_some());
        assert_eq!(record.dlq_reason, Some(DlqReason::MaxAttemptsExceeded));

        let cf_id = queue_key.family.id();
        let txn = store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin tx");
        let header_result = txn
            .get(&QueueActor::header_key(&queue_key, msg_id))
            .expect("midge get header");
        assert!(
            header_result.is_some(),
            "DLQ header should remain in storage"
        );
        let body_result = txn
            .get(&QueueActor::body_key(&queue_key, msg_id))
            .expect("midge get body");
        assert!(body_result.is_some(), "DLQ body should remain in storage");
    }

    #[test]
    fn should_not_requeue_dlq_message_after_restart() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-dlq-restart");
        let msg_id = {
            let mut actor = QueueActor::with_clock(
                RouteFamily::new(0),
                queue_key.clone(),
                store.clone(),
                Box::new(clock.clone()),
                Some(2),
                crate::utils::idempotency::default_dedup_store(),
            );

            let enqueue_response = actor.handle_send(Bytes::from("test message"), None);
            let msg_id = match enqueue_response {
                QueueResponse::Sent { id } => id,
                _ => panic!("Expected Sent response"),
            };

            for _ in 0..2 {
                match actor.handle_receive(30, Some(1)) {
                    QueueResponse::Received { messages } => assert_eq!(messages.len(), 1),
                    other => panic!("Expected Received response, found {other:?}"),
                }
                clock.advance(Duration::from_secs(31));
                actor.process_expired_timers();
            }

            assert_eq!(actor.dlq_count, 1);
            msg_id
        };

        // Act
        let mut recovered = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key,
            store,
            Box::new(clock),
            Some(2),
            crate::utils::idempotency::default_dedup_store(),
        );
        let reserve_response = recovered.handle_receive(30, Some(1));

        // Assert
        assert_eq!(recovered.ready_len(), 0);
        assert_eq!(recovered.dlq_count, 1);
        match reserve_response {
            QueueResponse::NotFound => {}
            QueueResponse::Received { messages } => assert!(messages.is_empty()),
            other => panic!("Expected empty queue after restart, found {other:?}"),
        }

        let (record, layout) = recovered
            .load_record_metadata_from_store(msg_id)
            .expect("dlq record should remain in storage after restart");
        assert_eq!(layout, StoredRecordLayout::SplitHeaderBody);
        assert_eq!(record.state, QueueState::Dlq);
    }

    #[test]
    fn should_report_admin_dead_letters_given_retained_dlq_messages() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-admin-dlq");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key,
            store,
            Box::new(clock.clone()),
            Some(2),
            crate::utils::idempotency::default_dedup_store(),
        );
        let msg_id = match actor.handle_send(Bytes::from("test message"), None) {
            QueueResponse::Sent { id } => id,
            other => panic!("Expected Sent response, found {other:?}"),
        };
        for _ in 0..2 {
            match actor.handle_receive(30, Some(1)) {
                QueueResponse::Received { messages } => assert_eq!(messages.len(), 1),
                other => panic!("Expected Received response, found {other:?}"),
            }
            clock.advance(Duration::from_secs(31));
            actor.process_expired_timers();
        }

        // Act
        let snapshot = actor.admin_snapshot();
        let dead_letters = actor.admin_dead_letters();

        // Assert
        assert_eq!(snapshot.messages_ready, 0);
        assert_eq!(snapshot.messages_delayed, 0);
        assert_eq!(snapshot.messages_inflight, 0);
        assert_eq!(snapshot.messages_dead_lettered, 1);
        assert_eq!(snapshot.messages_total, 1);
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].message_id, msg_id.as_u64());
        assert_eq!(dead_letters[0].attempts, 2);
        assert_eq!(dead_letters[0].reason, "max_attempts_exceeded");
        assert!(dead_letters[0].dead_lettered_at_epoch_ms > 0);
    }

    #[test]
    fn should_replay_dead_letter_given_retained_dlq_message() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-dlq-replay");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key,
            store,
            Box::new(clock.clone()),
            Some(2),
            crate::utils::idempotency::default_dedup_store(),
        );
        let msg_id = match actor.handle_send(Bytes::from("test message"), None) {
            QueueResponse::Sent { id } => id,
            other => panic!("Expected Sent response, found {other:?}"),
        };
        for _ in 0..2 {
            match actor.handle_receive(30, Some(1)) {
                QueueResponse::Received { messages } => assert_eq!(messages.len(), 1),
                other => panic!("Expected Received response, found {other:?}"),
            }
            clock.advance(Duration::from_secs(31));
            actor.process_expired_timers();
        }

        // Act
        let replayed = actor
            .replay_dead_letter(msg_id)
            .expect("replay dead letter");
        let reserve_response = actor.handle_receive(30, Some(1));

        // Assert
        assert!(replayed);
        assert_eq!(actor.dlq_count, 0);
        match reserve_response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, msg_id);
                assert_eq!(messages[0].attempts, 1);
            }
            other => panic!("Expected Received response after replay, found {other:?}"),
        }
    }

    #[test]
    fn should_purge_dead_letter_given_retained_dlq_message() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-dlq-purge");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key,
            store,
            Box::new(clock.clone()),
            Some(2),
            crate::utils::idempotency::default_dedup_store(),
        );
        let msg_id = match actor.handle_send(Bytes::from("test message"), None) {
            QueueResponse::Sent { id } => id,
            other => panic!("Expected Sent response, found {other:?}"),
        };
        for _ in 0..2 {
            match actor.handle_receive(30, Some(1)) {
                QueueResponse::Received { messages } => assert_eq!(messages.len(), 1),
                other => panic!("Expected Received response, found {other:?}"),
            }
            clock.advance(Duration::from_secs(31));
            actor.process_expired_timers();
        }

        // Act
        let purged = actor.purge_dead_letter(msg_id).expect("purge dead letter");
        let reserve_response = actor.handle_receive(30, Some(1));

        // Assert
        assert!(purged);
        assert_eq!(actor.dlq_count, 0);
        match reserve_response {
            QueueResponse::NotFound => {}
            QueueResponse::Received { messages } => assert!(messages.is_empty()),
            other => panic!("Expected empty queue after purge, found {other:?}"),
        }
    }

    #[test]
    fn should_allow_unlimited_retries_when_max_attempts_is_none() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-unlimited");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            Box::new(clock.clone()),
            None, // No max_attempts limit
            crate::utils::idempotency::default_dedup_store(),
        );

        // Act - Enqueue message
        let body = Bytes::from("test message");
        actor.handle_send(body, None);

        // Simulate 10 failed delivery attempts
        for attempt in 1..=10 {
            // Reserve
            let reserve_response = actor.handle_receive(30, Some(1));
            match reserve_response {
                QueueResponse::Received { messages } => {
                    assert_eq!(messages.len(), 1);
                    assert_eq!(messages[0].attempts, attempt);
                }
                _ => panic!("Expected Reserved response on attempt {}", attempt),
            }

            // Expire inflight entry
            clock.advance(Duration::from_secs(31));
            actor.process_expired_timers();

            // Should always be back in ready queue (unlimited retries)
            assert_eq!(actor.ready_len(), 1);
            assert_eq!(actor.inflight.len(), 0);
        }

        // Assert - Message still available after 10 attempts
        let reserve_response = actor.handle_receive(30, Some(1));
        match reserve_response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].attempts, 11);
            }
            _ => panic!("Expected Received response"),
        }
    }
}
