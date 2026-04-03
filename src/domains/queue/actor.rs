//! QueueActor: manages a single durable message queue
//!
//! Each queue has:
//! - Identity: (realm, area, resource) from route
//! - Durable storage: Message headers and bodies persisted separately in Midge
//! - Ephemeral leases: In-memory visibility tracking
//!
//! # Invariants
//!
//! 1. **Crash-safe ID reservation**: persisted ID reservations prevent collisions across restarts
//! 2. **At-least-once delivery**: Messages may be delivered multiple times
//! 3. **Lease isolation**: Reserved messages invisible to other consumers
//! 4. **Automatic redelivery**: Expired leases or crashes return messages to ready queue
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
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fxhash::FxBuildHasher;

use crate::observability as obs;
use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::RouteFamily;

use super::protocol::{MessageId, QueueKey, QueueMessage, QueueResponse, ReservedMessage};

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_ACK_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

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
    /// Number of delivery attempts (starts at 0, increments on successful lease grant).
    attempts: u32,
    /// Visibility timestamp (milliseconds since UNIX epoch).
    ///
    /// For delayed rows this is the time the message becomes ready. For leased
    /// rows this is the lease expiry. For ready rows it is 0.
    visible_at_ms: u64,
    /// When the message was first committed to the queue.
    first_enqueued_at_ms: u64,
    /// Last successful lease grant timestamp.
    last_leased_at_ms: Option<u64>,
    /// Monotonic lease epoch; increments on every successful lease grant.
    lease_epoch: u64,
    /// Current durable lease token, if any.
    lease_token: Option<u64>,
    /// Durable lease expiry in UNIX epoch milliseconds, if leased.
    lease_expires_at_ms: Option<u64>,
    /// Durable DLQ timestamp.
    dead_lettered_at_ms: Option<u64>,
    /// Durable DLQ reason code.
    dlq_reason: Option<DlqReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueueState {
    Ready = 0,
    Delayed = 1,
    Leased = 2,
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
            last_leased_at_ms: None,
            lease_epoch: 0,
            lease_token: None,
            lease_expires_at_ms: None,
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
            last_leased_at_ms: None,
            lease_epoch: 0,
            lease_token: None,
            lease_expires_at_ms: None,
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
            last_leased_at_ms: None,
            lease_epoch: 0,
            lease_token: None,
            lease_expires_at_ms: None,
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
            last_leased_at_ms: self.last_leased_at_ms,
            lease_epoch: self.lease_epoch,
            lease_token: self.lease_token,
            lease_expires_at_ms: self.lease_expires_at_ms,
            dead_lettered_at_ms: self.dead_lettered_at_ms,
            dlq_reason: self.dlq_reason,
        }
    }
}

/// In-flight message lease (ephemeral, actor-owned)
#[derive(Debug, Clone)]
pub struct Inflight {
    /// Random token for operation validation
    token: u64,
    /// Absolute expiration time
    expires_at: Instant,
    /// Durable lease expiry in UNIX epoch milliseconds.
    expires_at_epoch_ms: u64,
    /// Owning live session, if the lease was created through the broker session layer.
    owner_session_id: Option<u64>,
    /// Delivery attempt count presented with the current lease.
    attempts: u32,
    /// Durable lease epoch for stale-event suppression.
    lease_epoch: u64,
}

/// Point-in-time warm-actor queue counts for admin diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueAdminSnapshot {
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_leased: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_message_age_seconds: u64,
}

/// Point-in-time live lease snapshot for admin diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueLeaseSnapshot {
    pub message_id: u64,
    pub lease_token: u64,
    pub session_id: Option<u64>,
    pub expires_at_epoch_ms: u64,
    pub attempts: usize,
}

/// Point-in-time dead-letter snapshot for admin diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDeadLetterSnapshot {
    pub message_id: u64,
    pub dead_lettered_at_epoch_ms: u64,
    pub attempts: usize,
    pub reason: &'static str,
}

/// Timer event for lease expiration
#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaseExpiry {
    /// Message ID to re-enqueue
    id: MessageId,
    /// Lease epoch to detect stale expiry events after extend/reassign.
    lease_epoch: u64,
    /// Expiration time (for ordering in heap)
    expires_at: Instant,
    /// Expiration time in UNIX epoch milliseconds.
    expires_at_ms: u64,
}

impl Ord for LeaseExpiry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Earlier expiration = higher priority (min-heap via Reverse wrapper)
        other.expires_at.cmp(&self.expires_at)
    }
}

impl PartialOrd for LeaseExpiry {
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
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct QueueMetaSnapshot {
    next_id: u64,
    next_ready_seq: u64,
    ready_count: u64,
    delayed_count: u64,
    leased_count: u64,
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

/// Clock abstraction for testable time
pub trait Clock: Send + Sync {
    fn now_instant(&self) -> Instant;
    fn now_epoch_ms(&self) -> u64;
}

/// System clock using Instant::now()
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_instant(&self) -> Instant {
        Instant::now()
    }

    fn now_epoch_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_secs(0))
            .as_millis() as u64
    }
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
/// - `inflight`: Map of leased messages (id Ã¢â€ â€™ Inflight)
/// - `timers`: Min-heap of expiration events (earliest first)
/// - `clock`: Time source for expiration checks
///
/// # Actor Responsibilities
///
/// - Maintain best-effort ordering via sharded ready queues
/// - Track inflight leases with expiration
/// - Re-enqueue expired messages
/// - Increment attempts on redelivery
/// - Persist deletes on successful completion
/// - Never persist lease state
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

    /// Cached Midge leased-index prefix for this queue.
    #[allow(dead_code)]
    lease_index_prefix: Vec<u8>,

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

    /// Inflight map: leased messages (id Ã¢â€ â€™ Inflight)
    pub inflight: FastMap<MessageId, Inflight>,

    /// Timer heap: lease expiration events (earliest first, min-heap)
    timers: BinaryHeap<Reverse<LeaseExpiry>>,

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

    /// Pending availability notification (debounced)
    pending_publish: Option<crate::runtime::domain_event::DomainPublishEvent>,

    /// Debounce timer for availability notifications
    notify_timer: Option<crate::runtime::context::TimerId>,

    /// Flag indicating that availability notification is needed (set by enqueue)
    needs_notify_availability: bool,

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
    const NOTICE_DEBOUNCE_MS: u64 = 25;
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
        Self::with_clock_and_write_options(
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
            lease_index_prefix: Self::lease_index_prefix(&queue_key),
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
            pending_publish: None,
            notify_timer: None,
            needs_notify_availability: false,
            clock,
            max_attempts,
            dedup_store,
            // Initialize deadlines to now (will process on first receive if queues are not empty)
            next_expiration_deadline: now,
            next_delayed_deadline: now,
        };

        actor.recover_from_store();
        actor
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

    /// Generate a random lease token
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

    /// Midge key for queue metadata
    fn meta_key(queue_key: &QueueKey) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:meta",
            queue_key.realm, queue_key.area, queue_key.resource
        )
        .into_bytes()
    }

    fn index_meta_key(queue_key: &QueueKey) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:idx:meta",
            queue_key.realm, queue_key.area, queue_key.resource
        )
        .into_bytes()
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
        format!(
            "queue:{}:{}:{}:hdr:{}",
            queue_key.realm, queue_key.area, queue_key.resource, ""
        )
        .into_bytes()
    }

    fn body_key_prefix(queue_key: &QueueKey) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:body:{}",
            queue_key.realm, queue_key.area, queue_key.resource, ""
        )
        .into_bytes()
    }

    fn ready_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:idx:ready:",
            queue_key.realm, queue_key.area, queue_key.resource
        )
        .into_bytes()
    }

    fn delayed_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:idx:delay:",
            queue_key.realm, queue_key.area, queue_key.resource
        )
        .into_bytes()
    }

    fn lease_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:idx:lease:",
            queue_key.realm, queue_key.area, queue_key.resource
        )
        .into_bytes()
    }

    fn dlq_index_prefix(queue_key: &QueueKey) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:idx:dlq:",
            queue_key.realm, queue_key.area, queue_key.resource
        )
        .into_bytes()
    }

    fn ack_dedup_prefix(queue_key: &QueueKey) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:ack:",
            queue_key.realm, queue_key.area, queue_key.resource
        )
        .into_bytes()
    }

    fn legacy_message_key_prefix(queue_key: &QueueKey) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:msg:{}",
            queue_key.realm, queue_key.area, queue_key.resource, ""
        )
        .into_bytes()
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
        out.extend_from_slice(&meta.leased_count.to_le_bytes());
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
                leased_count: 0,
                dlq_count: 0,
                oldest_ready_enqueued_at_ms: None,
            });
        }

        if bytes.first().copied()? != Self::META_VERSION_V2 || bytes.len() < 57 {
            return None;
        }

        let next_id = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
        let next_ready_seq = u64::from_le_bytes(bytes[9..17].try_into().ok()?);
        let ready_count = u64::from_le_bytes(bytes[17..25].try_into().ok()?);
        let delayed_count = u64::from_le_bytes(bytes[25..33].try_into().ok()?);
        let leased_count = u64::from_le_bytes(bytes[33..41].try_into().ok()?);
        let dlq_count = u64::from_le_bytes(bytes[41..49].try_into().ok()?);
        let oldest_ready = u64::from_le_bytes(bytes[49..57].try_into().ok()?);
        Some(QueueMetaSnapshot {
            next_id,
            next_ready_seq,
            ready_count,
            delayed_count,
            leased_count,
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
                eprintln!(
                    "WARN: Failed to begin meta recovery tx for queue {:?}: {:?}, starting from 1",
                    self.queue_key, e
                );
                return 1;
            }
        };

        match txn.get(&self.meta_key) {
            Ok(Some(bytes)) => Self::decode_next_id(Some(bytes.as_ref())),
            Ok(None) => 1,
            Err(e) if Self::is_missing_read_snapshot_error(&e) => 1,
            Err(e) => {
                eprintln!(
                    "WARN: Failed to recover next_id for queue {:?}: {:?}, starting from 1",
                    self.queue_key, e
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
    fn lease_index_key(&self, expires_at_ms: u64, lease_epoch: u64, id: MessageId) -> Vec<u8> {
        let mut key =
            Vec::with_capacity(self.lease_index_prefix.len() + (Self::PADDED_U64_WIDTH * 3) + 2);
        key.extend_from_slice(&self.lease_index_prefix);
        Self::append_padded_u64(&mut key, expires_at_ms);
        key.push(b':');
        Self::append_padded_u64(&mut key, lease_epoch);
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
        buf.extend_from_slice(&record.last_leased_at_ms.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&record.lease_epoch.to_le_bytes());
        buf.extend_from_slice(&record.lease_token.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&record.lease_expires_at_ms.unwrap_or(0).to_le_bytes());
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

    fn shard_for_id(id: MessageId) -> usize {
        id.as_u64() as usize & (Self::READY_SHARDS - 1)
    }

    fn range_len(range: ReadyRange) -> usize {
        (((range.end - range.next) / Self::READY_SHARDS as u64) + 1) as usize
    }

    fn reset_live_ready_state(&mut self) {
        self.ready.clear();
        for shard in &mut self.ready_shards {
            shard.clear();
        }
        self.ready_count = 0;
        self.next_ready_shard = 0;
    }

    fn reset_persisted_index_state(&mut self) {
        for shard in &mut self.persisted_ready_shards {
            shard.clear();
        }
        self.persisted_ready_count = 0;
        self.clear_persisted_delayed();
        self.clear_persisted_dlq();
    }

    fn push_range_into(shards: &mut [VecDeque<ReadyRange>], shard: usize, range: ReadyRange) {
        let step = Self::READY_SHARDS as u64;

        if let Some(existing) = shards[shard].back_mut() {
            if range.next == existing.end.saturating_add(step) {
                existing.end = range.end;
                return;
            }
        }

        shards[shard].push_back(range);
    }

    fn prepare_persisted_ready_append(
        tail: Option<ReadyRange>,
        id: MessageId,
    ) -> (usize, ReadyRange) {
        let shard = Self::shard_for_id(id);
        let id_u64 = id.as_u64();
        let range = match tail {
            Some(existing) if id_u64 == existing.end.saturating_add(Self::READY_SHARDS as u64) => {
                ReadyRange {
                    next: existing.next,
                    end: id_u64,
                }
            }
            _ => ReadyRange {
                next: id_u64,
                end: id_u64,
            },
        };

        (shard, range)
    }

    #[cfg(test)]
    fn stage_persisted_ready_append(
        shards: &mut [VecDeque<ReadyRange>],
        id: MessageId,
    ) -> (usize, ReadyRange) {
        let shard = Self::shard_for_id(id);
        let (_, range) = Self::prepare_persisted_ready_append(shards[shard].back().copied(), id);

        if let Some(existing) = shards[shard].back_mut() {
            if range.next == existing.next {
                existing.end = range.end;
                return (shard, range);
            }
        }

        shards[shard].push_back(range);
        (shard, range)
    }

    fn push_ready(&mut self, id: MessageId) {
        self.push_ready_entry(self.next_ready_seq, id);
        self.next_ready_seq = self.next_ready_seq.saturating_add(1);
    }

    fn push_ready_entry(&mut self, ready_seq: u64, id: MessageId) {
        self.ready.push_back(ReadyEntry { ready_seq, id });
        self.ready_count = self.ready.len();
        if let Some(record) = self.records.get(&id) {
            let enqueue_ms = if record.first_enqueued_at_ms != 0 {
                record.first_enqueued_at_ms
            } else {
                self.clock.now_epoch_ms()
            };
            self.oldest_ready_enqueued_at_ms = Some(
                self.oldest_ready_enqueued_at_ms
                    .map(|current| current.min(enqueue_ms))
                    .unwrap_or(enqueue_ms),
            );
        }
    }

    fn push_persisted_ready(&mut self, id: MessageId) {
        let shard = Self::shard_for_id(id);
        let range = ReadyRange {
            next: id.as_u64(),
            end: id.as_u64(),
        };
        Self::push_range_into(&mut self.persisted_ready_shards, shard, range);
        self.persisted_ready_count += 1;
    }

    fn push_persisted_ready_range(&mut self, range: ReadyRange) {
        let shard = range.next as usize & (Self::READY_SHARDS - 1);
        Self::push_range_into(&mut self.persisted_ready_shards, shard, range);
        self.persisted_ready_count += Self::range_len(range);
    }

    fn recompute_oldest_ready_enqueued_at_ms(&mut self) {
        self.oldest_ready_enqueued_at_ms = self.ready.front().and_then(|entry| {
            self.records.get(&entry.id).and_then(|record| {
                (record.first_enqueued_at_ms != 0).then_some(record.first_enqueued_at_ms)
            })
        });
    }

    fn pop_ready_entry(&mut self) -> Option<ReadyEntry> {
        let entry = self.ready.pop_front()?;
        self.ready_count = self.ready.len();
        if self.ready.is_empty() {
            self.oldest_ready_enqueued_at_ms = None;
        } else {
            self.recompute_oldest_ready_enqueued_at_ms();
        }
        Some(entry)
    }

    fn plan_ready_index_mutation(
        shards: &[VecDeque<ReadyRange>],
        id: MessageId,
    ) -> Option<(usize, PersistedReadyMutation)> {
        let shard = Self::shard_for_id(id);
        let id_u64 = id.as_u64();
        let step = Self::READY_SHARDS as u64;
        let ranges = &shards[shard];

        for range in ranges.iter().copied() {
            if id_u64 < range.next
                || id_u64 > range.end
                || !(id_u64 - range.next).is_multiple_of(step)
            {
                continue;
            }

            let mutation = if range.next == range.end {
                PersistedReadyMutation::Delete { removed: range }
            } else if id_u64 == range.next {
                let inserted = ReadyRange {
                    next: range.next.saturating_add(step),
                    end: range.end,
                };
                PersistedReadyMutation::Replace {
                    removed: range,
                    inserted,
                }
            } else if id_u64 == range.end {
                let inserted = ReadyRange {
                    next: range.next,
                    end: range.end.saturating_sub(step),
                };
                PersistedReadyMutation::Replace {
                    removed: range,
                    inserted,
                }
            } else {
                let left = ReadyRange {
                    next: range.next,
                    end: id_u64.saturating_sub(step),
                };
                let right = ReadyRange {
                    next: id_u64.saturating_add(step),
                    end: range.end,
                };
                PersistedReadyMutation::Split {
                    removed: range,
                    left,
                    right,
                }
            };

            return Some((shard, mutation));
        }

        None
    }

    fn apply_ready_index_mutation(&mut self, shard: usize, mutation: PersistedReadyMutation) {
        Self::apply_ready_index_mutation_to_shards(
            &mut self.persisted_ready_shards,
            shard,
            mutation,
        );

        let removed_len = match mutation {
            PersistedReadyMutation::Delete { removed }
            | PersistedReadyMutation::Replace { removed, .. }
            | PersistedReadyMutation::Split { removed, .. } => Self::range_len(removed),
        };
        let inserted_len = match mutation {
            PersistedReadyMutation::Delete { .. } => 0,
            PersistedReadyMutation::Replace { inserted, .. } => Self::range_len(inserted),
            PersistedReadyMutation::Split { left, right, .. } => {
                Self::range_len(left) + Self::range_len(right)
            }
        };

        self.persisted_ready_count = self
            .persisted_ready_count
            .saturating_sub(removed_len)
            .saturating_add(inserted_len);
    }

    fn apply_ready_index_mutation_to_shards(
        shards: &mut [VecDeque<ReadyRange>],
        shard: usize,
        mutation: PersistedReadyMutation,
    ) {
        let ranges = &mut shards[shard];
        let removed = match mutation {
            PersistedReadyMutation::Delete { removed }
            | PersistedReadyMutation::Replace { removed, .. }
            | PersistedReadyMutation::Split { removed, .. } => removed,
        };

        let Some(idx) = ranges.iter().position(|range| *range == removed) else {
            return;
        };

        match mutation {
            PersistedReadyMutation::Delete { .. } => {
                ranges.remove(idx);
            }
            PersistedReadyMutation::Replace { inserted, .. } => {
                ranges[idx] = inserted;
            }
            PersistedReadyMutation::Split { left, right, .. } => {
                ranges[idx] = left;
                ranges.insert(idx + 1, right);
            }
        }
    }

    fn cache_record(&mut self, id: MessageId, record: QueueRecord, layout: StoredRecordLayout) {
        if self.records.insert(id, record).is_none() {
            self.record_cache_fifo.push_back(id);
        }
        self.record_layouts.insert(id, layout);

        self.compact_record_cache_fifo_if_needed();

        while self.records.len() > Self::RECORD_CACHE_LIMIT {
            let Some(evicted_id) = self.record_cache_fifo.pop_front() else {
                break;
            };
            self.records.remove(&evicted_id);
            self.record_layouts.remove(&evicted_id);
        }
    }

    fn evict_cached_record(&mut self, id: MessageId) {
        self.records.remove(&id);
        self.record_layouts.remove(&id);
        self.compact_record_cache_fifo_if_needed();
    }

    fn compact_record_cache_fifo_if_needed(&mut self) {
        let max_fifo_len = Self::RECORD_CACHE_LIMIT * Self::RECORD_CACHE_FIFO_SLACK_MULTIPLIER
            + self.records.len();
        if self.record_cache_fifo.len() <= max_fifo_len {
            return;
        }

        self.record_cache_fifo
            .retain(|id| self.records.contains_key(id));
    }

    fn cache_body(&mut self, id: MessageId, body: Bytes) {
        if self.body_cache.contains_key(&id) {
            return;
        }

        let body_len = body.len();
        if body_len > Self::BODY_CACHE_LIMIT_BYTES {
            return;
        }

        self.body_cache.insert(id, body);
        self.body_cache_fifo.push_back(id);
        self.body_cache_bytes = self.body_cache_bytes.saturating_add(body_len);
        self.compact_body_cache_fifo_if_needed();

        while self.body_cache.len() > Self::BODY_CACHE_LIMIT
            || self.body_cache_bytes > Self::BODY_CACHE_LIMIT_BYTES
        {
            let Some(evicted_id) = self.body_cache_fifo.pop_front() else {
                break;
            };
            self.evict_cached_body(evicted_id);
        }
    }

    fn evict_cached_body(&mut self, id: MessageId) {
        if let Some(body) = self.body_cache.remove(&id) {
            self.body_cache_bytes = self.body_cache_bytes.saturating_sub(body.len());
        }
        self.compact_body_cache_fifo_if_needed();
    }

    fn compact_body_cache_fifo_if_needed(&mut self) {
        let max_fifo_len =
            Self::BODY_CACHE_LIMIT * Self::BODY_CACHE_FIFO_SLACK_MULTIPLIER + self.body_cache.len();
        if self.body_cache_fifo.len() <= max_fifo_len {
            return;
        }

        self.body_cache_fifo
            .retain(|id| self.body_cache.contains_key(id));
    }

    fn pop_ready(&mut self) -> Option<MessageId> {
        self.pop_ready_entry().map(|entry| entry.id)
    }

    pub fn ready_len(&self) -> usize {
        self.ready.len()
    }

    pub fn admin_snapshot(&self) -> QueueAdminSnapshot {
        let now_epoch_ms = self.clock.now_epoch_ms();
        QueueAdminSnapshot {
            messages_ready: self.ready.len(),
            messages_delayed: self.persisted_delayed.len(),
            messages_leased: self.inflight.len(),
            messages_dead_lettered: self.persisted_dlq.len(),
            messages_total: self.ready.len()
                + self.inflight.len()
                + self.persisted_delayed.len()
                + self.persisted_dlq.len(),
            oldest_message_age_seconds: self
                .oldest_ready_enqueued_at_ms
                .map(|timestamp| now_epoch_ms.saturating_sub(timestamp) / 1_000)
                .unwrap_or(0),
        }
    }

    pub fn admin_leases(&self) -> Vec<QueueLeaseSnapshot> {
        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();

        self.inflight
            .iter()
            .map(|(id, inflight)| QueueLeaseSnapshot {
                message_id: id.as_u64(),
                lease_token: inflight.token,
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

    /// Drop any live leases owned by a disconnected session and return the
    /// committed messages to the ready queue. The lease ownership itself is
    /// ephemeral and is not durably recovered.
    pub fn cleanup_session_leases(&mut self, session_id: u64) -> usize {
        let released: Vec<_> = self
            .inflight
            .iter()
            .filter_map(|(id, inflight)| {
                (inflight.owner_session_id == Some(session_id)).then_some(*id)
            })
            .collect();

        for id in released.iter().copied() {
            self.inflight.remove(&id);
            self.push_ready(id);
        }

        released.len()
    }

    pub fn ready_contains(&self, id: MessageId) -> bool {
        self.ready.iter().any(|entry| entry.id == id)
    }

    /// Returns true if an availability notification should be sent (queue transitioned empty->non-empty).
    /// Clears the flag. Used by the domain sink when the actor is not run with a timer.
    pub fn take_needs_notify_availability(&mut self) -> bool {
        std::mem::take(&mut self.needs_notify_availability)
    }

    /// Schedule debounced availability notification
    /// Uses base queue route (queue://realm/area/resource) so subscription pattern matches.
    fn schedule_availability_notification(&mut self, ctx: &mut Context<Self>) {
        let route_str = format!(
            "queue://{}/{}/{}",
            self.queue_key.realm, self.queue_key.area, self.queue_key.resource
        );
        let route = crate::runtime::routing::Route::new(route_str);

        let payload = bytes::Bytes::from("{}");
        let publish_event =
            crate::runtime::domain_event::DomainPublishEvent::new(self.family, route, payload);

        self.pending_publish = Some(publish_event);
        if self.notify_timer.is_none() {
            let timer_id = ctx
                .timer_manager()
                .schedule_once(std::time::Duration::from_millis(Self::NOTICE_DEBOUNCE_MS));
            self.notify_timer = Some(timer_id);
        }
    }

    /// Deserialize a legacy combined QueueRecord from an owned buffer without copying
    /// the body again.
    fn decode_legacy_record<B: Into<Bytes>>(bytes: B) -> Result<QueueRecord, String> {
        let bytes = bytes.into();

        if bytes.len() < 16 {
            return Err("Invalid record format".to_string());
        }

        let attempts = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let visible_at_ms = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        let body_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;

        if bytes.len() < 16 + body_len {
            return Err("Truncated record body".to_string());
        }

        Ok(QueueRecord::loaded_legacy(
            bytes.slice(16..16 + body_len),
            attempts,
            visible_at_ms,
        ))
    }

    /// Deserialize only queue metadata without hydrating the body.
    fn decode_record_header(bytes: &[u8]) -> Result<QueueRecord, String> {
        if bytes.len() == 12 {
            let attempts = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
            let visible_at_ms = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
            return Ok(QueueRecord::metadata_only(attempts, visible_at_ms));
        }

        if bytes.len() < 79 || bytes[0] != Self::HEADER_VERSION_V2 {
            return Err("Invalid record format".to_string());
        }

        let state = match bytes[1] {
            0 => QueueState::Ready,
            1 => QueueState::Delayed,
            2 => QueueState::Leased,
            3 => QueueState::Dlq,
            other => return Err(format!("Unknown queue state {}", other)),
        };
        let enqueue_seq = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
        let ready_seq = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
        let attempts = u32::from_le_bytes(bytes[18..22].try_into().unwrap());
        let visible_at_ms = u64::from_le_bytes(bytes[22..30].try_into().unwrap());
        let first_enqueued_at_ms = u64::from_le_bytes(bytes[30..38].try_into().unwrap());
        let last_leased_at_ms = u64::from_le_bytes(bytes[38..46].try_into().unwrap());
        let lease_epoch = u64::from_le_bytes(bytes[46..54].try_into().unwrap());
        let lease_token = u64::from_le_bytes(bytes[54..62].try_into().unwrap());
        let lease_expires_at_ms = u64::from_le_bytes(bytes[62..70].try_into().unwrap());
        let dead_lettered_at_ms = u64::from_le_bytes(bytes[70..78].try_into().unwrap());
        let dlq_reason = match bytes[78] {
            0 => None,
            1 => Some(DlqReason::MaxAttemptsExceeded),
            other => return Err(format!("Unknown DLQ reason {}", other)),
        };

        Ok(QueueRecord {
            body: None,
            state,
            enqueue_seq,
            ready_seq: (ready_seq != 0).then_some(ready_seq),
            attempts,
            visible_at_ms,
            first_enqueued_at_ms,
            last_leased_at_ms: (last_leased_at_ms != 0).then_some(last_leased_at_ms),
            lease_epoch,
            lease_token: (lease_token != 0).then_some(lease_token),
            lease_expires_at_ms: (lease_expires_at_ms != 0).then_some(lease_expires_at_ms),
            dead_lettered_at_ms: (dead_lettered_at_ms != 0).then_some(dead_lettered_at_ms),
            dlq_reason,
        })
    }

    fn load_record_metadata_from_store(
        &self,
        id: MessageId,
    ) -> Result<(QueueRecord, StoredRecordLayout), String> {
        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let legacy_key = self.cached_legacy_message_key(id);
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("Failed to begin read tx for message {}: {:?}", id, e))?;

        match txn.get(&header_key) {
            Ok(Some(bytes)) => {
                let layout = if Self::is_versioned_header(&bytes) || bytes.len() == 12 {
                    StoredRecordLayout::SplitHeaderBody
                } else {
                    StoredRecordLayout::EmbeddedHeader
                };
                match layout {
                    StoredRecordLayout::SplitHeaderBody => {
                        Self::decode_record_header(&bytes).map(|record| (record, layout))
                    }
                    StoredRecordLayout::EmbeddedHeader => Self::decode_legacy_record(bytes)
                        .map(|record| (record.metadata_only_from(), layout)),
                    StoredRecordLayout::LegacyKey => unreachable!(),
                }
            }
            Ok(None) => match txn.get(&legacy_key) {
                Ok(Some(bytes)) => {
                    let record = Self::decode_legacy_record(bytes)?;
                    Ok((record.metadata_only_from(), StoredRecordLayout::LegacyKey))
                }
                Ok(None) => Err(format!("Message {} disappeared from storage", id)),
                Err(e) => Err(format!("Failed to read legacy message {}: {:?}", id, e)),
            },
            Err(e) => Err(format!("Failed to read message header {}: {:?}", id, e)),
        }
    }

    fn load_body_from_store(&self, id: MessageId) -> Result<Bytes, String> {
        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);
        let legacy_key = self.cached_legacy_message_key(id);
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("Failed to begin read tx for message body {}: {:?}", id, e))?;

        match txn.get(&body_key) {
            Ok(Some(bytes)) => Ok(bytes),
            Ok(None) => match txn.get(&header_key) {
                Ok(Some(bytes)) if !Self::is_versioned_header(&bytes) && bytes.len() >= 16 => {
                    let record = Self::decode_legacy_record(bytes)?;
                    record
                        .body
                        .ok_or_else(|| format!("Embedded message {} body missing", id))
                }
                Ok(Some(_)) | Ok(None) => match txn.get(&legacy_key) {
                    Ok(Some(bytes)) => {
                        let record = Self::decode_legacy_record(bytes)?;
                        record
                            .body
                            .ok_or_else(|| format!("Legacy message {} body missing", id))
                    }
                    Ok(None) => Err(format!("Message body {} disappeared from storage", id)),
                    Err(e) => Err(format!(
                        "Failed to read legacy message body {}: {:?}",
                        id, e
                    )),
                },
                Err(e) => Err(format!("Failed to read message header {}: {:?}", id, e)),
            },
            Err(e) => Err(format!("Failed to read message body {}: {:?}", id, e)),
        }
    }

    fn load_record_for_receive_from_store(
        &self,
        id: MessageId,
    ) -> Result<(QueueRecord, StoredRecordLayout), String> {
        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);
        let legacy_key = self.cached_legacy_message_key(id);
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("Failed to begin read tx for message {}: {:?}", id, e))?;

        match txn.get(&header_key) {
            Ok(Some(header_bytes)) => {
                if !Self::is_versioned_header(&header_bytes) && header_bytes.len() >= 16 {
                    if let Ok(record) = Self::decode_legacy_record(header_bytes.clone()) {
                        return Ok((record, StoredRecordLayout::EmbeddedHeader));
                    }
                }
                let header = Self::decode_record_header(&header_bytes)?;
                match txn.get(&body_key) {
                    Ok(Some(body_bytes)) => Ok((
                        QueueRecord {
                            body: Some(body_bytes),
                            ..header
                        },
                        StoredRecordLayout::SplitHeaderBody,
                    )),
                    Ok(None) => Err(format!("Message body {} disappeared from storage", id)),
                    Err(e) => Err(format!("Failed to read message body {}: {:?}", id, e)),
                }
            }
            Err(e) => Err(format!("Failed to read message {}: {:?}", id, e)),
            Ok(None) => match txn.get(&legacy_key) {
                Ok(Some(bytes)) => Self::decode_legacy_record(bytes)
                    .map(|record| (record, StoredRecordLayout::LegacyKey)),
                Ok(None) => Err(format!("Message {} disappeared from storage", id)),
                Err(e) => Err(format!("Failed to read legacy message {}: {:?}", id, e)),
            },
        }
    }

    fn load_full_record_for_admin_mutation(
        &mut self,
        id: MessageId,
    ) -> Result<(QueueRecord, StoredRecordLayout), String> {
        let (record, layout) = if let Some(record) = self.records.get(&id).cloned() {
            (
                record,
                self.record_layouts
                    .get(&id)
                    .copied()
                    .unwrap_or(StoredRecordLayout::EmbeddedHeader),
            )
        } else {
            self.load_record_metadata_from_store(id)?
        };

        let body = if let Some(body) = record.body.clone() {
            body
        } else if let Some(body) = self.body_cache.get(&id).cloned() {
            body
        } else {
            self.load_body_from_store(id)?
        };

        Ok((
            QueueRecord {
                body: Some(body),
                ..record
            },
            layout,
        ))
    }

    fn observe_histogram_us(metric_name: &str, value_us: u64) {
        crate::boot::observability::histogram_observe_us(metric_name, value_us);
    }

    fn observe_elapsed_us(metric_name: &str, start: Instant) {
        Self::observe_histogram_us(metric_name, start.elapsed().as_micros() as u64);
    }

    fn increment_counter(metric_name: &str) {
        crate::boot::observability::counter_inc(metric_name);
    }

    fn is_missing_read_snapshot_error(error: &impl std::fmt::Debug) -> bool {
        format!("{:?}", error).contains("read snapshot not available")
    }

    fn hydrate_record_for_receive(&mut self, id: MessageId) -> Result<(Bytes, u32), String> {
        if let Some(record) = self.records.get(&id) {
            let attempts = record.attempts;
            if let Some(body) = self.body_cache.get(&id) {
                return Ok((body.clone(), attempts));
            }

            let start = Instant::now();
            let body = self.load_body_from_store(id)?;
            Self::observe_elapsed_us(obs::METRIC_QUEUE_RECEIVE_HYDRATE_LATENCY, start);
            self.cache_body(id, body.clone());
            return Ok((body, attempts));
        }

        let start = Instant::now();
        let (record, layout) = self.load_record_for_receive_from_store(id)?;
        Self::observe_elapsed_us(obs::METRIC_QUEUE_RECEIVE_HYDRATE_LATENCY, start);
        let body = record
            .body
            .clone()
            .ok_or_else(|| format!("Message {} body missing after hydration", id))?;
        self.cache_record(id, record.metadata_only_from(), layout);
        self.cache_body(id, body.clone());
        Ok((body, record.attempts))
    }

    fn is_versioned_header(bytes: &[u8]) -> bool {
        bytes.len() >= 79 && bytes.first().copied() == Some(Self::HEADER_VERSION_V2)
    }

    #[allow(dead_code)]
    fn encode_ack_dedup_value(expires_at_ms: u64) -> Vec<u8> {
        expires_at_ms.to_le_bytes().to_vec()
    }

    #[allow(dead_code)]
    fn decode_ack_dedup_value(bytes: &[u8]) -> Option<u64> {
        Some(u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?))
    }

    #[allow(dead_code)]
    fn durable_meta_snapshot(&self) -> QueueMetaSnapshot {
        QueueMetaSnapshot {
            next_id: self.next_id,
            next_ready_seq: self.next_ready_seq,
            ready_count: self.ready.len() as u64,
            delayed_count: self.persisted_delayed.len() as u64,
            leased_count: self.inflight.len() as u64,
            dlq_count: self.persisted_dlq.len() as u64,
            oldest_ready_enqueued_at_ms: self.oldest_ready_enqueued_at_ms,
        }
    }

    #[allow(dead_code)]
    fn write_meta_snapshot(
        &self,
        txn: &mut cntryl_midge::Transaction,
        meta: QueueMetaSnapshot,
    ) -> Result<(), String> {
        txn.put(self.meta_key.clone(), Self::encode_meta(meta), None)
            .map_err(|e| format!("Failed to write queue meta: {:?}", e))
    }

    #[allow(dead_code)]
    fn write_record_as_split(
        &self,
        txn: &mut cntryl_midge::Transaction,
        id: MessageId,
        record: &QueueRecord,
        prior_layout: Option<StoredRecordLayout>,
    ) -> Result<(), String> {
        if matches!(prior_layout, Some(StoredRecordLayout::LegacyKey)) {
            txn.delete(self.cached_legacy_message_key(id))
                .map_err(|e| format!("Failed to delete legacy queue record: {:?}", e))?;
        }

        txn.put(
            self.cached_header_key(id),
            Self::encode_record_header(record),
            None,
        )
        .map_err(|e| format!("Failed to write queue header: {:?}", e))?;

        let body = record
            .body
            .as_ref()
            .ok_or_else(|| format!("Queue record {} missing body for write", id))?;
        txn.put(self.cached_body_key(id), body.to_vec(), None)
            .map_err(|e| format!("Failed to write queue body: {:?}", e))?;
        Ok(())
    }

    #[allow(dead_code)]
    fn read_durable_ack_dedup(&self, id: MessageId, token: u64, now_epoch_ms: u64) -> Option<bool> {
        let cf_id = self.queue_key.family.id();
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .ok()?;
        let bytes = txn.get(&self.ack_dedup_key(id, token)).ok()??;
        Some(
            Self::decode_ack_dedup_value(bytes.as_ref())
                .map(|expires_at_ms| expires_at_ms > now_epoch_ms)
                .unwrap_or(false),
        )
    }

    #[allow(dead_code)]
    fn cache_record_state(
        &mut self,
        id: MessageId,
        record: &QueueRecord,
        layout: StoredRecordLayout,
    ) {
        self.cache_record(id, record.metadata_only_from(), layout);
        if let Some(body) = record.body.as_ref() {
            self.cache_body(id, body.clone());
        }
    }

    fn commit_ack_transaction(
        txn: cntryl_midge::Transaction,
        write_options: cntryl_midge::WriteOptions,
    ) -> Result<(), String> {
        #[cfg(test)]
        {
            let should_fail = FAIL_NEXT_ACK_COMMIT.with(|cell| {
                let should_fail = cell.get();
                if should_fail {
                    cell.set(false);
                }
                should_fail
            });

            if should_fail {
                return Err("Injected queue ack commit failure".to_string());
            }
        }

        txn.commit(write_options).map_err(|e| format!("{:?}", e))
    }

    #[cfg(test)]
    fn fail_next_ack_commit_for_tests() {
        FAIL_NEXT_ACK_COMMIT.with(|cell| cell.set(true));
    }

    /// Handle send operation
    pub fn handle_send(&mut self, body: Bytes, delay_seconds: Option<u64>) -> QueueResponse {
        // Track empty state before send for notification
        let was_empty = self.ready_count == 0;

        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let delay_ms = delay_seconds.unwrap_or(0).saturating_mul(1_000);

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
                }
            }
        };

        // Allocate message ID
        let id = MessageId::new(self.next_id);
        let visible_at_ms = now_epoch_ms.saturating_add(delay_ms);
        let visible_at = now_instant + Duration::from_millis(delay_ms);

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

        // Emit availability notification if queue transitioned from empty to non-empty
        // (only for immediately visible messages, not delayed ones)
        if was_empty && visible_at <= now_instant && self.ready_count > 0 {
            self.needs_notify_availability = true;
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
                }
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
            let delay_ms = delay_seconds.unwrap_or(0).saturating_mul(1_000);
            let id = MessageId::new(next_id);
            let visible_at_ms = now_epoch_ms.saturating_add(delay_ms);
            let visible_at = now_instant + Duration::from_millis(delay_ms);

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
    /// Long polling is handled at the RPC layer:
    /// 1. RPC calls this method synchronously
    /// 2. If empty and wait_seconds > 0, RPC subscribes to notice://.../available
    /// 3. RPC waits up to wait_seconds for notice or timeout
    /// 4. RPC retries reserve on notice or timeout
    ///
    /// NOTE: The notice:// reference above is a runtime convention handled entirely
    /// within the RPC domain layer -- it does NOT create a compile-time dependency
    /// between the Queue domain and the Notice domain.
    ///
    /// QueueActor never stores waiters or blocks on empty queues.
    pub fn handle_receive(
        &mut self,
        lease_seconds: u64,
        batch_size: Option<usize>,
    ) -> QueueResponse {
        self.handle_receive_internal(None, lease_seconds, batch_size)
    }

    pub fn handle_receive_for_session(
        &mut self,
        session_id: u64,
        lease_seconds: u64,
        batch_size: Option<usize>,
    ) -> QueueResponse {
        self.handle_receive_internal(Some(session_id), lease_seconds, batch_size)
    }

    fn handle_receive_internal(
        &mut self,
        owner_session_id: Option<u64>,
        lease_seconds: u64,
        batch_size: Option<usize>,
    ) -> QueueResponse {
        let batch_size = batch_size.unwrap_or(1);
        let now = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let lease_duration = Duration::from_secs(lease_seconds);

        let mut messages = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            // Pop from ready queue
            let id = match self.pop_ready() {
                Some(id) => id,
                None => break, // No more messages
            };

            let (body, attempts) = match self.hydrate_record_for_receive(id) {
                Ok(record) => record,
                Err(e) => {
                    eprintln!("WARN: {}", e);
                    continue;
                }
            };

            // Generate lease token
            let token = Self::generate_token();
            let expires_at = now + lease_duration;

            // Create inflight entry
            self.inflight.insert(
                id,
                Inflight {
                    token,
                    expires_at,
                    expires_at_epoch_ms: now_epoch_ms
                        .saturating_add(lease_seconds.saturating_mul(1_000)),
                    owner_session_id,
                    attempts: attempts + 1,
                    lease_epoch: 0,
                },
            );

            // Schedule expiration timer
            self.timers.push(Reverse(LeaseExpiry {
                id,
                lease_epoch: 0,
                expires_at,
                expires_at_ms: now_epoch_ms.saturating_add(lease_seconds.saturating_mul(1_000)),
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
                lease_seconds,
                attempts: attempts + 1, // First attempt is 1 (not 0)
            });
        }

        // If no messages were reserved, return an empty response (avoid NotFound)
        // Clients expect an empty slice when the queue is empty rather than an error.
        // Long‑polling is handled at the RPC layer using wait_seconds (see docs above).
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
        lease_seconds: u64,
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
            // Remove stale inflight entry
            self.inflight.remove(&id);
            return QueueResponse::LeaseExpired;
        }

        // Extend expiration
        let new_expires_at = now + Duration::from_secs(lease_seconds);
        inflight.expires_at = new_expires_at;
        inflight.expires_at_epoch_ms =
            now_epoch_ms.saturating_add(lease_seconds.saturating_mul(1_000));

        // Schedule new timer (old timer will be ignored when it fires)
        self.timers.push(Reverse(LeaseExpiry {
            id,
            lease_epoch: inflight.lease_epoch,
            expires_at: new_expires_at,
            expires_at_ms: inflight.expires_at_epoch_ms,
        }));

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
            identifier: DedupIdentifier::QueueComplete(id.as_u64(), token),
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
            return match bincode::deserialize(&cached_response) {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!(
                        message_id = id.as_u64(),
                        token = token,
                        error = ?e,
                        "Failed to deserialize cached COMPLETE response, processing normally"
                    );
                    // Fall through to normal processing if deserialization fails
                    QueueResponse::NotFound
                }
            };
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
            let response = QueueResponse::InvalidToken;
            // Don't cache invalid token - security: wrong token should fail every time
            return response;
        }

        // Check if already expired
        if inflight.expires_at <= now {
            // Remove stale inflight entry
            self.inflight.remove(&id);
            let response = QueueResponse::LeaseExpired;
            // Cache expired response
            if let Ok(bytes) = bincode::serialize(&response) {
                self.dedup_store.record(dedup_key, bytes);
            }
            return response;
        }

        let (stored_record, stored_layout) = if let Some(record) = self.records.get(&id).cloned() {
            (
                Some(record),
                self.record_layouts
                    .get(&id)
                    .copied()
                    .unwrap_or(StoredRecordLayout::EmbeddedHeader),
            )
        } else {
            match self.load_record_metadata_from_store(id) {
                Ok((record, layout)) => (Some(record), layout),
                Err(_) => (None, StoredRecordLayout::EmbeddedHeader),
            }
        };
        let persisted_ready_mutation =
            Self::plan_ready_index_mutation(&self.persisted_ready_shards, id);
        let removing_delayed = self.persisted_delayed.contains_key(&id);
        let staged_ready_count = self.staged_ready_count_after_mutation(persisted_ready_mutation);
        let staged_delayed_count = self
            .persisted_delayed
            .len()
            .saturating_sub(usize::from(removing_delayed));
        let staged_next_delayed_visibility = if removing_delayed {
            self.min_persisted_delayed_visibility_ms_excluding(id)
        } else {
            self.min_persisted_delayed_visibility_ms()
        };

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
                    eprintln!("WARN: Failed to delete message {} in txn: {:?}", id, e);
                    Err(format!("Failed to delete message {} in txn: {:?}", id, e))
                }
                Ok(()) => {
                    let mut index_update_failed = false;
                    if let Some((shard, mutation)) = persisted_ready_mutation {
                        let ready_result = match mutation {
                            PersistedReadyMutation::Delete { removed } => {
                                txn.delete(self.ready_range_key(shard, removed.next))
                            }
                            PersistedReadyMutation::Replace { removed, inserted } => txn
                                .delete(self.ready_range_key(shard, removed.next))
                                .and_then(|_| {
                                    txn.put(
                                        self.ready_range_key(shard, inserted.next),
                                        Self::encode_ready_range_value(inserted),
                                        None,
                                    )
                                }),
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
                                }),
                        };

                        if let Err(e) = ready_result {
                            eprintln!(
                                "WARN: Failed to update ready index for message {}: {:?}",
                                id, e
                            );
                            index_update_failed = true;
                        }
                    }

                    if let Some(record) = stored_record.as_ref() {
                        if let Err(e) = txn.delete(self.delayed_index_key(record.visible_at_ms, id))
                        {
                            eprintln!(
                                "WARN: Failed to update delayed index for message {}: {:?}",
                                id, e
                            );
                            index_update_failed = true;
                        }
                    }

                    if let Err(e) = txn.put(
                        self.index_meta_key.clone(),
                        Self::encode_index_meta(
                            self.next_id_limit,
                            staged_ready_count as u64,
                            staged_delayed_count as u64,
                            staged_next_delayed_visibility,
                        ),
                        None,
                    ) {
                        eprintln!(
                            "WARN: Failed to update queue index meta for message {}: {:?}",
                            id, e
                        );
                        index_update_failed = true;
                    }

                    if index_update_failed {
                        return QueueResponse::Error {
                            message: format!(
                                "Failed to update queue recovery index while deleting message {}",
                                id
                            ),
                        };
                    }

                    match Self::commit_ack_transaction(txn, self.commit_write_options) {
                        Ok(()) => {
                            if let Some((shard, mutation)) = persisted_ready_mutation {
                                self.apply_ready_index_mutation(shard, mutation);
                            }
                            self.remove_persisted_delayed(id);
                            Ok(())
                        }
                        Err(e) => {
                            eprintln!(
                                "WARN: Failed to commit delete txn for message {}: {}",
                                id, e
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
        record.lease_token = None;
        record.lease_expires_at_ms = None;
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

    /// Handle lease expiration (internal timer event)
    fn handle_lease_expired(&mut self, id: MessageId) {
        // Check if message is still inflight
        let inflight = match self.inflight.get(&id) {
            Some(inflight) => inflight.clone(),
            None => return, // Already completed or extended
        };

        let now = self.clock.now_instant();

        // Verify expiration (ignore stale timers from extend operations)
        if inflight.expires_at > now {
            return; // Lease was extended, ignore this timer
        }
        let now_epoch_ms = self.clock.now_epoch_ms();

        // Remove inflight entry
        self.inflight.remove(&id);

        // Increment attempts in storage and check DLQ threshold
        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);
        let legacy_key = self.cached_legacy_message_key(id);

        let (mut record, record_layout) = if let Some(cached) = self.records.get(&id) {
            (
                cached.clone(),
                self.record_layouts
                    .get(&id)
                    .copied()
                    .unwrap_or(StoredRecordLayout::EmbeddedHeader),
            )
        } else {
            match self.load_record_metadata_from_store(id) {
                Ok((record, layout)) => (record, layout),
                Err(e) => {
                    eprintln!(
                        "WARN: Failed to load message {} during redelivery: {}",
                        id, e
                    );
                    return;
                }
            }
        };

        record.attempts += 1;

        let is_dlq = if let Some(max) = self.max_attempts {
            record.attempts >= max
        } else {
            false
        };

        match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(mut txn) => {
                let has_split_record = match txn.get(&header_key) {
                    Ok(Some(_)) => true,
                    Ok(None) => false,
                    Err(e) => {
                        eprintln!(
                            "WARN: Failed to inspect queue storage layout for message {}: {:?}",
                            id, e
                        );
                        return;
                    }
                };
                let has_body_key = if has_split_record {
                    match txn.get(&body_key) {
                        Ok(Some(_)) => true,
                        Ok(None) => false,
                        Err(e) => {
                            eprintln!(
                                "WARN: Failed to inspect body storage layout for message {}: {:?}",
                                id, e
                            );
                            false
                        }
                    }
                } else {
                    false
                };

                if is_dlq {
                    let prior_visible_at_ms = record.visible_at_ms;
                    let dead_lettered_at_ms = now_epoch_ms;
                    record.state = QueueState::Dlq;
                    record.ready_seq = None;
                    record.visible_at_ms = 0;
                    record.lease_token = None;
                    record.lease_expires_at_ms = None;
                    record.dead_lettered_at_ms = Some(dead_lettered_at_ms);
                    record.dlq_reason = Some(DlqReason::MaxAttemptsExceeded);

                    let persisted_ready_mutation =
                        Self::plan_ready_index_mutation(&self.persisted_ready_shards, id);
                    let removing_delayed = self.persisted_delayed.contains_key(&id);
                    let staged_ready_count =
                        self.staged_ready_count_after_mutation(persisted_ready_mutation);
                    let staged_delayed_count = self
                        .persisted_delayed
                        .len()
                        .saturating_sub(usize::from(removing_delayed));
                    let staged_next_delayed_visibility = if removing_delayed {
                        self.min_persisted_delayed_visibility_ms_excluding(id)
                    } else {
                        self.min_persisted_delayed_visibility_ms()
                    };
                    let write_result =
                        if matches!(record_layout, StoredRecordLayout::SplitHeaderBody)
                            && has_body_key
                        {
                            txn.put(
                                header_key.clone(),
                                Self::encode_record_header(&record),
                                None,
                            )
                            .map_err(|e| format!("Failed to write DLQ header: {:?}", e))
                        } else {
                            if record.body.is_none() {
                                match self.load_body_from_store(id) {
                                    Ok(body) => record.body = Some(body),
                                    Err(e) => {
                                        eprintln!(
                                            "WARN: Failed to load body for DLQ message {}: {}",
                                            id, e
                                        );
                                        return;
                                    }
                                }
                            }

                            self.write_record_as_split(&mut txn, id, &record, Some(record_layout))
                        };

                    if let Err(e) = write_result {
                        eprintln!("WARN: Failed to persist DLQ message {}: {:?}", id, e);
                        return;
                    }

                    let mut index_update_failed = false;
                    if let Some((shard, mutation)) = persisted_ready_mutation {
                        let ready_result = match mutation {
                            PersistedReadyMutation::Delete { removed } => {
                                txn.delete(self.ready_range_key(shard, removed.next))
                            }
                            PersistedReadyMutation::Replace { removed, inserted } => txn
                                .delete(self.ready_range_key(shard, removed.next))
                                .and_then(|_| {
                                    txn.put(
                                        self.ready_range_key(shard, inserted.next),
                                        Self::encode_ready_range_value(inserted),
                                        None,
                                    )
                                }),
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
                                }),
                        };

                        if let Err(e) = ready_result {
                            eprintln!(
                                "WARN: Failed to update ready index for DLQ message {}: {:?}",
                                id, e
                            );
                            index_update_failed = true;
                        }
                    }

                    if removing_delayed {
                        if let Err(e) = txn.delete(self.delayed_index_key(prior_visible_at_ms, id))
                        {
                            eprintln!(
                                "WARN: Failed to update delayed index for DLQ message {}: {:?}",
                                id, e
                            );
                            index_update_failed = true;
                        }
                    }

                    if let Err(e) = txn.put(
                        self.dlq_index_key(dead_lettered_at_ms, id),
                        Vec::new(),
                        None,
                    ) {
                        eprintln!(
                            "WARN: Failed to write DLQ index for message {}: {:?}",
                            id, e
                        );
                        index_update_failed = true;
                    }

                    if let Err(e) = txn.put(
                        self.index_meta_key.clone(),
                        Self::encode_index_meta(
                            self.next_id_limit,
                            staged_ready_count as u64,
                            staged_delayed_count as u64,
                            staged_next_delayed_visibility,
                        ),
                        None,
                    ) {
                        eprintln!(
                            "WARN: Failed to update queue index meta for DLQ message {}: {:?}",
                            id, e
                        );
                        index_update_failed = true;
                    }

                    if index_update_failed {
                        return;
                    }

                    let update_start = Instant::now();
                    if let Err(e) = txn.commit(self.commit_write_options) {
                        eprintln!("WARN: Failed to commit DLQ transition txn {}: {:?}", id, e);
                        return;
                    }

                    Self::observe_elapsed_us(
                        obs::METRIC_QUEUE_REDELIVERY_UPDATE_LATENCY,
                        update_start,
                    );
                    if let Some((shard, mutation)) = persisted_ready_mutation {
                        self.apply_ready_index_mutation(shard, mutation);
                    }
                    self.remove_persisted_delayed(id);
                    self.insert_persisted_dlq(id, dead_lettered_at_ms);
                    self.cache_record(
                        id,
                        record.metadata_only_from(),
                        StoredRecordLayout::SplitHeaderBody,
                    );
                    self.evict_cached_body(id);

                    eprintln!(
                        "DLQ: queue={:?} message_id={} attempts={} - Message moved to dead letter queue",
                        self.queue_key, id, record.attempts
                    );

                    return;
                }

                let write_result = if has_split_record {
                    match txn.get(&header_key) {
                        Ok(Some(bytes)) if !has_body_key && bytes.len() >= 16 => {
                            match Self::decode_legacy_record(bytes) {
                                Ok(mut embedded_record) => {
                                    embedded_record.attempts = record.attempts;
                                    embedded_record.visible_at_ms = record.visible_at_ms;
                                    let value = Self::encode_legacy_record(&embedded_record);
                                    txn.put(header_key.clone(), value, None)
                                }
                                Err(e) => {
                                    eprintln!(
                                        "WARN: Failed to decode embedded message {} during redelivery: {}",
                                        id, e
                                    );
                                    return;
                                }
                            }
                        }
                        Ok(Some(_)) => {
                            let value = Self::encode_record_header(&record);
                            txn.put(header_key.clone(), value, None)
                        }
                        Ok(None) => {
                            eprintln!("WARN: Message {} disappeared during redelivery", id);
                            return;
                        }
                        Err(e) => {
                            eprintln!(
                                "WARN: Failed to read message {} during redelivery: {:?}",
                                id, e
                            );
                            return;
                        }
                    }
                } else {
                    match txn.get(&legacy_key) {
                        Ok(Some(bytes)) => match Self::decode_legacy_record(bytes) {
                            Ok(mut legacy_record) => {
                                legacy_record.attempts = record.attempts;
                                legacy_record.visible_at_ms = record.visible_at_ms;
                                let value = Self::encode_legacy_record(&legacy_record);
                                txn.put(legacy_key.clone(), value, None)
                            }
                            Err(e) => {
                                eprintln!(
                                    "WARN: Failed to decode legacy message {} during redelivery: {}",
                                    id, e
                                );
                                return;
                            }
                        },
                        Ok(None) => {
                            eprintln!("WARN: Legacy message {} disappeared during redelivery", id);
                            return;
                        }
                        Err(e) => {
                            eprintln!(
                                "WARN: Failed to read legacy message {} during redelivery: {:?}",
                                id, e
                            );
                            return;
                        }
                    }
                };

                if let Err(e) = write_result {
                    eprintln!(
                        "WARN: Failed to increment attempts for message {}: {:?}",
                        id, e
                    );
                } else {
                    let update_start = Instant::now();
                    if let Err(e) = txn.commit(self.commit_write_options) {
                        eprintln!(
                            "WARN: Failed to commit retry txn for message {}: {:?}",
                            id, e
                        );
                    } else {
                        Self::observe_elapsed_us(
                            obs::METRIC_QUEUE_REDELIVERY_UPDATE_LATENCY,
                            update_start,
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "WARN: Failed to begin txn for redelivery for message {}: {:?}",
                    id, e
                );
                return;
            }
        }

        self.cache_record(
            id,
            QueueRecord::metadata_only(record.attempts, record.visible_at_ms),
            record_layout,
        );

        // Re-enqueue to ready queue (back of queue for FIFO)
        self.push_ready(id);
    }

    /// Process expired timers (called periodically or on message receive)
    /// Updates the cached deadline for the next expiration check
    pub fn process_expired_timers(&mut self) {
        let now = self.clock.now_instant();

        while let Some(Reverse(expiry)) = self.timers.peek() {
            if expiry.expires_at > now {
                // Found next expiration deadline, cache it
                self.next_expiration_deadline = expiry.expires_at;
                break;
            }

            // Pop expired timer
            let expiry = self.timers.pop().unwrap().0;

            // Handle expiration
            self.handle_lease_expired(expiry.id);
        }

        // If no more timers, set deadline to far future
        if self.timers.is_empty() {
            self.next_expiration_deadline = now + Duration::from_secs(3600); // 1 hour
        }
    }

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
            } => {
                let resp = self.handle_send(body, delay_seconds);
                // Check if availability notification is needed
                if self.needs_notify_availability {
                    self.schedule_availability_notification(ctx);
                    self.needs_notify_availability = false;
                }
                resp
            }

            QueueMessage::Receive {
                lease_seconds,
                batch_size,
                wait_seconds,
                ..
            } => {
                // NOTE: wait_seconds is handled at RPC layer, not in QueueActor
                // QueueActor always returns immediately (never blocks)
                // If empty and wait_seconds > 0, RPC layer will:
                //   1. Subscribe to notice://{realm}/{area}/{resource}/available
                //   2. Wait up to wait_seconds for notice or timeout
                //   3. Retry receive on notice or timeout
                let _ = wait_seconds; // Unused by actor, used by RPC layer
                self.handle_receive(lease_seconds, batch_size)
            }

            QueueMessage::Extend {
                id,
                token,
                lease_seconds,
                ..
            } => self.handle_extend(id, token, lease_seconds),

            QueueMessage::Ack { id, token, .. } => self.handle_ack(id, token),

            QueueMessage::LeaseExpired { id } => {
                self.handle_lease_expired(id);
                return; // No response needed for internal timer message
            }

            // Subscribe/Unsubscribe/UnsubscribeAll are handled at QueueDomainSink level, not in actor
            QueueMessage::Subscribe { .. }
            | QueueMessage::Unsubscribe { .. }
            | QueueMessage::UnsubscribeAll { .. } => {
                return; // No response needed, handled by sink
            }
        };

        // Send response back to the client via reply
        let _ = ctx.reply(response).ok();
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // Recovery is handled during actor construction; started() is a no-op.
    }

    fn on_timer(&mut self, timer_id: crate::runtime::context::TimerId, ctx: &mut Context<Self>) {
        // If availability notification timer fired, send pending publish
        if self.notify_timer.is_some() && Some(timer_id) == self.notify_timer {
            if let Some(event) = self.pending_publish.take() {
                let _ = ctx.publish_event(event);
            }
            self.notify_timer = None;
        }
    }
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

    fn try_recover_from_index(&mut self) -> IndexRecoveryAttempt {
        let cf_id = self.queue_key.family.id();
        let start = Instant::now();
        let txn = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        {
            Ok(txn) => txn,
            Err(e) => {
                self.index_meta_written = false;
                return IndexRecoveryAttempt::Error {
                    next_id: 1,
                    reason: format!("Failed to begin index recovery tx: {:?}", e),
                };
            }
        };

        let index_meta = match txn.get(&self.index_meta_key) {
            Ok(value) => value,
            Err(e) if Self::is_missing_read_snapshot_error(&e) => {
                self.index_meta_written = false;
                Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_MISSING);
                return IndexRecoveryAttempt::Missing {
                    next_id: self.load_next_id_from_meta_key(),
                };
            }
            Err(e) => {
                self.index_meta_written = false;
                return IndexRecoveryAttempt::Error {
                    next_id: self.load_next_id_from_meta_key(),
                    reason: format!("Failed to read queue index meta: {:?}", e),
                };
            }
        };
        let Some(index_meta) = index_meta else {
            self.index_meta_written = false;
            Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_MISSING);
            return IndexRecoveryAttempt::Missing {
                next_id: self.load_next_id_from_meta_key(),
            };
        };

        let meta_snapshot = match Self::decode_index_meta(&index_meta) {
            Ok(DecodedIndexMeta::V2(snapshot)) => snapshot,
            Ok(DecodedIndexMeta::LegacyV1) => {
                self.index_meta_written = false;
                Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_INVALID);
                return IndexRecoveryAttempt::Invalid {
                    next_id: self.load_next_id_from_meta_key(),
                    reason: "Queue index meta is legacy v1 and missing authoritative counters"
                        .to_string(),
                };
            }
            Err(reason) => {
                self.index_meta_written = false;
                Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_INVALID);
                return IndexRecoveryAttempt::Invalid {
                    next_id: self.load_next_id_from_meta_key(),
                    reason,
                };
            }
        };

        if !Self::index_meta_is_valid(&index_meta) {
            self.index_meta_written = false;
            Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_INVALID);
            return IndexRecoveryAttempt::Invalid {
                next_id: self.load_next_id_from_meta_key(),
                reason: "Queue index meta is missing version or validity marker".to_string(),
            };
        }

        self.index_meta_written = true;
        self.reset_recovery_state();

        let ready_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.ready_index_prefix));
        let delayed_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.delayed_index_prefix));
        let dlq_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.dlq_index_prefix));

        let mut ready_iter = match txn.scan(&ready_query) {
            Ok(iter) => iter,
            Err(e) => {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: format!("Failed to scan queue ready index: {:?}", e),
                };
            }
        };
        let mut delayed_iter = match txn.scan(&delayed_query) {
            Ok(iter) => iter,
            Err(e) => {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: format!("Failed to scan queue delayed index: {:?}", e),
                };
            }
        };
        let mut dlq_iter = match txn.scan(&dlq_query) {
            Ok(iter) => iter,
            Err(e) => {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: format!("Failed to scan queue DLQ index: {:?}", e),
                };
            }
        };

        let now_epoch_ms = self.clock.now_epoch_ms();
        let now_instant = self.clock.now_instant();
        let mut max_id = None::<u64>;
        let mut scanned_ready_count = 0_u64;
        let mut scanned_delayed_count = 0_u64;
        let mut scanned_next_delayed = None::<u64>;
        let mut matured_delayed_ids = Vec::new();

        while let Some((key_bytes, value_bytes)) = ready_iter.next() {
            let Some((shard, start_id)) =
                Self::parse_ready_range_key(&key_bytes, &self.ready_index_prefix)
            else {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: "Malformed queue ready index key".to_string(),
                };
            };
            let Some(range) = Self::decode_ready_range(start_id, &value_bytes) else {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: "Malformed queue ready index value".to_string(),
                };
            };
            if shard != (range.next as usize & (Self::READY_SHARDS - 1)) {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: "Queue ready index shard does not match message ID".to_string(),
                };
            }
            self.push_persisted_ready_range(range);
            scanned_ready_count += Self::range_len(range) as u64;
            max_id = Some(max_id.map(|m| m.max(range.end)).unwrap_or(range.end));
        }

        while let Some((key_bytes, _value_bytes)) = delayed_iter.next() {
            let Some((visible_at_ms, id)) =
                Self::parse_delayed_index_key(&key_bytes, &self.delayed_index_prefix)
            else {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: "Malformed queue delayed index key".to_string(),
                };
            };
            self.insert_persisted_delayed(id, visible_at_ms);
            scanned_delayed_count += 1;
            scanned_next_delayed = Some(
                scanned_next_delayed
                    .map(|current| current.min(visible_at_ms))
                    .unwrap_or(visible_at_ms),
            );
            max_id = Some(max_id.map(|m| m.max(id.as_u64())).unwrap_or(id.as_u64()));

            if visible_at_ms <= now_epoch_ms {
                matured_delayed_ids.push(id);
            } else {
                let delay_ms = visible_at_ms.saturating_sub(now_epoch_ms);
                let visible_at = now_instant + Duration::from_millis(delay_ms);
                self.delayed.push(Reverse(DelayedMessage {
                    id,
                    enqueue_seq: id.as_u64(),
                    visible_at,
                    visible_at_ms,
                }));
                if visible_at < self.next_delayed_deadline {
                    self.next_delayed_deadline = visible_at;
                }
            }
        }

        while let Some((key_bytes, _value_bytes)) = dlq_iter.next() {
            let Some((dead_lettered_at_ms, id)) =
                Self::parse_dlq_index_key(&key_bytes, &self.dlq_index_prefix)
            else {
                return IndexRecoveryAttempt::Error {
                    next_id: meta_snapshot.next_id,
                    reason: "Malformed queue DLQ index key".to_string(),
                };
            };

            self.insert_persisted_dlq(id, dead_lettered_at_ms);
            max_id = Some(max_id.map(|m| m.max(id.as_u64())).unwrap_or(id.as_u64()));
        }

        if self.delayed.is_empty() {
            self.next_delayed_deadline = now_instant + Duration::from_secs(3600);
        }

        self.populate_live_ready_from_persisted(&matured_delayed_ids);

        if meta_snapshot.ready_count != scanned_ready_count
            || meta_snapshot.delayed_count != scanned_delayed_count
            || meta_snapshot.next_delayed_visibility_ms != scanned_next_delayed
        {
            self.index_meta_written = false;
            Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_INVALID);
            return IndexRecoveryAttempt::Invalid {
                next_id: self.load_next_id_from_meta_key(),
                reason: format!(
                    "Queue index meta counters mismatch (meta ready={}, scanned ready={}, meta delayed={}, scanned delayed={})",
                    meta_snapshot.ready_count,
                    scanned_ready_count,
                    meta_snapshot.delayed_count,
                    scanned_delayed_count
                ),
            };
        }

        Self::observe_elapsed_us(obs::METRIC_QUEUE_RECOVERY_INDEX_LOAD_LATENCY, start);
        Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_HITS);
        IndexRecoveryAttempt::Hit {
            next_id: meta_snapshot.next_id,
            max_id,
        }
    }

    fn rewrite_index_from_memory(&mut self, next_id: u64) -> Result<(), String> {
        let cf_id = self.queue_key.family.id();
        let mut txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("Failed to begin queue index rebuild tx: {:?}", e))?;

        let ready_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.ready_index_prefix));
        let delayed_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.delayed_index_prefix));
        let dlq_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.dlq_index_prefix));

        let mut ready_iter = txn
            .scan(&ready_query)
            .map_err(|e| format!("Failed to scan ready index for rebuild: {:?}", e))?;
        let mut delayed_iter = txn
            .scan(&delayed_query)
            .map_err(|e| format!("Failed to scan delayed index for rebuild: {:?}", e))?;
        let mut dlq_iter = txn
            .scan(&dlq_query)
            .map_err(|e| format!("Failed to scan DLQ index for rebuild: {:?}", e))?;
        let mut ready_keys = Vec::new();
        let mut delayed_keys = Vec::new();
        let mut dlq_keys = Vec::new();

        while let Some((key, _)) = ready_iter.next() {
            ready_keys.push(key.to_vec());
        }
        while let Some((key, _)) = delayed_iter.next() {
            delayed_keys.push(key.to_vec());
        }
        while let Some((key, _)) = dlq_iter.next() {
            dlq_keys.push(key.to_vec());
        }

        for key in ready_keys.into_iter().chain(delayed_keys).chain(dlq_keys) {
            txn.delete(key)
                .map_err(|e| format!("Failed to delete stale queue index key: {:?}", e))?;
        }

        for (shard, ranges) in self.persisted_ready_shards.iter().enumerate() {
            for range in ranges {
                txn.put(
                    self.ready_range_key(shard, range.next),
                    Self::encode_ready_range_value(*range),
                    None,
                )
                .map_err(|e| format!("Failed to write queue ready index: {:?}", e))?;
            }
        }

        for (&id, &visible_at_ms) in &self.persisted_delayed {
            txn.put(self.delayed_index_key(visible_at_ms, id), Vec::new(), None)
                .map_err(|e| format!("Failed to write queue delayed index: {:?}", e))?;
        }

        for (&id, &dead_lettered_at_ms) in &self.persisted_dlq {
            txn.put(
                self.dlq_index_key(dead_lettered_at_ms, id),
                Vec::new(),
                None,
            )
            .map_err(|e| format!("Failed to write queue DLQ index: {:?}", e))?;
        }

        txn.put(
            self.index_meta_key.clone(),
            Self::encode_index_meta(
                next_id,
                self.persisted_ready_count as u64,
                self.persisted_delayed.len() as u64,
                self.min_persisted_delayed_visibility_ms(),
            ),
            None,
        )
        .map_err(|e| format!("Failed to write queue index meta: {:?}", e))?;

        txn.commit(self.commit_write_options)
            .map_err(|e| format!("Failed to commit queue index rebuild: {:?}", e))?;
        self.index_meta_written = true;
        Ok(())
    }

    fn recover_from_scan_and_rebuild_index(
        &mut self,
        fallback_next_id: u64,
    ) -> Result<Option<u64>, String> {
        let cf_id = self.queue_key.family.id();
        let start = Instant::now();
        let txn = self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("Failed to begin recovery scan tx: {:?}", e))?;

        self.reset_recovery_state();

        let header_query =
            cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&self.header_key_prefix));
        let legacy_query = cntryl_midge::Query::new()
            .prefix(Bytes::copy_from_slice(&self.legacy_message_key_prefix));

        let mut header_iter = match txn.scan(&header_query) {
            Ok(iter) => iter,
            Err(e) if Self::is_missing_read_snapshot_error(&e) => return Ok(None),
            Err(e) => {
                return Err(format!(
                    "Failed to scan queue headers for recovery: {:?}",
                    e
                ))
            }
        };
        let mut legacy_iter = match txn.scan(&legacy_query) {
            Ok(iter) => iter,
            Err(e) if Self::is_missing_read_snapshot_error(&e) => return Ok(None),
            Err(e) => {
                return Err(format!(
                    "Failed to scan legacy queue records for recovery: {:?}",
                    e
                ))
            }
        };

        if header_iter.remaining() == 0 && legacy_iter.remaining() == 0 {
            return Ok(None);
        }

        let recovered_count = header_iter.remaining() + legacy_iter.remaining();
        let per_shard = recovered_count / Self::READY_SHARDS + 1;
        for shard in &mut self.ready_shards {
            shard.reserve(per_shard);
        }
        for shard in &mut self.persisted_ready_shards {
            shard.reserve(per_shard);
        }
        self.delayed.reserve(recovered_count);
        let mut recovered_ready_ids = Vec::with_capacity(recovered_count);

        let now_epoch_ms = self.clock.now_epoch_ms();
        let now_instant = self.clock.now_instant();
        let mut max_id = None::<u64>;

        while let Some((key_bytes, value_bytes)) = header_iter.next() {
            let Some(id) = Self::parse_message_id_from_key(&key_bytes, &self.header_key_prefix)
            else {
                continue;
            };
            let record = if let Ok(record) = Self::decode_record_header(&value_bytes) {
                record
            } else if let Ok(record) = Self::decode_legacy_record(value_bytes.clone()) {
                record.metadata_only_from()
            } else {
                continue;
            };

            max_id = Some(max_id.map(|m| m.max(id.as_u64())).unwrap_or(id.as_u64()));
            if matches!(record.state, QueueState::Dlq) {
                self.insert_persisted_dlq(
                    id,
                    record
                        .dead_lettered_at_ms
                        .unwrap_or(record.first_enqueued_at_ms),
                );
                continue;
            }
            if record.visible_at_ms <= now_epoch_ms {
                recovered_ready_ids.push(id);
            } else {
                let delay_ms = record.visible_at_ms.saturating_sub(now_epoch_ms);
                let visible_at = now_instant + Duration::from_millis(delay_ms);
                self.delayed.push(Reverse(DelayedMessage {
                    id,
                    enqueue_seq: if record.enqueue_seq != 0 {
                        record.enqueue_seq
                    } else {
                        id.as_u64()
                    },
                    visible_at,
                    visible_at_ms: record.visible_at_ms,
                }));
                self.insert_persisted_delayed(id, record.visible_at_ms);
                if visible_at < self.next_delayed_deadline {
                    self.next_delayed_deadline = visible_at;
                }
            }
        }

        while let Some((key_bytes, value_bytes)) = legacy_iter.next() {
            let Some(id) =
                Self::parse_message_id_from_key(&key_bytes, &self.legacy_message_key_prefix)
            else {
                continue;
            };
            let record = match Self::decode_legacy_record(value_bytes) {
                Ok(record) => record,
                Err(_) => continue,
            };

            max_id = Some(max_id.map(|m| m.max(id.as_u64())).unwrap_or(id.as_u64()));
            if matches!(record.state, QueueState::Dlq) {
                self.insert_persisted_dlq(
                    id,
                    record
                        .dead_lettered_at_ms
                        .unwrap_or(record.first_enqueued_at_ms),
                );
                continue;
            }
            if record.visible_at_ms <= now_epoch_ms {
                recovered_ready_ids.push(id);
            } else {
                let delay_ms = record.visible_at_ms.saturating_sub(now_epoch_ms);
                let visible_at = now_instant + Duration::from_millis(delay_ms);
                self.delayed.push(Reverse(DelayedMessage {
                    id,
                    enqueue_seq: if record.enqueue_seq != 0 {
                        record.enqueue_seq
                    } else {
                        id.as_u64()
                    },
                    visible_at,
                    visible_at_ms: record.visible_at_ms,
                }));
                self.insert_persisted_delayed(id, record.visible_at_ms);
                if visible_at < self.next_delayed_deadline {
                    self.next_delayed_deadline = visible_at;
                }
            }
        }

        if self.delayed.is_empty() {
            self.next_delayed_deadline = now_instant + Duration::from_secs(3600);
        }

        recovered_ready_ids.sort_unstable_by_key(|id| id.as_u64());
        for id in recovered_ready_ids {
            self.push_ready(id);
            self.push_persisted_ready(id);
        }

        Self::observe_elapsed_us(obs::METRIC_QUEUE_RECOVERY_FALLBACK_SCAN_LATENCY, start);
        Self::increment_counter(obs::METRIC_QUEUE_RECOVERY_INDEX_FALLBACKS);

        let rebuild_next_id = max_id
            .map(|value| value.saturating_add(1))
            .unwrap_or(fallback_next_id)
            .max(fallback_next_id);

        if let Err(e) = self.rewrite_index_from_memory(rebuild_next_id) {
            eprintln!(
                "WARN: Failed to rewrite queue recovery index for queue {:?}: {}",
                self.queue_key, e
            );
        }
        Ok(max_id)
    }

    /// Recover ready queue and delayed queue from durable storage (V-003 Fix)
    ///
    /// This is called during actor construction to restore state after restart.
    /// For competing consumers, this is CRITICAL: messages that were in-flight
    /// when the process crashed are recovered and re-enqueued (automatic redelivery).
    ///
    /// # Algorithm
    ///
    /// 1. Scan all persisted messages (msg:{id})
    /// 2. For each message:
    ///    - Determine visibility status based on visible_at_ms epoch
    ///    - If visible_at_ms <= now: add to ready queue
    ///    - If visible_at_ms > now: add to delayed queue
    /// 3. Track maximum ID seen (for next_id)
    ///
    /// # Competing Consumer Semantics
    ///
    /// In-flight messages (leases that were held when process crashed) are automatically
    /// redelivered because they're not persisted in the inflight map (ephemeral).
    /// This is by design: lease state is not durable, so any reserved message is
    /// immediately available for another competing consumer to reserve after restart.
    ///
    /// # Minimal Data Loss Model
    ///
    /// Messages may be lost if:
    /// - Batch commit was buffered and never flushed before crash
    /// - But we use sync() writes, so this is unlikely
    ///
    /// Messages are guaranteed to survive if:
    /// - Batch commit returned successfully (sync())
    fn recover_from_store(&mut self) {
        let (mut next_id, max_id) = match self.try_recover_from_index() {
            IndexRecoveryAttempt::Hit { next_id, max_id } => {
                self.recovery_path = RecoveryPath::IndexHit;
                (next_id, max_id)
            }
            IndexRecoveryAttempt::Missing { next_id } => {
                self.recovery_path = RecoveryPath::IndexMissingFallback;
                let max_id = match self.recover_from_scan_and_rebuild_index(next_id) {
                    Ok(max_id) => max_id,
                    Err(e) => {
                        eprintln!(
                            "WARN: Failed to rebuild queue index for queue {:?}: {}",
                            self.queue_key, e
                        );
                        None
                    }
                };
                (next_id, max_id)
            }
            IndexRecoveryAttempt::Invalid { next_id, reason } => {
                eprintln!(
                    "WARN: Queue index recovery found invalid index for queue {:?}: {}, falling back to full scan",
                    self.queue_key, reason
                );
                self.recovery_path = RecoveryPath::IndexInvalidFallback;
                let max_id = match self.recover_from_scan_and_rebuild_index(next_id) {
                    Ok(max_id) => max_id,
                    Err(scan_err) => {
                        eprintln!(
                            "WARN: Queue fallback recovery failed for queue {:?}: {}",
                            self.queue_key, scan_err
                        );
                        None
                    }
                };
                (next_id, max_id)
            }
            IndexRecoveryAttempt::Error { next_id, reason } => {
                eprintln!(
                    "WARN: Queue index recovery failed for queue {:?}: {}, falling back to full scan",
                    self.queue_key, reason
                );
                self.recovery_path = RecoveryPath::IndexErrorFallback;
                let max_id = match self.recover_from_scan_and_rebuild_index(next_id) {
                    Ok(max_id) => max_id,
                    Err(scan_err) => {
                        eprintln!(
                            "WARN: Queue fallback recovery failed for queue {:?}: {}",
                            self.queue_key, scan_err
                        );
                        None
                    }
                };
                (next_id, max_id)
            }
        };

        if let Some(max_id) = max_id {
            next_id = next_id.max(max_id.saturating_add(1));
        }

        self.next_id = next_id;
        self.next_id_limit = next_id;
        if self.ready_len() == 0
            && self.delayed.is_empty()
            && self.persisted_dlq.is_empty()
            && self.recovery_path == RecoveryPath::IndexHit
        {
            self.recovery_path = RecoveryPath::Empty;
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::runtime::routing::RouteFamily;
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

    #[test]
    fn should_reserve_enqueued_message() {
        // Arrange
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs");
        // TODO: Use CF=0 due to Midge test limitation (only default CF exists in in-memory engine)
        // Production uses proper RouteFamily â†’ CF mapping once Midge supports multi-CF registration
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key,
            store,
            None,
            crate::utils::idempotency::global_dedup_store(),
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
                assert_eq!(messages[0].lease_seconds, 30);
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
                crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
                crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
                crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
                crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
    fn should_extend_lease_with_valid_token() {
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
    fn should_redeliver_message_when_lease_expires() {
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
        );

        let body = Bytes::from("test message");
        actor.handle_send(body, None);
        let reserve_response = actor.handle_receive(30, Some(1));

        let (msg_id, token) = match reserve_response {
            QueueResponse::Received { messages } => (messages[0].id, messages[0].token),
            _ => panic!("Expected Received response"),
        };

        // Act - Extend before first timer expires
        clock.advance(Duration::from_secs(15));
        actor.handle_extend(msg_id, token, 60);

        // Advance to first timer expiration (30s total)
        clock.advance(Duration::from_secs(15));
        actor.process_expired_timers();

        // Assert - Message still inflight (stale timer ignored)
        assert_eq!(actor.ready_len(), 0);
        assert_eq!(actor.inflight.len(), 1);
    }

    #[test]
    fn should_reject_operations_on_expired_lease() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-expired-lease");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::global_dedup_store(),
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
        // However the logic checks expiration first, so it returns LeaseExpired
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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

            // Expire lease (simulating failed processing)
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
                crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
        assert_eq!(snapshot.messages_leased, 0);
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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
            crate::utils::idempotency::global_dedup_store(),
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

            // Expire lease
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
