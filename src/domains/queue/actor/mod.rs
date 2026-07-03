//! `QueueActor`: manages a single message queue with configurable durability
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
//! 5. **Policy-scoped recovery**: State that reached durable storage is restored on restart
//! 6. **Correct time semantics**: Delays use absolute `SystemTime` epochs (V-002 Fix)
//! 7. **Fair distribution**: Competing consumers get best-effort ready-queue order
//!
//! # Intent vs Events
//!
//! Queues represent **intent** (work to be done), not events of record.
//! Minimal data loss is acceptable under the fast queue write policy (producers can regenerate work items).
//! Messages commit together with any required ID-reservation extension.
//! Crashes may create ID gaps, but never ID reuse or collisions.
//!
//! # State Model
//!
//! ```text
//! ENQUEUE -> [READY QUEUE]
//!             | reserve
//!           [INFLIGHT] --complete-> DELETED
//!             | expire
//!           [READY QUEUE] (redelivery with attempts++)
//! ```
//!
//! # Performance Model
//!
//! - ready shards: `VecDeque` of compressed ranges - O(1) `push_back`, O(1) `pop_front`
//! - inflight: `HashMap` - O(1) lookup, O(1) insert, O(1) remove
//! - timers: `BinaryHeap` - O(log n) push, O(1) peek, O(log n) pop
//!
//! # Storage Model
//!
//! Midge keys:
//! - `queue:{realm}:{area}:{resource}:msg:{id}` -> `QueueRecord` (body, attempts)
//! - `queue:{realm}:{area}:{resource}:meta` -> [`next_id:8`]

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fxhash::FxBuildHasher;
use lexkey::LexKey;

use crate::observability as obs;
use crate::runtime::clock::{Clock, SystemClock};
use crate::runtime::routing::RouteFamily;
use crate::utils::storage_key::{self, DomainKeyspace};

use super::{
    MessageId, QueueAdminSnapshot, QueueDeadLetterSnapshot, QueueInflightSnapshot, QueueKey,
    QueueResponse, ReservedMessage,
};

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_ACK_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_NEXT_REDELIVERY_COMMIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

pub(crate) mod recovery;
pub(crate) mod state;
pub(crate) mod storage;
pub(crate) mod timers;

type FastMap<K, V> = HashMap<K, V, FxBuildHasher>;

const CACHED_RESPONSE_VERSION: u8 = 1;
const CACHED_RESPONSE_ACKED: u8 = 0;
const CACHED_RESPONSE_NOT_FOUND: u8 = 1;
const QUEUE_KEY_FAMILY_META: u8 = 0x01;
const QUEUE_KEY_FAMILY_INDEX_META: u8 = 0x02;
const QUEUE_KEY_FAMILY_HEADER: u8 = 0x03;
const QUEUE_KEY_FAMILY_BODY: u8 = 0x04;
const QUEUE_KEY_FAMILY_LEGACY_MESSAGE: u8 = 0x05;
const QUEUE_KEY_FAMILY_READY_INDEX: u8 = 0x10;
const QUEUE_KEY_FAMILY_DELAYED_INDEX: u8 = 0x11;
const QUEUE_KEY_FAMILY_INFLIGHT_INDEX: u8 = 0x12;
const QUEUE_KEY_FAMILY_DLQ_INDEX: u8 = 0x13;
const QUEUE_KEY_FAMILY_ACK_DEDUP: u8 = 0x14;

fn encode_cached_response(response: &QueueResponse) -> Option<Vec<u8>> {
    match response {
        QueueResponse::Acked => Some(vec![CACHED_RESPONSE_VERSION, CACHED_RESPONSE_ACKED]),
        QueueResponse::NotFound => Some(vec![CACHED_RESPONSE_VERSION, CACHED_RESPONSE_NOT_FOUND]),
        _ => None,
    }
}

fn decode_cached_response(bytes: &[u8]) -> Result<QueueResponse, String> {
    match bytes {
        [version, tag] if *version == CACHED_RESPONSE_VERSION => match *tag {
            CACHED_RESPONSE_ACKED => Ok(QueueResponse::Acked),
            CACHED_RESPONSE_NOT_FOUND => Ok(QueueResponse::NotFound),
            other => Err(format!("Unknown cached response tag {other}")),
        },
        [version, ..] => Err(format!("Unknown cached response version {version}")),
        _ => Err("Cached response payload too short".to_string()),
    }
}

/// Durable queue record (persisted to Midge)
///
/// All time values use `SystemTime::UNIX_EPOCH` (milliseconds).
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
#[allow(clippy::struct_field_names)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct QueueActorLiveCounts {
    pub(crate) ready: usize,
    pub(crate) delayed: usize,
    pub(crate) inflight: usize,
    pub(crate) dead_letters: usize,
}

impl QueueActorLiveCounts {
    #[must_use]
    pub(crate) fn total(self) -> usize {
        self.ready
            .saturating_add(self.delayed)
            .saturating_add(self.inflight)
            .saturating_add(self.dead_letters)
    }
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

#[derive(Debug, Clone)]
struct RollingRateWindow {
    events_epoch_ms: VecDeque<u64>,
    total: u64,
}

impl RollingRateWindow {
    const WINDOW_MS: u64 = 60_000;

    fn new() -> Self {
        Self {
            events_epoch_ms: VecDeque::new(),
            total: 0,
        }
    }

    fn record(&mut self, now_epoch_ms: u64, count: u64) {
        self.total = self.total.saturating_add(count);
        for _ in 0..count {
            self.events_epoch_ms.push_back(now_epoch_ms);
        }
        self.prune(now_epoch_ms);
    }

    fn total(&self) -> u64 {
        self.total
    }

    fn rate_per_second(&self, now_epoch_ms: u64) -> f64 {
        let cutoff = now_epoch_ms.saturating_sub(Self::WINDOW_MS);
        let current_window_count = self
            .events_epoch_ms
            .iter()
            .filter(|&&event_epoch_ms| event_epoch_ms >= cutoff)
            .count();
        QueueActor::usize_to_f64(current_window_count)
            / (QueueActor::u64_to_f64(Self::WINDOW_MS) / 1_000.0)
    }

    fn prune(&mut self, now_epoch_ms: u64) {
        let cutoff = now_epoch_ms.saturating_sub(Self::WINDOW_MS);
        while self
            .events_epoch_ms
            .front()
            .is_some_and(|event_epoch_ms| *event_epoch_ms < cutoff)
        {
            self.events_epoch_ms.pop_front();
        }
    }
}

impl QueueActor {
    fn ready_shards_u64() -> u64 {
        u64::try_from(Self::READY_SHARDS).unwrap_or(u64::MAX)
    }

    fn ready_shard_index(id: u64) -> usize {
        let shard_mask = u64::try_from(Self::READY_SHARDS - 1).unwrap_or(u64::MAX);
        usize::try_from(id & shard_mask).unwrap_or(0)
    }

    fn usize_to_u64(value: usize) -> u64 {
        u64::try_from(value).unwrap_or(u64::MAX)
    }

    fn usize_to_f64(value: usize) -> f64 {
        f64::from(u32::try_from(value).unwrap_or(u32::MAX))
    }

    fn u64_to_f64(value: u64) -> f64 {
        f64::from(u32::try_from(value).unwrap_or(u32::MAX))
    }
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
/// - `family`: `RouteFamily` this actor serves (for validation)
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

    /// Current-process successful enqueue accounting for admin visibility.
    enqueue_success_window: RollingRateWindow,

    /// Current-process successful COMPLETE accounting for admin visibility.
    complete_success_window: RollingRateWindow,

    /// Cached next expiration deadline (deferred timer processing)
    /// Only process timers if current time >= this deadline
    next_expiration_deadline: Instant,

    /// Cached next delayed message deadline (deferred delayed processing)
    /// Only process delayed messages if current time >= this deadline
    next_delayed_deadline: Instant,
}

mod admin_snapshot;
mod constructors_validation;
mod dead_letters_and_timers;
mod enqueue;
mod recovery_state;
mod reserve_and_ack;
mod storage_keys;

#[cfg(test)]
mod tests;
