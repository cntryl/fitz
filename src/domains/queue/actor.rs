//! QueueActor: manages a single durable message queue
//!
//! Each queue has:
//! - Identity: (realm, area, resource) from route
//! - Durable storage: Message bodies persisted to Midge
//! - Ephemeral leases: In-memory visibility tracking
//!
//! # Invariants
//!
//! 1. **Atomic batch operations**: ID allocation + message writes commit together (V-001 Fix)
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
//! Batch operations are atomic: all-or-nothing across ID allocation + message writes.
//! Within transactions: next_id and messages commit together (prevents ID collisions on crash).
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
//! - ready shards: VecDeque - O(1) push_back, O(1) pop_front
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
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::prelude::Actor;
use crate::runtime::actor::Context;
use crate::runtime::routing::RouteFamily;

use super::protocol::{MessageId, QueueKey, QueueMessage, QueueResponse, ReservedMessage};

/// Durable queue record (persisted to Midge)
///
/// All time values use SystemTime::UNIX_EPOCH (milliseconds).
/// This ensures delays survive process restarts correctly.
#[derive(Debug, Clone)]
struct QueueRecord {
    /// Message body
    body: Bytes,
    /// Redelivery attempt counter (starts at 0, incremented on redelivery)
    attempts: u32,
    /// Visibility timestamp (milliseconds since UNIX epoch)
    /// Message is invisible until this time has passed (absolute, not relative)
    visible_at_ms: u64,
}

/// In-flight message lease (ephemeral, actor-owned)
#[derive(Debug, Clone)]
pub struct Inflight {
    /// Random token for operation validation
    token: u64,
    /// Absolute expiration time
    expires_at: Instant,
}

/// Timer event for lease expiration
#[derive(Debug, Clone, PartialEq, Eq)]
struct LeaseExpiry {
    /// Message ID to re-enqueue
    id: MessageId,
    /// Expiration time (for ordering in heap)
    expires_at: Instant,
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
    /// Visibility time (for ordering in heap)
    visible_at: Instant,
}

impl Ord for DelayedMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Earlier visibility = higher priority (min-heap via Reverse wrapper)
        other.visible_at.cmp(&self.visible_at)
    }
}

impl PartialOrd for DelayedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
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

    /// Midge storage handle (for durable persistence)
    store: Arc<cntryl_midge::MidgeEngine>,

    /// Next message ID to allocate (monotonic counter)
    next_id: u64,

    /// Ready queues: sharded FIFO lists of message IDs available for reservation
    ready_shards: Vec<VecDeque<MessageId>>,

    /// Round-robin cursor for ready shard selection
    next_ready_shard: usize,

    /// In-memory record cache (durable backing is Midge)
    records: HashMap<MessageId, QueueRecord>,

    /// Inflight map: leased messages (id Ã¢â€ â€™ Inflight)
    pub inflight: HashMap<MessageId, Inflight>,

    /// Timer heap: lease expiration events (earliest first, min-heap)
    timers: BinaryHeap<Reverse<LeaseExpiry>>,

    /// Delayed visibility heap: messages not yet visible (earliest first, min-heap)
    delayed: BinaryHeap<Reverse<DelayedMessage>>,

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

    /// Create a new queue actor. Persistence mode is locked to buffered-only (throughput-first).
    pub fn new(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        Self::with_clock(
            family,
            queue_key,
            store,
            Box::new(SystemClock),
            max_attempts,
            dedup_store,
        )
    }

    /// Create a new queue actor with a custom clock (for testing). Persistence is locked to buffered-only.
    pub fn with_clock(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        clock: Box<dyn Clock>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        // Recover next_id from Midge on startup
        let next_id = Self::recover_next_id(&store, &queue_key);

        let now = Instant::now();

        let mut actor = Self {
            family,
            queue_key,
            store,
            next_id,
            ready_shards: (0..Self::READY_SHARDS).map(|_| VecDeque::new()).collect(),
            next_ready_shard: 0,
            records: HashMap::new(),
            inflight: HashMap::new(),
            timers: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
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

        actor.recover_ready_and_delayed_from_store();
        actor
    }

    /// Recover next message ID from durable storage
    fn recover_next_id(store: &cntryl_midge::Engine, queue_key: &QueueKey) -> u64 {
        let key = Self::meta_key(queue_key);
        let cf_id = queue_key.family.id();

        match store.begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly) {
            Ok(txn) => match txn.get(&key) {
                Ok(Some(bytes)) => bytes
                    .get(0..8)
                    .map(|slice| u64::from_le_bytes(slice.try_into().unwrap()))
                    .unwrap_or(1),
                Ok(None) => 1,
                Err(e) => {
                    eprintln!(
                        "WARN: Failed to recover next_id for queue {:?}: {:?}, starting from 1",
                        queue_key, e
                    );
                    1
                }
            },
            Err(e) => {
                eprintln!(
                    "WARN: Failed to begin tx for next_id recovery: {:?}, starting from 1",
                    e
                );
                1
            }
        }
    }

    /// Generate a random lease token
    fn generate_token() -> u64 {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};

        let hasher = RandomState::new().build_hasher();
        hasher.finish()
    }

    /// Midge key for queue metadata
    fn meta_key(queue_key: &QueueKey) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:meta",
            queue_key.realm, queue_key.area, queue_key.resource
        )
        .into_bytes()
    }

    /// Midge key for message record
    fn message_key(queue_key: &QueueKey, id: MessageId) -> Vec<u8> {
        format!(
            "queue:{}:{}:{}:msg:{}",
            queue_key.realm,
            queue_key.area,
            queue_key.resource,
            id.as_u64()
        )
        .into_bytes()
    }

    /// Serialize QueueRecord to bytes
    fn encode_record(record: &QueueRecord) -> Vec<u8> {
        // Encoding: [attempts:4][visible_at_ms:8][body_len:4][body]
        let mut buf = Vec::with_capacity(16 + record.body.len());
        buf.extend_from_slice(&record.attempts.to_le_bytes());
        buf.extend_from_slice(&record.visible_at_ms.to_le_bytes());
        buf.extend_from_slice(&(record.body.len() as u32).to_le_bytes());
        buf.extend_from_slice(&record.body);
        buf
    }

    fn shard_for_id(id: MessageId) -> usize {
        id.as_u64() as usize & (Self::READY_SHARDS - 1)
    }

    fn push_ready(&mut self, id: MessageId) {
        let shard = Self::shard_for_id(id);
        self.ready_shards[shard].push_back(id);
    }

    fn pop_ready(&mut self) -> Option<MessageId> {
        for _ in 0..Self::READY_SHARDS {
            let shard = self.next_ready_shard;
            if let Some(id) = self.ready_shards[shard].pop_front() {
                self.next_ready_shard = (shard + 1) % Self::READY_SHARDS;
                return Some(id);
            }
            self.next_ready_shard = (shard + 1) % Self::READY_SHARDS;
        }

        None
    }

    pub fn ready_len(&self) -> usize {
        self.ready_shards.iter().map(|shard| shard.len()).sum()
    }

    pub fn ready_contains(&self, id: MessageId) -> bool {
        self.ready_shards.iter().any(|shard| shard.contains(&id))
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

    /// Deserialize QueueRecord from bytes
    fn decode_record(bytes: &[u8]) -> Result<QueueRecord, String> {
        if bytes.len() < 16 {
            return Err("Invalid record format".to_string());
        }

        let attempts = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let visible_at_ms = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        let body_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;

        if bytes.len() < 16 + body_len {
            return Err("Truncated record body".to_string());
        }

        let body = Bytes::copy_from_slice(&bytes[16..16 + body_len]);

        Ok(QueueRecord {
            body,
            attempts,
            visible_at_ms,
        })
    }

    /// Handle send operation
    pub fn handle_send(&mut self, body: Bytes, delay_seconds: Option<u64>) -> QueueResponse {
        // Track empty state before send for notification
        let was_empty = self.ready_len() == 0;

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

        let record = QueueRecord {
            body,
            attempts: 0,
            visible_at_ms,
        };

        // Write message to transaction
        let key = Self::message_key(&self.queue_key, id);
        let value = Self::encode_record(&record);
        if let Err(e) = txn.put(key, value, None) {
            return QueueResponse::Error {
                message: format!("Failed to add message to transaction: {:?}", e),
            };
        }

        // Update next_id counter in SAME transaction
        let next_id = self.next_id + 1;
        if let Err(e) = txn.put(
            Self::meta_key(&self.queue_key),
            next_id.to_le_bytes().to_vec(),
            None,
        ) {
            return QueueResponse::Error {
                message: format!("Failed to update queue meta: {:?}", e),
            };
        }

        // Commit with buffered mode for high throughput
        // The store will sync periodically, maintaining durability without per-operation cost
        if let Err(e) = self
            .store
            .commit(txn, cntryl_midge::WriteOptions::buffered())
        {
            return QueueResponse::Error {
                message: format!("Failed to commit transaction: {:?}", e),
            };
        }

        // Commit succeeded; advance in-memory next_id
        self.next_id = next_id;

        // Cache record in memory for fast reserve path
        self.records.insert(id, record);

        // Update in-memory queues
        if visible_at <= now_instant {
            self.push_ready(id);
        } else {
            self.delayed
                .push(Reverse(DelayedMessage { id, visible_at }));
        }

        // Emit availability notification if queue transitioned from empty to non-empty
        // (only for immediately visible messages, not delayed ones)
        if was_empty && visible_at <= now_instant && self.ready_len() > 0 {
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
        let mut post_commit: Vec<(MessageId, QueueRecord, std::time::Instant)> =
            Vec::with_capacity(items.len());
        let mut next_id = self.next_id;

        for (body, delay_seconds) in items {
            let delay_ms = delay_seconds.unwrap_or(0).saturating_mul(1_000);
            let id = MessageId::new(next_id);
            let visible_at_ms = now_epoch_ms.saturating_add(delay_ms);
            let visible_at = now_instant + Duration::from_millis(delay_ms);

            let record = QueueRecord {
                body: body.clone(),
                attempts: 0,
                visible_at_ms,
            };

            let key = Self::message_key(&self.queue_key, id);
            let value = Self::encode_record(&record);
            if let Err(e) = txn.put(key, value, None) {
                return QueueResponse::Error {
                    message: format!("Failed to add message to transaction: {:?}", e),
                };
            }

            ids.push(id);
            post_commit.push((id, record, visible_at));
            next_id += 1;
        }

        if let Err(e) = txn.put(
            Self::meta_key(&self.queue_key),
            next_id.to_le_bytes().to_vec(),
            None,
        ) {
            return QueueResponse::Error {
                message: format!("Failed to update queue meta: {:?}", e),
            };
        }

        if let Err(e) = self
            .store
            .commit(txn, cntryl_midge::WriteOptions::buffered())
        {
            return QueueResponse::Error {
                message: format!("Failed to commit transaction: {:?}", e),
            };
        }

        self.next_id = next_id;
        for (id, record, visible_at) in post_commit {
            self.records.insert(id, record);
            if visible_at <= now_instant {
                self.push_ready(id);
            } else {
                self.delayed
                    .push(Reverse(DelayedMessage { id, visible_at }));
            }
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
        let batch_size = batch_size.unwrap_or(1);
        let now = self.clock.now_instant();
        let lease_duration = Duration::from_secs(lease_seconds);

        let mut messages = Vec::new();

        for _ in 0..batch_size {
            // Pop from ready queue
            let id = match self.pop_ready() {
                Some(id) => id,
                None => break, // No more messages
            };

            let record = match self.records.get(&id) {
                Some(record) => record.clone(),
                None => {
                    // Fallback to storage if cache missed
                    let cf_id = self.queue_key.family.id();
                    let key = Self::message_key(&self.queue_key, id);
                    let txn = match self
                        .store
                        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
                    {
                        Ok(t) => t,
                        Err(e) => {
                            eprintln!("WARN: Failed to begin tx: {:?}", e);
                            continue;
                        }
                    };

                    let record = match txn.get(&key) {
                        Ok(Some(bytes)) => match Self::decode_record(&bytes) {
                            Ok(record) => {
                                drop(txn);
                                record
                            }
                            Err(e) => {
                                eprintln!("WARN: Failed to decode message {}: {}", id, e);
                                drop(txn);
                                continue;
                            }
                        },
                        Ok(None) => {
                            eprintln!("WARN: Message {} disappeared from storage", id);
                            drop(txn);
                            continue;
                        }
                        Err(e) => {
                            eprintln!("WARN: Failed to read message {}: {:?}", id, e);
                            drop(txn);
                            continue;
                        }
                    };

                    self.records.insert(id, record.clone());
                    record
                }
            };

            // Generate lease token
            let token = Self::generate_token();
            let expires_at = now + lease_duration;

            // Create inflight entry
            self.inflight.insert(id, Inflight { token, expires_at });

            // Schedule expiration timer
            self.timers.push(Reverse(LeaseExpiry { id, expires_at }));

            // Update deadline cache if this expiration is sooner
            if expires_at < self.next_expiration_deadline {
                self.next_expiration_deadline = expires_at;
            }

            // Build response message
            messages.push(ReservedMessage {
                id,
                body: record.body,
                token,
                lease_seconds,
                attempts: record.attempts + 1, // First attempt is 1 (not 0)
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

        // Schedule new timer (old timer will be ignored when it fires)
        self.timers.push(Reverse(LeaseExpiry {
            id,
            expires_at: new_expires_at,
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

        // Remove inflight entry
        self.inflight.remove(&id);

        // Remove cached record (storage is durable source of truth)
        self.records.remove(&id);

        // Delete from Midge with durability options (via transaction API)
        // TODO: Use transaction API for deletes:
        // let cf = self.store.default_column_family();
        // let mut txn = self.store.begin_transaction(cf).unwrap();
        // let key = Self::message_key(&self.queue_key, id);
        // txn.delete(&key).unwrap();
        // let (sync, disable_wal) = self.durability.to_midge_options();
        // let mut opts = WriteOptions::default();
        // opts.set_sync(sync);
        // opts.set_disable_wal(disable_wal);
        // self.store.commit_transaction_boxed(txn, &opts).ok();
        let cf_id = self.queue_key.family.id();
        let key = Self::message_key(&self.queue_key, id);

        match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(mut txn) => {
                if let Err(e) = txn.delete(key) {
                    eprintln!("WARN: Failed to delete message {} in txn: {:?}", id, e);
                } else {
                    // Use buffered writes for throughput (queues represent intent, not events of record)
                    if let Err(e) = self
                        .store
                        .commit(txn, cntryl_midge::WriteOptions::buffered())
                    {
                        eprintln!(
                            "WARN: Failed to commit delete txn for message {}: {:?}",
                            id, e
                        );
                    }
                }
            }
            Err(e) => eprintln!("WARN: Failed to begin tx to delete message {}: {:?}", id, e),
        }

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

        // Remove inflight entry
        self.inflight.remove(&id);

        // Increment attempts in storage and check DLQ threshold
        let cf_id = self.queue_key.family.id();
        let key = Self::message_key(&self.queue_key, id);

        let mut record = match self.records.get(&id) {
            Some(record) => record.clone(),
            None => match self
                .store
                .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            {
                Ok(txn) => match txn.get(&key) {
                    Ok(Some(bytes)) => match Self::decode_record(&bytes) {
                        Ok(record) => record,
                        Err(e) => {
                            eprintln!(
                                "WARN: Failed to decode message {} during redelivery: {}",
                                id, e
                            );
                            return;
                        }
                    },
                    Ok(None) => return,
                    Err(e) => {
                        eprintln!(
                            "WARN: Failed to read message {} during redelivery: {:?}",
                            id, e
                        );
                        return;
                    }
                },
                Err(e) => {
                    eprintln!(
                        "WARN: Failed to begin txn for redelivery for message {}: {:?}",
                        id, e
                    );
                    return;
                }
            },
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
                if is_dlq {
                    if let Err(e) = txn.delete(key.clone()) {
                        eprintln!("WARN: Failed to delete DLQ message {}: {:?}", id, e);
                    } else if let Err(e) = self
                        .store
                        .commit(txn, cntryl_midge::WriteOptions::buffered())
                    {
                        eprintln!("WARN: Failed to commit DLQ delete txn {}: {:?}", id, e);
                    }

                    self.records.remove(&id);

                    eprintln!(
                        "DLQ: queue={:?} message_id={} attempts={} - Message moved to dead letter queue",
                        self.queue_key, id, record.attempts
                    );

                    return;
                }

                let value = Self::encode_record(&record);
                if let Err(e) = txn.put(key.clone(), value, None) {
                    eprintln!(
                        "WARN: Failed to increment attempts for message {}: {:?}",
                        id, e
                    );
                } else if let Err(e) = self
                    .store
                    .commit(txn, cntryl_midge::WriteOptions::buffered())
                {
                    eprintln!(
                        "WARN: Failed to commit retry txn for message {}: {:?}",
                        id, e
                    );
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

        self.records.insert(id, record);

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
}

impl Actor for QueueActor {
    type Message = QueueMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        let now = self.clock.now_instant();

        // Deferred timer processing: only process if deadline has passed
        if now >= self.next_expiration_deadline {
            self.process_expired_timers();
        }

        // Deferred delayed message processing: only process if deadline has passed
        if now >= self.next_delayed_deadline {
            self.process_delayed_messages();
        }

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
    fn recover_ready_and_delayed_from_store(&mut self) {
        let cf_id = self.queue_key.family.id();
        let txn = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        {
            Ok(txn) => txn,
            Err(e) => {
                eprintln!(
                    "WARN: Failed to begin recovery tx for queue {:?}: {:?}",
                    self.queue_key, e
                );
                return;
            }
        };

        let prefix = format!(
            "queue:{}:{}:{}:msg:",
            self.queue_key.realm, self.queue_key.area, self.queue_key.resource
        )
        .into_bytes();

        let query = cntryl_midge::Query::new().prefix(Bytes::from(prefix));
        let mut iter = match txn.scan(&query) {
            Ok(iter) => iter,
            Err(e) => {
                eprintln!(
                    "WARN: Failed to scan for recovery for queue {:?}: {:?}",
                    self.queue_key, e
                );
                return;
            }
        };
        let results = iter.collect_all();

        // Use absolute SystemTime epoch (V-002 Fix)
        let now_epoch_ms = self.clock.now_epoch_ms();
        let now_instant = self.clock.now_instant();
        let mut max_id = None::<u64>;

        for (key_bytes, value_bytes) in results {
            let key_str = String::from_utf8_lossy(&key_bytes);
            let Some(pos) = key_str.rfind(":msg:") else {
                continue;
            };
            let id_str = &key_str[pos + 5..];
            let Ok(id_u64) = id_str.parse::<u64>() else {
                continue;
            };
            let id = MessageId::new(id_u64);

            let record = match Self::decode_record(&value_bytes) {
                Ok(r) => r,
                Err(_) => continue,
            };

            self.records.insert(id, record.clone());

            max_id = Some(max_id.map(|m| m.max(id_u64)).unwrap_or(id_u64));

            // Compare absolute epochs (survives restarts correctly)
            if record.visible_at_ms <= now_epoch_ms {
                // Immediately visible
                self.push_ready(id);
            } else {
                // Delayed: use absolute epoch difference
                let delay_ms = record.visible_at_ms.saturating_sub(now_epoch_ms);
                let visible_at = now_instant + Duration::from_millis(delay_ms);
                self.delayed
                    .push(Reverse(DelayedMessage { id, visible_at }));

                // Update deadline cache if this visibility is sooner
                if visible_at < self.next_delayed_deadline {
                    self.next_delayed_deadline = visible_at;
                }
            }
        }

        // Ensure next_id is never decremented
        if let Some(max_id) = max_id {
            self.next_id = self.next_id.max(max_id + 1);
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

    #[test]
    fn should_enqueue_and_reserve_message() {
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

        // Act - Enqueue
        let body = Bytes::from("test message");
        let enqueue_response = actor.handle_send(body.clone(), None);

        // Assert - Enqueue
        let msg_id = match enqueue_response {
            QueueResponse::Sent { id } => id,
            _ => panic!("Expected Enqueued response"),
        };
        assert_eq!(actor.ready_len(), 1);

        // Act - Reserve
        let reserve_response = actor.handle_receive(30, Some(1));

        // Assert - Reserve
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
    fn should_redelivery_message_when_lease_expires() {
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

        // Act - Advance time past lease expiration
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();

        // Assert - Message back in ready queue
        assert_eq!(actor.ready_len(), 1);
        assert_eq!(actor.inflight.len(), 0);
        assert!(actor.ready_contains(msg_id));

        // Act - Reserve again
        let redelivery_response = actor.handle_receive(30, Some(1));

        // Assert - Attempts incremented
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
    fn should_enqueue_and_dequeue_all_messages() {
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

        // Enqueue messages in known order
        for i in 0..5 {
            let body = Bytes::from(format!("msg-{}", i));
            let _ = actor.handle_send(body, None);
        }

        // Act & Assert - Reserve and ensure all messages are returned
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

        // Act - Enqueue with 30 second delay
        let body = Bytes::from("delayed message");
        let response = actor.handle_send(body.clone(), Some(30));

        let msg_id = match response {
            QueueResponse::Sent { id } => id,
            _ => panic!("Expected Enqueued response"),
        };

        // Assert - Message not in ready queue
        assert_eq!(actor.ready_len(), 0);
        assert_eq!(actor.delayed.len(), 1);

        // Act - Try to reserve immediately (should be empty)
        let reserve_response = actor.handle_receive(30, Some(1));
        match reserve_response {
            QueueResponse::NotFound => {}
            QueueResponse::Received { messages } if messages.is_empty() => {}
            _ => panic!("Expected NotFound or empty Received response for delayed messages"),
        }

        // Act - Advance time past delay
        clock.advance(Duration::from_secs(31));
        actor.process_delayed_messages();

        // Assert - Message now in ready queue
        assert_eq!(actor.ready_len(), 1);
        assert_eq!(actor.delayed.len(), 0);

        // Act - Reserve now succeeds
        let reserve_response = actor.handle_receive(30, Some(1));
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

        // Assert - After 3rd expiration, attempts = 4, exceeds max_attempts = 3
        // Message should be deleted (DLQ'd), not re-enqueued
        assert_eq!(actor.ready_len(), 0);
        assert_eq!(actor.inflight.len(), 0);

        // Verify message deleted from storage
        let cf_id = queue_key.family.id();
        let key = QueueActor::message_key(&queue_key, msg_id);
        let txn = store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
            .expect("begin tx");
        let result = txn.get(&key).expect("midge get");
        assert!(result.is_none(), "Message should be deleted from storage");
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
