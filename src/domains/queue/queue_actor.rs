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
//! 7. **Fair distribution**: Competing consumers get messages in ready-queue order
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
//! - ready: VecDeque - O(1) push_back, O(1) pop_front
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
/// - `ready`: FIFO queue of ready message IDs
/// - `inflight`: Map of leased messages (id Ã¢â€ â€™ Inflight)
/// - `timers`: Min-heap of expiration events (earliest first)
/// - `clock`: Time source for expiration checks
///
/// # Actor Responsibilities
///
/// - Maintain FIFO ordering via ready queue
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

    /// Ready queue: FIFO list of message IDs available for reservation
    pub ready: VecDeque<MessageId>,

    /// Inflight map: leased messages (id Ã¢â€ â€™ Inflight)
    pub inflight: HashMap<MessageId, Inflight>,

    /// Timer heap: lease expiration events (earliest first, min-heap)
    timers: BinaryHeap<Reverse<LeaseExpiry>>,

    /// Delayed visibility heap: messages not yet visible (earliest first, min-heap)
    delayed: BinaryHeap<Reverse<DelayedMessage>>,

    /// Clock for time-based operations
    clock: Box<dyn Clock>,

    /// Maximum delivery attempts before DLQ (None = unlimited retries)
    max_attempts: Option<u32>,
}

impl QueueActor {
    /// Create a new queue actor. Persistence mode is locked to buffered-only (throughput-first).
    pub fn new(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        max_attempts: Option<u32>,
    ) -> Self {
        Self::with_clock(
            family,
            queue_key,
            store,
            Box::new(SystemClock),
            max_attempts,
        )
    }

    /// Create a new queue actor with a custom clock (for testing). Persistence is locked to buffered-only.
    pub fn with_clock(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        clock: Box<dyn Clock>,
        max_attempts: Option<u32>,
    ) -> Self {
        // Recover next_id from Midge on startup
        let next_id = Self::recover_next_id(&store, &queue_key);

        let mut actor = Self {
            family,
            queue_key,
            store,
            next_id,
            ready: VecDeque::new(),
            inflight: HashMap::new(),
            timers: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
            clock,
            max_attempts,
        };

        actor.recover_ready_and_delayed_from_store();
        actor
    }

    /// Recover next message ID from durable storage
    fn recover_next_id(store: &cntryl_midge::Engine, queue_key: &QueueKey) -> u64 {
        let key = Self::meta_key(queue_key);
        let cf_id = cntryl_midge::ColumnFamilyId(queue_key.family.id() as u32);

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

    /// Handle enqueue operation
    pub fn handle_enqueue(&mut self, body: Bytes, delay_seconds: Option<u64>) -> QueueResponse {
        // Delegate to batch enqueue with single message
        match self.handle_enqueue_batch(vec![body], delay_seconds) {
            QueueResponse::EnqueuedBatch { ids } => QueueResponse::Enqueued { id: ids[0] },
            other => other,
        }
    }

    /// Handle batch enqueue operation (first-class batch API)
    ///
    /// # Atomicity Guarantee (V-001 Fix)
    ///
    /// All-or-nothing transactionality:
    /// - ID allocation happens INSIDE Midge transaction
    /// - All message writes + next_id update happen in SINGLE transaction
    /// - If commit fails, no IDs are lost or duplicated
    /// - If crash happens before commit, no next_id corruption
    ///
    /// # Invariants
    /// - Message IDs assigned in input order (consistent ordering)
    /// - All messages written in ONE Midge transaction
    /// - next_id updated in same transaction as batch
    /// - All messages succeed or all fail (no partial visibility)
    /// - Minimal data loss: buffered writes OK (producers can retry)
    ///
    /// # Performance
    /// - Uses sync() writes to ensure reserve() sees enqueued messages immediately
    /// - Per-message sync() overhead amortized by batching
    /// - Competing consumers need consistency over peak throughput
    pub fn handle_enqueue_batch(
        &mut self,
        messages: Vec<Bytes>,
        delay_seconds: Option<u64>,
    ) -> QueueResponse {
        if messages.is_empty() {
            return QueueResponse::BadRequest {
                reason: "Empty batch".to_string(),
            };
        }

        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let delay_ms = delay_seconds.unwrap_or(0).saturating_mul(1_000);

        // Start transaction (ID allocation will happen inside)
        let cf_id = cntryl_midge::ColumnFamilyId(self.queue_key.family.id() as u32);
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

        // Allocate message IDs and write records inside transaction
        let base_id = self.next_id;
        let mut batch: Vec<(MessageId, QueueRecord, Instant)> = Vec::with_capacity(messages.len());

        for (idx, body) in messages.into_iter().enumerate() {
            let id_u64 = base_id + (idx as u64);
            let id = MessageId::new(id_u64);

            // Use absolute epoch_ms for visibility (survives restarts)
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

            batch.push((id, record, visible_at));
        }

        // Update next_id counter in SAME transaction (atomicity guarantee)
        let next_id = base_id + (batch.len() as u64);
        if let Err(e) = txn.put(
            Self::meta_key(&self.queue_key),
            next_id.to_le_bytes().to_vec(),
            None,
        ) {
            return QueueResponse::Error {
                message: format!("Failed to update queue meta: {:?}", e),
            };
        }

        // Commit with sync() to ensure reserve() sees messages immediately
        // While this reduces throughput vs buffered(), correctness is non-negotiable for competing consumers.
        // High-throughput scenarios should use batch enqueue to amortize fsync cost.
        if let Err(e) = self.store.commit(txn, cntryl_midge::WriteOptions::sync()) {
            return QueueResponse::Error {
                message: format!("Failed to commit transaction: {:?}", e),
            };
        }

        // Commit succeeded; advance in-memory next_id ONLY after durable success
        self.next_id = next_id;

        // Update in-memory queues after durable persistence
        let mut ready_count = 0;
        let message_ids: Vec<MessageId> = batch.iter().map(|(id, _, _)| *id).collect();

        for (id, _, visible_at) in batch {
            if visible_at <= now_instant {
                // Immediately visible
                self.ready.push_back(id);
                ready_count += 1;
            } else {
                // Delayed visibility
                self.delayed
                    .push(Reverse(DelayedMessage { id, visible_at }));
            }
        }

        // Emit at most ONE availability notice per batch
        if ready_count > 0 {
            // TODO: Emit notice://{realm}/{area}/{resource}/available
            // This hint allows long-polling RPC clients to wake up and retry reserve.
            // Emission is optional (best-effort, not guaranteed delivery).
        }

        QueueResponse::EnqueuedBatch { ids: message_ids }
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
    /// QueueActor never stores waiters or blocks on empty queues.
    pub fn handle_reserve(
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
            let id = match self.ready.pop_front() {
                Some(id) => id,
                None => break, // No more messages
            };

            // Load from Midge
            let cf_id = cntryl_midge::ColumnFamilyId(self.queue_key.family.id() as u32);
            let key = Self::message_key(&self.queue_key, id);

            // Use a ReadWrite transaction to see buffered writes from enqueue.
            // In LSM engines with deferred durability (buffered mode), ReadOnly snapshots
            // may not see recent writes. ReadWrite ensures read-your-writes visibility.
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
                        // Drop transaction before processing
                        drop(txn);
                        record
                    }
                    Err(e) => {
                        eprintln!("WARN: Failed to decode message {}: {}", id, e);
                        drop(txn);
                        continue; // Skip corrupted message
                    }
                },
                Ok(None) => {
                    eprintln!("WARN: Message {} disappeared from storage", id);
                    drop(txn);
                    continue; // Message was deleted
                }
                Err(e) => {
                    eprintln!("WARN: Failed to read message {}: {:?}", id, e);
                    drop(txn);
                    continue; // Storage error
                }
            };

            // Generate lease token
            let token = Self::generate_token();
            let expires_at = now + lease_duration;

            // Create inflight entry
            self.inflight.insert(id, Inflight { token, expires_at });

            // Schedule expiration timer
            self.timers.push(Reverse(LeaseExpiry { id, expires_at }));

            // Build response message
            messages.push(ReservedMessage {
                id,
                body: record.body,
                token,
                lease_seconds,
                attempts: record.attempts + 1, // First attempt is 1 (not 0)
            });
        }

        QueueResponse::Reserved { messages }
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

        QueueResponse::Extended
    }

    /// Handle complete operation
    pub fn handle_complete(&mut self, id: MessageId, token: u64) -> QueueResponse {
        let now = self.clock.now_instant();

        // Check if message is inflight
        let inflight = match self.inflight.get(&id) {
            Some(inflight) => inflight.clone(),
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

        // Remove inflight entry
        self.inflight.remove(&id);

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
        let cf_id = cntryl_midge::ColumnFamilyId(self.queue_key.family.id() as u32);
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

        QueueResponse::Completed
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
        let cf_id = cntryl_midge::ColumnFamilyId(self.queue_key.family.id() as u32);
        let key = Self::message_key(&self.queue_key, id);

        match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(mut txn) => {
                match txn.get(&key) {
                    Ok(Some(bytes)) => {
                        match Self::decode_record(&bytes) {
                            Ok(mut record) => {
                                record.attempts += 1;

                                // Check if attempts reached or exceeded max_attempts threshold
                                let is_dlq = if let Some(max) = self.max_attempts {
                                    record.attempts >= max
                                } else {
                                    false
                                };

                                if is_dlq {
                                    // DLQ: Delete message from storage (in txn)
                                    if let Err(e) = txn.delete(key.clone()) {
                                        eprintln!(
                                            "WARN: Failed to delete DLQ message {}: {:?}",
                                            id, e
                                        );
                                    } else if let Err(e) = self
                                        .store
                                        .commit(txn, cntryl_midge::WriteOptions::buffered())
                                    {
                                        eprintln!(
                                            "WARN: Failed to commit DLQ delete txn {}: {:?}",
                                            id, e
                                        );
                                    }

                                    // Log DLQ event
                                    eprintln!(
                                        "DLQ: queue={:?} message_id={} attempts={} - Message moved to dead letter queue",
                                        self.queue_key, id, record.attempts
                                    );

                                    // Do NOT re-enqueue
                                    return;
                                }

                                // Normal retry: increment attempts and persist
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
                                    "WARN: Failed to decode message {} during redelivery: {}",
                                    id, e
                                );
                            }
                        }
                    }
                    Ok(None) => {
                        // Message was deleted (completed after lease expired but before timer fired)
                        return;
                    }
                    Err(e) => {
                        eprintln!(
                            "WARN: Failed to read message {} during redelivery: {:?}",
                            id, e
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "WARN: Failed to begin txn for redelivery for message {}: {:?}",
                    id, e
                );
            }
        }

        // Re-enqueue to ready queue (back of queue for FIFO)
        self.ready.push_back(id);
    }

    /// Process expired timers (called periodically or on message receive)
    pub fn process_expired_timers(&mut self) {
        let now = self.clock.now_instant();

        while let Some(Reverse(expiry)) = self.timers.peek() {
            if expiry.expires_at > now {
                break; // No more expired timers
            }

            // Pop expired timer
            let expiry = self.timers.pop().unwrap().0;

            // Handle expiration
            self.handle_lease_expired(expiry.id);
        }
    }

    /// Process delayed messages that are now visible
    pub fn process_delayed_messages(&mut self) {
        let now = self.clock.now_instant();

        while let Some(Reverse(delayed)) = self.delayed.peek() {
            if delayed.visible_at > now {
                break; // No more visible messages
            }

            // Pop now-visible message
            let delayed = self.delayed.pop().unwrap().0;

            // Add to ready queue
            self.ready.push_back(delayed.id);
        }
    }
}

impl Actor for QueueActor {
    type Message = QueueMessage;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        // Process expired timers and delayed messages on every message
        self.process_expired_timers();
        self.process_delayed_messages();

        match msg {
            QueueMessage::Enqueue {
                body,
                delay_seconds,
                ..
            } => {
                let _response = self.handle_enqueue(body, delay_seconds);
                // TODO: Send response back to caller via reply
            }

            QueueMessage::EnqueueBatch {
                messages,
                delay_seconds,
                ..
            } => {
                let _response = self.handle_enqueue_batch(messages, delay_seconds);
                // TODO: Send response back to caller via reply
            }

            QueueMessage::Reserve {
                lease_seconds,
                batch_size,
                wait_seconds,
                ..
            } => {
                let _response = self.handle_reserve(lease_seconds, batch_size);
                // NOTE: wait_seconds is handled at RPC layer, not in QueueActor
                // QueueActor always returns immediately (never blocks)
                // If empty and wait_seconds > 0, RPC layer will:
                //   1. Subscribe to notice://{realm}/{area}/{resource}/available
                //   2. Wait up to wait_seconds for notice or timeout
                //   3. Retry reserve on notice or timeout
                let _ = wait_seconds; // Unused by actor, used by RPC layer
                                      // TODO: Send response back to caller via reply
            }

            QueueMessage::Extend {
                id,
                token,
                lease_seconds,
                ..
            } => {
                let _response = self.handle_extend(id, token, lease_seconds);
                // TODO: Send response back to caller via reply
            }

            QueueMessage::Complete { id, token, .. } => {
                let _response = self.handle_complete(id, token);
                // TODO: Send response back to caller via reply
            }

            QueueMessage::LeaseExpired { id } => {
                self.handle_lease_expired(id);
            }
        }
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        // Recovery is handled during actor construction; started() is a no-op.
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
        let cf_id = cntryl_midge::ColumnFamilyId(self.queue_key.family.id() as u32);
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

            max_id = Some(max_id.map(|m| m.max(id_u64)).unwrap_or(id_u64));

            // Compare absolute epochs (survives restarts correctly)
            if record.visible_at_ms <= now_epoch_ms {
                // Immediately visible
                self.ready.push_back(id);
            } else {
                // Delayed: use absolute epoch difference
                let delay_ms = record.visible_at_ms.saturating_sub(now_epoch_ms);
                self.delayed.push(Reverse(DelayedMessage {
                    id,
                    visible_at: now_instant + Duration::from_millis(delay_ms),
                }));
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
        let mut actor = QueueActor::new(RouteFamily::new(0), queue_key, store, None);

        // Act - Enqueue
        let body = Bytes::from("test message");
        let enqueue_response = actor.handle_enqueue(body.clone(), None);

        // Assert - Enqueue
        let msg_id = match enqueue_response {
            QueueResponse::Enqueued { id } => id,
            _ => panic!("Expected Enqueued response"),
        };
        assert_eq!(actor.ready.len(), 1);

        // Act - Reserve
        let reserve_response = actor.handle_reserve(30, Some(1));

        // Assert - Reserve
        match reserve_response {
            QueueResponse::Reserved { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, msg_id);
                assert_eq!(messages[0].body, body);
                assert_eq!(messages[0].attempts, 1);
                assert_eq!(messages[0].lease_seconds, 30);
            }
            _ => panic!("Expected Reserved response"),
        }
        assert_eq!(actor.ready.len(), 0);
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
        );

        // Act
        let response = actor.handle_reserve(30, Some(10));

        // Assert
        match response {
            QueueResponse::Reserved { messages } => {
                assert_eq!(messages.len(), 0);
            }
            _ => panic!("Expected Reserved response"),
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
        );

        let body = Bytes::from("test message");
        actor.handle_enqueue(body, None);
        let reserve_response = actor.handle_reserve(30, Some(1));

        let (msg_id, token) = match reserve_response {
            QueueResponse::Reserved { messages } => (messages[0].id, messages[0].token),
            _ => panic!("Expected Reserved response"),
        };

        // Act
        let response = actor.handle_complete(msg_id, token);

        // Assert
        assert_eq!(response, QueueResponse::Completed);
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
        );

        let body = Bytes::from("test message");
        actor.handle_enqueue(body, None);
        let reserve_response = actor.handle_reserve(30, Some(1));

        let msg_id = match reserve_response {
            QueueResponse::Reserved { messages } => messages[0].id,
            _ => panic!("Expected Reserved response"),
        };

        // Act
        let response = actor.handle_complete(msg_id, 99999);

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
        );

        let body = Bytes::from("test message");
        actor.handle_enqueue(body, None);
        let reserve_response = actor.handle_reserve(30, Some(1));

        let (msg_id, token) = match reserve_response {
            QueueResponse::Reserved { messages } => (messages[0].id, messages[0].token),
            _ => panic!("Expected Reserved response"),
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
        );

        let body = Bytes::from("test message");
        actor.handle_enqueue(body, None);
        let reserve_response = actor.handle_reserve(30, Some(1));

        let msg_id = match reserve_response {
            QueueResponse::Reserved { messages } => messages[0].id,
            _ => panic!("Expected Reserved response"),
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
        );

        let body = Bytes::from("test message");
        actor.handle_enqueue(body.clone(), None);
        let reserve_response = actor.handle_reserve(30, Some(1));

        let msg_id = match reserve_response {
            QueueResponse::Reserved { messages } => messages[0].id,
            _ => panic!("Expected Reserved response"),
        };

        assert_eq!(actor.ready.len(), 0);
        assert_eq!(actor.inflight.len(), 1);

        // Act - Advance time past lease expiration
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();

        // Assert - Message back in ready queue
        assert_eq!(actor.ready.len(), 1);
        assert_eq!(actor.inflight.len(), 0);
        assert_eq!(actor.ready.front().unwrap(), &msg_id);

        // Act - Reserve again
        let redelivery_response = actor.handle_reserve(30, Some(1));

        // Assert - Attempts incremented
        match redelivery_response {
            QueueResponse::Reserved { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, msg_id);
                assert_eq!(messages[0].attempts, 2); // Incremented from 1 to 2
            }
            _ => panic!("Expected Reserved response"),
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
        );

        // Enqueue 5 messages
        for i in 0..5 {
            let body = Bytes::from(format!("message {}", i));
            actor.handle_enqueue(body, None);
        }

        // Act
        let response = actor.handle_reserve(30, Some(3));

        // Assert
        match response {
            QueueResponse::Reserved { messages } => {
                assert_eq!(messages.len(), 3);
                assert_eq!(actor.ready.len(), 2); // 2 remaining
                assert_eq!(actor.inflight.len(), 3);
            }
            _ => panic!("Expected Reserved response"),
        }
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
        );

        let body = Bytes::from("test message");
        actor.handle_enqueue(body, None);
        let reserve_response = actor.handle_reserve(30, Some(1));

        let (msg_id, token) = match reserve_response {
            QueueResponse::Reserved { messages } => (messages[0].id, messages[0].token),
            _ => panic!("Expected Reserved response"),
        };

        // Act - Extend before first timer expires
        clock.advance(Duration::from_secs(15));
        actor.handle_extend(msg_id, token, 60);

        // Advance to first timer expiration (30s total)
        clock.advance(Duration::from_secs(15));
        actor.process_expired_timers();

        // Assert - Message still inflight (stale timer ignored)
        assert_eq!(actor.ready.len(), 0);
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
        );

        let body = Bytes::from("test message");
        actor.handle_enqueue(body, None);
        let reserve_response = actor.handle_reserve(30, Some(1));

        let (msg_id, token) = match reserve_response {
            QueueResponse::Reserved { messages } => (messages[0].id, messages[0].token),
            _ => panic!("Expected Reserved response"),
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

        let complete_response = actor.handle_complete(msg_id, token);
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
        );
        let fake_id = MessageId::new(99999);

        // Act
        let extend_response = actor.handle_extend(fake_id, 12345, 60);
        let complete_response = actor.handle_complete(fake_id, 12345);

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
        );

        // Act - Enqueue with 30 second delay
        let body = Bytes::from("delayed message");
        let response = actor.handle_enqueue(body.clone(), Some(30));

        let msg_id = match response {
            QueueResponse::Enqueued { id } => id,
            _ => panic!("Expected Enqueued response"),
        };

        // Assert - Message not in ready queue
        assert_eq!(actor.ready.len(), 0);
        assert_eq!(actor.delayed.len(), 1);

        // Act - Try to reserve immediately (should be empty)
        let reserve_response = actor.handle_reserve(30, Some(1));
        match reserve_response {
            QueueResponse::Reserved { messages } => {
                assert_eq!(messages.len(), 0);
            }
            _ => panic!("Expected Reserved response"),
        }

        // Act - Advance time past delay
        clock.advance(Duration::from_secs(31));
        actor.process_delayed_messages();

        // Assert - Message now in ready queue
        assert_eq!(actor.ready.len(), 1);
        assert_eq!(actor.delayed.len(), 0);

        // Act - Reserve now succeeds
        let reserve_response = actor.handle_reserve(30, Some(1));
        match reserve_response {
            QueueResponse::Reserved { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, msg_id);
                assert_eq!(messages[0].body, body);
            }
            _ => panic!("Expected Reserved response"),
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
        );

        // Act - Enqueue message
        let body = Bytes::from("test message");
        let enqueue_response = actor.handle_enqueue(body.clone(), None);
        let msg_id = match enqueue_response {
            QueueResponse::Enqueued { id } => id,
            _ => panic!("Expected Enqueued response"),
        };

        // Simulate 3 failed delivery attempts
        for attempt in 1..=3 {
            // Reserve
            let reserve_response = actor.handle_reserve(30, Some(1));
            match reserve_response {
                QueueResponse::Reserved { messages } => {
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
                assert_eq!(actor.ready.len(), 1);
                assert_eq!(actor.inflight.len(), 0);
            }
        }

        // Assert - After 3rd expiration, attempts = 4, exceeds max_attempts = 3
        // Message should be deleted (DLQ'd), not re-enqueued
        assert_eq!(actor.ready.len(), 0);
        assert_eq!(actor.inflight.len(), 0);

        // Verify message deleted from storage
        let cf_id = cntryl_midge::ColumnFamilyId(queue_key.family.id() as u32);
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
        );

        // Act - Enqueue message
        let body = Bytes::from("test message");
        actor.handle_enqueue(body, None);

        // Simulate 10 failed delivery attempts
        for attempt in 1..=10 {
            // Reserve
            let reserve_response = actor.handle_reserve(30, Some(1));
            match reserve_response {
                QueueResponse::Reserved { messages } => {
                    assert_eq!(messages.len(), 1);
                    assert_eq!(messages[0].attempts, attempt);
                }
                _ => panic!("Expected Reserved response on attempt {}", attempt),
            }

            // Expire lease
            clock.advance(Duration::from_secs(31));
            actor.process_expired_timers();

            // Should always be back in ready queue (unlimited retries)
            assert_eq!(actor.ready.len(), 1);
            assert_eq!(actor.inflight.len(), 0);
        }

        // Assert - Message still available after 10 attempts
        let reserve_response = actor.handle_reserve(30, Some(1));
        match reserve_response {
            QueueResponse::Reserved { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].attempts, 11);
            }
            _ => panic!("Expected Reserved response"),
        }
    }

    #[test]
    fn should_enqueue_batch_atomically() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-batch-atomic");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
        );

        // Act - Enqueue batch of 5 messages
        let messages = vec![
            Bytes::from("msg1"),
            Bytes::from("msg2"),
            Bytes::from("msg3"),
            Bytes::from("msg4"),
            Bytes::from("msg5"),
        ];
        let response = actor.handle_enqueue_batch(messages, None);

        // Assert - Response contains all message IDs
        let ids = match response {
            QueueResponse::EnqueuedBatch { ids } => ids,
            other => panic!("Expected EnqueuedBatch response, got {:?}", other),
        };
        assert_eq!(ids.len(), 5);

        // Assert - All messages available for reservation in FIFO order
        let reserve_response = actor.handle_reserve(30, Some(5));
        let reserved = match reserve_response {
            QueueResponse::Reserved { messages } => messages,
            _ => panic!("Expected Reserved response"),
        };
        assert_eq!(reserved.len(), 5);

        // Verify IDs match returned batch IDs
        for (i, msg) in reserved.iter().enumerate() {
            assert_eq!(msg.id, ids[i]);
        }
    }

    #[test]
    fn should_reject_empty_batch_enqueue() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-batch-empty");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
        );

        // Act
        let response = actor.handle_enqueue_batch(vec![], None);

        // Assert
        match response {
            QueueResponse::BadRequest { reason } => {
                assert_eq!(reason, "Empty batch");
            }
            other => panic!("Expected BadRequest response, got {:?}", other),
        }
    }

    #[test]
    fn should_preserve_fifo_order_in_batch_enqueue() {
        // Arrange
        let clock = MockClock::new();
        let store = Arc::new(
            cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                .expect("Failed to open Midge"),
        );
        let queue_key = unique_queue_key("jobs-batch-fifo");
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0), /* CF=0 for Midge test limitation */
            queue_key,
            store,
            Box::new(clock.clone()),
            None,
        );

        // Act - Enqueue batch with distinct payloads
        let messages = vec![
            Bytes::from("first"),
            Bytes::from("second"),
            Bytes::from("third"),
        ];
        let response = actor.handle_enqueue_batch(messages.clone(), None);
        let ids = match response {
            QueueResponse::EnqueuedBatch { ids } => ids,
            _ => panic!("Expected EnqueuedBatch response"),
        };

        // Assert - Reserve in FIFO order
        let reserve_response = actor.handle_reserve(30, Some(3));
        let reserved = match reserve_response {
            QueueResponse::Reserved { messages: msgs } => msgs,
            _ => panic!("Expected Reserved response"),
        };

        // Verify order matches input order
        assert_eq!(reserved.len(), 3);
        assert_eq!(reserved[0].body, messages[0]);
        assert_eq!(reserved[1].body, messages[1]);
        assert_eq!(reserved[2].body, messages[2]);

        // Verify IDs are sequential
        assert_eq!(reserved[0].id, ids[0]);
        assert_eq!(reserved[1].id, ids[1]);
        assert_eq!(reserved[2].id, ids[2]);
        assert_eq!(ids[1].as_u64(), ids[0].as_u64() + 1);
        assert_eq!(ids[2].as_u64(), ids[1].as_u64() + 1);
    }
}
