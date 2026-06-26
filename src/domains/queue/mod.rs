//! Queue domain: competing consumer work queues with automatic redelivery
//!
//! Provides work queues optimized for competing consumers:
//! - **Competing consumer semantics**: Multiple consumers can reserve messages fairly
//! - **Inflight-based visibility**: Reserved messages invisible to other consumers
//! - **Automatic redelivery**: Expired or crashed inflight entries return messages to ready queue
//! - **At-least-once delivery**: Messages may be delivered multiple times
//! - **Minimal fairness**: Messages distributed fairly among consumers (not strict FIFO)
//! - **Atomic batch operations**: All-or-nothing for ID allocation + message writes
//! - **Configurable durability**: Fast queue writes trade a small crash-loss window for low latency
//!
//! # Competing Consumer Model
//!
//! Designed for scenarios like:
//! - Multiple worker threads processing tasks from a queue
//! - Multiple services consuming from the same queue
//! - Fair work distribution (not strict FIFO ordering)
//!
//! Example:
//! ```text
//! Consumer A reserves message 1 (inflight 30s)
//! Consumer B reserves message 2 (inflight 30s)
//! Consumer C tries to reserve → gets nothing (queue empty)
//! Consumer A crashes before completing message 1
//! After 30s, inflight entry expires → message 1 returns to ready queue
//! Consumer C tries again → gets message 1 (redelivery)
//! ```
//!
//! # Key Design Decisions
//!
//! - **Not strict FIFO**: Multiple competing consumers naturally break FIFO ordering.
//!   Messages are delivered in ready-queue order, but reserve order is non-deterministic.
//! - **Policy-scoped durability**: Uses atomic batch operations (ID allocation + writes commit together).
//!   Success responses are returned after the configured queue write policy accepts the mutation.
//!   The default fast policy skips WAL on the hot path and flushes dirty queue column families
//!   in the background, so accepted recent mutations can be lost on process or host crash before
//!   the configured loss window elapses.
//! - **Automatic redelivery**: Inflight expiration automatically returns messages (ephemeral inflight state).
//!   Crashes automatically trigger redelivery (inflight state not persisted).
//! - **Fair distribution**: Reserve operations pop from front of ready queue (simple FIFO internally).
//!   Multiple competing consumers naturally distribute work.
//!
//! # Intent vs Events
//!
//! Queues represent **intent** (work to be done), not events of record.
//! - Queue state durability follows `FITZ_QUEUE_WRITE_POLICY`
//! - The default `fast` policy accepts a bounded recent-data-loss window
//! - Producers can regenerate lost work items
//! - Inflight entries and inflight tokens remain broker-local, in-memory coordination state
//!
//! # Key Features
//!
//! - **Single-node**: No distributed coordination (MVP)
//! - **Ephemeral inflight state**: Inflight state lost on restart (automatic redelivery)
//! - **Microsecond latency**: In-memory reserve/complete with persistent backing
//! - **Token-based operations**: Random tokens prevent accidental duplicate operations
//! - **Dead Letter Queue (DLQ)**: Optional max_attempts threshold for failed messages
//! - **Recovery**: Flushed or WAL-backed state restored after restart (messages + delayed visibility)
//!
//! # Route Format
//!
//! Queues use hierarchical routes: `queue://{realm}/{area}/{resource}`
//!
//! Example: `queue://acme/jobs/email-sender`
//!
//! Queue identity is `(family_id, realm, area, resource)` extracted from the route.
//!
//! # Operations
//!
//! - **enqueue**: Add message to queue (returns MessageId after commit)
//! - **reserve**: Mark one or more messages inflight for processing (fair distribution)
//! - **extend**: Extend inflight expiration for a reserved message
//! - **complete**: Mark message as processed and delete it after commit
//!
//! # Competing Consumer Protocol
//!
//! ```text
//! 1. Consumer A: Reserve(inflight_secs=30) → (id=1, token=abc123, body="task")
//! 2. Consumer B: Reserve(inflight_secs=30) → (id=2, token=def456, body="task2")
//! 3. Consumer A: [Processing task 1...]
//! 4. Consumer B: [Processing task 2...]
//! 5. Consumer A crashes → Inflight entry expires after 30 seconds
//! 6. Consumer C: Reserve(inflight_secs=30) → (id=1, token=xyz789, body="task")
//!    ^^^ Message 1 redelivered with same body but new token
//! 7. Consumer A recovers: Try Complete(id=1, token=abc123) → InflightExpired (old token invalid)
//! 8. Consumer C: Complete(id=1, token=xyz789) → OK (new token valid)
//! ```
//!
//! # Atomicity Guarantee (V-001 Fix)
//!
//! Batch enqueue operations are atomic:
//! - ID allocation happens INSIDE Midge transaction
//! - All message writes + next_id update in SINGLE transaction
//! - If crash before commit: no IDs lost or duplicated
//! - Prevents ID collisions across restarts
//!
//! # Recovery Guarantee (V-003 Fix)
//!
//! Process restart fully recovers queue state:
//! - All persisted messages recovered from storage
//! - Delayed messages have correct visibility windows (using absolute epoch_ms)
//! - In-flight messages automatically redelivered (inflight state is ephemeral)
//! - next_id correctly initialized to prevent duplicate IDs
//!
//! # Time Semantics (V-002 Fix)
//!
//! All persisted times use SystemTime::UNIX_EPOCH (milliseconds):
//! - visible_at_ms is absolute epoch, not relative delay
//! - Delays survive process restarts correctly
//! - No clock skew issues between restarts
//!
//! # Dead Letter Queue (DLQ) Policy
//!
//! When creating a QueueActor with `max_attempts: Some(n)`:
//! - Each inflight expiration increments `attempts`
//! - When `attempts >= max_attempts`:
//!   - Message transitions into durable DLQ state
//!   - Header and body remain in storage with DLQ metadata
//!   - Log message emitted: `DLQ: queue={...} message_id={...} attempts={...}`
//!   - Message is NOT re-enqueued
//! - QueueActor retains DLQ rows durably and keeps them out of reserve/redelivery
//! - Higher-level admin, replay, and purge surfaces build on top of this retained state
//!
//! When `max_attempts: None` (default):
//! - Messages retry indefinitely on inflight expiration
//! - No DLQ behavior
//!
//! # Queue Watches
//!
//! Queue consumers can watch readiness routes instead of parking reserve requests:
//! - QueueActor always returns immediately (never blocks)
//! - QueueDomainSink owns ephemeral watch state for the current broker process
//! - Watches target `queue://{realm}/{area}/{resource}/ready`
//! - Notifications signal availability and never carry queue message bodies
//! - Delayed visibility and inflight-expiry transitions are surfaced through queue-local runtime sweeps
//!
//! # Usage
//!
//! Queue operations are dispatched via RPC or WebSocket messages.

pub mod actor;
pub mod core;
pub mod events;
pub mod metrics;
pub mod projection;
pub mod protocol;
pub mod sink;

pub use crate::runtime::clock::{Clock, SystemClock};
pub use actor::QueueActor;
pub use core::{MessageId, QueueKey, ReservedMessage};
pub use metrics::QueueMetrics;
pub use projection::{QueueAdminSnapshot, QueueDeadLetterSnapshot, QueueInflightSnapshot};
pub use protocol::{QueueMessage, QueueNotification, QueueResponse, QueueSubscriptionMessage};
