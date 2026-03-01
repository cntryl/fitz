//! Queue domain: competing consumer work queues with automatic redelivery
//!
//! Provides work queues optimized for competing consumers:
//! - **Competing consumer semantics**: Multiple consumers can reserve messages fairly
//! - **Lease-based visibility**: Reserved messages invisible to other consumers
//! - **Automatic redelivery**: Expired or crashed leases return messages to ready queue
//! - **At-least-once delivery**: Messages may be delivered multiple times
//! - **Minimal fairness**: Messages distributed fairly among consumers (not strict FIFO)
//! - **Atomic batch operations**: All-or-nothing for ID allocation + message writes
//! - **Full recovery**: Persisted state survives process crashes
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
//! Consumer A reserves message 1 (lease 30s)
//! Consumer B reserves message 2 (lease 30s)
//! Consumer C tries to reserve → gets nothing (queue empty)
//! Consumer A crashes before completing message 1
//! After 30s, lease expires → message 1 returns to ready queue
//! Consumer C tries again → gets message 1 (redelivery)
//! ```
//!
//! # Key Design Decisions
//!
//! - **Not strict FIFO**: Multiple competing consumers naturally break FIFO ordering.
//!   Messages are delivered in ready-queue order, but reserve order is non-deterministic.
//! - **Minimal data loss**: Uses atomic batch operations (ID allocation + writes commit together).
//!   Messages may be lost only if batch commit itself fails (unlikely with sync writes).
//! - **Automatic redelivery**: Lease expiration automatically returns messages (ephemeral leases).
//!   Crashes automatically trigger redelivery (inflight state not persisted).
//! - **Fair distribution**: Reserve operations pop from front of ready queue (simple FIFO internally).
//!   Multiple competing consumers naturally distribute work.
//!
//! # Intent vs Events
//!
//! Queues represent **intent** (work to be done), not events of record.
//! - Minimal data loss acceptable (batch commits are atomic)
//! - Producers can regenerate lost work items
//! - Batch operations use sync() writes for consistency (competing consumers need correctness over peak throughput)
//!
//! # Key Features
//!
//! - **Single-node**: No distributed coordination (MVP)
//! - **Ephemeral leases**: Lease state lost on restart (automatic redelivery)
//! - **Microsecond latency**: In-memory reserve/complete with persistent backing
//! - **Token-based operations**: Random tokens prevent accidental duplicate operations
//! - **Dead Letter Queue (DLQ)**: Optional max_attempts threshold for failed messages
//! - **Atomic recovery**: Full state restored after restart (messages + delayed visibility)
//!
//! # Route Format
//!
//! Queues use hierarchical routes: `queue://{realm}/{area}/{resource}/{operation}`
//!
//! Example: `queue://acme/jobs/email-sender/enqueue`
//!
//! Queue identity is `(family_id, realm, area, resource)` extracted from the route.
//!
//! # Operations
//!
//! - **enqueue**: Add message to queue (returns MessageId, atomic batch operation)
//! - **reserve**: Lease one or more messages for processing (fair distribution)
//! - **extend**: Extend lease expiration for a reserved message
//! - **complete**: Mark message as processed and delete it
//!
//! # Competing Consumer Protocol
//!
//! ```text
//! 1. Consumer A: Reserve(lease_secs=30) → (id=1, token=abc123, body="task")
//! 2. Consumer B: Reserve(lease_secs=30) → (id=2, token=def456, body="task2")
//! 3. Consumer A: [Processing task 1...]
//! 4. Consumer B: [Processing task 2...]
//! 5. Consumer A crashes → Lease expires after 30 seconds
//! 6. Consumer C: Reserve(lease_secs=30) → (id=1, token=xyz789, body="task")
//!    ^^^ Message 1 redelivered with same body but new token
//! 7. Consumer A recovers: Try Complete(id=1, token=abc123) → LeaseExpired (old token invalid)
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
//! - In-flight messages automatically redelivered (leases are ephemeral)
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
//! - Each lease expiration increments `attempts`
//! - When `attempts > max_attempts`:
//!   - Message is deleted from storage (DLQ'd)
//!   - Log message emitted: `DLQ: queue={...} message_id={...} attempts={...}`
//!   - Message is NOT re-enqueued
//! - DLQ handling is explicit and external:
//!   - Monitor logs/metrics for DLQ events
//!   - External systems emit notices (e.g., `notice://{realm}/{area}/dead`)
//!   - QueueActor never auto-enqueues to another queue
//!
//! When `max_attempts: None` (default):
//! - Messages retry indefinitely on lease expiration
//! - No DLQ behavior
//!
//! # Long Polling (RPC-Level Only)
//!
//! Reserve operations support optional long polling for empty queues:
//! - QueueActor always returns immediately (never blocks)
//! - `wait_seconds` parameter handled at RPC layer:
//!   1. RPC calls `handle_reserve()` synchronously
//!   2. If empty and `wait_seconds > 0`:
//!      - Subscribe to `notice://{realm}/{area}/{resource}/available`
//!      - Wait up to `wait_seconds` for notice or timeout
//!      - Retry `handle_reserve()` on notice or timeout
//! - Notices are hints (at-most-once), not delivery guarantees
//! - QueueActor never stores waiters or blocking state
//! - Benefits:
//!   - Reduces polling overhead for idle queues
//!   - Maintains deterministic actor performance
//!   - Decouples waiting from queue state
//!
//! # Usage
//!
//! Queue operations are dispatched via RPC or WebSocket messages.

pub mod actor;
pub mod protocol;
pub mod session;

pub use actor::{Clock, QueueActor, SystemClock};
pub use protocol::{MessageId, QueueKey, QueueMessage, QueueResponse, ReservedMessage};
pub use session::SessionActor;
