//! Queue domain: durable message queues with at-least-once delivery
//!
//! Provides FIFO message queues with:
//! - **Durable storage**: Messages persist across restarts
//! - **Lease-based visibility**: Reserved messages invisible to other consumers
//! - **Automatic redelivery**: Expired leases return messages to ready queue
//! - **At-least-once delivery**: Messages may be delivered multiple times
//! - **FIFO ordering**: Messages delivered in enqueue order
//! - **Producer batching**: Client-side batching for multi-million msg/sec throughput
//!
//! # Key Features
//!
//! - **Single-node**: No distributed coordination (MVP)
//! - **Ephemeral leases**: Lease state lost on restart
//! - **Microsecond latency**: In-memory scheduling with durable persistence
//! - **Token-based operations**: Random tokens prevent accidental duplicate operations
//! - **Dead Letter Queue (DLQ)**: Optional max_attempts threshold for failed messages
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
//! - **enqueue**: Add message to queue (returns MessageId)
//! - **reserve**: Lease one or more messages for processing
//! - **extend**: Extend lease expiration for a reserved message
//! - **complete**: Mark message as processed and delete it
//!
//! # Lease Protocol
//!
//! ```text
//! 1. Client A: Enqueue("task") → id=1
//! 2. Client B: Reserve(lease_secs=30) → (id=1, token=abc123, body="task")
//! 3. Client B: [Processing...]
//! 4. Client B: Complete(id=1, token=abc123) → OK
//! ```
//!
//! If Client B crashes before completing:
//! - Lease expires after 30 seconds
//! - Message returns to ready queue
//! - Message.attempts incremented
//! - Next reserve gets the message again
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
//! ```ignore
//! use fitz::domains::queue::{QueueActor, QueueMessage};
//! use fitz::runtime::scheduler::Scheduler;
//!
//! let scheduler = Scheduler::new(1);
//! let store = Arc::new(cntryl_midge::MidgeEngine::open("data").unwrap());
//!
//! // Queue with DLQ (max 3 attempts)
//! let queue_actor = QueueActor::new(RouteFamily::new(1), queue_key, store, Some(3));
//! let actor_ref = scheduler.spawn(queue_actor, 1000);
//!
//! // Enqueue a message
//! actor_ref.send(QueueMessage::Enqueue {
//!     family_id,
//!     route,
//!     body: Bytes::from("task data"),
//!     delay_seconds: None,
//! });
//!
//! // Reserve with long polling (handled by RPC layer)
//! actor_ref.send(QueueMessage::Reserve {
//!     family_id,
//!     route,
//!     lease_seconds: 30,
//!     batch_size: Some(10),
//!     wait_seconds: Some(60), // RPC will wait up to 60s if empty
//! });
//! ```

pub mod protocol;
pub mod queue_actor;
pub mod session;
pub mod producer;
pub mod durability;

pub use protocol::{MessageId, QueueKey, QueueMessage, QueueResponse, ReservedMessage};
pub use queue_actor::{Clock, QueueActor, SystemClock};
pub use producer::QueueProducer;
pub use durability::{QueueDurabilityPolicy, QueueWriteOptions};
