//! Lease domain for distributed locking with fencing tokens
//!
//! The lease domain provides exclusive locks with time-to-live (TTL) and
//! fencing tokens for ordering guarantees. This is the foundation for
//! distributed coordination in Fitz.
//!
//! # Key Features
//!
//! - **Exclusive ownership**: Only one owner per lease at a time
//! - **Fencing tokens**: Monotonically increasing tokens prevent split-brain
//! - **TTL-based expiration**: Leases automatically expire
//! - **Idempotent operations**: Safe to retry
//! - **Crash-safe design**: State transitions designed for log replay
//!
//! # Usage
//!
//! ```ignore
//! use fitz::domains::lease::{LeaseActor, LeaseMessage, LeaseResponse};
//! use fitz::runtime::scheduler::Scheduler;
//!
//! let scheduler = Scheduler::new(1);
//! let lease_actor = LeaseActor::new();
//! let actor_ref = scheduler.spawn(lease_actor, 100);
//!
//! // Acquire a lease
//! actor_ref.send(LeaseMessage::Acquire {
//!     lease_id: "my-lock".to_string(),
//!     owner_id: "client-1".to_string(),
//!     ttl_secs: 30,
//! });
//! ```
//!
//! # Fencing Token Protocol
//!
//! Clients must save the fencing token and include it in all operations:
//!
//! ```text
//! 1. Client A: Acquire("lock") → token=1
//! 2. Client A: Renew("lock", token=1) → OK
//! 3. Client A: Crashes, lease expires
//! 4. Client B: Acquire("lock") → token=2
//! 5. Client A: Renew("lock", token=1) → Fenced(current=2)
//! ```
//!
//! Client A learns it no longer holds the lease and must stop work.

pub mod actor;
pub mod guard;
pub mod protocol;

pub use actor::{Clock, LeaseActor, SystemClock};
pub use guard::{LeaseError, LeaseGuard, LeaseHandle};
pub use protocol::{LeaseMessage, LeaseResponse};
