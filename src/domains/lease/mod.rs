//! Lease domain: distributed locking with fencing tokens
//!
//! Provides exclusive locks with time-to-live (TTL) and monotonically increasing
//! fencing tokens for ordering guarantees in distributed systems.
//!
//! # Key Features
//!
//! - **Exclusive ownership**: Only one owner per lease at a time
//! - **Fencing tokens**: Monotonically increasing tokens prevent split-brain scenarios
//! - **TTL-based expiration**: Leases automatically expire after their time-to-live
//! - **Idempotent operations**: Safe to retry all operations
//! - **Non-durable**: In-memory only, no persistence (by design for performance)
//!
//! # Route Format
//!
//! Leases use hierarchical routes: `lease://{realm}/{area}/{resource}/{operation}`
//!
//! Example: `lease://acme/locks/db-migration/acquire`
//!
//! Lease identity is `(family_id, realm, area, resource)` extracted from the route.
//!
//! # Fencing Token Protocol
//!
//! Each acquisition returns a token. Clients must include this in subsequent operations:
//!
//! ```text
//! 1. Client A: Acquire("lock") → token=1
//! 2. Client A: Renew("lock", token=1) → OK
//! 3. Client A crashes, lease expires
//! 4. Client B: Acquire("lock") → token=2
//! 5. Client A: Renew("lock", token=1) → Fenced(current=2)
//! ```
//!
//! Client A learns it no longer holds the lease and must stop work.
//!
//! # Usage
//!
//! ```ignore
//! use fitz::domains::lease::{LeaseActor, LeaseMessage};
//! use fitz::runtime::scheduler::Scheduler;
//!
//! let scheduler = Scheduler::new(1);
//! let lease_actor = LeaseActor::new(RouteFamily::new(1));
//! let actor_ref = scheduler.spawn(lease_actor, 100);
//!
//! // Acquire a lease
//! actor_ref.send(LeaseMessage::Acquire {
//!     family_id,
//!     route,
//!     owner_id: "client-1".to_string(),
//!     ttl_secs: 30,
//! });
//! ```

pub mod guard;
pub mod lease_actor;
pub mod protocol;

// Test helper - lightweight SessionActor stub for testing lease authorization
#[cfg_attr(not(test), doc(hidden))]
pub mod session;

pub use guard::{LeaseError, LeaseGuard, LeaseHandle};
pub use lease_actor::{Clock, LeaseActor, SystemClock};
pub use protocol::{LeaseMessage, LeaseResponse};
