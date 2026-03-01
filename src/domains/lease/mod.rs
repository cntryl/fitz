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

pub mod actor;
pub mod guard;
pub mod protocol;
pub mod session;

pub use actor::{Clock, LeaseActor, SystemClock};
pub use guard::{LeaseError, LeaseGuard, LeaseHandle};
pub use protocol::{LeaseMessage, LeaseResponse};
pub use session::SessionActor;
