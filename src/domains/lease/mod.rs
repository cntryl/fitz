//! Lease domain: ephemeral in-memory coordination
//!
//! Leases are a single-broker coordination primitive with TTL-based ownership
//! and fencing tokens for ordering inside one running process. They are not a
//! durable or distributed lease service, and state is expected to disappear on
//! broker restart or session disconnect.
//!
//! # Contract
//!
//! - **Exclusive ownership**: one owner per lease within the running broker
//! - **Fencing tokens**: process-local monotonic tokens used only while the
//!   broker instance is alive
//! - **TTL-based expiration**: leases expire when their local TTL elapses
//! - **Explicit lease watches**: clients can subscribe to `LEASE_NOTIFY (409)`
//!   for concrete lease routes without polling
//! - **Session cleanup**: disconnect removes session-owned lease state
//! - **No recovery**: restart-loss is expected and must remain visible in tests
//!
//! # Route Format
//!
//! Leases use hierarchical routes: `lease://{realm}/{area}/{resource}`
//!
//! Example: `lease://acme/locks/db-migration`
//!
//! Lease identity is `(family_id, realm, area, resource)` extracted from the route.
//!
//! # Fencing Token Scope
//!
//! Each acquisition returns a token that is only meaningful inside the current
//! broker process. A restart resets the token lineage, so clients must not treat
//! tokens as cluster-wide or durable identifiers.

pub mod actor;
pub mod events;
pub mod guard;
pub mod metrics;
pub mod projection;
pub mod protocol;
pub mod session;
pub mod sink;

pub use crate::runtime::clock::{Clock, SystemClock};
pub use actor::LeaseActor;
pub use guard::{LeaseError, LeaseGuard, LeaseHandle};
pub use metrics::LeaseMetrics;
pub use protocol::{
    LeaseClientFrame, LeaseClientNotification, LeaseClientRequest, LeaseClientResponse,
    LeaseMessage, LeaseResponse, LeaseSubscriptionMessage,
};
pub use session::SessionActor;
