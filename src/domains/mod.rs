//! High-level personas implemented as actors.
//!
//! Each persona owns exclusive state and coordination logic:
//! - SessionActor: WebSocket/TCP session management
//! - RealmActor: Realm-specific routing and memberships
//! - RouterActor: Global routing tables and wildcard matching
//! - StreamActor: Subscriptions, fanout, ephemeral cursors
//! - QueueActor: Queue scheduling and visibility timers
//! - RpcActor: In-flight RPC, timeouts, correlation
//! - LeaseActor: Ephemeral leases (in-memory only)
//! - MetricsActor: Counters, histograms, OTEL emission

pub mod session_actor;
pub mod realm_actor;
pub mod router_actor;
pub mod stream_actor;
pub mod queue_actor;
pub mod rpc_actor;
pub mod lease_actor;
pub mod metrics_actor;
