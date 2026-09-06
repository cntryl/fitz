//! Key-Value domain: durable committed data with session-scoped transaction
//! state over Midge.
//!
//! # Architecture
//!
//! The KV domain provides a thin, strict wrapper over Midge transactions.
//! Committed reads and writes persist according to the Fitz `WritePolicy` selected
//! at `begin`. Open `tx_id` handles, uncommitted writes, and resource-lock
//! coordination are live broker-local state only and disappear on disconnect or
//! broker restart.
//!
//! It enforces:
//! - All operations execute within a transaction
//! - Transactions are scoped to a single resource (table)
//! - Explicit `RouteFamily` -> `ColumnFamily` mapping (no default CF)
//! - Direct exposure of Midge semantics (no buffering, retries, caching)
//!
//! # Routes
//!
//! Format: `kv://{realm}/{area}/{resource}`
//!
//! Transaction control:
//! - `begin` - Start a transaction bound to `{resource}`
//! - `commit` - Commit the active transaction
//! - `rollback` - Abort the active transaction
//!
//! KV operations (require active transaction):
//! - `get` - Retrieve value by key
//! - `put` - Upsert key-value pair
//! - `insert` - Insert key-value pair (fails if exists)
//! - `delete` - Delete key
//! - `delete_range` - Delete range of keys
//! - `scan` - Scan range of keys
//!
//! # KV Watches
//!
//! Clients can watch KV resources for committed mutations:
//! - `KvDomainSink` owns ephemeral watch state for the current broker process
//! - Watches target `kv://{realm}/{area}/{resource}` and wildcard resource patterns
//! - Notifications are emitted only after a transaction commits at least one mutating operation
//! - Uncommitted writes, empty commits, disconnect cleanup, and broker restart do not replay notifications
//!
//! # Column Family Mapping
//!
//! KV domain uses explicit `RouteFamily` -> `ColumnFamily` mapping:
//! - `ColumnFamilyId` = `RouteFamily`.id (cast to `u32`)
//! - Default column family (CF=0) is FORBIDDEN
//! - All KV persistence MUST specify explicit CF via `RouteFamily`

mod actor;
mod admin_projection;
mod inventory;
pub(crate) mod metrics;
mod protocol;
mod scan_wire_budget;
pub mod sink;
mod watch_registry;
pub(crate) mod write_policy;

pub use actor::KvActor;
pub use protocol::{
    KvClientFrame, KvClientNotification, KvClientRequest, KvClientResponse, KvError, KvMessage,
    KvNotification, KvPair, KvResourceScope, KvResponse, KvSubscriptionMessage, ScanQuery, TxMode,
};
pub use sink::{
    AdminKvCommittedPair, AdminKvPrefixScanResult, AdminKvRowsRequest, AdminKvRowsResult,
    KvDomainSink,
};
