//! Stream domain: durable append-only event logs with multi-level ordering
//!
//! # Architecture
//!
//! - **StreamActor** ([stream_actor]): Manages single resource stream with sequential offsets
//! - **AreaActor** ([area_actor]): Coordinates ordering across resources within an area
//! - **RealmActor** ([realm_actor]): Coordinates ordering across areas within a realm
//! - **SessionActor**: Enforces authentication/authorization before forwarding to actors
//!
//! # Three-Level Ordering
//!
//! 1. **Resource ordering**: Strict sequential offsets within a single resource stream (caller-assigned)
//! 2. **Area ordering**: Global ordering across all resources in an area (system-assigned)
//! 3. **Realm ordering**: Global ordering across all areas in a realm (system-assigned)
//!
//! # Semantics
//!
//! - **Strictly ordered**: Events are totally ordered at each level
//! - **Gap-free**: No gaps in offset sequences (enforced by watermarks)
//! - **Durable**: All events persisted to Midge LSM storage
//! - **Optimistic concurrency**: Caller assigns resource_offset for conflict detection
//! - **Watermark-gated reads**: Consumers only see gap-free committed events
//!
//! # Route Format
//!
//! `stream://{realm}/{area}/{resource}/{operation}`
//!
//! Examples:
//! - `stream://acme/orders/checkout/append`
//! - `stream://acme/orders/checkout/read`
//! - `stream://acme/orders/*/read` (area-level read)
//! - `stream://acme/*/*/read` (realm-level read)
//!
//! # Offset Assignment
//!
//! - **resource_offset**: Assigned by caller (for optimistic concurrency)
//! - **area_offset**: Assigned by AreaActor (for area-wide ordering)
//! - **realm_offset**: Assigned by RealmActor (for realm-wide ordering)
//!
//! # Watermarks
//!
//! - **Area watermark**: Highest contiguous area_offset with no gaps
//! - **Realm watermark**: min(all area watermarks)
//! - Reads are blocked beyond watermarks to ensure gap-free ordering

pub mod protocol;
pub mod storage;
pub mod store;
pub mod stream_actor;
pub mod area_actor;
pub mod realm_actor;
pub mod session;

// Re-exports
pub use stream_actor::StreamActor;
pub use area_actor::AreaActor;
pub use realm_actor::RealmActor;
pub use protocol::{StreamMessage, StreamRecord, StreamError, AppendResponse, ReadResponse};
pub use store::StreamStore;
