//! Stream domain: durable append-only event logs with store-authoritative
//! commit-time sequencing.
//!
//! # Architecture
//!
//! - **StreamActor** ([actor]): warm per-resource append-session owner for the
//!   current broker process
//! - **StreamStore** ([store]): durable authority for committed resource,
//!   area, and realm ordering
//! - **SessionActor**: enforces authentication and authorization before
//!   forwarding to the stream runtime
//!
//! # Ordering
//!
//! 1. **Resource ordering**: strict sequential offsets within one resource stream
//! 2. **Area ordering**: global ordering across resources in an area
//! 3. **Realm ordering**: global ordering across areas in a realm
//!
//! Area and realm ordering follow commit order, not begin order.
//!
//! # Semantics
//!
//! - **Durable committed events**: committed events and offsets survive restart
//! - **Ephemeral append sessions**: one active append session per resource,
//!   lost on disconnect cleanup or broker restart
//! - **Optimistic concurrency**: caller provides `expected_offset` for conflict detection
//! - **Client-managed resume**: `ReadCursor` is response metadata only, not a durable broker cursor
//! - **Watermark-gated reads**: area and realm reads stop at committed watermarks
//!
//! # Route Format
//!
//! `stream://{realm}/{area}/{resource}/{operation}`
//!
//! Examples:
//! - `stream://acme/orders/checkout/append`
//! - `stream://acme/orders/checkout/read`
//! - `stream://acme/orders/*/read`
//! - `stream://acme/*/*/read`

pub mod actor;
pub mod constants;
pub mod events;
pub mod metrics;
pub mod protocol;
pub mod session;
pub mod storage;
pub mod store;

mod area_actor;
mod realm_actor;

pub use actor::StreamActor;
pub use constants::{
    DEFAULT_LEASE_SIZE, DEFAULT_REALM_LEASE_BLOCK, INTERNAL_REALM_SEGMENT, NOTICE_DEBOUNCE_MS,
};
pub use metrics::StreamMetrics;
pub use protocol::{
    AppendResponse, GetMetadataResponse, ReadResponse, StreamError, StreamMessage, StreamMetadata,
    StreamRecord,
};
pub use session::SessionActor;
pub use store::StreamStore;
