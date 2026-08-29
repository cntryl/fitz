//! Stream domain: durable append-only event logs with store-authoritative
//! commit-time sequencing.
//!
//! # Architecture
//!
//! - **`StreamActor`** ([actor]): warm per-resource append-session owner for the
//!   current broker process
//! - **`StreamStore`** ([store]): durable authority for committed resource,
//!   area, and realm ordering
//! - **`StreamDomainSink`** ([sink]): enforces live session ownership before
//!   forwarding append-session operations to the stream runtime
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
//! - **Optimistic concurrency**: caller provides `expected_offset` on each append for conflict detection
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
pub(crate) mod metrics;
pub mod protocol;
pub(crate) mod route_grammar;
pub mod sink;
pub mod storage;
pub mod store;

mod area_actor;
mod realm_actor;
mod watermark_notification;

pub use actor::StreamActor;
pub use constants::{
    INTERNAL_AREA_SEGMENT, INTERNAL_REALM_SEGMENT, MAX_POSTING_ENTRIES_EXAMINED,
    MAX_POSTING_FRAGMENTS_FETCHED, MAX_READ_ITEMS, MAX_WATERMARK_COORDINATORS, NOTICE_DEBOUNCE_MS,
};
pub(crate) use metrics::StreamMetrics;
pub use protocol::{
    AppendResponse, GetMetadataResponse, ReadResponse, StreamClientFrame, StreamClientNotification,
    StreamClientRequest, StreamClientResponse, StreamClientResponseBody, StreamDiscriminator,
    StreamError, StreamFilterClause, StreamFilterSet, StreamFilteredReason, StreamMessage,
    StreamMetadata, StreamReadItem, StreamRecord, StreamSubscriptionFailure,
};
pub use store::{StreamMaintenanceResult, StreamStorageLayout, StreamStore};
