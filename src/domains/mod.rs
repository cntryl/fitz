//! Domain-specific actors and protocols

pub mod kv;
pub mod lease;
pub mod notice;
pub mod queue;
pub mod rpc;
pub mod schedule;
pub mod stream;

// Backwards compatibility alias
pub use notice as notification;
