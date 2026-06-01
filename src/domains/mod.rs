//! Domain-specific actors and protocols

pub mod kv;
pub mod lease;
pub mod notice;
pub mod queue;
pub mod rpc;
pub mod schedule;
pub mod stream;
pub(crate) mod subscription_state;

// Backwards compatibility alias
pub use notice as notification;
