//! Domain-specific actors and protocols

pub mod kv;
pub mod lease;
pub mod notice;
pub mod queue;
pub mod rpc;
pub mod schedule;
pub mod stream;
pub(crate) mod subscription_state;

pub(crate) const DOMAIN_ACTOR_MAILBOX_CAPACITY: usize = 16_384;
