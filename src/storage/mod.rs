//! Integration with Midge for durable storage.
//!
//! Only one actor touches Midge: MidgeActor.
//! All persistence flows through messages to MidgeActor,
//! keeping the hot path (other actors) purely in-memory.

pub mod midge_actor;
pub mod api;
pub mod types;

pub use midge_actor::MidgeActor;
pub use api::{DurableApi, StreamOp, QueueOp, KvOp};
pub use types::{StreamRecord, QueueRecord, KvItem};
