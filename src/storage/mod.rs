//! Storage module - integrates Midge as the storage backend

pub mod markers;
pub mod midge_adapter;
pub mod traits;

pub use crate::routing::RouteFamilyId;
pub use traits::{KvStore, KvTransaction};

/// Initialize storage subsystem (stub)
pub fn init() {
    // TODO: initialize storage backends
}
