//! Storage module - integrates Midge as the storage backend

pub mod midge_adapter;
pub mod traits;

pub use traits::{KvStore, KvTransaction};
pub use crate::routing::RouteFamilyId;

/// Initialize storage subsystem (stub)
pub fn init() {
    // TODO: initialize storage backends
}
