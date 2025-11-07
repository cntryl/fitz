//! Storage module - integrates Midge as the storage backend

pub mod traits;
pub mod midge_adapter;

pub use traits::{KvStore, KvTransaction};
pub use midge_adapter::{create_local_store, create_memory_store, create_store_with_options, MidgeEngine};

/// Initialize storage subsystem (stub)
pub fn init() {
    // TODO: initialize storage backends
}
