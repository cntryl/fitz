//! Storage module

pub mod traits;

pub use traits::{KvStore, KvTransaction};

/// Initialize storage subsystem (stub)
pub fn init() {
    // TODO: initialize storage backends
}
