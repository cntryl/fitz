//! Storage module - integrates Midge as the storage backend

pub mod traits;
pub mod midge_adapter;

pub use traits::{KvStore, KvTransaction};
pub use cntryl_midge_adapter::{create_local_store, create_memory_store, create_store_with_options, MidgeEngine};

/// Route Family identifier for tenant/shard isolation
///
/// Fitz defines its own RouteFamilyId type (decoupled from midge) to:
/// - Maintain clear separation between routing logic and storage implementation
/// - Enable potential RF metadata, validation, or lifecycle management in the future
/// - Provide flexibility in mapping between Fitz RFs and midge storage backend
pub type RouteFamilyId = u32;

/// Default route family for backwards compatibility (typically for single-tenant scenarios)
pub const DEFAULT_RF: RouteFamilyId = 0;

/// Initialize storage subsystem (stub)
pub fn init() {
    // TODO: initialize storage backends
}
