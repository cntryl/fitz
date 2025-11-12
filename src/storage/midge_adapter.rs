//! Midge storage adapter - provides Midge instances for Fitz
//!
//! Since Midge already implements the KvStore and KvTransaction traits
//! that Fitz uses, we just need to re-export and provide helper functions
//! for creating Midge instances with appropriate configurations.

pub use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode};

use std::path::Path;
use std::sync::Arc;

/// Create a new Midge instance with default options for local filesystem storage
pub fn create_local_store<P: AsRef<Path>>(path: P) -> Result<Arc<MidgeEngine>, String> {
    let mut options = MidgeOptions::default();
    options.storage_mode = StorageMode::LocalFs;
    
    MidgeEngine::new(path, options)
        .map(Arc::new)
        .map_err(|e| format!("Failed to create Midge store: {:?}", e))
}

/// Create a new in-memory Midge instance for testing
pub fn create_memory_store() -> Result<Arc<MidgeEngine>, String> {
    let mut options = MidgeOptions::default();
    options.storage_mode = StorageMode::InMemory;
    
    MidgeEngine::new("", options)
        .map(Arc::new)
        .map_err(|e| format!("Failed to create Midge store: {:?}", e))
}

/// Create a new Midge instance with custom options
pub fn create_store_with_options<P: AsRef<Path>>(
    path: P,
    options: MidgeOptions,
) -> Result<Arc<MidgeEngine>, String> {
    MidgeEngine::new(path, options)
        .map(Arc::new)
        .map_err(|e| format!("Failed to create Midge store: {:?}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_memory_store() {
        let result = create_memory_store();
        assert!(result.is_ok(), "Should create in-memory store");
    }
}
