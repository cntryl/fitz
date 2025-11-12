//! Midge storage adapter - provides KvStore instances for Fitz
//!
//! Since Midge already implements the KvStore and KvTransaction traits
//! that Fitz uses, we just need to provide helper functions for creating
//! Midge instances with appropriate configurations.

use cntryl_midge::config::ConfigBuilder;
use cntryl_midge::core::{KvStoreAdapter, MidgeEngine};
use cntryl_midge::KvStore;

use std::path::Path;
use std::sync::Arc;

/// Create a new in-memory KvStore for testing
pub fn create_memory_store() -> Result<Arc<dyn KvStore>, String> {
    // Create temporary directory for in-memory storage
    let temp_dir = std::env::temp_dir().join("fitz_memstore");
    let config = ConfigBuilder::new(&temp_dir)
        .build()
        .map_err(|e| format!("Failed to build config: {:?}", e))?;
    let engine = MidgeEngine::open_with_config(config)
        .map_err(|e| format!("Failed to create memory store: {:?}", e))?;
    let adapter = KvStoreAdapter::new(Arc::new(engine));
    Ok(Arc::new(adapter) as Arc<dyn KvStore>)
}

/// Create a new KvStore with local filesystem storage
pub fn create_local_store<P: AsRef<Path>>(path: P) -> Result<Arc<dyn KvStore>, String> {
    let config = ConfigBuilder::new(path.as_ref())
        .build()
        .map_err(|e| format!("Failed to build config: {:?}", e))?;
    let engine = MidgeEngine::open_with_config(config)
        .map_err(|e| format!("Failed to create local disk store: {:?}", e))?;
    let adapter = KvStoreAdapter::new(Arc::new(engine));
    Ok(Arc::new(adapter) as Arc<dyn KvStore>)
}

/// Create a new KvStore with cloud storage backend
pub fn create_cloud_store(_cloud_config: &str) -> Result<Arc<dyn KvStore>, String> {
    // TODO: Implement cloud storage configuration once midge supports it
    Err("Cloud storage not yet supported".to_string())
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
