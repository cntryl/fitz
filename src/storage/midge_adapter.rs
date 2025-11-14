//! Midge storage adapter - provides KvStore instances for Fitz
//!
//! Since Midge already implements the KvStore trait that Fitz uses,
//! we provide helper functions for creating Midge instances with
//! appropriate configurations using Midge's factory methods.

use cntryl_midge::{MidgeEngine, MidgeOptions, StorageMode, KvStore};

use std::path::Path;
use std::sync::Arc;

/// Create a new in-memory KvStore for testing
pub fn create_memory_store() -> Result<Arc<dyn KvStore>, String> {
    let opts = MidgeOptions {
        storage_mode: StorageMode::Memory,
        ..Default::default()
    };
    let engine = Arc::new(
        MidgeEngine::open(opts)
            .map_err(|e| format!("Failed to create memory store: {:?}", e))?
    );
    Ok(Arc::new(engine.as_kv_store()) as Arc<dyn KvStore>)
}

/// Create a new KvStore with local filesystem storage
pub fn create_local_store<P: AsRef<Path>>(path: P) -> Result<Arc<dyn KvStore>, String> {
    let opts = MidgeOptions {
        storage_mode: StorageMode::LocalDisk {
            db_path: path.as_ref().to_path_buf(),
        },
        ..Default::default()
    };
    let engine = Arc::new(
        MidgeEngine::open(opts)
            .map_err(|e| format!("Failed to create local disk store: {:?}", e))?
    );
    Ok(Arc::new(engine.as_kv_store()) as Arc<dyn KvStore>)
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
        // Arrange
        // No setup needed

        // Act
        let result = create_memory_store();

        // Assert
        assert!(result.is_ok(), "Should create in-memory store");
    }

    #[test]
    fn should_create_local_store() {
        // Arrange
        let temp_dir = std::env::temp_dir().join("fitz_test_local_store");

        // Act
        let result = create_local_store(&temp_dir);

        // Assert
        assert!(result.is_ok(), "Should create local disk store");
    }
}
