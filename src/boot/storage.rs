//! Storage initialization

use crate::boot::runtime::{BootConfig, BootResult, StorageMode};
use std::sync::Arc;
use tracing::info;

/// Initialize Midge storage engine based on configured storage mode.
pub async fn init(config: &BootConfig) -> BootResult<Arc<cntryl_midge::Engine>> {
    match &config.storage_mode {
        StorageMode::Memory => init_memory(config).await,
        StorageMode::LocalDisk { db_path } => init_local_disk(config, db_path).await,
        StorageMode::CloudBacked {
            provider,
            bucket,
            prefix,
            local_cache_path,
        } => {
            init_cloud(
                config,
                provider,
                bucket,
                prefix.as_deref(),
                local_cache_path,
            )
            .await
        }
    }
}

/// Ensure required column families exist.
fn ensure_column_families(engine: &cntryl_midge::Engine) -> BootResult<()> {
    ensure_route_family(engine, crate::runtime::routing::RouteFamily::new(1))
}

/// Ensure the storage column family aligned with a RouteFamily exists.
pub fn ensure_route_family(
    engine: &cntryl_midge::Engine,
    family: crate::runtime::routing::RouteFamily,
) -> BootResult<()> {
    let cf_name = if family.id() == 1 {
        "tenant_default".to_string()
    } else {
        format!("tenant_{}", family.id())
    };

    let cf = engine
        .create_column_family(&cf_name)
        .map_err(|e| format!("Failed to create tenant CF {}: {}", family.id(), e))?;

    if cf.id() != family.id() {
        return Err(format!(
            "RouteFamily {} mapped to unexpected column family {}",
            family.id(),
            cf.id()
        )
        .into());
    }

    info!(
        cf_id = cf.id(),
        cf_name = %cf_name,
        "Ensured tenant column family exists"
    );

    Ok(())
}

/// Initialize in-memory storage.
async fn init_memory(_config: &BootConfig) -> BootResult<Arc<cntryl_midge::Engine>> {
    info!("Initializing in-memory storage (ephemeral, no persistence)");

    let store = cntryl_midge::Engine::open(cntryl_midge::OpenOptions::in_memory().build())
        .map_err(|e| format!("Failed to open in-memory Midge: {}", e))?;

    ensure_column_families(&store)?;

    info!("In-memory storage ready (data lost on shutdown)");
    Ok(Arc::new(store))
}

/// Initialize local disk storage.
async fn init_local_disk(
    _config: &BootConfig,
    db_path: &str,
) -> BootResult<Arc<cntryl_midge::Engine>> {
    info!("Initializing local disk storage at {}", db_path);

    tokio::fs::create_dir_all(db_path)
        .await
        .map_err(|e| format!("Failed to create storage directory {}: {}", db_path, e))?;

    let store = cntryl_midge::Engine::open(cntryl_midge::OpenOptions::local(db_path).build())
        .map_err(|e| format!("Failed to open Midge at {}: {}", db_path, e))?;

    ensure_column_families(&store)?;

    info!("Local disk storage ready at {}", db_path);
    Ok(Arc::new(store))
}

/// Initialize cloud-backed storage.
async fn init_cloud(
    _config: &BootConfig,
    provider: &str,
    bucket: &str,
    prefix: Option<&str>,
    local_cache_path: &str,
) -> BootResult<Arc<cntryl_midge::Engine>> {
    info!(
        "Initializing cloud storage: provider={} bucket={} prefix={:?} cache={}",
        provider, bucket, prefix, local_cache_path
    );

    match provider {
        "s3" => {
            if std::env::var("AWS_ACCESS_KEY_ID").is_err() && std::env::var("AWS_PROFILE").is_err()
            {
                return Err("AWS_ACCESS_KEY_ID or AWS_PROFILE required for S3 storage".into());
            }
            info!("S3 credentials detected");
        }
        "gcs" => {
            if std::env::var("GOOGLE_APPLICATION_CREDENTIALS").is_err() {
                return Err("GOOGLE_APPLICATION_CREDENTIALS required for GCS storage".into());
            }
            info!("GCS credentials detected");
        }
        "azure" => {
            if std::env::var("AZURE_STORAGE_ACCOUNT_NAME").is_err() {
                return Err("AZURE_STORAGE_ACCOUNT_NAME required for Azure storage".into());
            }
            info!("Azure credentials detected");
        }
        other => {
            return Err(format!("Unsupported cloud provider: {}", other).into());
        }
    }

    tokio::fs::create_dir_all(local_cache_path)
        .await
        .map_err(|e| {
            format!(
                "Failed to create cloud cache directory {}: {}",
                local_cache_path, e
            )
        })?;

    let store = cntryl_midge::Engine::open(
        cntryl_midge::OpenOptions::cloud(
            local_cache_path,
            bucket.to_string(),
            prefix.unwrap_or_default().to_string(),
        )
        .build(),
    )
    .map_err(|e| format!("Failed to open cloud-backed Midge: {}", e))?;

    ensure_column_families(&store)?;

    info!(
        "Cloud storage ready: {} bucket={} prefix={:?} cache={}",
        provider, bucket, prefix, local_cache_path
    );
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cntryl_midge::{TransactionMode, WriteOptions};
    use tempfile::TempDir;

    fn write_marker(engine: &cntryl_midge::Engine, cf_id: u32, key: &[u8], value: &[u8]) {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(key.to_vec(), value.to_vec(), None)
            .expect("write marker");
        tx.commit(WriteOptions::buffered()).expect("commit marker");
    }

    fn read_marker(engine: &cntryl_midge::Engine, cf_id: u32, key: &[u8]) -> Option<Vec<u8>> {
        let tx = engine
            .begin_tx(cf_id, TransactionMode::ReadOnly)
            .expect("begin read tx");
        tx.get(key)
            .expect("read marker")
            .map(|value| value.to_vec())
    }

    #[test]
    fn should_create_boot_config_for_test_storage() {
        // Arrange
        let config = BootConfig::with_memory_storage();

        // Act
        match &config.storage_mode {
            crate::boot::runtime::StorageMode::Memory => {}
            _ => panic!("Expected memory storage mode"),
        }

        // Assert
    }

    #[test]
    fn should_detect_local_storage_by_default() {
        // Arrange
        let config = BootConfig::new();

        // Act
        match &config.storage_mode {
            crate::boot::runtime::StorageMode::LocalDisk { db_path } => {
                // Assert
                assert_eq!(db_path, "./.fitz");
            }
            _ => panic!("Expected local disk storage mode"),
        }
    }

    #[test]
    fn should_support_memory_storage_mode() {
        // Arrange
        let config = BootConfig::with_memory_storage();

        // Act
        let is_memory_mode = matches!(
            config.storage_mode,
            crate::boot::runtime::StorageMode::Memory
        );

        // Assert
        assert!(is_memory_mode);
    }

    #[test]
    fn should_support_local_storage_mode() {
        // Arrange
        let config = BootConfig::with_local_storage("/data/fitz");

        // Act
        match &config.storage_mode {
            crate::boot::runtime::StorageMode::LocalDisk { db_path } => {
                // Assert
                assert_eq!(db_path, "/data/fitz");
            }
            _ => panic!("Expected local disk storage mode"),
        }
    }

    #[test]
    fn should_support_cloud_storage_mode() {
        // Arrange
        let config = BootConfig::default().with_storage_mode(
            crate::boot::runtime::StorageMode::CloudBacked {
                provider: "s3".to_string(),
                bucket: "fitz-data".to_string(),
                prefix: Some("prod".to_string()),
                local_cache_path: "./.fitz-cloud-cache".to_string(),
            },
        );

        // Act
        match &config.storage_mode {
            crate::boot::runtime::StorageMode::CloudBacked {
                provider,
                bucket,
                prefix,
                local_cache_path,
            } => {
                // Assert
                assert_eq!(provider, "s3");
                assert_eq!(bucket, "fitz-data");
                assert_eq!(prefix, &Some("prod".to_string()));
                assert_eq!(local_cache_path, "./.fitz-cloud-cache");
            }
            _ => panic!("Expected cloud storage mode"),
        }
    }

    #[tokio::test]
    async fn should_persist_local_disk_storage_across_restarts() {
        let tempdir = TempDir::new().expect("tempdir");
        let db_path = tempdir.path().join("fitz-local");
        let config = BootConfig::with_local_storage(db_path.to_string_lossy().to_string());

        let store = init(&config).await.expect("open first store");
        let cf = store
            .get_column_family("tenant_default")
            .expect("tenant_default cf");
        write_marker(store.as_ref(), cf.id(), b"marker", b"value");
        drop(store);

        let reopened = init(&config).await.expect("reopen store");
        let reopened_cf = reopened
            .get_column_family("tenant_default")
            .expect("tenant_default cf after reopen");

        assert_eq!(
            read_marker(reopened.as_ref(), reopened_cf.id(), b"marker"),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn should_reject_cloud_storage_without_bucket() {
        // Arrange
        let config = BootConfig::default().with_storage_mode(StorageMode::CloudBacked {
            provider: "s3".to_string(),
            bucket: String::new(),
            prefix: Some("prod".to_string()),
            local_cache_path: "./.fitz-cloud-cache".to_string(),
        });

        // Act
        let validation_result = config.validate();

        // Assert
        assert!(validation_result.is_err());
    }
}
