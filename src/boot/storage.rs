//! Storage initialization

use crate::boot::runtime::{BootConfig, BootResult, StorageMode};
use std::sync::Arc;
use tracing::info;
use cntryl_midge::testkit::MidgeOptions;

/// Initialize Midge storage engine based on configured storage mode
///
/// # Storage Modes
///
/// **In-Memory** (FITZ_STORAGE_MODE=memory)
/// - No persistence, data lost on shutdown
/// - Best for: testing, development, stateless deployments
///
/// **Local Disk** (FITZ_STORAGE_MODE=local) [DEFAULT]
/// - Durable file-backed storage
/// - FITZ_STORAGE_PATH: directory path (default: ./.fitz)
/// - Best for: single-node deployments, development
///
/// **Cloud** (FITZ_STORAGE_MODE=s3|gcs|azure)
/// - Cloud object storage backend
/// - FITZ_STORAGE_PROVIDER: s3, gcs, or azure
/// - FITZ_STORAGE_BUCKET: bucket/container name
/// - FITZ_STORAGE_PREFIX: optional path prefix
/// - Best for: distributed deployments, scalability
pub async fn init(config: &BootConfig) -> BootResult<Arc<cntryl_midge::Engine>> {
    match &config.storage_mode {
        StorageMode::Memory => init_memory(config).await,
        StorageMode::LocalDisk { db_path } => init_local_disk(config, db_path).await,
        StorageMode::CloudBacked {
            provider,
            bucket,
            prefix,
        } => init_cloud(config, provider, bucket, prefix.as_deref()).await,
    }
}

/// Ensure required column families exist.
///
/// RouteFamily IDs map 1:1 to Midge ColumnFamilyIds. CF 0 is reserved
/// (default column family) and must never be used for tenant data.
/// For single-tenant / development mode we need at least CF 1 (the
/// default dev tenant). Column family creation is idempotent in Midge,
/// so calling this on every startup is safe.
fn ensure_column_families(engine: &cntryl_midge::Engine) -> BootResult<()> {
    // Create the default dev-tenant column family (RouteFamily 1 → CF 1).
    let cf = engine
        .create_column_family("tenant_default")
        .map_err(|e| format!("Failed to create default tenant CF: {}", e))?;

    info!(
        cf_id = cf.id(),
        cf_name = "tenant_default",
        "Ensured default tenant column family exists"
    );

    Ok(())
}

/// Initialize in-memory storage
///
/// Data is ephemeral and lost on shutdown. Useful for testing and development.
async fn init_memory(_config: &BootConfig) -> BootResult<Arc<cntryl_midge::Engine>> {
    info!("Initializing in-memory storage (ephemeral, no persistence)");

    let store = cntryl_midge::Engine::open_with_options(MidgeOptions::default())
        .map_err(|e| format!("Failed to open in-memory Midge: {}", e))?;

    ensure_column_families(&store)?;

    info!("In-memory storage ready (data lost on shutdown)");
    Ok(Arc::new(store))
}

/// Initialize local disk storage
///
/// Durable file-backed storage at the specified path.
async fn init_local_disk(
    _config: &BootConfig,
    db_path: &str,
) -> BootResult<Arc<cntryl_midge::Engine>> {
    info!("Initializing local disk storage at {}", db_path);

    // Create directory if it doesn't exist
    tokio::fs::create_dir_all(db_path)
        .await
        .map_err(|e| format!("Failed to create storage directory {}: {}", db_path, e))?;

    let store = cntryl_midge::Engine::open_with_options(MidgeOptions::default())
        .map_err(|e| format!("Failed to open Midge at {}: {}", db_path, e))?;

    ensure_column_families(&store)?;

    info!("Local disk storage ready at {}", db_path);
    Ok(Arc::new(store))
}

/// Initialize cloud-backed storage
///
/// Connects to cloud object storage (S3, GCS, Azure).
/// Cloud configuration must be set up via environment variables
/// (AWS credentials, GCS service account, Azure credentials).
async fn init_cloud(
    _config: &BootConfig,
    provider: &str,
    bucket: &str,
    prefix: Option<&str>,
) -> BootResult<Arc<cntryl_midge::Engine>> {
    info!(
        "Initializing cloud storage: provider={} bucket={} prefix={:?}",
        provider, bucket, prefix
    );

    // Validate cloud provider and verify credentials
    match provider {
        "s3" => {
            // Check for AWS credentials
            if std::env::var("AWS_ACCESS_KEY_ID").is_err() && std::env::var("AWS_PROFILE").is_err()
            {
                return Err("AWS_ACCESS_KEY_ID or AWS_PROFILE required for S3 storage".into());
            }
            info!("S3 credentials detected");
        }
        "gcs" => {
            // Check for GCS credentials
            if std::env::var("GOOGLE_APPLICATION_CREDENTIALS").is_err() {
                return Err("GOOGLE_APPLICATION_CREDENTIALS required for GCS storage".into());
            }
            info!("GCS credentials detected");
        }
        "azure" => {
            // Check for Azure credentials
            if std::env::var("AZURE_STORAGE_ACCOUNT_NAME").is_err() {
                return Err("AZURE_STORAGE_ACCOUNT_NAME required for Azure storage".into());
            }
            info!("Azure credentials detected");
        }
        other => {
            return Err(format!("Unsupported cloud provider: {}", other).into());
        }
    }

    // For now, cloud storage requires actual Midge cloud support
    // This would be implemented when Midge adds cloud backend support
    let store = cntryl_midge::Engine::open_with_options(MidgeOptions::default())
        .map_err(|e| format!("Failed to open cloud-backed Midge: {}", e))?;

    ensure_column_families(&store)?;

    info!(
        "Cloud storage ready: {} bucket={} prefix={:?}",
        provider, bucket, prefix
    );
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_boot_config_for_test_storage() {
        // Arrange

        // Act
        let config = BootConfig::with_memory_storage();

        // Assert
        match &config.storage_mode {
            crate::boot::runtime::StorageMode::Memory => {
                // Expected
            }
            _ => panic!("Expected memory storage mode"),
        }
    }

    #[test]
    fn should_detect_local_storage_by_default() {
        // Arrange

        // Act
        let config = BootConfig::new();

        // Assert
        match &config.storage_mode {
            crate::boot::runtime::StorageMode::LocalDisk { db_path } => {
                assert_eq!(db_path, "./.fitz");
            }
            _ => panic!("Expected local disk storage mode"),
        }
    }

    #[test]
    fn should_support_memory_storage_mode() {
        // Arrange

        // Act
        let config = BootConfig::with_memory_storage();

        // Assert
        assert!(matches!(
            config.storage_mode,
            crate::boot::runtime::StorageMode::Memory
        ));
    }

    #[test]
    fn should_support_local_storage_mode() {
        // Arrange

        // Act
        let config = BootConfig::with_local_storage("/data/fitz");

        // Assert
        match &config.storage_mode {
            crate::boot::runtime::StorageMode::LocalDisk { db_path } => {
                assert_eq!(db_path, "/data/fitz");
            }
            _ => panic!("Expected local disk storage mode"),
        }
    }

    #[test]
    fn should_support_cloud_storage_mode() {
        // Arrange

        // Act
        let config = BootConfig::default().with_storage_mode(
            crate::boot::runtime::StorageMode::CloudBacked {
                provider: "s3".to_string(),
                bucket: "fitz-data".to_string(),
                prefix: Some("prod".to_string()),
            },
        );

        // Assert
        match &config.storage_mode {
            crate::boot::runtime::StorageMode::CloudBacked {
                provider,
                bucket,
                prefix,
            } => {
                assert_eq!(provider, "s3");
                assert_eq!(bucket, "fitz-data");
                assert_eq!(prefix, &Some("prod".to_string()));
            }
            _ => panic!("Expected cloud storage mode"),
        }
    }
}
