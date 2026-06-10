//! Storage initialization

use crate::boot::runtime::{BootConfig, BootResult, CloudStorageConfig, StorageMode};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const LOCAL_DISK_OPEN_MAX_RETRIES: u32 = 10;
const LOCAL_DISK_OPEN_BASE_BACKOFF: Duration = Duration::from_millis(250);
const LOCAL_DISK_OPEN_MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Initialize Midge storage engine based on configured storage mode.
pub async fn init(config: &BootConfig) -> BootResult<Arc<cntryl_midge::Engine>> {
    match &config.storage_mode {
        StorageMode::Memory => init_memory(config).await,
        StorageMode::LocalDisk { db_path } => init_local_disk(config, db_path).await,
        StorageMode::CloudBacked(cloud) => init_cloud(config, cloud).await,
        StorageMode::Invalid { reason } => Err(reason.clone().into()),
    }
}

/// Ensure required column families exist.
fn ensure_column_families(engine: &cntryl_midge::Engine, config: &BootConfig) -> BootResult<()> {
    for family in &config.route_families {
        ensure_route_family(
            engine,
            crate::runtime::routing::RouteFamily::new((*family).into()),
        )?;
    }
    Ok(())
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
async fn init_memory(config: &BootConfig) -> BootResult<Arc<cntryl_midge::Engine>> {
    info!("Initializing in-memory storage (ephemeral, no persistence)");

    let open_options = build_midge_open_options(cntryl_midge::OpenOptions::in_memory(), config)?;
    let store = cntryl_midge::Engine::open(open_options)
        .map_err(|e| format!("Failed to open in-memory Midge: {}", e))?;

    ensure_column_families(&store, config)?;

    info!("In-memory storage ready (data lost on shutdown)");
    Ok(Arc::new(store))
}

/// Initialize local disk storage.
async fn init_local_disk(
    config: &BootConfig,
    db_path: &str,
) -> BootResult<Arc<cntryl_midge::Engine>> {
    info!("Initializing local disk storage at {}", db_path);

    tokio::fs::create_dir_all(db_path)
        .await
        .map_err(|e| format!("Failed to create storage directory {}: {}", db_path, e))?;

    let store = open_local_disk_with_retry(config, db_path).await?;

    ensure_column_families(&store, config)?;

    info!("Local disk storage ready at {}", db_path);
    Ok(Arc::new(store))
}

async fn open_local_disk_with_retry(
    config: &BootConfig,
    db_path: &str,
) -> BootResult<cntryl_midge::Engine> {
    let open_options = build_midge_open_options(cntryl_midge::OpenOptions::local(db_path), config)?;
    let mut retry_attempt = 0;

    loop {
        match cntryl_midge::Engine::open(open_options.clone()) {
            Ok(store) => return Ok(store),
            Err(error)
                if should_retry_local_disk_open(&error)
                    && retry_attempt < LOCAL_DISK_OPEN_MAX_RETRIES =>
            {
                let delay = local_disk_open_retry_delay(retry_attempt);
                warn!(
                    db_path = db_path,
                    retry_attempt = retry_attempt + 1,
                    max_retries = LOCAL_DISK_OPEN_MAX_RETRIES,
                    delay_ms = delay.as_millis() as u64,
                    error = %error,
                    "Local disk storage open hit an active writer lease; retrying with exponential backoff"
                );
                retry_attempt += 1;
                tokio::time::sleep(delay).await;
            }
            Err(error) => {
                return Err(format!("Failed to open Midge at {}: {}", db_path, error).into())
            }
        }
    }
}

fn should_retry_local_disk_open(error: &cntryl_midge::MidgeError) -> bool {
    matches!(
        error,
        cntryl_midge::MidgeError::Internal(message)
            if message.contains("another Midge instance is already running against this storage")
    )
}

fn local_disk_open_retry_delay(attempt: u32) -> Duration {
    let multiplier = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX);
    let delay_ms = (LOCAL_DISK_OPEN_BASE_BACKOFF.as_millis() as u64)
        .saturating_mul(multiplier)
        .min(LOCAL_DISK_OPEN_MAX_BACKOFF.as_millis() as u64);
    Duration::from_millis(delay_ms)
}

fn build_midge_open_options(
    open_options: cntryl_midge::OpenOptions,
    config: &BootConfig,
) -> BootResult<cntryl_midge::OpenOptions> {
    let open_options = if matches!(&config.storage_mode, StorageMode::CloudBacked(_))
        && config.storage_memtable_bytes().is_none()
    {
        info!("Configuring cloud-backed Midge for throughput-oriented write batching");
        open_options
            .goal(cntryl_midge::Goal::Throughput)
            .workload(cntryl_midge::WorkloadProfile::WriteHeavy)
    } else {
        open_options
    };

    let open_options = match config.storage_memtable_bytes() {
        Some(memtable_bytes) => {
            info!(
                memtable_bytes = memtable_bytes,
                "Configuring Midge memtable size from FITZ_STORAGE_MEMTABLE_BYTES"
            );
            open_options.with_memtable_size_limit(memtable_bytes)
        }
        None => open_options,
    };

    Ok(open_options.build())
}

/// Initialize cloud-backed storage.
async fn init_cloud(
    config: &BootConfig,
    cloud: &CloudStorageConfig,
) -> BootResult<Arc<cntryl_midge::Engine>> {
    info!(
        "Initializing cloud storage: provider={} namespace={} prefix={:?} cache={}",
        cloud.provider_name,
        cloud.provider_config.bucket_or_container(),
        cloud.prefix,
        cloud.local_cache_path
    );

    tokio::fs::create_dir_all(&cloud.local_cache_path)
        .await
        .map_err(|e| {
            format!(
                "Failed to create cloud cache directory {}: {}",
                cloud.local_cache_path, e
            )
        })?;

    let open_options = build_midge_open_options(
        cntryl_midge::OpenOptions::cloud(
            cloud.local_cache_path.clone(),
            cloud.provider_config.clone(),
            cloud.prefix.clone().unwrap_or_default(),
        ),
        config,
    )?;
    // Cloud engine bootstrap may create and drop an internal Tokio runtime.
    // Run it on a blocking thread to avoid dropping that runtime inside async context.
    let store = tokio::task::spawn_blocking(move || cntryl_midge::Engine::open(open_options))
        .await
        .map_err(|e| format!("Cloud-backed Midge open task failed: {}", e))?
        .map_err(|e| format!("Failed to open cloud-backed Midge: {}", e))?;

    ensure_column_families(&store, config)?;

    info!(
        "Cloud storage ready: {} namespace={} prefix={:?} cache={}",
        cloud.provider_name,
        cloud.provider_config.bucket_or_container(),
        cloud.prefix,
        cloud.local_cache_path
    );
    Ok(Arc::new(store))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cntryl_midge::{Goal, MemoryBudget, TransactionMode, WorkloadProfile, WriteOptions};
    use std::time::Duration;
    use tempfile::TempDir;

    fn write_marker(engine: &cntryl_midge::Engine, cf_id: u32, key: &[u8], value: &[u8]) {
        write_marker_with_options(engine, cf_id, key, value, WriteOptions::buffered());
    }

    fn write_marker_with_options(
        engine: &cntryl_midge::Engine,
        cf_id: u32,
        key: &[u8],
        value: &[u8],
        write_options: WriteOptions,
    ) {
        let mut tx = engine
            .begin_tx(cf_id, TransactionMode::ReadWrite)
            .expect("begin write tx");
        tx.put(key.to_vec(), value.to_vec(), None)
            .expect("write marker");
        tx.commit(write_options).expect("commit marker");
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

    #[tokio::test]
    async fn should_provision_configured_route_family_column_families() {
        // Arrange
        let config = BootConfig::with_memory_storage().with_route_families(vec![1, 2]);

        // Act
        let store = init(&config).await.expect("open memory store");

        // Assert
        assert_eq!(
            store
                .get_column_family("tenant_default")
                .expect("default route family column family")
                .id(),
            1
        );
        assert_eq!(
            store
                .get_column_family("tenant_2")
                .expect("second route family column family")
                .id(),
            2
        );
    }

    #[test]
    fn should_apply_configured_storage_memtable_bytes_to_midge_options() {
        // Arrange
        let memtable_bytes = 8 * 1024 * 1024;
        let config = BootConfig::with_memory_storage().with_storage_memtable_bytes(memtable_bytes);

        // Act
        let open_options =
            build_midge_open_options(cntryl_midge::OpenOptions::in_memory(), &config)
                .expect("build open options");

        // Assert
        assert_eq!(open_options.memtable_size_limit(), memtable_bytes);
    }

    #[tokio::test]
    async fn should_open_storage_with_configured_midge_memtable_size() {
        // Arrange
        let memtable_bytes = 128 * 1024;
        let config = BootConfig::with_memory_storage().with_storage_memtable_bytes(memtable_bytes);

        // Act
        let store = init(&config).await.expect("open memory store");
        let metrics = store.get_runtime_metrics().expect("runtime metrics");

        // Assert
        assert_eq!(metrics.memtable_size_limit, memtable_bytes);
        assert_eq!(metrics.memtable_flush_threshold, memtable_bytes);
    }

    #[test]
    fn should_apply_cloud_throughput_defaults_when_memtable_is_auto() {
        // Arrange
        let config = BootConfig::default().with_storage_mode(StorageMode::CloudBacked(Box::new(
            CloudStorageConfig {
                provider_name: "peas-s3".to_string(),
                provider_config: cntryl_midge::CloudProviderConfig::peas_s3("fitz-cost-tuning"),
                prefix: Some("tests".to_string()),
                local_cache_path: "./.fitz-cloud-cache".to_string(),
            },
        )));

        let open_options = cntryl_midge::OpenOptions::cloud_simulated(
            "./target/tmp/fitz-cloud-cost-baseline",
            "fitz-cost-tuning",
            "tests",
        )
        .memory_budget(MemoryBudget::Bytes(512 * 1024 * 1024));

        // Act
        let tuned = build_midge_open_options(open_options, &config).expect("build tuned options");

        // Assert
        assert_eq!(tuned.goal, Goal::Throughput);
        assert_eq!(tuned.workload, WorkloadProfile::WriteHeavy);
        assert_eq!(tuned.memtable_size_limit(), 256 * 1024 * 1024);
        assert_eq!(tuned.wal_buffer_size(), 1024 * 1024);
        assert_eq!(tuned.target_sst_size(), 512 * 1024 * 1024);
    }

    #[test]
    fn should_respect_cloud_memtable_override_before_tuning() {
        // Arrange
        let memtable_bytes = 8 * 1024 * 1024;
        let config = BootConfig::default()
            .with_storage_memtable_bytes(memtable_bytes)
            .with_storage_mode(StorageMode::CloudBacked(Box::new(CloudStorageConfig {
                provider_name: "peas-s3".to_string(),
                provider_config: cntryl_midge::CloudProviderConfig::peas_s3("fitz-cost-tuning"),
                prefix: Some("tests".to_string()),
                local_cache_path: "./.fitz-cloud-cache".to_string(),
            })));

        let open_options = cntryl_midge::OpenOptions::cloud_simulated(
            "./target/tmp/fitz-cloud-cost-override",
            "fitz-cost-tuning",
            "tests",
        )
        .memory_budget(MemoryBudget::Bytes(512 * 1024 * 1024));

        // Act
        let tuned = build_midge_open_options(open_options, &config).expect("build tuned options");

        // Assert
        let engine = cntryl_midge::Engine::open(tuned).expect("open tuned cloud engine");
        let metrics = engine.get_runtime_metrics().expect("runtime metrics");

        assert_eq!(metrics.memtable_size_limit, memtable_bytes);
        assert_eq!(metrics.memtable_flush_threshold, memtable_bytes);
    }

    #[test]
    fn should_reduce_cloud_wal_flush_churn_with_throughput_tuning_on_cloud_simulated_storage() {
        // Arrange
        let tempdir = TempDir::new().expect("tempdir");
        let burst_value = vec![b'x'; 1024 * 1024];
        let write_count = 80;
        let budget = MemoryBudget::Bytes(512 * 1024 * 1024);

        let config = BootConfig::default().with_storage_mode(StorageMode::CloudBacked(Box::new(
            CloudStorageConfig {
                provider_name: "peas-s3".to_string(),
                provider_config: cntryl_midge::CloudProviderConfig::peas_s3("fitz-cost-tuning"),
                prefix: Some("tests".to_string()),
                local_cache_path: tempdir.path().join("cache").to_string_lossy().to_string(),
            },
        )));

        let baseline_opts = cntryl_midge::OpenOptions::cloud_simulated(
            tempdir.path().join("baseline"),
            "fitz-cost-tuning",
            "tests",
        )
        .memory_budget(budget)
        .build();
        let tuned_opts = build_midge_open_options(
            cntryl_midge::OpenOptions::cloud_simulated(
                tempdir.path().join("tuned"),
                "fitz-cost-tuning",
                "tests",
            )
            .memory_budget(budget),
            &config,
        )
        .expect("build tuned cloud options");

        assert_eq!(baseline_opts.wal_buffer_size(), 128 * 1024);
        assert_eq!(tuned_opts.wal_buffer_size(), 1024 * 1024);

        // Act
        let baseline_metrics = exercise_cloud_burst(
            baseline_opts,
            write_count,
            &burst_value,
            Duration::from_secs(2),
        );
        let tuned_metrics = exercise_cloud_burst(
            tuned_opts,
            write_count,
            &burst_value,
            Duration::from_secs(2),
        );

        // Assert
        assert!(
            baseline_metrics.sst_count >= tuned_metrics.sst_count,
            "expected no more SST churn with throughput tuning; baseline={} tuned={}",
            baseline_metrics.sst_count,
            tuned_metrics.sst_count
        );
        assert!(
            baseline_metrics.pending_cloud_uploads >= tuned_metrics.pending_cloud_uploads,
            "expected no more pending cloud uploads with throughput tuning; baseline={} tuned={}",
            baseline_metrics.pending_cloud_uploads,
            tuned_metrics.pending_cloud_uploads
        );
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

    #[tokio::test]
    async fn should_persist_local_disk_storage_across_restarts() {
        // Arrange
        let tempdir = TempDir::new().expect("tempdir");
        let db_path = tempdir.path().join("fitz-local");
        let config = BootConfig::with_local_storage(db_path.to_string_lossy().to_string());

        // Act
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

        // Assert
        assert_eq!(
            read_marker(reopened.as_ref(), reopened_cf.id(), b"marker"),
            Some(b"value".to_vec())
        );
    }

    #[tokio::test]
    async fn should_retry_local_disk_open_given_active_writer_lease_when_holder_releases() {
        // Arrange
        let tempdir = TempDir::new().expect("tempdir");
        let db_path = tempdir.path().join("fitz-local-retry");
        let config = BootConfig::with_local_storage(db_path.to_string_lossy().to_string());
        let store = init(&config).await.expect("open first store");
        let release_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            shutdown_store(store);
        });

        // Act
        let reopened = tokio::time::timeout(Duration::from_secs(5), init(&config))
            .await
            .expect("local disk retry should not hang")
            .expect("reopen store after retry");

        // Assert
        release_task.await.expect("release first store");
        shutdown_store(reopened);
    }

    #[test]
    fn should_reject_cloud_storage_without_bucket() {
        // Arrange
        let config = BootConfig::default().with_storage_mode(StorageMode::CloudBacked(Box::new(
            CloudStorageConfig {
                provider_name: "peas-s3".to_string(),
                provider_config: cntryl_midge::CloudProviderConfig::peas_s3(""),
                prefix: Some("prod".to_string()),
                local_cache_path: "./.fitz-cloud-cache".to_string(),
            },
        )));

        // Act
        let validation_result = config.validate();

        // Assert
        assert!(validation_result.is_err());
    }

    #[tokio::test]
    async fn should_return_error_given_cloud_open_failure_when_called_inside_async_runtime() {
        // Arrange
        let tempdir = TempDir::new().expect("tempdir");
        let config = BootConfig::default().with_storage_mode(StorageMode::CloudBacked(Box::new(
            CloudStorageConfig {
                provider_name: "s3-compatible".to_string(),
                provider_config: cntryl_midge::CloudProviderConfig::s3_compatible_static(
                    "fitz-runtime-drop-test",
                    "http://127.0.0.1:1",
                    "test-access-key",
                    "test-secret-key",
                ),
                prefix: Some(format!("tests/{}/", uuid::Uuid::new_v4())),
                local_cache_path: tempdir.path().join("cache").to_string_lossy().to_string(),
            },
        )));

        // Act
        let init_result = tokio::time::timeout(Duration::from_secs(5), init(&config))
            .await
            .expect("cloud init should not hang");

        // Assert
        match init_result {
            Ok(_) => panic!("cloud init should surface an open error"),
            Err(error) => assert!(
                error
                    .to_string()
                    .contains("Failed to open cloud-backed Midge"),
                "expected cloud open error, got: {error}"
            ),
        }
    }

    #[tokio::test]
    async fn should_recover_marker_from_peas_s3_after_cache_loss() {
        // Arrange
        let provider = cntryl_midge::CloudProviderConfig::peas_s3("fitz-peas-s3");

        // Act
        let recovered = match recover_marker_from_peas("peas-s3", provider).await {
            Ok(value) => value,
            Err(error) if should_skip_peas_test(&error) => {
                eprintln!("Skipping peas-s3 recovery test: {error}");
                return;
            }
            Err(error) => panic!("peas-s3 recovery failed: {error}"),
        };

        // Assert
        assert_eq!(recovered, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn should_recover_marker_from_peas_azure_after_cache_loss() {
        // Arrange
        let provider = cntryl_midge::CloudProviderConfig::peas_azure("fitz-peas-azure");

        // Act
        let recovered = match recover_marker_from_peas("peas-azure", provider).await {
            Ok(value) => value,
            Err(error) if should_skip_peas_test(&error) => {
                eprintln!("Skipping peas-azure recovery test: {error}");
                return;
            }
            Err(error) => panic!("peas-azure recovery failed: {error}"),
        };

        // Assert
        assert_eq!(recovered, Some(b"value".to_vec()));
    }

    #[tokio::test]
    async fn should_recover_marker_from_peas_gcs_after_cache_loss() {
        // Arrange
        let provider = cntryl_midge::CloudProviderConfig::peas_gcs("fitz-peas-gcs");

        // Act
        let recovered = match recover_marker_from_peas("peas-gcs", provider).await {
            Ok(value) => value,
            Err(error) if should_skip_peas_test(&error) => {
                eprintln!("Skipping peas-gcs recovery test: {error}");
                return;
            }
            Err(error) => panic!("peas-gcs recovery failed: {error}"),
        };

        // Assert
        assert_eq!(recovered, Some(b"value".to_vec()));
    }

    fn exercise_cloud_burst(
        engine_opts: cntryl_midge::OpenOptions,
        write_count: usize,
        value: &[u8],
        wait_time: Duration,
    ) -> cntryl_midge::RuntimeMetricsSnapshot {
        let engine = cntryl_midge::Engine::open(engine_opts).expect("open cloud-simulated engine");
        let cf = engine.get_column_family("default").expect("default cf");

        for index in 0..write_count {
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("begin write tx");
            let key = format!("cloud-cost-key-{index:04}");
            tx.put(key.into_bytes(), value.to_vec(), None)
                .expect("write burst value");
            tx.commit(WriteOptions::buffered())
                .expect("commit burst value");
        }

        std::thread::sleep(wait_time);
        engine.get_runtime_metrics().expect("runtime metrics")
    }

    async fn recover_marker_from_peas(
        provider_name: &str,
        provider_config: cntryl_midge::CloudProviderConfig,
    ) -> Result<Option<Vec<u8>>, String> {
        ensure_peas_namespace(&provider_config)
            .await
            .map_err(|error| format!("prepare Peas namespace failed: {error}"))?;

        let tempdir = TempDir::new().expect("tempdir");
        let prefix = format!("manual/{}/", uuid::Uuid::new_v4());
        let first_cache = tempdir.path().join("first-cache");
        let second_cache = tempdir.path().join("second-cache");
        let first_config = peas_boot_config(
            provider_name,
            provider_config.clone(),
            &prefix,
            &first_cache,
        );

        let store = init(&first_config)
            .await
            .map_err(|error| format!("open first cloud store: {error}"))?;
        let cf = store
            .get_column_family("tenant_default")
            .expect("tenant_default cf");
        write_marker_with_options(
            store.as_ref(),
            cf.id(),
            b"marker",
            b"value",
            WriteOptions::cloud_strict(),
        );
        store.flush_cf(&cf).expect("force cloud SST upload");
        shutdown_store(store);
        std::fs::remove_dir_all(first_cache)
            .map_err(|error| format!("delete first cloud cache: {error}"))?;

        let second_config =
            peas_boot_config(provider_name, provider_config, &prefix, &second_cache);
        let reopened = init(&second_config)
            .await
            .map_err(|error| format!("reopen cloud store: {error}"))?;
        let reopened_cf = reopened
            .get_column_family("tenant_default")
            .expect("tenant_default cf after reopen");
        Ok(read_marker(reopened.as_ref(), reopened_cf.id(), b"marker"))
    }

    fn should_skip_peas_test(error: &str) -> bool {
        let lower = error.to_ascii_lowercase();
        lower.contains("connection refused")
            || lower.contains("timed out")
            || lower.contains("dns")
            || lower.contains("signaturedoesnotmatch")
            || lower.contains("status 403")
            || lower.contains("status 500")
            || lower.contains("lease acquisition i/o error")
    }

    fn peas_boot_config(
        provider_name: &str,
        provider_config: cntryl_midge::CloudProviderConfig,
        prefix: &str,
        cache_path: &std::path::Path,
    ) -> BootConfig {
        BootConfig::default().with_storage_mode(StorageMode::CloudBacked(Box::new(
            CloudStorageConfig {
                provider_name: provider_name.to_string(),
                provider_config,
                prefix: Some(prefix.to_string()),
                local_cache_path: cache_path.to_string_lossy().to_string(),
            },
        )))
    }

    fn shutdown_store(store: Arc<cntryl_midge::Engine>) {
        let engine = Arc::try_unwrap(store).unwrap_or_else(|store| {
            panic!(
                "Midge shutdown blocked by {} leftover engine references",
                Arc::strong_count(&store)
            );
        });
        engine.shutdown().expect("shutdown Midge");
    }

    async fn ensure_peas_namespace(
        provider: &cntryl_midge::CloudProviderConfig,
    ) -> Result<(), String> {
        match provider {
            cntryl_midge::CloudProviderConfig::AwsS3 { .. } => Ok(()),
            cntryl_midge::CloudProviderConfig::S3Compatible { bucket, .. }
            | cntryl_midge::CloudProviderConfig::Minio { bucket, .. }
            | cntryl_midge::CloudProviderConfig::Wasabi { bucket, .. }
            | cntryl_midge::CloudProviderConfig::OciS3Compatible { bucket, .. } => {
                ensure_peas_s3_bucket(bucket).await
            }
            cntryl_midge::CloudProviderConfig::Gcs { bucket, .. } => {
                ensure_peas_gcs_bucket(bucket).await
            }
            cntryl_midge::CloudProviderConfig::AzureBlob { container, .. } => {
                ensure_peas_azure_container(container).await
            }
        }
    }

    async fn ensure_peas_s3_bucket(bucket: &str) -> Result<(), String> {
        signed_s3_request("PUT", &format!("/{bucket}"), b"")
            .await
            .map(|_| ())
    }

    async fn signed_s3_request(method: &str, path: &str, body: &[u8]) -> Result<Vec<u8>, String> {
        use sha2::{Digest, Sha256};

        let host = "127.0.0.1:9000";
        let region = "us-east-1";
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let payload_hash = hex::encode(Sha256::digest(body));
        let mut headers = [
            ("host".to_string(), host.to_string()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        headers.sort_by(|left, right| left.0.cmp(&right.0));
        let canonical_headers = headers
            .iter()
            .map(|(name, value)| format!("{}:{}\n", name, value))
            .collect::<String>();
        let signed_headers = headers
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_request = format!(
            "{}\n{}\n\n{}\n{}\n{}",
            method, path, canonical_headers, signed_headers, payload_hash
        );
        let scope = format!("{date}/{region}/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            scope,
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );
        let k_date = hmac_sha256(format!("AWS{}", peas_secret_key()).as_bytes(), &date);
        let k_region = hmac_sha256(&k_date, region);
        let k_service = hmac_sha256(&k_region, "s3");
        let k_signing = hmac_sha256(&k_service, "aws4_request");
        let signature = hex::encode(hmac_sha256(&k_signing, &string_to_sign));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            peas_access_key(),
            scope,
            signed_headers,
            signature
        );
        let response = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| error.to_string())?
            .request(
                reqwest::Method::from_bytes(method.as_bytes())
                    .map_err(|error| error.to_string())?,
                format!("{}{}", peas_endpoint(), path),
            )
            .header("host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", authorization)
            .body(body.to_vec())
            .send()
            .await
            .map_err(|error| error.to_string())?;
        cloud_setup_response("S3", method, path, response).await
    }

    fn hmac_sha256(key: &[u8], data: &str) -> Vec<u8> {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;

        let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
        mac.update(data.as_bytes());
        mac.finalize().into_bytes().to_vec()
    }

    async fn ensure_peas_gcs_bucket(bucket: &str) -> Result<(), String> {
        signed_gcs_request("PUT", &format!("/{bucket}"), "", b"")
            .await
            .map(|_| ())
    }

    async fn signed_gcs_request(
        method: &str,
        path: &str,
        content_type: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, String> {
        use hmac::{Hmac, KeyInit, Mac};
        use sha1::Sha1;

        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let string_to_sign = format!("{method}\n\n{content_type}\n{date}\n{path}");
        let mut mac = Hmac::<Sha1>::new_from_slice(peas_secret_key().as_bytes())
            .map_err(|error| error.to_string())?;
        mac.update(string_to_sign.as_bytes());
        let signature = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            mac.finalize().into_bytes(),
        );
        let mut request = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| error.to_string())?
            .request(
                reqwest::Method::from_bytes(method.as_bytes())
                    .map_err(|error| error.to_string())?,
                format!("{}{}", peas_endpoint(), path),
            )
            .header("date", date)
            .header(
                "authorization",
                format!("GOOG1 {}:{signature}", peas_access_key()),
            )
            .body(body.to_vec());
        if !content_type.is_empty() {
            request = request.header("content-type", content_type);
        }
        let response = request.send().await.map_err(|error| error.to_string())?;
        cloud_setup_response("GCS", method, path, response).await
    }

    async fn ensure_peas_azure_container(container: &str) -> Result<(), String> {
        signed_azure_request(
            "PUT",
            &format!("/{}/{container}", peas_access_key()),
            "restype=container",
            b"",
            vec![],
        )
        .await
        .map(|_| ())
    }

    async fn signed_azure_request(
        method: &str,
        path: &str,
        query: &str,
        body: &[u8],
        extra_headers: Vec<(&str, &str)>,
    ) -> Result<Vec<u8>, String> {
        use hmac::{Hmac, KeyInit, Mac};
        use sha2::Sha256;

        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        let mut headers = vec![
            ("x-ms-date".to_string(), date),
            ("x-ms-version".to_string(), "2024-11-04".to_string()),
        ];
        headers.extend(
            extra_headers
                .into_iter()
                .map(|(name, value)| (name.to_string(), value.to_string())),
        );
        let header_value = |name: &str| -> String {
            headers
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        let content_length = if matches!(method, "GET" | "HEAD") || body.is_empty() {
            String::new()
        } else {
            body.len().to_string()
        };
        let mut x_ms = headers
            .iter()
            .filter(|(name, _)| name.to_ascii_lowercase().starts_with("x-ms-"))
            .map(|(name, value)| {
                (
                    name.to_ascii_lowercase(),
                    value.split_whitespace().collect::<Vec<_>>().join(" "),
                )
            })
            .collect::<Vec<_>>();
        x_ms.sort_by(|left, right| left.0.cmp(&right.0));
        let canonical_headers = x_ms
            .into_iter()
            .map(|(name, value)| format!("{name}:{value}\n"))
            .collect::<String>();
        let mut canonical_resource = format!("/{}{}", peas_access_key(), path);
        if !query.is_empty() {
            let mut query_pairs = query
                .split('&')
                .map(|pair| {
                    let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                    (key.to_ascii_lowercase(), value.to_string())
                })
                .collect::<Vec<_>>();
            query_pairs.sort();
            for (key, value) in query_pairs {
                canonical_resource.push_str(&format!("\n{key}:{value}"));
            }
        }
        let string_to_sign = [
            method.to_string(),
            header_value("Content-Encoding"),
            header_value("Content-Language"),
            content_length,
            header_value("Content-MD5"),
            header_value("Content-Type"),
            String::new(),
            header_value("If-Modified-Since"),
            header_value("If-Match"),
            header_value("If-None-Match"),
            header_value("If-Unmodified-Since"),
            header_value("Range"),
            canonical_headers,
            canonical_resource,
        ]
        .join("\n");
        let key = base64::Engine::decode(
            &base64::engine::general_purpose::STANDARD,
            peas_secret_key(),
        )
        .unwrap_or_else(|_| peas_secret_key().as_bytes().to_vec());
        let mut mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|error| error.to_string())?;
        mac.update(string_to_sign.as_bytes());
        let signature = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            mac.finalize().into_bytes(),
        );
        let url = if query.is_empty() {
            format!("{}{}", peas_endpoint(), path)
        } else {
            format!("{}{}?{query}", peas_endpoint(), path)
        };
        let mut request = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|error| error.to_string())?
            .request(
                reqwest::Method::from_bytes(method.as_bytes())
                    .map_err(|error| error.to_string())?,
                url,
            )
            .header(
                "authorization",
                format!("SharedKey {}:{signature}", peas_access_key()),
            )
            .body(body.to_vec());
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = request.send().await.map_err(|error| error.to_string())?;
        cloud_setup_response("Azure", method, path, response).await
    }

    async fn cloud_setup_response(
        provider: &str,
        method: &str,
        path: &str,
        response: reqwest::Response,
    ) -> Result<Vec<u8>, String> {
        let status = response.status();
        let response_body = response
            .bytes()
            .await
            .map_err(|error| error.to_string())?
            .to_vec();
        if status.is_success() || status.as_u16() == 409 || status.as_u16() == 500 {
            Ok(response_body)
        } else {
            Err(format!(
                "{} setup request {} {} failed with status {}: {}",
                provider,
                method,
                path,
                status,
                String::from_utf8_lossy(&response_body)
            ))
        }
    }

    fn peas_endpoint() -> &'static str {
        "http://127.0.0.1:9000"
    }

    fn peas_access_key() -> &'static str {
        "admin"
    }

    fn peas_secret_key() -> &'static str {
        "easy-peasy"
    }
}
