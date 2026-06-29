//! Storage initialization

use crate::boot::runtime::{BootConfig, BootResult, CloudStorageConfig, StorageMode};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

const STORAGE_OPEN_RETRY_BUDGET: Duration = Duration::from_secs(60);
const STORAGE_OPEN_BASE_BACKOFF: Duration = Duration::from_millis(250);
const STORAGE_OPEN_MAX_BACKOFF: Duration = Duration::from_secs(5);
const STORAGE_OPEN_MAX_JITTER_MS: u64 = 250;

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
    let retry_started_at = Instant::now();
    let mut retry_attempt = 0;

    loop {
        match cntryl_midge::Engine::open(open_options.clone()) {
            Ok(store) => return Ok(store),
            Err(error) if should_retry_storage_open(&error) => {
                let Some(delay) = storage_open_retry_delay(retry_started_at, retry_attempt) else {
                    return Err(format!("Failed to open Midge at {}: {}", db_path, error).into());
                };
                warn!(
                    db_path = db_path,
                    retry_attempt = retry_attempt + 1,
                    retry_budget_ms = STORAGE_OPEN_RETRY_BUDGET.as_millis() as u64,
                    elapsed_ms = retry_started_at.elapsed().as_millis() as u64,
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

fn should_retry_storage_open(error: &cntryl_midge::MidgeError) -> bool {
    matches!(
        error,
        cntryl_midge::MidgeError::Internal(message)
            if message.contains("another Midge instance is already running against this storage")
    )
}

fn storage_open_retry_delay(started_at: Instant, attempt: u32) -> Option<Duration> {
    let remaining_budget = STORAGE_OPEN_RETRY_BUDGET.checked_sub(started_at.elapsed())?;
    let multiplier = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX);
    let base_delay_ms = (STORAGE_OPEN_BASE_BACKOFF.as_millis() as u64)
        .saturating_mul(multiplier)
        .min(STORAGE_OPEN_MAX_BACKOFF.as_millis() as u64);
    let jitter_ms = storage_open_retry_jitter_ms(attempt);
    let delay = Duration::from_millis(base_delay_ms.saturating_add(jitter_ms));

    Some(delay.min(remaining_budget))
}

fn storage_open_retry_jitter_ms(attempt: u32) -> u64 {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let salt = now_nanos ^ (attempt as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    salt % (STORAGE_OPEN_MAX_JITTER_MS + 1)
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
    let store = open_cloud_with_retry(open_options, cloud).await?;

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

async fn open_cloud_with_retry(
    open_options: cntryl_midge::OpenOptions,
    cloud: &CloudStorageConfig,
) -> BootResult<cntryl_midge::Engine> {
    let retry_started_at = Instant::now();
    let mut retry_attempt = 0;

    loop {
        let cloud_open_options = open_options.clone();

        // Cloud engine bootstrap may create and drop an internal Tokio runtime.
        // Run it on a blocking thread to avoid dropping that runtime inside async context.
        match tokio::task::spawn_blocking(move || cntryl_midge::Engine::open(cloud_open_options))
            .await
        {
            Ok(Ok(store)) => return Ok(store),
            Ok(Err(error)) if should_retry_storage_open(&error) => {
                let Some(delay) = storage_open_retry_delay(retry_started_at, retry_attempt) else {
                    return Err(format!("Failed to open cloud-backed Midge: {}", error).into());
                };
                warn!(
                    provider = %cloud.provider_name,
                    namespace = %cloud.provider_config.bucket_or_container(),
                    prefix = ?cloud.prefix,
                    retry_attempt = retry_attempt + 1,
                    retry_budget_ms = STORAGE_OPEN_RETRY_BUDGET.as_millis() as u64,
                    elapsed_ms = retry_started_at.elapsed().as_millis() as u64,
                    delay_ms = delay.as_millis() as u64,
                    error = %error,
                    "Cloud-backed storage open hit an active writer lease; retrying with exponential backoff"
                );
                retry_attempt += 1;
                tokio::time::sleep(delay).await;
            }
            Ok(Err(error)) => {
                return Err(format!("Failed to open cloud-backed Midge: {}", error).into())
            }
            Err(error) => {
                return Err(format!("Cloud-backed Midge open task failed: {}", error).into())
            }
        }
    }
}

#[cfg(test)]
mod tests;
