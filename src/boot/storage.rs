//! Storage initialization

use super::shutdown::ShutdownSignal;
use crate::boot::runtime::{BootConfig, BootResult, CloudStorageConfig, StorageMode};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tracing::{debug, info, warn};

mod backoff;
mod contention;
const STORAGE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const STORAGE_SHUTDOWN_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);
const STORAGE_SHUTDOWN_RETRY_DELAY: Duration = Duration::from_millis(50);
const STORAGE_REFERENCE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const STORAGE_REFERENCE_RETRY_DELAY: Duration = Duration::from_millis(25);

pub(crate) enum StorageInitOutcome {
    Ready(Arc<cntryl_midge::Engine>),
    ShutdownRequested,
}

enum OpenAttempt {
    Ready(cntryl_midge::Engine),
    OpenFailed(cntryl_midge::MidgeError),
    ProvisionFailed {
        engine: cntryl_midge::Engine,
        error: String,
    },
}

/// Initialize Midge storage engine based on configured storage mode.
///
/// # Errors
///
/// Returns an error when the selected storage backend cannot be opened, when
/// storage directories cannot be created, or when required route-family column
/// families cannot be ensured.
pub async fn init(config: &BootConfig) -> BootResult<Arc<cntryl_midge::Engine>> {
    let (_shutdown_guard, shutdown) = watch::channel(None);
    match init_with_shutdown(config, shutdown).await? {
        StorageInitOutcome::Ready(store) => Ok(store),
        StorageInitOutcome::ShutdownRequested => {
            Err("storage initialization stopped unexpectedly".into())
        }
    }
}

pub(crate) async fn init_with_shutdown(
    config: &BootConfig,
    mut shutdown: watch::Receiver<Option<ShutdownSignal>>,
) -> BootResult<StorageInitOutcome> {
    if shutdown_requested(&shutdown) {
        return Ok(StorageInitOutcome::ShutdownRequested);
    }

    match &config.storage_mode {
        StorageMode::Memory => init_memory(config, &shutdown).await,
        StorageMode::LocalDisk { db_path } => init_local_disk(config, db_path, &mut shutdown).await,
        StorageMode::CloudBacked(cloud) => init_cloud(config, cloud, &mut shutdown).await,
        StorageMode::Invalid { reason } => Err(reason.clone().into()),
    }
}

/// Ensure required column families exist.
fn ensure_column_families(engine: &cntryl_midge::Engine, route_families: &[u32]) -> BootResult<()> {
    for family in route_families {
        ensure_route_family(engine, crate::runtime::routing::RouteFamily::new(*family))?;
    }
    Ok(())
}

/// Ensure the storage column family aligned with a `RouteFamily` exists.
///
/// # Errors
///
/// Returns an error when Midge cannot create the aligned column family or when
/// the created column-family id does not match the expected route family.
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
async fn init_memory(
    config: &BootConfig,
    shutdown: &watch::Receiver<Option<ShutdownSignal>>,
) -> BootResult<StorageInitOutcome> {
    info!("Initializing in-memory storage (ephemeral, no persistence)");

    let open_options = build_midge_open_options(cntryl_midge::OpenOptions::in_memory(), config)?;
    let attempt = open_and_provision(open_options, config.route_families.clone()).await?;
    if shutdown_requested(shutdown) {
        shutdown_cancelled_open_attempt(attempt).await?;
        return Ok(StorageInitOutcome::ShutdownRequested);
    }
    let store = finish_open_attempt(attempt, "Failed to open in-memory Midge").await?;

    info!("In-memory storage ready (data lost on shutdown)");
    Ok(StorageInitOutcome::Ready(Arc::new(store)))
}

/// Initialize local disk storage.
async fn init_local_disk(
    config: &BootConfig,
    db_path: &str,
    shutdown: &mut watch::Receiver<Option<ShutdownSignal>>,
) -> BootResult<StorageInitOutcome> {
    info!("Initializing local disk storage at {}", db_path);

    tokio::fs::create_dir_all(db_path)
        .await
        .map_err(|e| format!("Failed to create storage directory {db_path}: {e}"))?;

    if shutdown_requested(shutdown) {
        return Ok(StorageInitOutcome::ShutdownRequested);
    }
    let Some(store) = open_local_disk_with_retry(config, db_path, shutdown).await? else {
        return Ok(StorageInitOutcome::ShutdownRequested);
    };

    info!("Local disk storage ready at {}", db_path);
    Ok(StorageInitOutcome::Ready(Arc::new(store)))
}

async fn open_local_disk_with_retry(
    config: &BootConfig,
    db_path: &str,
    shutdown: &mut watch::Receiver<Option<ShutdownSignal>>,
) -> BootResult<Option<cntryl_midge::Engine>> {
    let open_options = build_midge_open_options(cntryl_midge::OpenOptions::local(db_path), config)?;
    open_with_retry(
        open_options,
        config.route_families.clone(),
        shutdown,
        contention::is_retryable,
        |attempt, elapsed, delay, error| {
            log_local_lease_contention(db_path, attempt, elapsed, delay, error);
        },
        |error| format!("Failed to open Midge at {db_path}: {error}"),
    )
    .await
}

async fn open_with_retry<P, L, C>(
    open_options: cntryl_midge::OpenOptions,
    route_families: Vec<u32>,
    shutdown: &mut watch::Receiver<Option<ShutdownSignal>>,
    should_retry: P,
    log_contention: L,
    error_context: C,
) -> BootResult<Option<cntryl_midge::Engine>>
where
    P: Fn(&cntryl_midge::MidgeError) -> bool,
    L: Fn(u32, Duration, Duration, &cntryl_midge::MidgeError),
    C: Fn(&cntryl_midge::MidgeError) -> String,
{
    let retry_started_at = Instant::now();
    let mut retry_attempt = 0;

    loop {
        if shutdown_requested(shutdown) {
            return Ok(None);
        }
        let attempt = open_and_provision(open_options.clone(), route_families.clone()).await?;
        if shutdown_requested(shutdown) {
            shutdown_cancelled_open_attempt(attempt).await?;
            return Ok(None);
        }
        match attempt {
            OpenAttempt::Ready(store) => return Ok(Some(store)),
            OpenAttempt::OpenFailed(error) if should_retry(&error) => {
                let delay = backoff::retry_delay(retry_attempt);
                retry_attempt = retry_attempt.saturating_add(1);
                log_contention(retry_attempt, retry_started_at.elapsed(), delay, &error);
                if wait_for_retry_or_shutdown(delay, shutdown).await? {
                    return Ok(None);
                }
            }
            OpenAttempt::OpenFailed(error) => return Err(error_context(&error).into()),
            OpenAttempt::ProvisionFailed { engine, error } => {
                return Err(shutdown_after_provision_failure(engine, error).await.into());
            }
        }
    }
}

async fn open_and_provision(
    open_options: cntryl_midge::OpenOptions,
    route_families: Vec<u32>,
) -> BootResult<OpenAttempt> {
    tokio::task::spawn_blocking(move || match cntryl_midge::Engine::open(open_options) {
        Ok(engine) => match ensure_column_families(&engine, &route_families) {
            Ok(()) => OpenAttempt::Ready(engine),
            Err(error) => OpenAttempt::ProvisionFailed {
                engine,
                error: error.to_string(),
            },
        },
        Err(error) => OpenAttempt::OpenFailed(error),
    })
    .await
    .map_err(|error| format!("Midge open task failed: {error}").into())
}

async fn finish_open_attempt(
    attempt: OpenAttempt,
    error_context: &str,
) -> BootResult<cntryl_midge::Engine> {
    match attempt {
        OpenAttempt::Ready(engine) => Ok(engine),
        OpenAttempt::OpenFailed(error) => Err(format!("{error_context}: {error}").into()),
        OpenAttempt::ProvisionFailed { engine, error } => {
            Err(shutdown_after_provision_failure(engine, error).await.into())
        }
    }
}

async fn shutdown_after_provision_failure(engine: cntryl_midge::Engine, error: String) -> String {
    match shutdown_owned_engine(engine).await {
        Ok(()) => error,
        Err(cleanup_error) => {
            format!("{error}; Midge cleanup after provisioning failure failed: {cleanup_error}")
        }
    }
}

async fn shutdown_cancelled_open_attempt(attempt: OpenAttempt) -> BootResult<()> {
    match attempt {
        OpenAttempt::Ready(engine) => shutdown_owned_engine(engine).await,
        OpenAttempt::OpenFailed(_) => Ok(()),
        OpenAttempt::ProvisionFailed { engine, error } => {
            warn!(%error, "Midge provisioning failed while shutdown was requested");
            shutdown_owned_engine(engine).await
        }
    }
}

async fn shutdown_owned_engine(mut engine: cntryl_midge::Engine) -> BootResult<()> {
    tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        loop {
            let remaining = STORAGE_SHUTDOWN_TIMEOUT.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(cntryl_midge::MidgeError::Timeout(
                    "Fitz storage shutdown deadline elapsed".to_string(),
                ));
            }
            let attempt_timeout = remaining.min(STORAGE_SHUTDOWN_ATTEMPT_TIMEOUT);
            match engine.shutdown(attempt_timeout) {
                Ok(()) => return Ok(()),
                Err(
                    error @ (cntryl_midge::MidgeError::Busy(_)
                    | cntryl_midge::MidgeError::Timeout(_)),
                ) if started.elapsed() < STORAGE_SHUTDOWN_TIMEOUT => {
                    warn!(
                        elapsed_ms = duration_millis_u64(started.elapsed()),
                        remaining_ms = duration_millis_u64(
                            STORAGE_SHUTDOWN_TIMEOUT.saturating_sub(started.elapsed())
                        ),
                        error = %error,
                        "Midge shutdown is still draining; retrying"
                    );
                    std::thread::sleep(STORAGE_SHUTDOWN_RETRY_DELAY);
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .map_err(|error| format!("Midge shutdown task failed: {error}"))?
    .map_err(|error| format!("Midge shutdown failed: {error}").into())
}

pub(crate) async fn shutdown_store(mut store: Arc<cntryl_midge::Engine>) -> BootResult<()> {
    let started = Instant::now();
    loop {
        match Arc::try_unwrap(store) {
            Ok(engine) => return shutdown_owned_engine(engine).await,
            Err(shared) if started.elapsed() < STORAGE_REFERENCE_DRAIN_TIMEOUT => {
                // Listener and domain tasks may briefly retain upgraded weak
                // references while their shutdown joins complete.
                store = shared;
                tokio::time::sleep(STORAGE_REFERENCE_RETRY_DELAY).await;
            }
            Err(shared) => {
                let lingering_references = Arc::strong_count(&shared).saturating_sub(1);
                return Err(format!(
                    "Midge shutdown blocked by {lingering_references} lingering engine references after waiting {}ms",
                    duration_millis_u64(STORAGE_REFERENCE_DRAIN_TIMEOUT)
                )
                .into());
            }
        }
    }
}

fn shutdown_requested(shutdown: &watch::Receiver<Option<ShutdownSignal>>) -> bool {
    shutdown.borrow().is_some()
}

async fn wait_for_retry_or_shutdown(
    delay: Duration,
    shutdown: &mut watch::Receiver<Option<ShutdownSignal>>,
) -> BootResult<bool> {
    if shutdown_requested(shutdown) {
        return Ok(true);
    }

    tokio::select! {
        biased;
        changed = shutdown.changed() => {
            changed.map_err(|_| "shutdown coordinator stopped during storage retry")?;
            Ok(shutdown_requested(shutdown))
        }
        () = tokio::time::sleep(delay) => Ok(false),
    }
}

fn should_warn_about_contention(attempt: u32) -> bool {
    attempt == 1 || attempt.is_multiple_of(12)
}

fn log_local_lease_contention(
    db_path: &str,
    attempt: u32,
    elapsed: Duration,
    delay: Duration,
    error: &cntryl_midge::MidgeError,
) {
    if should_warn_about_contention(attempt) {
        warn!(
            db_path,
            retry_attempt = attempt,
            elapsed_ms = duration_millis_u64(elapsed),
            delay_ms = duration_millis_u64(delay),
            error = %error,
            "Local disk storage is waiting for the active Midge writer lease"
        );
    } else {
        debug!(
            db_path,
            retry_attempt = attempt,
            elapsed_ms = duration_millis_u64(elapsed),
            delay_ms = duration_millis_u64(delay),
            error = %error,
            "Local disk storage writer lease remains held"
        );
    }
}

fn log_cloud_lease_contention(
    cloud: &CloudStorageConfig,
    attempt: u32,
    elapsed: Duration,
    delay: Duration,
    error: &cntryl_midge::MidgeError,
) {
    if should_warn_about_contention(attempt) {
        warn!(
            provider = %cloud.provider_name,
            namespace = %cloud.provider_config.bucket_or_container(),
            prefix = ?cloud.prefix,
            retry_attempt = attempt,
            elapsed_ms = duration_millis_u64(elapsed),
            delay_ms = duration_millis_u64(delay),
            error = %error,
            "Cloud-backed storage is waiting for the active Midge writer lease"
        );
    } else {
        debug!(
            provider = %cloud.provider_name,
            namespace = %cloud.provider_config.bucket_or_container(),
            prefix = ?cloud.prefix,
            retry_attempt = attempt,
            elapsed_ms = duration_millis_u64(elapsed),
            delay_ms = duration_millis_u64(delay),
            error = %error,
            "Cloud-backed storage writer lease remains held"
        );
    }
}

fn build_midge_open_options(
    open_options: cntryl_midge::OpenOptionsBuilder,
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

    open_options
        .build()
        .map_err(|error| format!("Invalid Midge open options: {error}").into())
}

/// Initialize cloud-backed storage.
async fn init_cloud(
    config: &BootConfig,
    cloud: &CloudStorageConfig,
    shutdown: &mut watch::Receiver<Option<ShutdownSignal>>,
) -> BootResult<StorageInitOutcome> {
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

    if shutdown_requested(shutdown) {
        return Ok(StorageInitOutcome::ShutdownRequested);
    }

    let open_options = build_midge_open_options(
        cntryl_midge::OpenOptions::cloud(
            cloud.local_cache_path.clone(),
            cntryl_midge::CloudStorageLocation::new(
                cloud.provider_config.clone(),
                cloud.prefix.clone().unwrap_or_default(),
            ),
        ),
        config,
    )?;
    let Some(store) =
        open_cloud_with_retry(open_options, config.route_families.clone(), cloud, shutdown).await?
    else {
        return Ok(StorageInitOutcome::ShutdownRequested);
    };

    info!(
        "Cloud storage ready: {} namespace={} prefix={:?} cache={}",
        cloud.provider_name,
        cloud.provider_config.bucket_or_container(),
        cloud.prefix,
        cloud.local_cache_path
    );
    Ok(StorageInitOutcome::Ready(Arc::new(store)))
}

async fn open_cloud_with_retry(
    open_options: cntryl_midge::OpenOptions,
    route_families: Vec<u32>,
    cloud: &CloudStorageConfig,
    shutdown: &mut watch::Receiver<Option<ShutdownSignal>>,
) -> BootResult<Option<cntryl_midge::Engine>> {
    open_with_retry(
        open_options,
        route_families,
        shutdown,
        contention::is_retryable,
        |attempt, elapsed, delay, error| {
            log_cloud_lease_contention(cloud, attempt, elapsed, delay, error);
        },
        |error| format!("Failed to open cloud-backed Midge: {error}"),
    )
    .await
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;
