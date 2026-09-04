use super::BootResult;
use crate::domains::stream::StreamStorageLayout;
use std::path::Path;
use std::time::Duration;

const DEFAULT_LOCAL_STORAGE_PATH: &str = "./.fitz";
const DEFAULT_CLOUD_CACHE_PATH: &str = "./.fitz-cloud-cache";
const DEFAULT_SQRZL_EMULATOR_ENDPOINT: &str = "http://127.0.0.1:9000";
const DEFAULT_SQRZL_EMULATOR_ACCESS_KEY: &str = "admin";
const DEFAULT_SQRZL_EMULATOR_SECRET_KEY: &str = "sqrzl-secret";
const DEFAULT_SQRZL_EMULATOR_BUCKET: &str = "fitz";
const DEFAULT_METRICS_BIND_ADDR: &str = "127.0.0.1";
const DEFAULT_METRICS_PORT: u16 = 9090;
const ENV_STORAGE_MEMTABLE_BYTES: &str = "FITZ_STORAGE_MEMTABLE_BYTES";
const ENV_STORAGE_LEASE_TTL_SECS: &str = "FITZ_STORAGE_LEASE_TTL_SECS";
const ENV_QUEUE_WRITE_POLICY: &str = "FITZ_QUEUE_WRITE_POLICY";
const ENV_QUEUE_LOSS_WINDOW_MS: &str = "FITZ_QUEUE_LOSS_WINDOW_MS";
const ENV_KV_IDLE_TRANSACTION_TTL_SECS: &str = "FITZ_KV_IDLE_TRANSACTION_TTL_SECS";
const ENV_SCHEDULE_PRELOAD_TIMEOUT_SECS: &str = "FITZ_SCHEDULE_PRELOAD_TIMEOUT_SECS";
const ENV_DRAIN_GRACE_SECONDS: &str = "FITZ_DRAIN_GRACE_SECONDS";
const ENV_DRAIN_CLOSE_REASON: &str = "FITZ_DRAIN_CLOSE_REASON";
const DEFAULT_QUEUE_LOSS_WINDOW_MS: u64 = 100;
const DEFAULT_STORAGE_LEASE_TTL_SECS: u64 = 30;
const MIN_STORAGE_LEASE_TTL_SECS: u64 = 30;
const DEFAULT_KV_IDLE_TRANSACTION_TTL_SECS: u64 = 300;
const DEFAULT_DRAIN_GRACE_SECONDS: u64 = 25;
const DEFAULT_DRAIN_CLOSE_REASON: &str = "broker draining for redeploy";
const DEFAULT_LOCAL_WS_ALLOWED_ORIGIN_VALUES: [&str; 4] = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:4090",
    "http://127.0.0.1:4090",
];

/// Cloud commit durability policy for broker-selected durable writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloudDurabilityMode {
    /// Local WAL visibility with background provider upload.
    Background,
    /// Wait for provider acknowledgement when a cloud-backed sync write is requested.
    Strict,
    /// Invalid durability configuration captured for later validation.
    Invalid { reason: String },
}

impl CloudDurabilityMode {
    fn from_env() -> Self {
        match env_non_empty("FITZ_STORAGE_CLOUD_DURABILITY")
            .unwrap_or_else(|| "background".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "background" => Self::Background,
            "strict" => Self::Strict,
            other => Self::Invalid {
                reason: format!(
                    "unsupported FITZ_STORAGE_CLOUD_DURABILITY='{other}'; expected background or strict"
                ),
            },
        }
    }
}

/// Queue commit policy for durable queue mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueWritePolicy {
    /// Skip WAL on the enqueue path and flush dirty queue column families in the background.
    Fast,
    /// Use buffered WAL writes without forcing fsync per operation.
    Buffered,
    /// Wait for local sync or cloud provider acknowledgement before accepting a mutation.
    Strict,
    /// Invalid queue write policy captured for later validation.
    Invalid { reason: String },
}

impl QueueWritePolicy {
    fn from_env_with_source() -> (Self, QueueWritePolicySource) {
        let raw_value = env_non_empty(ENV_QUEUE_WRITE_POLICY);
        let source = if raw_value.is_none() {
            QueueWritePolicySource::Defaulted
        } else {
            QueueWritePolicySource::Explicit
        };
        let value = raw_value.unwrap_or_else(|| "fast".to_string());

        let policy = match value.to_ascii_lowercase().as_str() {
            "fast" => Self::Fast,
            "buffered" => Self::Buffered,
            "strict" => Self::Strict,
            other => Self::Invalid {
                reason: format!(
                    "unsupported {ENV_QUEUE_WRITE_POLICY}='{other}'; expected fast, buffered, or strict"
                ),
            },
        };

        (policy, source)
    }

    fn validate(&self) -> Result<(), String> {
        match self {
            Self::Fast | Self::Buffered | Self::Strict => Ok(()),
            Self::Invalid { reason } => Err(reason.clone()),
        }
    }
}

pub(crate) fn warn_defaulted_fast_queue_policy(config: &BootConfig) {
    if !config.queue_write_policy_defaulted_fast() {
        return;
    }

    // Fast mode accepts a bounded loss window. An implicit choice deserves an
    // operator warning because it is materially different from durable writes.
    tracing::warn!(
        queue_write_policy_env = "FITZ_QUEUE_WRITE_POLICY",
        queue_loss_window_env = "FITZ_QUEUE_LOSS_WINDOW_MS",
        loss_window_ms = config.queue_loss_window_ms,
        loss_window = ?config.queue_fast_flush_interval(),
        "FITZ_QUEUE_WRITE_POLICY is unset; defaulting Queue to fast best-effort writes"
    );
}

/// Source for the resolved queue write policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueWritePolicySource {
    /// The operator set `FITZ_QUEUE_WRITE_POLICY`.
    Explicit,
    /// `FITZ_QUEUE_WRITE_POLICY` was absent and the default policy was used.
    Defaulted,
}

impl QueueWritePolicySource {
    #[must_use]
    pub fn is_defaulted(self) -> bool {
        matches!(self, Self::Defaulted)
    }
}

/// Optional storage memtable tuning for the embedded Midge engine.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StorageMemtableConfig {
    /// Use Midge's storage-mode default.
    #[default]
    Auto,
    /// Requested runtime memtable size in bytes.
    Bytes(usize),
    /// Invalid environment configuration captured for later validation.
    Invalid { reason: String },
}

impl StorageMemtableConfig {
    fn from_env() -> Self {
        let Some(value) = env_non_empty(ENV_STORAGE_MEMTABLE_BYTES) else {
            return Self::Auto;
        };

        match value.parse::<usize>() {
            Ok(0) => Self::Invalid {
                reason: format!("{ENV_STORAGE_MEMTABLE_BYTES} must be greater than 0"),
            },
            Ok(bytes) => Self::Bytes(bytes),
            Err(_) => Self::Invalid {
                reason: format!(
                    "{ENV_STORAGE_MEMTABLE_BYTES} must be an unsigned integer byte count"
                ),
            },
        }
    }

    fn validate(&self) -> Result<(), String> {
        match self {
            Self::Bytes(0) => Err(format!(
                "{ENV_STORAGE_MEMTABLE_BYTES} must be greater than 0"
            )),
            Self::Auto | Self::Bytes(_) => Ok(()),
            Self::Invalid { reason } => Err(reason.clone()),
        }
    }

    #[must_use]
    pub fn bytes(&self) -> Option<usize> {
        match self {
            Self::Bytes(bytes) => Some(*bytes),
            Self::Auto | Self::Invalid { .. } => None,
        }
    }
}

/// Parsed cloud provider configuration for Midge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudStorageConfig {
    /// Fitz provider label from `FITZ_STORAGE_PROVIDER`.
    pub provider_name: String,
    /// Provider configuration consumed by Midge.
    pub provider_config: cntryl_midge::CloudProviderConfig,
    /// Optional object prefix inside the bucket/container.
    pub prefix: Option<String>,
    /// Local cache path used by the cloud-backed engine.
    pub local_cache_path: String,
}

/// Storage backend configuration.
#[derive(Debug, Clone)]
pub enum StorageMode {
    /// In-memory only (no persistence)
    Memory,
    /// Local file-backed storage
    LocalDisk {
        /// Path to database directory
        db_path: String,
    },
    /// Cloud-backed storage using a typed Midge provider configuration.
    CloudBacked(Box<CloudStorageConfig>),
    /// Invalid storage configuration captured for later validation.
    Invalid { reason: String },
}

impl Default for StorageMode {
    fn default() -> Self {
        Self::LocalDisk {
            db_path: DEFAULT_LOCAL_STORAGE_PATH.to_string(),
        }
    }
}

impl StorageMode {
    /// Detect storage mode from environment variables.
    pub fn from_env() -> Self {
        let mode = std::env::var("FITZ_STORAGE_MODE")
            .unwrap_or_else(|_| "local".to_string())
            .to_lowercase();

        match mode.as_str() {
            "memory" => {
                tracing::info!("Storage: IN-MEMORY (ephemeral, no persistence)");
                Self::Memory
            }
            "local" => {
                let db_path = env_non_empty("FITZ_STORAGE_PATH")
                    .unwrap_or_else(|| DEFAULT_LOCAL_STORAGE_PATH.to_string());
                tracing::info!("Storage: LOCAL DISK at {}", db_path);
                Self::LocalDisk { db_path }
            }
            "cloud" => match CloudStorageConfig::from_env() {
                Ok(config) => {
                    tracing::info!(
                        provider = %config.provider_name,
                        namespace = %config.provider_config.bucket_or_container(),
                        prefix = ?config.prefix,
                        cache = %config.local_cache_path,
                        "Storage: CLOUD"
                    );
                    Self::CloudBacked(Box::new(config))
                }
                Err(error) => Self::Invalid { reason: error },
            },
            "s3" | "gcs" | "azure" => Self::Invalid {
                reason: format!(
                    "FITZ_STORAGE_MODE={mode} is no longer supported; set FITZ_STORAGE_MODE=cloud and FITZ_STORAGE_PROVIDER=..."
                ),
            },
            _ => Self::Invalid {
                reason: format!(
                    "unsupported FITZ_STORAGE_MODE='{mode}'; expected memory, local, or cloud"
                ),
            },
        }
    }

    /// Validate storage backend configuration loaded into `StorageMode`.
    ///
    /// # Errors
    ///
    /// Returns an error when local storage paths are empty or when cloud
    /// provider, namespace, or cache-path settings are missing or malformed.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            StorageMode::Memory => Ok(()),
            StorageMode::LocalDisk { db_path } => {
                if db_path.trim().is_empty() {
                    return Err("local disk storage requires a non-empty db_path".to_string());
                }
                Ok(())
            }
            StorageMode::CloudBacked(config) => {
                if config.provider_name.trim().is_empty() {
                    return Err("cloud storage requires FITZ_STORAGE_PROVIDER".to_string());
                }
                if config
                    .provider_config
                    .bucket_or_container()
                    .trim()
                    .is_empty()
                {
                    return Err("cloud storage requires a bucket/container namespace".to_string());
                }
                if config.local_cache_path.trim().is_empty()
                    || Path::new(&config.local_cache_path).as_os_str().is_empty()
                {
                    return Err("cloud storage requires a valid local cache path".to_string());
                }
                Ok(())
            }
            StorageMode::Invalid { reason } => Err(reason.clone()),
        }
    }
}

impl CloudStorageConfig {
    fn from_env() -> Result<Self, String> {
        let provider_name = required_env("FITZ_STORAGE_PROVIDER")?.to_ascii_lowercase();
        let prefix = env_non_empty("FITZ_STORAGE_PREFIX");
        let local_cache_path = env_non_empty("FITZ_STORAGE_CACHE_PATH")
            .unwrap_or_else(|| DEFAULT_CLOUD_CACHE_PATH.to_string());
        let provider_config = build_cloud_provider_config(provider_name.as_str())?;

        Ok(Self {
            provider_name,
            provider_config,
            prefix,
            local_cache_path,
        })
    }
}

mod cloud_provider;
use cloud_provider::build_cloud_provider_config;
mod env;
use env::{
    drain_close_reason_from_env, drain_grace_seconds_from_env, env_non_empty,
    kv_idle_transaction_ttl_seconds_from_env, queue_loss_window_ms_from_env, required_env,
    schedule_preload_timeout_seconds_from_env, storage_lease_ttl_seconds_from_env,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalListenerExposure {
    Direct,
    HostLoopbackEdge,
}

impl LocalListenerExposure {
    fn from_assumption(assume_local_loopback_edge: bool) -> Self {
        if assume_local_loopback_edge {
            Self::HostLoopbackEdge
        } else {
            Self::Direct
        }
    }

    pub(crate) fn is_host_loopback_edge(self) -> bool {
        self == Self::HostLoopbackEdge
    }
}

mod validation;
use validation::{
    configured_ws_allowed_origins, default_local_ws_allowed_origins,
    parse_ws_allowed_origins_from_env, validate_admin_browser_security,
    validate_ingress_security_boundary, validate_local_loopback_edge_origins,
    validate_public_origin_security,
};

struct TransportConfig<'a> {
    boot: &'a BootConfig,
}

impl<'a> TransportConfig<'a> {
    fn from_boot_config(boot: &'a BootConfig) -> Self {
        Self { boot }
    }

    fn validate(&self, protected_admin_configured: bool) -> BootResult<()> {
        let config = self.boot;
        if config.metrics_bind_addr.trim().is_empty() {
            return Err("FITZ_METRICS_BIND_ADDR must not be empty".into());
        }
        if let Some(value) = env_non_empty("FITZ_TCP_ENABLED") {
            value.parse::<bool>().map_err(|_| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "FITZ_TCP_ENABLED must be true or false",
                )) as Box<dyn std::error::Error>
            })?;
        }
        let public_bind = validate_ingress_security_boundary(
            config.assume_external_tls,
            config.local_listener_exposure,
            &config.bind_addr,
            config.auth_required || protected_admin_configured,
        )?;
        let origins = configured_ws_allowed_origins(
            &config.ws_allowed_origins,
            config.ws_allowed_origins_error.as_ref(),
        )?;
        validate_local_loopback_edge_origins(config.local_listener_exposure, &origins)?;
        validate_public_origin_security("FITZ_WS_ALLOWED_ORIGINS", &origins)?;
        if config.auth_required && public_bind && origins.is_empty() {
            return Err("FITZ_WS_ALLOWED_ORIGINS is required when authenticated WebSocket listeners bind to a non-loopback address".into());
        }
        validate_admin_browser_security(
            protected_admin_configured,
            public_bind,
            config.assume_local_loopback_edge(),
        )
    }
}

struct StorageConfig<'a> {
    boot: &'a BootConfig,
}

impl<'a> StorageConfig<'a> {
    fn from_boot_config(boot: &'a BootConfig) -> Self {
        Self { boot }
    }

    fn validate(&self) -> BootResult<()> {
        let config = self.boot;
        if let CloudDurabilityMode::Invalid { reason } = &config.cloud_durability {
            return Err(reason.clone().into());
        }
        config
            .queue_write_policy
            .validate()
            .map_err(boxed_config_error)?;
        if let Some(error) = &config.queue_loss_window_error {
            return Err(error.clone().into());
        }
        if config.queue_loss_window_ms == 0 {
            return Err(format!("{ENV_QUEUE_LOSS_WINDOW_MS} must be greater than 0").into());
        }
        if let Some(error) = &config.kv_idle_transaction_ttl_error {
            return Err(error.clone().into());
        }
        if config.kv_idle_transaction_ttl_seconds == 0 {
            return Err(
                format!("{ENV_KV_IDLE_TRANSACTION_TTL_SECS} must be greater than 0").into(),
            );
        }
        if let Some(error) = &config.schedule_preload_timeout_error {
            return Err(error.clone().into());
        }
        if config.schedule_preload_timeout_seconds == 0 {
            return Err(
                format!("{ENV_SCHEDULE_PRELOAD_TIMEOUT_SECS} must be greater than 0").into(),
            );
        }
        if let Some(error) = &config.storage_lease_ttl_error {
            return Err(error.clone().into());
        }
        if config.storage_lease_ttl_seconds < MIN_STORAGE_LEASE_TTL_SECS {
            return Err(format!(
                "{ENV_STORAGE_LEASE_TTL_SECS} must be at least {MIN_STORAGE_LEASE_TTL_SECS} seconds"
            )
            .into());
        }
        config
            .storage_memtable
            .validate()
            .map_err(boxed_config_error)?;
        config.storage_mode.validate().map_err(boxed_config_error)
    }
}

struct DrainConfig<'a> {
    boot: &'a BootConfig,
}

impl<'a> DrainConfig<'a> {
    fn from_boot_config(boot: &'a BootConfig) -> Self {
        Self { boot }
    }

    fn validate(&self) -> BootResult<()> {
        let config = self.boot;
        if let Some(error) = &config.drain_config_error {
            return Err(error.clone().into());
        }
        if config.drain_grace_seconds == 0 {
            return Err(format!("{ENV_DRAIN_GRACE_SECONDS} must be greater than 0").into());
        }
        if config.drain_close_reason.trim().is_empty() {
            return Err(format!("{ENV_DRAIN_CLOSE_REASON} must not be empty").into());
        }
        Ok(())
    }
}

fn boxed_config_error(error: String) -> Box<dyn std::error::Error> {
    error.into()
}

/// Boot configuration for the Fitz broker.
#[derive(Debug, Clone)]
pub struct BootConfig {
    /// HTTP/WebSocket port
    pub http_port: u16,
    /// TCP port
    pub tcp_port: u16,
    /// Whether the raw TCP listener is enabled.
    pub tcp_enabled: bool,
    /// Bind address (default: "0.0.0.0")
    pub bind_addr: String,
    /// Unauthenticated Prometheus listener bind address.
    pub metrics_bind_addr: String,
    /// Unauthenticated Prometheus listener port.
    pub metrics_port: u16,
    /// Storage mode (memory, local disk, or cloud)
    pub storage_mode: StorageMode,
    /// Stream storage layout selector
    pub stream_storage_layout: StreamStorageLayout,
    /// Whether authentication is required (default: true)
    pub auth_required: bool,
    /// Explicit auth configuration for token verification
    pub auth_config: crate::auth::AuthConfig,
    /// Claim normalization behavior for authenticated CONNECT JWTs.
    pub auth_claims_config: crate::auth::AuthClaimsConfig,
    /// Broker-local route-family resolver for verified identity claims.
    pub route_family_resolver: crate::auth::RouteFamilyResolverConfig,
    /// Provisioned `RouteFamily` values accepted after identity resolution.
    pub route_families: Vec<u32>,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Frame size limit in bytes
    pub max_frame_size: usize,
    /// Channel capacity between transport and runtime
    pub channel_capacity: usize,
    /// Provider-ack durability behavior for cloud-backed sync writes.
    pub cloud_durability: CloudDurabilityMode,
    /// Optional explicit Midge memtable size in bytes.
    pub storage_memtable: StorageMemtableConfig,
    /// TTL for the embedded Midge primary storage-writer lease.
    pub storage_lease_ttl_seconds: u64,
    pub(crate) storage_lease_ttl_error: Option<String>,
    /// Commit policy for queue durable mutations.
    pub queue_write_policy: QueueWritePolicy,
    /// Whether queue write policy was explicit or resolved through the default.
    pub queue_write_policy_source: QueueWritePolicySource,
    /// Target dirty-data window before best-effort queue writes are flushed.
    pub queue_loss_window_ms: u64,
    pub(crate) queue_loss_window_error: Option<String>,
    /// Maximum inactivity before an open KV transaction is force-rolled back.
    pub kv_idle_transaction_ttl_seconds: u64,
    pub(crate) kv_idle_transaction_ttl_error: Option<String>,
    /// Maximum wait for required Schedule preload during broker startup.
    pub schedule_preload_timeout_seconds: u64,
    pub(crate) schedule_preload_timeout_error: Option<String>,
    /// Whether an external TLS terminator is explicitly protecting public listeners.
    pub assume_external_tls: bool,
    pub(crate) local_listener_exposure: LocalListenerExposure,
    /// Browser origins allowed to open the WebSocket data-plane endpoint.
    pub ws_allowed_origins: Vec<crate::api::origin::ExactOrigin>,
    pub(crate) ws_allowed_origins_error: Option<String>,
    /// Grace period used after planned drain starts before active sessions are closed.
    pub drain_grace_seconds: u64,
    /// Server close reason used when a planned drain closes sessions.
    pub drain_close_reason: String,
    pub(crate) drain_config_error: Option<String>,
}

impl BootConfig {
    #[must_use]
    pub fn storage_path(&self) -> String {
        match &self.storage_mode {
            StorageMode::LocalDisk { db_path } => db_path.clone(),
            StorageMode::Memory => ":memory:".to_string(),
            StorageMode::CloudBacked(config) => config.local_cache_path.clone(),
            StorageMode::Invalid { .. } => "<invalid>".to_string(),
        }
    }

    /// Write policy for Schedule definitions, due claims, and acknowledgements.
    ///
    /// Memory storage is explicitly best-effort and non-durable. Every
    /// persistent local commit waits for local sync; cloud strict mode also
    /// waits for provider acknowledgement.
    #[must_use]
    pub fn schedule_write_options(&self) -> cntryl_midge::WriteOptions {
        match (&self.storage_mode, &self.cloud_durability) {
            (StorageMode::Memory, _) => cntryl_midge::WriteOptions::best_effort(),
            (StorageMode::CloudBacked(_), durability) => cloud_durable_write_options(durability),
            _ => cntryl_midge::WriteOptions::sync(),
        }
    }

    #[must_use]
    pub fn request_sync_write_options(&self) -> cntryl_midge::WriteOptions {
        match (&self.storage_mode, &self.cloud_durability) {
            (StorageMode::CloudBacked(_), durability) => cloud_durable_write_options(durability),
            _ => cntryl_midge::WriteOptions::sync(),
        }
    }

    #[must_use]
    pub fn request_buffered_write_options(&self) -> cntryl_midge::WriteOptions {
        if matches!(self.storage_mode, StorageMode::CloudBacked(_)) {
            cntryl_midge::WriteOptions::cloud_async()
        } else {
            cntryl_midge::WriteOptions::buffered()
        }
    }

    #[must_use]
    pub fn queue_write_options(&self) -> cntryl_midge::WriteOptions {
        match (&self.storage_mode, &self.queue_write_policy) {
            (StorageMode::Memory, _) | (_, QueueWritePolicy::Fast) => {
                cntryl_midge::WriteOptions::best_effort()
            }
            (StorageMode::CloudBacked(_), QueueWritePolicy::Strict) => {
                cloud_durable_write_options(&CloudDurabilityMode::Strict)
            }
            (StorageMode::CloudBacked(_), QueueWritePolicy::Buffered) => {
                cloud_durable_write_options(&CloudDurabilityMode::Background)
            }
            (_, QueueWritePolicy::Strict) => cntryl_midge::WriteOptions::sync(),
            (_, QueueWritePolicy::Buffered | QueueWritePolicy::Invalid { .. }) => {
                cntryl_midge::WriteOptions::buffered()
            }
        }
    }

    #[must_use]
    pub fn queue_fast_flush_interval(&self) -> Option<Duration> {
        matches!(self.queue_write_policy, QueueWritePolicy::Fast)
            .then(|| Duration::from_millis(self.queue_loss_window_ms))
    }

    #[must_use]
    pub fn schedule_preload_timeout(&self) -> Duration {
        Duration::from_secs(self.schedule_preload_timeout_seconds)
    }

    #[must_use]
    pub fn queue_write_policy_defaulted_fast(&self) -> bool {
        self.queue_write_policy_source.is_defaulted()
            && matches!(self.queue_write_policy, QueueWritePolicy::Fast)
    }
}

fn cloud_durable_write_options(durability: &CloudDurabilityMode) -> cntryl_midge::WriteOptions {
    match durability {
        CloudDurabilityMode::Background | CloudDurabilityMode::Invalid { .. } => {
            cntryl_midge::WriteOptions::cloud_async()
        }
        CloudDurabilityMode::Strict => cntryl_midge::WriteOptions::cloud_strict(),
    }
}

impl Default for BootConfig {
    fn default() -> Self {
        let auth_required = std::env::var("FITZ_AUTH_REQUIRED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(true);
        let http_port = std::env::var("FITZ_HTTP_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(crate::prelude::DEFAULT_HTTP_PORT);
        let tcp_port = std::env::var("FITZ_TCP_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(crate::prelude::DEFAULT_TCP_PORT);
        let tcp_enabled = std::env::var("FITZ_TCP_ENABLED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(true);
        let bind_addr = std::env::var("FITZ_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0".to_string());
        let metrics_bind_addr = std::env::var("FITZ_METRICS_BIND_ADDR")
            .unwrap_or_else(|_| DEFAULT_METRICS_BIND_ADDR.to_string());
        let metrics_port = std::env::var("FITZ_METRICS_PORT")
            .ok()
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(DEFAULT_METRICS_PORT);
        let assume_external_tls = std::env::var("FITZ_ASSUME_EXTERNAL_TLS")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(false);
        let assume_local_loopback_edge = std::env::var("FITZ_ASSUME_LOCAL_LOOPBACK_EDGE")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(false);
        let local_listener_exposure =
            LocalListenerExposure::from_assumption(assume_local_loopback_edge);
        let (ws_allowed_origins, ws_allowed_origins_error) =
            parse_ws_allowed_origins_from_env().unwrap_or_else(default_local_ws_allowed_origins);
        let (drain_grace_seconds, drain_config_error) = drain_grace_seconds_from_env();
        let (queue_loss_window_ms, queue_loss_window_error) = queue_loss_window_ms_from_env();
        let (kv_idle_transaction_ttl_seconds, kv_idle_transaction_ttl_error) =
            kv_idle_transaction_ttl_seconds_from_env();
        let (schedule_preload_timeout_seconds, schedule_preload_timeout_error) =
            schedule_preload_timeout_seconds_from_env();
        let (storage_lease_ttl_seconds, storage_lease_ttl_error) =
            storage_lease_ttl_seconds_from_env();
        let (queue_write_policy, queue_write_policy_source) =
            QueueWritePolicy::from_env_with_source();
        let drain_close_reason = drain_close_reason_from_env();
        let route_families = std::env::var("FITZ_ROUTE_FAMILIES")
            .unwrap_or_else(|_| "1".to_string())
            .split(',')
            .map(|value| value.trim().parse::<u32>().unwrap_or(0))
            .collect();

        Self {
            http_port,
            tcp_port,
            tcp_enabled,
            bind_addr,
            metrics_bind_addr,
            metrics_port,
            storage_mode: StorageMode::from_env(),
            stream_storage_layout: StreamStorageLayout::from_env(),
            auth_required,
            auth_config: crate::auth::AuthConfig::from_env(auth_required),
            auth_claims_config: crate::auth::AuthClaimsConfig::from_env(),
            route_family_resolver: crate::auth::RouteFamilyResolverConfig::from_env(),
            route_families,
            max_connections: 10_000,
            max_frame_size: 1024 * 1024,
            channel_capacity: 1000,
            cloud_durability: CloudDurabilityMode::from_env(),
            storage_memtable: StorageMemtableConfig::from_env(),
            storage_lease_ttl_seconds,
            storage_lease_ttl_error,
            queue_write_policy,
            queue_write_policy_source,
            queue_loss_window_ms,
            queue_loss_window_error,
            kv_idle_transaction_ttl_seconds,
            kv_idle_transaction_ttl_error,
            schedule_preload_timeout_seconds,
            schedule_preload_timeout_error,
            assume_external_tls,
            local_listener_exposure,
            ws_allowed_origins,
            ws_allowed_origins_error,
            drain_grace_seconds,
            drain_close_reason,
            drain_config_error,
        }
    }
}

impl BootConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_memory_storage() -> Self {
        Self {
            storage_mode: StorageMode::Memory,
            ..Default::default()
        }
    }

    pub fn with_local_storage(path: impl Into<String>) -> Self {
        Self {
            storage_mode: StorageMode::LocalDisk {
                db_path: path.into(),
            },
            ..Default::default()
        }
    }

    #[must_use]
    pub fn with_http_port(mut self, port: u16) -> Self {
        self.http_port = port;
        self
    }

    #[must_use]
    pub fn with_tcp_port(mut self, port: u16) -> Self {
        self.tcp_port = port;
        self
    }

    #[must_use]
    pub fn with_tcp_enabled(mut self, enabled: bool) -> Self {
        self.tcp_enabled = enabled;
        self
    }

    #[must_use]
    pub fn with_bind_addr(mut self, addr: String) -> Self {
        self.bind_addr = addr;
        self
    }

    #[must_use]
    pub fn with_metrics_bind_addr(mut self, addr: String) -> Self {
        self.metrics_bind_addr = addr;
        self
    }

    #[must_use]
    pub fn with_metrics_port(mut self, port: u16) -> Self {
        self.metrics_port = port;
        self
    }

    #[must_use]
    pub fn with_assume_external_tls(mut self, assume_external_tls: bool) -> Self {
        self.assume_external_tls = assume_external_tls;
        self
    }

    #[must_use]
    pub fn with_assume_local_loopback_edge(mut self, assume_local_loopback_edge: bool) -> Self {
        self.local_listener_exposure =
            LocalListenerExposure::from_assumption(assume_local_loopback_edge);
        self
    }

    #[must_use]
    pub fn assume_external_tls(&self) -> bool {
        self.assume_external_tls
    }

    #[must_use]
    pub fn assume_local_loopback_edge(&self) -> bool {
        self.local_listener_exposure.is_host_loopback_edge()
    }

    #[must_use]
    pub fn with_ws_allowed_origins(
        mut self,
        origins: Vec<crate::api::origin::ExactOrigin>,
    ) -> Self {
        self.ws_allowed_origins = origins;
        self.ws_allowed_origins_error = None;
        self
    }

    #[must_use]
    pub fn with_drain_grace_seconds(mut self, seconds: u64) -> Self {
        self.drain_grace_seconds = seconds;
        self.drain_config_error = None;
        self
    }

    #[must_use]
    pub fn with_schedule_preload_timeout_seconds(mut self, seconds: u64) -> Self {
        self.schedule_preload_timeout_seconds = seconds;
        self.schedule_preload_timeout_error = None;
        self
    }

    #[must_use]
    pub fn with_drain_close_reason(mut self, reason: impl Into<String>) -> Self {
        self.drain_close_reason = reason.into();
        self
    }

    #[must_use]
    pub fn with_storage_mode(mut self, mode: StorageMode) -> Self {
        self.storage_mode = mode;
        self
    }

    #[must_use]
    pub fn with_stream_storage_layout(mut self, layout: StreamStorageLayout) -> Self {
        self.stream_storage_layout = layout;
        self
    }

    #[must_use]
    pub fn with_auth_config(mut self, auth_config: crate::auth::AuthConfig) -> Self {
        self.auth_required = !matches!(auth_config, crate::auth::AuthConfig::Disabled);
        self.auth_config = auth_config;
        self
    }

    #[must_use]
    pub fn with_auth_claims_config(
        mut self,
        auth_claims_config: crate::auth::AuthClaimsConfig,
    ) -> Self {
        self.auth_claims_config = auth_claims_config;
        self
    }

    #[must_use]
    pub fn with_route_family_resolver(
        mut self,
        route_family_resolver: crate::auth::RouteFamilyResolverConfig,
    ) -> Self {
        self.route_family_resolver = route_family_resolver;
        self
    }

    #[must_use]
    pub fn with_route_families(mut self, route_families: Vec<u32>) -> Self {
        self.route_families = route_families;
        self
    }

    #[must_use]
    pub fn with_storage_memtable_bytes(mut self, bytes: usize) -> Self {
        self.storage_memtable = StorageMemtableConfig::Bytes(bytes);
        self
    }

    #[must_use]
    pub fn storage_memtable_bytes(&self) -> Option<usize> {
        self.storage_memtable.bytes()
    }

    #[must_use]
    /// Override the embedded Midge primary storage-writer lease TTL.
    pub fn with_storage_lease_ttl_seconds(mut self, seconds: u64) -> Self {
        self.storage_lease_ttl_seconds = seconds;
        self.storage_lease_ttl_error = None;
        self
    }

    #[must_use]
    /// Return the configured embedded Midge primary storage-writer lease TTL.
    pub fn storage_lease_ttl(&self) -> Duration {
        Duration::from_secs(self.storage_lease_ttl_seconds)
    }

    /// Validate the full broker boot configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when drain, transport, auth, storage, origin-security,
    /// or route-family settings are invalid or incomplete for startup.
    pub fn validate(&self) -> BootResult<()> {
        DrainConfig::from_boot_config(self).validate()?;
        StorageConfig::from_boot_config(self).validate()?;
        let protected_admin_configured =
            crate::api::admin::auth::protected_admin_configured_from_env();
        TransportConfig::from_boot_config(self).validate(protected_admin_configured)?;
        self.auth_config
            .validate(self.auth_required)
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
        if self.route_families.is_empty() {
            return Err("FITZ_ROUTE_FAMILIES must contain at least one family".into());
        }
        for (index, family) in self.route_families.iter().copied().enumerate() {
            let expected = u32::try_from(index + 1).map_err(|_| {
                "FITZ_ROUTE_FAMILIES contains too many entries to validate contiguity".to_string()
            })?;
            if family != expected {
                return Err(format!(
                    "FITZ_ROUTE_FAMILIES must be contiguous non-zero values starting at 1; expected {expected}, found {family}"
                )
                .into());
            }
        }
        self.auth_claims_config
            .validate()
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
        self.route_family_resolver
            .validate(&self.route_families, self.auth_required)
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
