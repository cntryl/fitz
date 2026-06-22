use super::BootResult;
use crate::domains::stream::StreamStorageLayout;
use std::path::Path;

const DEFAULT_LOCAL_STORAGE_PATH: &str = "./.fitz";
const DEFAULT_CLOUD_CACHE_PATH: &str = "./.fitz-cloud-cache";
const DEFAULT_PEAS_ENDPOINT: &str = "http://127.0.0.1:9000";
const DEFAULT_PEAS_ACCESS_KEY: &str = "admin";
const DEFAULT_PEAS_SECRET_KEY: &str = "easy-peasy";
const DEFAULT_PEAS_BUCKET: &str = "fitz";
const ENV_STORAGE_MEMTABLE_BYTES: &str = "FITZ_STORAGE_MEMTABLE_BYTES";
const ENV_DRAIN_GRACE_SECONDS: &str = "FITZ_DRAIN_GRACE_SECONDS";
const ENV_DRAIN_CLOSE_REASON: &str = "FITZ_DRAIN_CLOSE_REASON";
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
                    "unsupported FITZ_STORAGE_CLOUD_DURABILITY='{}'; expected background or strict",
                    other
                ),
            },
        }
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
            Self::Auto => Ok(()),
            Self::Bytes(0) => Err(format!(
                "{ENV_STORAGE_MEMTABLE_BYTES} must be greater than 0"
            )),
            Self::Bytes(_) => Ok(()),
            Self::Invalid { reason } => Err(reason.clone()),
        }
    }

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
    /// Fitz provider label from FITZ_STORAGE_PROVIDER.
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
                    "FITZ_STORAGE_MODE={} is no longer supported; set FITZ_STORAGE_MODE=cloud and FITZ_STORAGE_PROVIDER=...",
                    mode
                ),
            },
            _ => Self::Invalid {
                reason: format!(
                    "unsupported FITZ_STORAGE_MODE='{}'; expected memory, local, or cloud",
                    mode
                ),
            },
        }
    }

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

fn build_cloud_provider_config(
    provider: &str,
) -> Result<cntryl_midge::CloudProviderConfig, String> {
    match provider {
        "peas-s3" => Ok(cntryl_midge::CloudProviderConfig::s3_compatible_static(
            env_non_empty("FITZ_STORAGE_BUCKET").unwrap_or_else(|| DEFAULT_PEAS_BUCKET.to_string()),
            env_non_empty("FITZ_STORAGE_ENDPOINT")
                .unwrap_or_else(|| DEFAULT_PEAS_ENDPOINT.to_string()),
            DEFAULT_PEAS_ACCESS_KEY,
            DEFAULT_PEAS_SECRET_KEY,
        )),
        "peas-azure" => Ok(cntryl_midge::CloudProviderConfig::AzureBlob {
            account: DEFAULT_PEAS_ACCESS_KEY.to_string(),
            container: env_non_empty("FITZ_STORAGE_CONTAINER")
                .unwrap_or_else(|| DEFAULT_PEAS_BUCKET.to_string()),
            endpoint: Some(
                env_non_empty("FITZ_STORAGE_ENDPOINT")
                    .unwrap_or_else(|| DEFAULT_PEAS_ENDPOINT.to_string()),
            ),
            credential: cntryl_midge::AzureCredentialSource::shared_key(DEFAULT_PEAS_SECRET_KEY),
        }),
        "peas-gcs" => Ok(cntryl_midge::CloudProviderConfig::Gcs {
            bucket: env_non_empty("FITZ_STORAGE_BUCKET")
                .unwrap_or_else(|| DEFAULT_PEAS_BUCKET.to_string()),
            project_id: "peas".to_string(),
            endpoint: Some(
                env_non_empty("FITZ_STORAGE_ENDPOINT")
                    .unwrap_or_else(|| DEFAULT_PEAS_ENDPOINT.to_string()),
            ),
            api: cntryl_midge::GcsApiStyle::Xml,
            credential: cntryl_midge::GcsCredentialSource::hmac_key(
                DEFAULT_PEAS_ACCESS_KEY,
                DEFAULT_PEAS_SECRET_KEY,
            ),
        }),
        "aws-s3" => Ok(cntryl_midge::CloudProviderConfig::aws_s3(
            required_env("FITZ_STORAGE_BUCKET")?,
            required_region()?,
        )),
        "s3-compatible" => Ok(cntryl_midge::CloudProviderConfig::S3Compatible {
            bucket: required_env("FITZ_STORAGE_BUCKET")?,
            region: env_non_empty("FITZ_STORAGE_REGION")
                .unwrap_or_else(|| "us-east-1".to_string()),
            endpoint: required_env("FITZ_STORAGE_ENDPOINT")?,
            path_style: env_bool("FITZ_STORAGE_FORCE_PATH_STYLE", true)?,
            credentials: cntryl_midge::S3CredentialSource::environment(),
        }),
        "minio" => Ok(cntryl_midge::CloudProviderConfig::Minio {
            bucket: required_env("FITZ_STORAGE_BUCKET")?,
            endpoint: required_env("FITZ_STORAGE_ENDPOINT")?,
            credentials: cntryl_midge::S3CredentialSource::environment(),
        }),
        "wasabi" => Ok(cntryl_midge::CloudProviderConfig::Wasabi {
            bucket: required_env("FITZ_STORAGE_BUCKET")?,
            region: required_env("FITZ_STORAGE_REGION")?,
            endpoint: env_non_empty("FITZ_STORAGE_ENDPOINT"),
            credentials: cntryl_midge::S3CredentialSource::environment(),
        }),
        "oci-s3" => Ok(cntryl_midge::CloudProviderConfig::OciS3Compatible {
            bucket: required_env("FITZ_STORAGE_BUCKET")?,
            namespace: required_env("FITZ_STORAGE_NAMESPACE")?,
            region: required_env("FITZ_STORAGE_REGION")?,
            endpoint: env_non_empty("FITZ_STORAGE_ENDPOINT"),
            path_style: env_bool("FITZ_STORAGE_FORCE_PATH_STYLE", false)?,
            credentials: cntryl_midge::S3CredentialSource::environment(),
        }),
        "azure-blob" => build_azure_blob_provider(),
        "gcs" => build_gcs_provider(),
        other => Err(format!(
            "unsupported FITZ_STORAGE_PROVIDER='{}'; expected peas-s3, peas-azure, peas-gcs, aws-s3, s3-compatible, minio, wasabi, oci-s3, azure-blob, or gcs",
            other
        )),
    }
}

fn build_azure_blob_provider() -> Result<cntryl_midge::CloudProviderConfig, String> {
    let endpoint = env_non_empty("FITZ_STORAGE_ENDPOINT");
    let container = required_env("FITZ_STORAGE_CONTAINER")
        .map_err(|_| "azure-blob storage requires FITZ_STORAGE_CONTAINER".to_string())?;

    let mut provider = if let Some(connection_string) =
        env_non_empty("AZURE_STORAGE_CONNECTION_STRING")
    {
        cntryl_midge::CloudProviderConfig::azure_blob_connection_string(
            container,
            connection_string,
        )
    } else {
        let account = env_non_empty("AZURE_STORAGE_ACCOUNT_NAME").ok_or_else(|| {
            "azure-blob storage requires AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING"
                .to_string()
        })?;
        if let Some(account_key) = env_non_empty("AZURE_STORAGE_ACCOUNT_KEY") {
            cntryl_midge::CloudProviderConfig::azure_blob_shared_key(
                account,
                container,
                account_key,
            )
        } else if let Some(sas_token) = env_non_empty("AZURE_STORAGE_SAS_TOKEN") {
            cntryl_midge::CloudProviderConfig::azure_blob_sas(account, container, sas_token)
        } else {
            cntryl_midge::CloudProviderConfig::azure_blob(account, container)
        }
    };

    if let Some(endpoint) = endpoint {
        provider = provider
            .with_endpoint(endpoint)
            .map_err(|error| error.to_string())?;
    }

    Ok(provider)
}

fn build_gcs_provider() -> Result<cntryl_midge::CloudProviderConfig, String> {
    let bucket = required_env("FITZ_STORAGE_BUCKET")?;
    let mut provider = match (
        env_non_empty("GCS_HMAC_ACCESS_ID"),
        env_non_empty("GCS_HMAC_SECRET"),
    ) {
        (Some(access_id), Some(secret)) => {
            cntryl_midge::CloudProviderConfig::gcs_hmac(bucket, access_id, secret)
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(
                "gcs HMAC storage requires both GCS_HMAC_ACCESS_ID and GCS_HMAC_SECRET".to_string(),
            )
        }
        (None, None) => {
            if let Some(path) = env_non_empty("GOOGLE_APPLICATION_CREDENTIALS") {
                cntryl_midge::CloudProviderConfig::gcs_service_account_file(bucket, path)
            } else {
                cntryl_midge::CloudProviderConfig::gcs(bucket)
            }
        }
    };

    if let Some(project_id) = env_non_empty("GOOGLE_CLOUD_PROJECT") {
        provider = provider
            .with_gcs_project_id(project_id)
            .map_err(|error| error.to_string())?;
    }
    if let Some(endpoint) = env_non_empty("FITZ_STORAGE_ENDPOINT") {
        provider = provider
            .with_endpoint(endpoint)
            .map_err(|error| error.to_string())?;
    }

    Ok(provider)
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn required_env(key: &str) -> Result<String, String> {
    env_non_empty(key).ok_or_else(|| format!("cloud storage requires {}", key))
}

fn required_region() -> Result<String, String> {
    env_non_empty("FITZ_STORAGE_REGION")
        .or_else(|| env_non_empty("AWS_REGION"))
        .or_else(|| env_non_empty("AWS_DEFAULT_REGION"))
        .ok_or_else(|| "aws-s3 storage requires FITZ_STORAGE_REGION or AWS_REGION".to_string())
}

fn env_bool(key: &str, default: bool) -> Result<bool, String> {
    match env_non_empty(key) {
        Some(value) => value
            .parse::<bool>()
            .map_err(|_| format!("{} must be true or false", key)),
        None => Ok(default),
    }
}

fn drain_grace_seconds_from_env() -> (u64, Option<String>) {
    let Some(value) = env_non_empty(ENV_DRAIN_GRACE_SECONDS) else {
        return (DEFAULT_DRAIN_GRACE_SECONDS, None);
    };

    match value.parse::<u64>() {
        Ok(0) => (
            0,
            Some(format!("{ENV_DRAIN_GRACE_SECONDS} must be greater than 0")),
        ),
        Ok(seconds) => (seconds, None),
        Err(_) => (
            0,
            Some(format!(
                "{ENV_DRAIN_GRACE_SECONDS} must be an unsigned integer second count"
            )),
        ),
    }
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
    /// Provisioned RouteFamily values accepted after identity resolution.
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
    /// Whether an external TLS terminator is explicitly protecting public listeners.
    pub assume_external_tls: bool,
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
    pub fn storage_path(&self) -> String {
        match &self.storage_mode {
            StorageMode::LocalDisk { db_path } => db_path.clone(),
            StorageMode::Memory => ":memory:".to_string(),
            StorageMode::CloudBacked(config) => config.local_cache_path.clone(),
            StorageMode::Invalid { .. } => "<invalid>".to_string(),
        }
    }

    pub fn server_write_options(&self) -> cntryl_midge::WriteOptions {
        match (&self.storage_mode, &self.cloud_durability) {
            (StorageMode::Memory, _) => cntryl_midge::WriteOptions::best_effort(),
            (StorageMode::CloudBacked(_), CloudDurabilityMode::Strict) => {
                cntryl_midge::WriteOptions::cloud_strict()
            }
            _ => cntryl_midge::WriteOptions::buffered(),
        }
    }

    pub fn request_sync_write_options(&self) -> cntryl_midge::WriteOptions {
        match (&self.storage_mode, &self.cloud_durability) {
            (StorageMode::CloudBacked(_), CloudDurabilityMode::Strict) => {
                cntryl_midge::WriteOptions::cloud_strict()
            }
            _ => cntryl_midge::WriteOptions::sync(),
        }
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
        let assume_external_tls = std::env::var("FITZ_ASSUME_EXTERNAL_TLS")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(false);
        let (ws_allowed_origins, ws_allowed_origins_error) =
            parse_ws_allowed_origins_from_env().unwrap_or_else(default_local_ws_allowed_origins);
        let (drain_grace_seconds, drain_config_error) = drain_grace_seconds_from_env();
        let drain_close_reason = env_non_empty(ENV_DRAIN_CLOSE_REASON)
            .unwrap_or_else(|| DEFAULT_DRAIN_CLOSE_REASON.to_string());
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
            assume_external_tls,
            ws_allowed_origins,
            ws_allowed_origins_error,
            drain_grace_seconds,
            drain_close_reason,
            drain_config_error,
        }
    }
}

impl BootConfig {
    pub fn new() -> Self {
        Self::default()
    }

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

    pub fn with_http_port(mut self, port: u16) -> Self {
        self.http_port = port;
        self
    }

    pub fn with_tcp_port(mut self, port: u16) -> Self {
        self.tcp_port = port;
        self
    }

    pub fn with_tcp_enabled(mut self, enabled: bool) -> Self {
        self.tcp_enabled = enabled;
        self
    }

    pub fn with_bind_addr(mut self, addr: String) -> Self {
        self.bind_addr = addr;
        self
    }

    pub fn with_assume_external_tls(mut self, assume_external_tls: bool) -> Self {
        self.assume_external_tls = assume_external_tls;
        self
    }

    pub fn with_ws_allowed_origins(
        mut self,
        origins: Vec<crate::api::origin::ExactOrigin>,
    ) -> Self {
        self.ws_allowed_origins = origins;
        self.ws_allowed_origins_error = None;
        self
    }

    pub fn with_drain_grace_seconds(mut self, seconds: u64) -> Self {
        self.drain_grace_seconds = seconds;
        self.drain_config_error = None;
        self
    }

    pub fn with_drain_close_reason(mut self, reason: impl Into<String>) -> Self {
        self.drain_close_reason = reason.into();
        self
    }

    pub fn with_storage_mode(mut self, mode: StorageMode) -> Self {
        self.storage_mode = mode;
        self
    }

    pub fn with_stream_storage_layout(mut self, layout: StreamStorageLayout) -> Self {
        self.stream_storage_layout = layout.normalize_requested();
        self
    }

    pub fn with_auth_config(mut self, auth_config: crate::auth::AuthConfig) -> Self {
        self.auth_required = !matches!(auth_config, crate::auth::AuthConfig::Disabled);
        self.auth_config = auth_config;
        self
    }

    pub fn with_auth_claims_config(
        mut self,
        auth_claims_config: crate::auth::AuthClaimsConfig,
    ) -> Self {
        self.auth_claims_config = auth_claims_config;
        self
    }

    pub fn with_route_family_resolver(
        mut self,
        route_family_resolver: crate::auth::RouteFamilyResolverConfig,
    ) -> Self {
        self.route_family_resolver = route_family_resolver;
        self
    }

    pub fn with_route_families(mut self, route_families: Vec<u32>) -> Self {
        self.route_families = route_families;
        self
    }

    pub fn with_storage_memtable_bytes(mut self, bytes: usize) -> Self {
        self.storage_memtable = StorageMemtableConfig::Bytes(bytes);
        self
    }

    pub fn storage_memtable_bytes(&self) -> Option<usize> {
        self.storage_memtable.bytes()
    }

    pub fn validate(&self) -> BootResult<()> {
        if let Some(error) = &self.drain_config_error {
            return Err(error.clone().into());
        }
        if self.drain_grace_seconds == 0 {
            return Err(format!("{ENV_DRAIN_GRACE_SECONDS} must be greater than 0").into());
        }
        if self.drain_close_reason.trim().is_empty() {
            return Err(format!("{ENV_DRAIN_CLOSE_REASON} must not be empty").into());
        }
        if let CloudDurabilityMode::Invalid { reason } = &self.cloud_durability {
            return Err(reason.clone().into());
        }
        self.storage_memtable
            .validate()
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
        self.storage_mode
            .validate()
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
        if let Some(value) = env_non_empty("FITZ_TCP_ENABLED") {
            value.parse::<bool>().map_err(|_| {
                Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "FITZ_TCP_ENABLED must be true or false",
                )) as Box<dyn std::error::Error>
            })?;
        }
        self.auth_config
            .validate(self.auth_required)
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
        let protected_admin_configured =
            crate::api::admin::auth::protected_admin_configured_from_env();
        let public_bind = !bind_addr_is_loopback(&self.bind_addr);
        let ws_allowed_origins = configured_ws_allowed_origins(
            &self.ws_allowed_origins,
            &self.ws_allowed_origins_error,
        )?;
        validate_public_origin_security("FITZ_WS_ALLOWED_ORIGINS", &ws_allowed_origins)?;
        if self.auth_required && public_bind && ws_allowed_origins.is_empty() {
            return Err(
                "FITZ_WS_ALLOWED_ORIGINS is required when authenticated WebSocket listeners bind to a non-loopback address"
                    .into(),
            );
        }
        validate_admin_browser_security(protected_admin_configured, public_bind)?;
        if self.route_families.is_empty() {
            return Err("FITZ_ROUTE_FAMILIES must contain at least one family".into());
        }
        for (index, family) in self.route_families.iter().copied().enumerate() {
            let expected = index as u32 + 1;
            if family != expected {
                return Err(format!(
                    "FITZ_ROUTE_FAMILIES must be contiguous non-zero values starting at 1; expected {}, found {}",
                    expected, family
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

fn configured_ws_allowed_origins(
    configured: &[crate::api::origin::ExactOrigin],
    configured_error: &Option<String>,
) -> BootResult<Vec<crate::api::origin::ExactOrigin>> {
    if let Some(error) = configured_error {
        return Err(error.clone().into());
    }
    match env_non_empty("FITZ_WS_ALLOWED_ORIGINS") {
        Some(value) => crate::api::origin::parse_exact_origin_list(&value)
            .map_err(|error| format!("FITZ_WS_ALLOWED_ORIGINS {error}").into()),
        None => Ok(configured.to_vec()),
    }
}

fn parse_ws_allowed_origins_from_env(
) -> Option<(Vec<crate::api::origin::ExactOrigin>, Option<String>)> {
    env_non_empty("FITZ_WS_ALLOWED_ORIGINS").map(|value| {
        crate::api::origin::parse_exact_origin_list(&value)
            .map(|origins| (origins, None))
            .unwrap_or_else(|error| (Vec::new(), Some(format!("FITZ_WS_ALLOWED_ORIGINS {error}"))))
    })
}

fn default_local_ws_allowed_origins() -> (Vec<crate::api::origin::ExactOrigin>, Option<String>) {
    let origins = DEFAULT_LOCAL_WS_ALLOWED_ORIGIN_VALUES
        .iter()
        .map(|origin| {
            crate::api::origin::parse_exact_origin(origin)
                .expect("default local WebSocket origin must be valid")
        })
        .collect();
    (origins, None)
}

fn validate_public_origin_security(
    env_key: &str,
    origins: &[crate::api::origin::ExactOrigin],
) -> BootResult<()> {
    for origin in origins {
        if origin.scheme() == "http" && !origin.is_loopback() {
            return Err(format!(
                "{env_key} entries must use https unless they are loopback origins"
            )
            .into());
        }
    }
    Ok(())
}

fn validate_admin_browser_security(
    protected_admin_configured: bool,
    public_bind: bool,
) -> BootResult<()> {
    if let Some(value) = env_non_empty("FITZ_ADMIN_COOKIE_SECURE") {
        let cookie_secure = value.parse::<bool>().map_err(|_| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "FITZ_ADMIN_COOKIE_SECURE must be true or false",
            )) as Box<dyn std::error::Error>
        })?;
        if protected_admin_configured && public_bind && !cookie_secure {
            return Err(
                "FITZ_ADMIN_COOKIE_SECURE=false is only allowed on loopback admin listeners".into(),
            );
        }
    }

    let Some(public_origin) = env_non_empty("FITZ_ADMIN_PUBLIC_ORIGIN") else {
        if protected_admin_configured && public_bind {
            return Err(
                "FITZ_ADMIN_PUBLIC_ORIGIN is required when protected admin binds to a non-loopback address"
                    .into(),
            );
        }
        return Ok(());
    };

    let origin = crate::api::origin::parse_exact_origin(&public_origin)
        .map_err(|error| format!("FITZ_ADMIN_PUBLIC_ORIGIN {error}"))?;
    if protected_admin_configured && public_bind && origin.scheme() != "https" {
        return Err(
            "FITZ_ADMIN_PUBLIC_ORIGIN must use https on non-loopback admin listeners".into(),
        );
    }
    validate_public_origin_security("FITZ_ADMIN_PUBLIC_ORIGIN", std::slice::from_ref(&origin))?;

    Ok(())
}

fn bind_addr_is_loopback(bind_addr: &str) -> bool {
    let host = bind_addr
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']');

    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }

    host.parse::<std::net::IpAddr>()
        .map(|addr| addr.is_loopback())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude;
    use serial_test::serial;

    fn with_env_var<T>(key: &str, value: &str, test: impl FnOnce() -> T) -> T {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value);
        }

        let result = test();

        match previous {
            Some(previous) => unsafe {
                std::env::set_var(key, previous);
            },
            None => unsafe {
                std::env::remove_var(key);
            },
        }

        result
    }

    fn with_auth_env<T>(values: &[(&str, &str)], test: impl FnOnce() -> T) -> T {
        let keys = [
            "FITZ_AUTH_REQUIRED",
            "FITZ_JWT_AUDIENCE",
            "FITZ_JWT_AUDIENCES",
            "FITZ_JWT_JWKS_MAP",
            "FITZ_JWT_HMAC_SECRET",
            "FITZ_JWT_ALLOW_INSECURE_HTTP",
            "FITZ_ROUTE_FAMILIES",
            "FITZ_ROUTE_FAMILY_MAP",
            "FITZ_ROUTE_FAMILY_CLAIM",
            "FITZ_AUTH_CUSTOM_CLAIM",
            "FITZ_AUTH_ROLE_CLAIM",
            "FITZ_AUTH_ALLOW_JWT_ROUTE_FAMILY",
            "FITZ_ASSUME_EXTERNAL_TLS",
            "FITZ_TCP_ENABLED",
            "FITZ_WS_ALLOWED_ORIGINS",
            "FITZ_ADMIN_AUTH_MODE",
            "FITZ_ADMIN_USERNAME",
            "FITZ_ADMIN_PASSWORD_HASH",
            "FITZ_ADMIN_COOKIE_SECURE",
            "FITZ_ADMIN_PUBLIC_ORIGIN",
            ENV_DRAIN_GRACE_SECONDS,
            ENV_DRAIN_CLOSE_REASON,
        ];
        let previous = keys
            .iter()
            .map(|key| (*key, std::env::var(key).ok()))
            .collect::<Vec<_>>();

        unsafe {
            for key in keys {
                std::env::remove_var(key);
            }
            for (key, value) in values {
                std::env::set_var(key, value);
            }
        }

        let result = test();

        unsafe {
            for (key, value) in previous {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }

        result
    }

    fn with_storage_env<T>(values: &[(&str, &str)], test: impl FnOnce() -> T) -> T {
        let keys = [
            "FITZ_STORAGE_MODE",
            "FITZ_STORAGE_PROVIDER",
            "FITZ_STORAGE_BUCKET",
            "FITZ_STORAGE_CONTAINER",
            "FITZ_STORAGE_PREFIX",
            "FITZ_STORAGE_CACHE_PATH",
            "FITZ_STORAGE_PATH",
            "FITZ_STORAGE_ENDPOINT",
            "FITZ_STORAGE_REGION",
            "FITZ_STORAGE_FORCE_PATH_STYLE",
            "FITZ_STORAGE_NAMESPACE",
            "FITZ_STORAGE_ACCOUNT",
            "FITZ_STORAGE_CLOUD_DURABILITY",
            "FITZ_STORAGE_MEMTABLE_BYTES",
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
            "AZURE_STORAGE_ACCOUNT_NAME",
            "AZURE_STORAGE_ACCOUNT_KEY",
            "AZURE_STORAGE_CONNECTION_STRING",
            "AZURE_STORAGE_SAS_TOKEN",
            "GOOGLE_APPLICATION_CREDENTIALS",
            "GOOGLE_CLOUD_PROJECT",
            "GCS_HMAC_ACCESS_ID",
            "GCS_HMAC_SECRET",
            "FITZ_ADMIN_AUTH_MODE",
            "FITZ_ADMIN_USERNAME",
            "FITZ_ADMIN_PASSWORD_HASH",
            ENV_DRAIN_GRACE_SECONDS,
            ENV_DRAIN_CLOSE_REASON,
        ];
        let previous = keys
            .iter()
            .map(|key| (*key, std::env::var(key).ok()))
            .collect::<Vec<_>>();

        unsafe {
            for key in keys {
                std::env::remove_var(key);
            }
            for (key, value) in values {
                std::env::set_var(key, value);
            }
        }

        let result = test();

        unsafe {
            for (key, value) in previous {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }

        result
    }

    fn with_clean_config_env<T>(test: impl FnOnce() -> T) -> T {
        with_storage_env(&[], || with_auth_env(&[], test))
    }

    fn cloud_config(config: &BootConfig) -> &CloudStorageConfig {
        match &config.storage_mode {
            StorageMode::CloudBacked(cloud) => cloud.as_ref(),
            other => panic!("expected cloud storage mode, got {other:?}"),
        }
    }

    #[test]
    #[serial]
    fn should_create_default_boot_config() {
        // Arrange

        // Act
        let config = BootConfig::default();

        // Assert
        assert_eq!(config.tcp_port, prelude::DEFAULT_TCP_PORT);
        assert_eq!(config.http_port, prelude::DEFAULT_HTTP_PORT);
        assert!(config.tcp_enabled);
        assert_eq!(config.bind_addr, "0.0.0.0");
        assert_eq!(config.max_connections, 10_000);
        assert_eq!(config.cloud_durability, CloudDurabilityMode::Background);
        assert_eq!(config.storage_memtable, StorageMemtableConfig::Auto);
        assert_eq!(config.drain_grace_seconds, DEFAULT_DRAIN_GRACE_SECONDS);
        assert_eq!(config.drain_close_reason, DEFAULT_DRAIN_CLOSE_REASON);
        assert!(!config.assume_external_tls);
        assert_eq!(config.route_families, vec![1]);
        assert_eq!(
            config.auth_claims_config.identity_claim,
            crate::auth::DEFAULT_ROUTE_FAMILY_CLAIM
        );
        assert!(config.route_family_resolver.mappings.is_empty());
        assert_eq!(
            config.stream_storage_layout,
            StreamStorageLayout::PromotionFrontier
        );
    }

    #[test]
    #[serial]
    fn should_use_implicit_local_ws_allowed_origins_by_default() {
        with_auth_env(&[("FITZ_AUTH_REQUIRED", "false")], || {
            // Arrange

            // Act
            let config = BootConfig::default();

            // Assert
            let origins = config
                .ws_allowed_origins
                .iter()
                .map(crate::api::origin::ExactOrigin::as_str)
                .collect::<Vec<_>>();
            assert_eq!(
                origins,
                vec![
                    "http://localhost:3000",
                    "http://127.0.0.1:3000",
                    "http://localhost:4090",
                    "http://127.0.0.1:4090",
                ]
            );
        });
    }

    #[test]
    fn should_customize_boot_config() {
        // Arrange

        // Act
        let config = BootConfig::new()
            .with_tcp_port(5091)
            .with_tcp_enabled(false)
            .with_http_port(5090)
            .with_bind_addr("127.0.0.1".to_string());

        // Assert
        assert_eq!(config.tcp_port, 5091);
        assert!(!config.tcp_enabled);
        assert_eq!(config.http_port, 5090);
        assert_eq!(config.bind_addr, "127.0.0.1");
    }

    #[test]
    #[serial]
    fn should_read_drain_config_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                (ENV_DRAIN_GRACE_SECONDS, "45"),
                (ENV_DRAIN_CLOSE_REASON, "planned deploy"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::default();

                // Assert
                assert_eq!(config.drain_grace_seconds, 45);
                assert_eq!(config.drain_close_reason, "planned deploy");
                assert!(config.validate().is_ok());
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_invalid_drain_grace_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                (ENV_DRAIN_GRACE_SECONDS, "nope"),
            ],
            || {
                // Arrange
                let config = BootConfig::default();

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains(ENV_DRAIN_GRACE_SECONDS));
            },
        );
    }

    fn auth_ready_config() -> BootConfig {
        BootConfig::new()
            .with_auth_config(crate::auth::AuthConfig::hmac("test-secret-key", "fitz"))
            .with_route_family_resolver(crate::auth::RouteFamilyResolverConfig::from_mappings(
                "tid",
                [("acme", 1)],
            ))
    }

    #[test]
    #[serial]
    fn should_allow_public_bind_without_external_tls_ack_when_auth_required() {
        with_auth_env(&[], || {
            // Arrange
            let config = auth_ready_config()
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(false);

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_ok());
            assert!(!config.assume_external_tls);
        });
    }

    #[test]
    #[serial]
    fn should_allow_public_bind_with_external_tls_ack_when_auth_required() {
        with_auth_env(
            &[("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com")],
            || {
                // Arrange
                let config = auth_ready_config()
                    .with_bind_addr("0.0.0.0".to_string())
                    .with_assume_external_tls(true);

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_ok());
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_public_auth_bind_with_empty_ws_allowed_origins() {
        with_auth_env(&[], || {
            // Arrange
            let config = auth_ready_config()
                .with_bind_addr("0.0.0.0".to_string())
                .with_ws_allowed_origins(Vec::new());

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_WS_ALLOWED_ORIGINS is required"));
        });
    }

    #[test]
    #[serial]
    fn should_allow_public_auth_bind_with_ws_allowed_origins() {
        with_auth_env(
            &[("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com")],
            || {
                // Arrange
                let config = auth_ready_config()
                    .with_bind_addr("0.0.0.0".to_string())
                    .with_assume_external_tls(true);

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_ok());
            },
        );
    }

    #[test]
    #[serial]
    fn should_allow_loopback_bind_without_external_tls_when_auth_required() {
        // Arrange
        let config = auth_ready_config()
            .with_bind_addr("127.0.0.1".to_string())
            .with_assume_external_tls(false);

        // Act
        let result = config.validate();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    #[serial]
    fn should_allow_public_bind_without_external_tls_ack_when_protected_admin_configured() {
        with_auth_env(
            &[
                ("FITZ_ADMIN_USERNAME", "admin"),
                (
                    "FITZ_ADMIN_PASSWORD_HASH",
                    "$argon2id$v=19$m=16,t=2,p=1$c2FsdA$hash",
                ),
                ("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com"),
            ],
            || {
                // Arrange
                let config = BootConfig::new()
                    .with_auth_config(crate::auth::AuthConfig::Disabled)
                    .with_bind_addr("0.0.0.0".to_string())
                    .with_assume_external_tls(false);

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_ok());
                assert!(!config.assume_external_tls);
            },
        );
    }

    #[test]
    #[serial]
    fn should_allow_loopback_bind_without_external_tls_when_protected_admin_configured() {
        with_auth_env(
            &[
                ("FITZ_ADMIN_USERNAME", "admin"),
                (
                    "FITZ_ADMIN_PASSWORD_HASH",
                    "$argon2id$v=19$m=16,t=2,p=1$c2FsdA$hash",
                ),
            ],
            || {
                // Arrange
                let config = BootConfig::new()
                    .with_auth_config(crate::auth::AuthConfig::Disabled)
                    .with_bind_addr("127.0.0.1".to_string())
                    .with_assume_external_tls(false);

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_ok());
            },
        );
    }

    #[test]
    #[serial]
    fn should_allow_public_bind_with_external_tls_ack_when_protected_admin_configured() {
        with_auth_env(
            &[
                ("FITZ_ADMIN_USERNAME", "admin"),
                (
                    "FITZ_ADMIN_PASSWORD_HASH",
                    "$argon2id$v=19$m=16,t=2,p=1$c2FsdA$hash",
                ),
                ("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com"),
            ],
            || {
                // Arrange
                let config = BootConfig::new()
                    .with_auth_config(crate::auth::AuthConfig::Disabled)
                    .with_bind_addr("0.0.0.0".to_string())
                    .with_assume_external_tls(true);

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_ok());
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_public_protected_admin_without_public_origin() {
        with_auth_env(
            &[
                ("FITZ_ADMIN_USERNAME", "admin"),
                (
                    "FITZ_ADMIN_PASSWORD_HASH",
                    "$argon2id$v=19$m=16,t=2,p=1$c2FsdA$hash",
                ),
            ],
            || {
                // Arrange
                let config = BootConfig::new()
                    .with_auth_config(crate::auth::AuthConfig::Disabled)
                    .with_bind_addr("0.0.0.0".to_string())
                    .with_assume_external_tls(true);

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_ADMIN_PUBLIC_ORIGIN is required"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_insecure_public_admin_origin() {
        with_auth_env(
            &[
                ("FITZ_ADMIN_USERNAME", "admin"),
                (
                    "FITZ_ADMIN_PASSWORD_HASH",
                    "$argon2id$v=19$m=16,t=2,p=1$c2FsdA$hash",
                ),
                ("FITZ_ADMIN_PUBLIC_ORIGIN", "http://admin.example.com"),
            ],
            || {
                // Arrange
                let config = BootConfig::new()
                    .with_auth_config(crate::auth::AuthConfig::Disabled)
                    .with_bind_addr("0.0.0.0".to_string())
                    .with_assume_external_tls(true);

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_ADMIN_PUBLIC_ORIGIN must use https"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_insecure_admin_cookie_on_public_bind() {
        with_auth_env(
            &[
                ("FITZ_ADMIN_USERNAME", "admin"),
                (
                    "FITZ_ADMIN_PASSWORD_HASH",
                    "$argon2id$v=19$m=16,t=2,p=1$c2FsdA$hash",
                ),
                ("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com"),
                ("FITZ_ADMIN_COOKIE_SECURE", "false"),
            ],
            || {
                // Arrange
                let config = BootConfig::new()
                    .with_auth_config(crate::auth::AuthConfig::Disabled)
                    .with_bind_addr("0.0.0.0".to_string())
                    .with_assume_external_tls(true);

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_ADMIN_COOKIE_SECURE=false is only allowed on loopback"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_allow_public_bind_without_external_tls_when_admin_open_mode_configured() {
        with_auth_env(
            &[
                ("FITZ_ADMIN_AUTH_MODE", "open"),
                ("FITZ_ADMIN_USERNAME", "admin"),
                (
                    "FITZ_ADMIN_PASSWORD_HASH",
                    "$argon2id$v=19$m=16,t=2,p=1$c2FsdA$hash",
                ),
            ],
            || {
                // Arrange
                let config = BootConfig::new()
                    .with_auth_config(crate::auth::AuthConfig::Disabled)
                    .with_bind_addr("0.0.0.0".to_string())
                    .with_assume_external_tls(false);

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_ok());
            },
        );
    }

    #[test]
    fn should_read_listener_ports_from_environment() {
        // Arrange

        // Act
        let config = with_env_var("FITZ_HTTP_PORT", "6080", || {
            with_env_var("FITZ_TCP_PORT", "6081", BootConfig::default)
        });

        // Assert
        assert_eq!(config.http_port, 6080);
        assert_eq!(config.tcp_port, 6081);
    }

    #[test]
    #[serial]
    fn should_read_tcp_enabled_from_environment() {
        // Arrange

        // Act
        let config = with_env_var("FITZ_TCP_ENABLED", "false", BootConfig::default);

        // Assert
        assert!(!config.tcp_enabled);
    }

    #[test]
    #[serial]
    fn should_reject_invalid_tcp_enabled_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_TCP_ENABLED", "maybe"),
            ],
            || {
                // Arrange
                let config = BootConfig::default();

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_TCP_ENABLED must be true or false"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_read_ws_allowed_origins_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                (
                    "FITZ_WS_ALLOWED_ORIGINS",
                    "https://app.example.com,http://localhost:3000",
                ),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::default();

                // Assert
                assert_eq!(config.ws_allowed_origins.len(), 2);
                assert_eq!(
                    config.ws_allowed_origins[0].as_str(),
                    "https://app.example.com"
                );
                assert_eq!(
                    config.ws_allowed_origins[1].as_str(),
                    "http://localhost:3000"
                );
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_invalid_ws_allowed_origins_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com/path"),
            ],
            || {
                // Arrange
                let config = BootConfig::default();

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_WS_ALLOWED_ORIGINS"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_preserve_invalid_ws_allowed_origins_error_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com/path"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::default();

                // Assert
                assert!(config.ws_allowed_origins_error.is_some());
                assert!(config
                    .ws_allowed_origins_error
                    .as_deref()
                    .unwrap()
                    .contains("FITZ_WS_ALLOWED_ORIGINS"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_ws_allowed_origin_with_trailing_slash_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com/"),
            ],
            || {
                // Arrange
                let config = BootConfig::default();

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_WS_ALLOWED_ORIGINS"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_admin_public_origin_with_trailing_slash_from_environment() {
        with_auth_env(
            &[
                ("FITZ_ADMIN_USERNAME", "admin"),
                (
                    "FITZ_ADMIN_PASSWORD_HASH",
                    "$argon2id$v=19$m=16,t=2,p=1$c2FsdA$hash",
                ),
                ("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com/"),
            ],
            || {
                // Arrange
                let config = BootConfig::new()
                    .with_auth_config(crate::auth::AuthConfig::Disabled)
                    .with_bind_addr("0.0.0.0".to_string())
                    .with_assume_external_tls(true);

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_ADMIN_PUBLIC_ORIGIN"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_allow_localhost_http_ws_allowed_origin() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_WS_ALLOWED_ORIGINS", "http://localhost:3000"),
            ],
            || {
                // Arrange
                let config = BootConfig::default();

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_ok());
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_public_http_ws_allowed_origin() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_WS_ALLOWED_ORIGINS", "http://app.example.com"),
            ],
            || {
                // Arrange
                let config = BootConfig::default();

                // Act
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_WS_ALLOWED_ORIGINS entries must use https"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_read_storage_memtable_bytes_from_environment() {
        with_storage_env(&[("FITZ_STORAGE_MEMTABLE_BYTES", "8388608")], || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);

            // Assert
            assert_eq!(
                config.storage_memtable,
                StorageMemtableConfig::Bytes(8 * 1024 * 1024)
            );
            assert_eq!(config.storage_memtable_bytes(), Some(8 * 1024 * 1024));
            assert!(config.validate().is_ok());
        });
    }

    #[test]
    #[serial]
    fn should_reject_zero_storage_memtable_bytes() {
        with_storage_env(&[("FITZ_STORAGE_MEMTABLE_BYTES", "0")], || {
            // Arrange

            // Act
            let config = BootConfig::default();
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_STORAGE_MEMTABLE_BYTES must be greater than 0"));
        });
    }

    #[test]
    #[serial]
    fn should_reject_invalid_storage_memtable_bytes() {
        with_storage_env(&[("FITZ_STORAGE_MEMTABLE_BYTES", "small")], || {
            // Arrange

            // Act
            let config = BootConfig::default();
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_STORAGE_MEMTABLE_BYTES must be an unsigned integer byte count"));
        });
    }

    #[test]
    fn should_customize_stream_storage_layout() {
        // Arrange

        // Act
        let config =
            BootConfig::new().with_stream_storage_layout(StreamStorageLayout::PromotionFrontier);

        // Assert
        assert_eq!(
            config.stream_storage_layout,
            StreamStorageLayout::PromotionFrontier
        );
    }

    #[test]
    fn should_normalize_legacy_stream_storage_layout_given_explicit_boot_config() {
        // Arrange

        // Act
        let config =
            BootConfig::new().with_stream_storage_layout(StreamStorageLayout::LegacyCovering);

        // Assert
        assert_eq!(
            config.stream_storage_layout,
            StreamStorageLayout::PromotionFrontier
        );
    }

    #[test]
    #[serial]
    fn should_accept_contiguous_route_family_allowlist() {
        with_clean_config_env(|| {
            // Arrange
            let config = BootConfig::new()
                .with_auth_config(crate::auth::AuthConfig::Disabled)
                .with_route_families(vec![1, 2, 3]);

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_ok());
        });
    }

    #[test]
    #[serial]
    fn should_reject_empty_route_family_allowlist() {
        with_clean_config_env(|| {
            // Arrange
            let config = BootConfig::new()
                .with_auth_config(crate::auth::AuthConfig::Disabled)
                .with_route_families(Vec::new());

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
        });
    }

    #[test]
    #[serial]
    fn should_reject_gapped_route_family_allowlist() {
        with_clean_config_env(|| {
            // Arrange
            let config = BootConfig::new()
                .with_auth_config(crate::auth::AuthConfig::Disabled)
                .with_route_families(vec![1, 3]);

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
        });
    }

    #[test]
    #[serial]
    fn should_read_route_family_identity_map_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_ROUTE_FAMILIES", "1,2,3"),
                ("FITZ_ROUTE_FAMILY_MAP", "abc=1,xyz=2,zzz=3"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);

                // Assert
                assert_eq!(config.route_family_resolver.mappings.get("abc"), Some(&1));
                assert_eq!(config.route_family_resolver.mappings.get("xyz"), Some(&2));
                assert_eq!(config.route_family_resolver.mappings.get("zzz"), Some(&3));
                assert!(config.validate().is_ok());
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_duplicate_route_family_identity_mapping() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_ROUTE_FAMILY_MAP", "abc=1,abc=2"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("duplicate identity"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_zero_route_family_identity_mapping() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_ROUTE_FAMILY_MAP", "abc=0"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result.unwrap_err().to_string().contains("route family 0"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_invalid_route_family_identity_mapping_integer() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_ROUTE_FAMILY_MAP", "abc=two"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("must be an unsigned integer"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_unprovisioned_route_family_identity_mapping() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_ROUTE_FAMILIES", "1"),
                ("FITZ_ROUTE_FAMILY_MAP", "xyz=2"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result.unwrap_err().to_string().contains("unprovisioned"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_removed_legacy_route_family_env() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_AUTH_ALLOW_JWT_ROUTE_FAMILY", "true"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_AUTH_ALLOW_JWT_ROUTE_FAMILY has been removed"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_removed_custom_claim_alias_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_AUTH_CUSTOM_CLAIM", "fitz"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_AUTH_CUSTOM_CLAIM=fitz is no longer supported"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_read_org_claim_override_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_AUTH_ORG_CLAIM", "fitz://org_id"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);

                // Assert
                assert_eq!(
                    config.auth_claims_config.org_claim_override.as_deref(),
                    Some("fitz://org_id")
                );
                assert_eq!(
                    config.route_family_resolver.org_claim_override.as_deref(),
                    Some("fitz://org_id")
                );
            },
        );
    }

    #[test]
    #[serial]
    fn should_read_permissions_claim_override_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_AUTH_PERMISSIONS_CLAIM", "fitz://permissions"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);

                // Assert
                assert_eq!(
                    config
                        .auth_claims_config
                        .permissions_claim_override
                        .as_deref(),
                    Some("fitz://permissions")
                );
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_invalid_override_collisions_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_AUTH_CUSTOM_CLAIM", "fitz://permissions"),
                ("FITZ_AUTH_PERMISSIONS_CLAIM", "fitz://permissions"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_AUTH_PERMISSIONS_CLAIM must not match FITZ_AUTH_CUSTOM_CLAIM"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_overlapping_role_claim_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_ROUTE_FAMILY_CLAIM", "tid"),
                ("FITZ_AUTH_ROLE_CLAIM", "tid"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("FITZ_AUTH_ROLE_CLAIM must not match FITZ_ROUTE_FAMILY_CLAIM"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_reserved_role_claim_from_environment() {
        with_auth_env(
            &[
                ("FITZ_AUTH_REQUIRED", "false"),
                ("FITZ_AUTH_ROLE_CLAIM", "scope"),
            ],
            || {
                // Arrange

                // Act
                let config =
                    BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result.unwrap_err().to_string().contains(
                    "FITZ_AUTH_ROLE_CLAIM must not overlap with top-level permission sources"
                ));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_cloud_mode_without_provider() {
        with_storage_env(&[("FITZ_STORAGE_MODE", "cloud")], || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("cloud storage requires FITZ_STORAGE_PROVIDER"));
        });
    }

    #[test]
    #[serial]
    fn should_reject_blank_provider_given_cloud_mode() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "   "),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("cloud storage requires FITZ_STORAGE_PROVIDER"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_parse_peas_s3_defaults_given_explicit_provider() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "peas-s3"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert_eq!(cloud.provider_name, "peas-s3");
                assert_eq!(cloud.local_cache_path, DEFAULT_CLOUD_CACHE_PATH);
                assert_eq!(cloud.prefix, None);
                match &cloud.provider_config {
                    cntryl_midge::CloudProviderConfig::S3Compatible {
                        bucket,
                        endpoint,
                        path_style,
                        credentials,
                        ..
                    } => {
                        assert_eq!(bucket, DEFAULT_PEAS_BUCKET);
                        assert_eq!(endpoint, DEFAULT_PEAS_ENDPOINT);
                        assert!(*path_style);
                        assert!(matches!(
                            credentials,
                            cntryl_midge::S3CredentialSource::Static { access_key, secret_key, .. }
                                if access_key == DEFAULT_PEAS_ACCESS_KEY
                                    && secret_key == DEFAULT_PEAS_SECRET_KEY
                        ));
                    }
                    other => panic!("expected Peas S3-compatible config, got {other:?}"),
                }
            },
        );
    }

    #[test]
    #[serial]
    fn should_parse_peas_azure_defaults_given_explicit_provider() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "peas-azure"),
                ("FITZ_STORAGE_BUCKET", "ignored-by-azure"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert_eq!(cloud.provider_name, "peas-azure");
                assert!(matches!(
                    &cloud.provider_config,
                    cntryl_midge::CloudProviderConfig::AzureBlob {
                        account,
                        container,
                        credential: cntryl_midge::AzureCredentialSource::SharedKey { account_key },
                        ..
                    } if account == DEFAULT_PEAS_ACCESS_KEY
                        && container == DEFAULT_PEAS_BUCKET
                        && account_key == DEFAULT_PEAS_SECRET_KEY
                ));
            },
        );
    }

    #[test]
    #[serial]
    fn should_parse_peas_gcs_defaults_given_explicit_provider() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "peas-gcs"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert_eq!(cloud.provider_name, "peas-gcs");
                assert!(matches!(
                    &cloud.provider_config,
                    cntryl_midge::CloudProviderConfig::Gcs {
                        bucket,
                        project_id,
                        api: cntryl_midge::GcsApiStyle::Xml,
                        credential: cntryl_midge::GcsCredentialSource::HmacKey { access_id, secret, },
                        ..
                    } if bucket == DEFAULT_PEAS_BUCKET
                        && project_id == "peas"
                        && access_id == DEFAULT_PEAS_ACCESS_KEY
                        && secret == DEFAULT_PEAS_SECRET_KEY
                ));
            },
        );
    }

    #[test]
    #[serial]
    fn should_parse_aws_s3_provider_given_cloud_env() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "aws-s3"),
                ("FITZ_STORAGE_BUCKET", "fitz-prod"),
                ("FITZ_STORAGE_REGION", "us-west-2"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert_eq!(cloud.provider_name, "aws-s3");
                assert!(matches!(
                    &cloud.provider_config,
                    cntryl_midge::CloudProviderConfig::AwsS3 {
                        bucket,
                        region,
                        credentials: cntryl_midge::S3CredentialSource::AwsDefaultChain,
                    } if bucket == "fitz-prod" && region == "us-west-2"
                ));
            },
        );
    }

    #[test]
    #[serial]
    fn should_parse_s3_compatible_provider_given_cloud_env() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "s3-compatible"),
                ("FITZ_STORAGE_BUCKET", "fitz-dev"),
                ("FITZ_STORAGE_ENDPOINT", "http://objects:9000"),
                ("FITZ_STORAGE_REGION", "us-east-2"),
                ("FITZ_STORAGE_FORCE_PATH_STYLE", "false"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert!(matches!(
                    &cloud.provider_config,
                    cntryl_midge::CloudProviderConfig::S3Compatible {
                        bucket,
                        region,
                        endpoint,
                        path_style: false,
                        credentials: cntryl_midge::S3CredentialSource::Environment,
                    } if bucket == "fitz-dev"
                        && region == "us-east-2"
                        && endpoint == "http://objects:9000"
                ));
            },
        );
    }

    #[test]
    #[serial]
    fn should_parse_s3_family_vendor_providers_given_cloud_env() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "minio"),
                ("FITZ_STORAGE_BUCKET", "fitz-minio"),
                ("FITZ_STORAGE_ENDPOINT", "http://minio:9000"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert!(matches!(
                    &cloud.provider_config,
                    cntryl_midge::CloudProviderConfig::Minio { bucket, endpoint, .. }
                        if bucket == "fitz-minio" && endpoint == "http://minio:9000"
                ));
            },
        );

        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "wasabi"),
                ("FITZ_STORAGE_BUCKET", "fitz-wasabi"),
                ("FITZ_STORAGE_REGION", "us-east-1"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert!(matches!(
                    &cloud.provider_config,
                    cntryl_midge::CloudProviderConfig::Wasabi { bucket, region, .. }
                        if bucket == "fitz-wasabi" && region == "us-east-1"
                ));
            },
        );

        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "oci-s3"),
                ("FITZ_STORAGE_NAMESPACE", "fitzns"),
                ("FITZ_STORAGE_BUCKET", "fitz-oci"),
                ("FITZ_STORAGE_REGION", "us-phoenix-1"),
                ("FITZ_STORAGE_FORCE_PATH_STYLE", "true"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert!(matches!(
                    &cloud.provider_config,
                    cntryl_midge::CloudProviderConfig::OciS3Compatible {
                        bucket,
                        namespace,
                        region,
                        path_style: true,
                        ..
                    } if bucket == "fitz-oci"
                        && namespace == "fitzns"
                        && region == "us-phoenix-1"
                ));
            },
        );
    }

    #[test]
    #[serial]
    fn should_parse_azure_blob_provider_given_cloud_env() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "azure-blob"),
                ("FITZ_STORAGE_CONTAINER", "fitz-container"),
                ("AZURE_STORAGE_ACCOUNT_NAME", "fitzaccount"),
                ("AZURE_STORAGE_ACCOUNT_KEY", "account-key"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert!(matches!(
                    &cloud.provider_config,
                    cntryl_midge::CloudProviderConfig::AzureBlob {
                        account,
                        container,
                        credential: cntryl_midge::AzureCredentialSource::SharedKey { account_key },
                        ..
                    } if account == "fitzaccount"
                        && container == "fitz-container"
                        && account_key == "account-key"
                ));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_azure_blob_bucket_alias_given_missing_container() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "azure-blob"),
                ("FITZ_STORAGE_BUCKET", "fitz-bucket"),
                ("AZURE_STORAGE_ACCOUNT_NAME", "fitzaccount"),
                ("AZURE_STORAGE_ACCOUNT_KEY", "account-key"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("azure-blob storage requires FITZ_STORAGE_CONTAINER"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_azure_blob_account_alias_given_missing_azure_account_name() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "azure-blob"),
                ("FITZ_STORAGE_CONTAINER", "fitz-container"),
                ("FITZ_STORAGE_ACCOUNT", "fitzaccount"),
                ("AZURE_STORAGE_ACCOUNT_KEY", "account-key"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("AZURE_STORAGE_ACCOUNT_NAME"));
            },
        );
    }

    #[test]
    #[serial]
    fn should_parse_gcs_provider_given_cloud_env() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "gcs"),
                ("FITZ_STORAGE_BUCKET", "fitz-gcs"),
                ("GOOGLE_APPLICATION_CREDENTIALS", "/var/run/gcp.json"),
                ("GOOGLE_CLOUD_PROJECT", "fitz-project"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert!(matches!(
                    &cloud.provider_config,
                    cntryl_midge::CloudProviderConfig::Gcs {
                        bucket,
                        project_id,
                        credential: cntryl_midge::GcsCredentialSource::ServiceAccountJsonFile { path },
                        ..
                    } if bucket == "fitz-gcs"
                        && project_id == "fitz-project"
                        && path == std::path::Path::new("/var/run/gcp.json")
                ));
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_missing_required_cloud_fields_given_real_provider() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "aws-s3"),
                ("FITZ_STORAGE_REGION", "us-east-1"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let result = config.validate();

                // Assert
                assert!(result.is_err());
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_old_cloud_mode_aliases_given_vendor_in_storage_mode() {
        with_storage_env(&[("FITZ_STORAGE_MODE", "s3")], || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_STORAGE_MODE=s3 is no longer supported"));
        });
    }

    #[test]
    #[serial]
    fn should_ignore_storage_path_given_cloud_cache_path() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "peas-s3"),
                ("FITZ_STORAGE_BUCKET", "   "),
                ("FITZ_STORAGE_PATH", "/legacy/path"),
                ("FITZ_STORAGE_CACHE_PATH", "/cache/path"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let cloud = cloud_config(&config);

                // Assert
                assert_eq!(cloud.local_cache_path, "/cache/path");
                assert_eq!(
                    cloud.provider_config.bucket_or_container(),
                    DEFAULT_PEAS_BUCKET
                );
            },
        );
    }

    #[test]
    #[serial]
    fn should_map_cloud_durability_to_write_options() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "peas-s3"),
                ("FITZ_STORAGE_CLOUD_DURABILITY", "strict"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();

                // Assert
                assert!(config.server_write_options().is_cloud_strict());
                assert!(config.request_sync_write_options().is_cloud_strict());
            },
        );
    }

    #[test]
    #[serial]
    fn should_accept_background_cloud_durability() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "peas-s3"),
                ("FITZ_STORAGE_CLOUD_DURABILITY", "background"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new().with_auth_config(crate::auth::AuthConfig::Disabled);
                let result = config.validate();

                // Assert
                assert!(result.is_ok());
                assert_eq!(config.cloud_durability, CloudDurabilityMode::Background);
                assert!(!config.server_write_options().is_cloud_strict());
            },
        );
    }

    #[test]
    #[serial]
    fn should_reject_invalid_cloud_durability() {
        with_storage_env(
            &[
                ("FITZ_STORAGE_MODE", "cloud"),
                ("FITZ_STORAGE_PROVIDER", "peas-s3"),
                ("FITZ_STORAGE_CLOUD_DURABILITY", "stict"),
            ],
            || {
                // Arrange

                // Act
                let config = BootConfig::new();
                let result = config.validate();

                // Assert
                assert!(result.is_err());
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("unsupported FITZ_STORAGE_CLOUD_DURABILITY='stict'"));
            },
        );
    }

    #[test]
    fn should_keep_non_cloud_sync_write_options_local() {
        // Arrange
        let config = BootConfig::with_local_storage("/data/fitz");

        // Act
        let write_options = config.request_sync_write_options();

        // Assert
        assert!(write_options.is_sync());
        assert!(!write_options.is_cloud_strict());
    }
}
