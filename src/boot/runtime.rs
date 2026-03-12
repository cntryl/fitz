//! Boot result and configuration types

use crate::api::ingress::IngressConfig;
use crate::runtime::Router;
use crate::session::manager::RuntimeIngress;
use std::sync::Arc;
use tracing::info;

pub type BootResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Storage backend configuration
#[derive(Debug, Clone)]
pub enum StorageMode {
    /// In-memory only (no persistence)
    Memory,
    /// Local file-backed storage
    LocalDisk {
        /// Path to database directory
        db_path: String,
    },
    /// Cloud-backed storage (S3 or similar)
    CloudBacked {
        /// Cloud provider (s3, gcs, azure, etc)
        provider: String,
        /// Bucket or container name
        bucket: String,
        /// Optional path prefix within bucket
        prefix: Option<String>,
    },
}

impl Default for StorageMode {
    fn default() -> Self {
        Self::LocalDisk {
            db_path: "./.fitz".to_string(),
        }
    }
}

impl StorageMode {
    /// Detect storage mode from environment variables
    ///
    /// Priority order:
    /// 1. FITZ_STORAGE_MODE env var (memory, local, s3, gcs, azure)
    /// 2. If local: FITZ_STORAGE_PATH
    /// 3. If cloud: FITZ_STORAGE_BUCKET, FITZ_STORAGE_PROVIDER
    /// 4. Default: LocalDisk at "./.fitz"
    pub fn from_env() -> Self {
        let mode = std::env::var("FITZ_STORAGE_MODE")
            .unwrap_or_else(|_| "local".to_string())
            .to_lowercase();

        match mode.as_str() {
            "memory" | "in-memory" | "inmemory" => {
                tracing::info!("Storage: IN-MEMORY (ephemeral, no persistence)");
                Self::Memory
            }
            "local" | "disk" | "file" => {
                let db_path =
                    std::env::var("FITZ_STORAGE_PATH").unwrap_or_else(|_| "./.fitz".to_string());
                tracing::info!("Storage: LOCAL DISK at {}", db_path);
                Self::LocalDisk { db_path }
            }
            "s3" | "gcs" | "azure" | "cloud" => {
                let provider =
                    std::env::var("FITZ_STORAGE_PROVIDER").unwrap_or_else(|_| mode.clone());
                let bucket = std::env::var("FITZ_STORAGE_BUCKET")
                    .expect("FITZ_STORAGE_BUCKET required for cloud storage");
                let prefix = std::env::var("FITZ_STORAGE_PREFIX").ok();

                tracing::info!(
                    "Storage: CLOUD ({}) - bucket={} prefix={:?}",
                    provider,
                    bucket,
                    prefix
                );

                Self::CloudBacked {
                    provider,
                    bucket,
                    prefix,
                }
            }
            _ => {
                tracing::warn!("Unknown storage mode '{}', defaulting to local disk", mode);
                Self::default()
            }
        }
    }
}

/// Boot configuration for the Fitz broker
#[derive(Debug, Clone)]
pub struct BootConfig {
    /// HTTP/WebSocket port
    pub http_port: u16,
    /// TCP port
    pub tcp_port: u16,
    /// Bind address (default: "0.0.0.0")
    pub bind_addr: String,
    /// Storage mode (memory, local disk, or cloud)
    pub storage_mode: StorageMode,
    /// Whether authentication is required (default: true)
    pub auth_required: bool,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Frame size limit in bytes
    pub max_frame_size: usize,
    /// Channel capacity between transport and runtime
    pub channel_capacity: usize,
}

/// Legacy storage_path for backwards compatibility
impl BootConfig {
    pub fn storage_path(&self) -> String {
        match &self.storage_mode {
            StorageMode::LocalDisk { db_path } => db_path.clone(),
            StorageMode::Memory => ":memory:".to_string(),
            StorageMode::CloudBacked { bucket, prefix, .. } => {
                format!(
                    "{}{}",
                    bucket,
                    prefix
                        .as_ref()
                        .map(|p| format!("/{}", p))
                        .unwrap_or_default()
                )
            }
        }
    }
}

impl Default for BootConfig {
    fn default() -> Self {
        // Read FITZ_AUTH_REQUIRED from environment (default: true)
        let auth_required = std::env::var("FITZ_AUTH_REQUIRED")
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        Self {
            http_port: crate::prelude::DEFAULT_HTTP_PORT,
            tcp_port: crate::prelude::DEFAULT_TCP_PORT,
            bind_addr: "0.0.0.0".to_string(),
            storage_mode: StorageMode::from_env(),
            auth_required,
            max_connections: 10_000,
            max_frame_size: 1024 * 1024, // 1 MB (production default; configurable via BootConfig)
            channel_capacity: 1000,
        }
    }
}

impl BootConfig {
    /// Create a new config with defaults (including env var detection)
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new config with in-memory storage (for testing)
    pub fn with_memory_storage() -> Self {
        Self {
            storage_mode: StorageMode::Memory,
            ..Default::default()
        }
    }

    /// Create a new config with local disk storage at path
    pub fn with_local_storage(path: impl Into<String>) -> Self {
        Self {
            storage_mode: StorageMode::LocalDisk {
                db_path: path.into(),
            },
            ..Default::default()
        }
    }

    /// Set HTTP port
    pub fn with_http_port(mut self, port: u16) -> Self {
        self.http_port = port;
        self
    }

    /// Set TCP port
    pub fn with_tcp_port(mut self, port: u16) -> Self {
        self.tcp_port = port;
        self
    }

    /// Set bind address
    pub fn with_bind_addr(mut self, addr: String) -> Self {
        self.bind_addr = addr;
        self
    }

    /// Set storage mode
    pub fn with_storage_mode(mut self, mode: StorageMode) -> Self {
        self.storage_mode = mode;
        self
    }
}

/// Type alias for the complex runtime initialization return type
type RuntimeComponents = (
    Arc<Router>,
    Arc<RuntimeIngress>,
    IngressConfig,
    crate::runtime::Scheduler,
    crate::boot::Runtime,
);

/// Initialize runtime infrastructure
///
/// Creates:
/// - Router for message delivery
/// - RuntimeIngress for session management
/// - IngressConfig for transport configuration
/// - Scheduler for actor execution
/// - Runtime stats tracker for observability
pub fn init(
    config: &BootConfig,
    store: &Arc<cntryl_midge::Engine>,
) -> BootResult<RuntimeComponents> {
    info!("Initializing runtime infrastructure");

    // Create runtime components
    let router = Arc::new(Router::new());
    // Attach router to ingress so frames can be dispatched into domains
    let ingress = Arc::new(
        RuntimeIngress::new(config.auth_required)
            .with_router(router.clone())
            .with_store(store.clone()),
    );

    let ingress_config = IngressConfig::default()
        .with_frame_size(config.max_frame_size)
        .with_channel_capacity(config.channel_capacity);

    // Create scheduler
    let num_workers = num_cpus::get();
    let scheduler = crate::runtime::Scheduler::new(num_workers);

    // Create runtime stats tracker
    let runtime = crate::boot::Runtime::new(router.clone());
    runtime.attach_ingress(ingress.clone());

    info!("Runtime initialized with {} worker threads", num_workers);

    Ok((router, ingress, ingress_config, scheduler, runtime))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude;

    #[test]
    fn should_create_default_boot_config() {
        // Arrange

        // Act
        let config = BootConfig::default();

        // Assert
        assert_eq!(config.tcp_port, prelude::DEFAULT_TCP_PORT);
        assert_eq!(config.http_port, prelude::DEFAULT_HTTP_PORT);
        assert_eq!(config.bind_addr, "0.0.0.0");
        assert_eq!(config.max_connections, 10_000);
    }

    #[test]
    fn should_customize_boot_config() {
        // Arrange

        // Act
        let config = BootConfig::new()
            .with_tcp_port(5091)
            .with_http_port(5090)
            .with_bind_addr("127.0.0.1".to_string());

        // Assert
        assert_eq!(config.tcp_port, 5091);
        assert_eq!(config.http_port, 5090);
        assert_eq!(config.bind_addr, "127.0.0.1");
    }
}
