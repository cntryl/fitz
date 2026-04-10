use super::BootResult;
use crate::domains::stream::StreamStorageLayout;
use std::path::Path;

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
    /// Cloud-backed storage (S3 or similar)
    CloudBacked {
        /// Cloud provider (s3, gcs, azure, etc)
        provider: String,
        /// Bucket or container name
        bucket: String,
        /// Optional path prefix within bucket
        prefix: Option<String>,
        /// Local cache path used by the cloud-backed engine
        local_cache_path: String,
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
    /// Detect storage mode from environment variables.
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
                let bucket = std::env::var("FITZ_STORAGE_BUCKET").unwrap_or_default();
                let prefix = std::env::var("FITZ_STORAGE_PREFIX").ok();
                let local_cache_path = std::env::var("FITZ_STORAGE_PATH")
                    .unwrap_or_else(|_| "./.fitz-cloud-cache".to_string());

                tracing::info!(
                    "Storage: CLOUD ({}) - bucket={} prefix={:?} cache={}",
                    provider,
                    bucket,
                    prefix,
                    local_cache_path
                );

                Self::CloudBacked {
                    provider,
                    bucket,
                    prefix,
                    local_cache_path,
                }
            }
            _ => {
                tracing::warn!("Unknown storage mode '{}', defaulting to local disk", mode);
                Self::default()
            }
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
            StorageMode::CloudBacked {
                provider,
                bucket,
                local_cache_path,
                ..
            } => {
                if provider.trim().is_empty() {
                    return Err("cloud storage requires a provider".to_string());
                }
                if bucket.trim().is_empty() {
                    return Err("cloud storage requires a bucket".to_string());
                }
                if local_cache_path.trim().is_empty()
                    || Path::new(local_cache_path).as_os_str().is_empty()
                {
                    return Err("cloud storage requires a valid local cache path".to_string());
                }
                Ok(())
            }
        }
    }
}

/// Boot configuration for the Fitz broker.
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
    /// Stream storage layout selector
    pub stream_storage_layout: StreamStorageLayout,
    /// Whether authentication is required (default: true)
    pub auth_required: bool,
    /// Explicit auth configuration for token verification
    pub auth_config: crate::auth::AuthConfig,
    /// Maximum concurrent connections
    pub max_connections: usize,
    /// Frame size limit in bytes
    pub max_frame_size: usize,
    /// Channel capacity between transport and runtime
    pub channel_capacity: usize,
}

impl BootConfig {
    pub fn storage_path(&self) -> String {
        match &self.storage_mode {
            StorageMode::LocalDisk { db_path } => db_path.clone(),
            StorageMode::Memory => ":memory:".to_string(),
            StorageMode::CloudBacked {
                local_cache_path, ..
            } => local_cache_path.clone(),
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
        let bind_addr = std::env::var("FITZ_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0".to_string());

        Self {
            http_port,
            tcp_port,
            bind_addr,
            storage_mode: StorageMode::from_env(),
            stream_storage_layout: StreamStorageLayout::from_env(),
            auth_required,
            auth_config: crate::auth::AuthConfig::from_env(auth_required),
            max_connections: 10_000,
            max_frame_size: 1024 * 1024,
            channel_capacity: 1000,
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

    pub fn with_bind_addr(mut self, addr: String) -> Self {
        self.bind_addr = addr;
        self
    }

    pub fn with_storage_mode(mut self, mode: StorageMode) -> Self {
        self.storage_mode = mode;
        self
    }

    pub fn with_stream_storage_layout(mut self, layout: StreamStorageLayout) -> Self {
        self.stream_storage_layout = layout;
        self
    }

    pub fn with_auth_config(mut self, auth_config: crate::auth::AuthConfig) -> Self {
        self.auth_required = !matches!(auth_config, crate::auth::AuthConfig::Disabled);
        self.auth_config = auth_config;
        self
    }

    pub fn validate(&self) -> BootResult<()> {
        self.storage_mode
            .validate()
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
        self.auth_config
            .validate(self.auth_required)
            .map_err(|error| -> Box<dyn std::error::Error> { error.into() })?;
        Ok(())
    }
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
        assert_eq!(
            config.stream_storage_layout,
            StreamStorageLayout::LegacyCovering
        );
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
}
