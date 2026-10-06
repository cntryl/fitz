use crate::stress_support::types::BenchFailure;
use fitz::testkit::TestServer;
use std::net::SocketAddr;
use std::path::Path;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) enum StorageProfile {
    #[default]
    LocalDisk,
    Memory,
}

impl StorageProfile {
    pub(crate) fn from_env() -> Result<Self, BenchFailure> {
        match std::env::var("FITZ_STRESS_STORAGE_PROFILE") {
            Ok(value) => match value.as_str() {
                "local_disk" => Ok(Self::LocalDisk),
                "memory" => Ok(Self::Memory),
                _ => Err(BenchFailure::validation(
                    "FITZ_STRESS_STORAGE_PROFILE must be local_disk or memory",
                )),
            },
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(std::env::VarError::NotUnicode(_)) => Err(BenchFailure::validation(
                "FITZ_STRESS_STORAGE_PROFILE must contain Unicode text",
            )),
        }
    }

    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::LocalDisk => "local_disk",
            Self::Memory => "memory",
        }
    }

    pub(crate) const fn durability_scope(self) -> &'static str {
        match self {
            Self::LocalDisk => "running_process_fast_local_disk",
            Self::Memory => "running_process_memory_fixture",
        }
    }
}

pub(crate) struct StorageDirectory {
    directory: Option<tempfile::TempDir>,
}

impl StorageDirectory {
    pub(crate) fn new(profile: StorageProfile) -> Result<Self, BenchFailure> {
        let directory = match profile {
            StorageProfile::LocalDisk => Some(
                tempfile::Builder::new()
                    .prefix("fitz-stress-")
                    .tempdir()
                    .map_err(BenchFailure::transport)?,
            ),
            StorageProfile::Memory => None,
        };
        Ok(Self { directory })
    }

    pub(crate) fn path(&self) -> Option<&Path> {
        self.directory.as_ref().map(tempfile::TempDir::path)
    }

    pub(crate) fn release(mut self) -> Result<(), BenchFailure> {
        if let Some(directory) = self.directory.take() {
            let path = directory.path().to_path_buf();
            directory.close().map_err(|error| {
                BenchFailure::transport(format!(
                    "benchmark storage cleanup failed at {}: {error}",
                    path.display()
                ))
            })?;
        }
        Ok(())
    }
}

impl Drop for StorageDirectory {
    fn drop(&mut self) {
        if let Some(directory) = self.directory.take() {
            // Cancellation and errors leave storage intact unless shutdown was confirmed.
            let _ = directory.keep();
        }
    }
}

pub(crate) struct BrokerFixture {
    server: TestServer,
    storage_directory: StorageDirectory,
}

impl BrokerFixture {
    pub(crate) async fn start(
        profile: StorageProfile,
        storage_directory: StorageDirectory,
    ) -> Result<Self, BenchFailure> {
        let server = match profile {
            StorageProfile::LocalDisk => {
                let path = storage_directory
                    .path()
                    .and_then(Path::to_str)
                    .ok_or_else(|| {
                        BenchFailure::validation("benchmark storage path must contain Unicode text")
                    })?;
                TestServer::start_with_local_storage(path)
                    .await
                    .map_err(BenchFailure::transport)?
            }
            StorageProfile::Memory => TestServer::start_with_write_heavy_memory()
                .await
                .map_err(BenchFailure::transport)?,
        };
        Ok(Self {
            server,
            storage_directory,
        })
    }

    pub(crate) const fn tcp_addr(&self) -> SocketAddr {
        self.server.tcp_addr
    }

    pub(crate) fn into_parts(self) -> (TestServer, StorageDirectory) {
        let Self {
            server,
            storage_directory,
        } = self;
        (server, storage_directory)
    }
}
