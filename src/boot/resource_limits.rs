//! Startup resource-limit checks.

use super::BootResult;
use std::path::{Path, PathBuf};

const DEFAULT_MIN_MEMORY_BYTES: u64 = 128 * 1024 * 1024;
const CGROUP_V1_UNLIMITED_THRESHOLD: u64 = 1 << 60;
const ENV_MIN_MEMORY_BYTES: &str = "FITZ_MIN_MEMORY_BYTES";

#[derive(Debug, Clone, PartialEq, Eq)]
struct CgroupMemoryLimit {
    bytes: u64,
    source: PathBuf,
}

/// Reject known-impossible container memory limits before heavier broker startup.
pub fn enforce_startup_resource_limits() -> BootResult<()> {
    let minimum = minimum_memory_limit_bytes_from_env()?;
    if minimum == 0 {
        return Ok(());
    }

    if let Some(limit) = detect_cgroup_memory_limit(Path::new("/sys/fs/cgroup")) {
        if limit.bytes < minimum {
            return Err(format!(
                "Fitz cgroup memory limit is too low: detected {} bytes from {}; minimum is {} bytes. Increase the container memory limit or set {}=0 to bypass this startup preflight.",
                limit.bytes,
                limit.source.display(),
                minimum,
                ENV_MIN_MEMORY_BYTES
            )
            .into());
        }
    }

    Ok(())
}

fn minimum_memory_limit_bytes_from_env() -> BootResult<u64> {
    let Some(raw) = std::env::var(ENV_MIN_MEMORY_BYTES)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return Ok(DEFAULT_MIN_MEMORY_BYTES);
    };

    raw.parse::<u64>().map_err(|_| {
        format!(
            "{} must be an unsigned integer byte count, got '{}'",
            ENV_MIN_MEMORY_BYTES, raw
        )
        .into()
    })
}

fn detect_cgroup_memory_limit(cgroup_root: &Path) -> Option<CgroupMemoryLimit> {
    let v2_controller_path = cgroup_root.join("cgroup.controllers");
    if v2_controller_path.exists() {
        return read_cgroup_limit_file(cgroup_root.join("memory.max"), true);
    }

    read_cgroup_limit_file(cgroup_root.join("memory/memory.limit_in_bytes"), false)
}

fn read_cgroup_limit_file(path: PathBuf, allow_max: bool) -> Option<CgroupMemoryLimit> {
    let value = std::fs::read_to_string(&path).ok()?;
    let trimmed = value.trim();
    if allow_max && trimmed.eq_ignore_ascii_case("max") {
        return None;
    }

    let bytes = trimmed.parse::<u64>().ok()?;
    if bytes == 0 || bytes >= CGROUP_V1_UNLIMITED_THRESHOLD {
        return None;
    }

    Some(CgroupMemoryLimit {
        bytes,
        source: path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use tempfile::TempDir;

    fn with_env_var<T>(key: &str, value: Option<&str>, test: impl FnOnce() -> T) -> T {
        let previous = std::env::var(key).ok();
        match value {
            Some(value) => unsafe {
                std::env::set_var(key, value);
            },
            None => unsafe {
                std::env::remove_var(key);
            },
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

    #[test]
    #[serial]
    fn should_use_default_minimum_memory_limit_when_env_is_absent() {
        with_env_var(ENV_MIN_MEMORY_BYTES, None, || {
            // Arrange

            // Act
            let minimum = minimum_memory_limit_bytes_from_env().expect("minimum");

            // Assert
            assert_eq!(minimum, DEFAULT_MIN_MEMORY_BYTES);
        });
    }

    #[test]
    #[serial]
    fn should_parse_minimum_memory_limit_from_env() {
        with_env_var(ENV_MIN_MEMORY_BYTES, Some("8388608"), || {
            // Arrange

            // Act
            let minimum = minimum_memory_limit_bytes_from_env().expect("minimum");

            // Assert
            assert_eq!(minimum, 8 * 1024 * 1024);
        });
    }

    #[test]
    #[serial]
    fn should_reject_invalid_minimum_memory_limit_env() {
        with_env_var(ENV_MIN_MEMORY_BYTES, Some("tiny"), || {
            // Arrange

            // Act
            let result = minimum_memory_limit_bytes_from_env();

            // Assert
            assert!(result.is_err());
        });
    }

    #[test]
    fn should_detect_cgroup_v2_memory_limit() {
        // Arrange
        let tempdir = TempDir::new().expect("tempdir");
        std::fs::write(tempdir.path().join("cgroup.controllers"), "memory\n")
            .expect("write controllers");
        std::fs::write(tempdir.path().join("memory.max"), "8388608\n").expect("write memory.max");

        // Act
        let limit = detect_cgroup_memory_limit(tempdir.path()).expect("limit");

        // Assert
        assert_eq!(limit.bytes, 8 * 1024 * 1024);
    }

    #[test]
    fn should_ignore_unlimited_cgroup_v2_memory_limit() {
        // Arrange
        let tempdir = TempDir::new().expect("tempdir");
        std::fs::write(tempdir.path().join("cgroup.controllers"), "memory\n")
            .expect("write controllers");
        std::fs::write(tempdir.path().join("memory.max"), "max\n").expect("write memory.max");

        // Act
        let limit = detect_cgroup_memory_limit(tempdir.path());

        // Assert
        assert_eq!(limit, None);
    }

    #[test]
    fn should_detect_cgroup_v1_memory_limit() {
        // Arrange
        let tempdir = TempDir::new().expect("tempdir");
        let memory_dir = tempdir.path().join("memory");
        std::fs::create_dir_all(&memory_dir).expect("create memory dir");
        std::fs::write(memory_dir.join("memory.limit_in_bytes"), "16777216\n")
            .expect("write v1 limit");

        // Act
        let limit = detect_cgroup_memory_limit(tempdir.path()).expect("limit");

        // Assert
        assert_eq!(limit.bytes, 16 * 1024 * 1024);
    }
}
