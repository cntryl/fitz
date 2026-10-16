use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

pub(super) const MAX_AUDIT_FILE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_AUDIT_RECORD_BYTES: usize = 2 * 1024;

#[derive(Serialize)]
struct PersistedActionAudit<'a> {
    timestamp_epoch_ms: u128,
    operation_id: &'a str,
    principal: &'a str,
    action: &'a str,
    target: &'a str,
    phase: &'a str,
}

pub(super) struct McpActionAuditSink {
    path: PathBuf,
    pub(super) file: parking_lot::Mutex<File>,
}

impl McpActionAuditSink {
    pub(super) fn open(path: PathBuf) -> Result<Self, String> {
        if !path.is_absolute() {
            return Err("MCP action audit path must be absolute".into());
        }
        if let Ok(metadata) = std::fs::symlink_metadata(&path) {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err("MCP action audit path must name a regular file".into());
            }
        } else if !path.parent().is_some_and(Path::is_dir) {
            return Err("MCP action audit parent must be an existing directory".into());
        }
        let mut options = OpenOptions::new();
        options.create(true).append(true).read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let file = options
            .open(&path)
            .map_err(|error| format!("could not open MCP action audit file: {error}"))?;
        let metadata = file
            .metadata()
            .map_err(|error| format!("could not inspect MCP action audit file: {error}"))?;
        if !metadata.is_file() {
            return Err("MCP action audit path must name a regular file".into());
        }
        if metadata.len() > MAX_AUDIT_FILE_BYTES {
            return Err("MCP action audit file exceeds its configured size limit".into());
        }
        restrict_audit_permissions(&file)?;
        Ok(Self {
            path,
            file: parking_lot::Mutex::new(file),
        })
    }

    pub(super) fn append(
        &self,
        operation_id: &str,
        principal: &str,
        action: &str,
        target: &str,
        phase: &str,
    ) -> bool {
        let Some(timestamp_epoch_ms) = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_millis())
        else {
            return false;
        };
        let record = PersistedActionAudit {
            timestamp_epoch_ms,
            operation_id,
            principal,
            action,
            target,
            phase,
        };
        let Ok(mut bytes) = serde_json::to_vec(&record) else {
            return false;
        };
        if bytes.len() > MAX_AUDIT_RECORD_BYTES {
            return false;
        }
        bytes.push(b'\n');
        let mut file = self.file.lock();
        let Ok(current_size) = file.metadata().map(|metadata| metadata.len()) else {
            return false;
        };
        if current_size.saturating_add(bytes.len() as u64) > MAX_AUDIT_FILE_BYTES {
            tracing::error!(path = %self.path.display(), "MCP action audit capacity reached");
            return false;
        }
        file.write_all(&bytes)
            .and_then(|()| file.sync_data())
            .is_ok()
    }
}

#[cfg(unix)]
fn restrict_audit_permissions(file: &File) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    file.set_permissions(std::fs::Permissions::from_mode(0o600))
        .map_err(|error| format!("could not restrict MCP action audit permissions: {error}"))
}

#[cfg(not(unix))]
fn restrict_audit_permissions(_file: &File) -> Result<(), String> {
    Ok(())
}
