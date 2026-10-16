use serde::Serialize;
use std::path::PathBuf;

#[derive(Clone, Serialize)]
pub struct Config {
    pub targets: Vec<u64>,
    pub batch_events: u64,
    pub payload_bytes: usize,
    pub stage_seconds: u64,
}

fn value(name: &str, fallback: u64, min: u64, max: u64) -> Result<u64, String> {
    let raw = std::env::var(name).unwrap_or_else(|_| fallback.to_string());
    let value = raw.parse::<u64>().map_err(|e| e.to_string())?;
    if !(min..=max).contains(&value) {
        return Err(format!("{name} must be {min}..={max}"));
    }
    Ok(value)
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let raw = std::env::var("FITZ_STREAM_PRESSURE_TARGETS")
            .unwrap_or_else(|_| "100,1000,10000,100000".into());
        let targets = raw
            .split(',')
            .map(str::parse::<u64>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        let payload_bytes =
            usize::try_from(value("FITZ_STREAM_PRESSURE_PAYLOAD_BYTES", 1024, 8, 32768)?)
                .map_err(|e| e.to_string())?;
        if targets.is_empty()
            || targets.len() > 16
            || targets.iter().any(|n| *n < 2 || *n > 1_000_000)
            || targets.windows(2).any(|n| n[0] >= n[1])
            || (targets.last().copied().unwrap_or(0) + 2)
                * u64::try_from(payload_bytes).map_err(|e| e.to_string())?
                > 512 * 1024 * 1024
        {
            return Err("targets must increase in 2..=1000000, at most 16 stages, <=512MiB retained payload including recovery probes".into());
        }
        Ok(Self {
            targets,
            payload_bytes,
            batch_events: value("FITZ_STREAM_PRESSURE_BATCH_EVENTS", 256, 1, 16384)?,
            stage_seconds: value("FITZ_STREAM_PRESSURE_STAGE_SECS", 300, 1, 600)?,
        })
    }
}

#[derive(Clone, Default, Serialize)]
pub struct Stage {
    pub target_events: u64,
    pub attempted_appends: u64,
    pub accepted_appends: u64,
    pub committed_events: u64,
    pub committed_batches: u64,
    pub commit_requests: u64,
    pub pending_batch_events: u64,
    pub rejection: Option<String>,
    pub active_elapsed_ns: u128,
    pub replay_elapsed_ns: u128,
    pub replayed_events: u64,
    pub failure: Option<String>,
}

#[derive(Serialize)]
pub struct Report {
    pub schema: &'static str,
    pub source_sha: Option<String>,
    pub source_dirty: Option<bool>,
    pub config: Config,
    pub storage_scope: &'static str,
    pub status: &'static str,
    pub cleanup_status: &'static str,
    pub failure: Option<String>,
    pub cleanup_failure: Option<String>,
    pub storage_path: PathBuf,
    pub stages: Vec<Stage>,
    pub current: Option<Stage>,
    pub committed_events: u64,
    pub probe_events: u64,
    pub clean_restart_verified_events: u64,
    pub recovery_probe_passed: bool,
    #[serde(skip)]
    pub path: PathBuf,
}

impl Report {
    pub fn new(config: Config, storage_path: PathBuf) -> Result<Self, String> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos();
        let directory = PathBuf::from("target/fitz-stress/stream-pressure");
        std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
        Ok(Self {
            schema: "fitz.stream-pressure.v1",
            source_sha: git(&["rev-parse", "HEAD"]).map(|s| s.trim().to_owned()),
            source_dirty: git(&["status", "--porcelain"]).map(|s| !s.is_empty()),
            config,
            storage_path,
            storage_scope: "local_disk_sync_commit_clean_restart",
            status: "running",
            cleanup_status: "not_started",
            failure: None,
            cleanup_failure: None,
            stages: Vec::new(),
            current: None,
            committed_events: 0,
            probe_events: 0,
            clean_restart_verified_events: 0,
            recovery_probe_passed: false,
            path: directory.join(format!("{stamp}.json")),
        })
    }

    pub fn save(&self) -> Result<(), String> {
        let data = serde_json::to_vec_pretty(self).map_err(|e| e.to_string())?;
        let temporary = self.path.with_extension("tmp");
        std::fs::write(&temporary, data).map_err(|e| e.to_string())?;
        std::fs::rename(temporary, &self.path).map_err(|e| e.to_string())
    }
}

fn git(args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8(output.stdout).ok())
        .flatten()
}
