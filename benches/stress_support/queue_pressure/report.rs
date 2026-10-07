use super::super::{histogram::Latencies, types::BenchFailure};
use super::{config::Config, ledger::Counts};
use serde::Serialize;
use std::path::PathBuf;

#[derive(Clone, Default, Serialize)]
pub(super) struct Stage {
    pub rate_per_second: u64,
    pub offered: u64,
    pub harness_missed: u64,
    pub active_elapsed_ns: u128,
    pub accepted_at_stop: u64,
    pub acknowledged_at_stop: u64,
    pub backlog_at_stop: u64,
    pub pending_enqueue_outcomes_at_stop: u64,
    pub reserve_rejections: u64,
    pub drain_elapsed_ns: u128,
    pub producer_settle_elapsed_ns: u128,
    pub termination: String,
    pub drained: bool,
    pub empty_verified: bool,
    pub cleanup_failure: Option<String>,
    pub enqueue_latencies: Latencies,
    pub p95_enqueue_upper_ns: u64,
    pub accounting: Counts,
    pub process_rss_bytes: Option<u64>,
    pub process_peak_rss_bytes: Option<u64>,
}

#[derive(Serialize)]
pub(super) struct Report {
    pub schema: &'static str,
    pub source_sha: Option<String>,
    pub source_dirty: Option<bool>,
    pub config: Config,
    pub durability_scope: &'static str,
    pub status: &'static str,
    pub failure: Option<String>,
    pub cleanup_status: &'static str,
    pub cleanup_failure: Option<String>,
    pub local_storage_path: Option<PathBuf>,
    pub stages: Vec<Stage>,
    pub current: Option<Stage>,
    pub process_rss_bytes: Option<u64>,
    pub recovery_probe_passed: bool,
    #[serde(skip)]
    pub path: PathBuf,
}

impl Report {
    pub fn new(config: Config) -> Result<Self, BenchFailure> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(BenchFailure::transport)?
            .as_nanos();
        let directory = PathBuf::from("target/fitz-stress/queue-pressure");
        std::fs::create_dir_all(&directory).map_err(BenchFailure::transport)?;
        Ok(Self {
            schema: "fitz.queue-pressure.v1",
            source_sha: git_output(&["rev-parse", "HEAD"]).map(|sha| sha.trim().to_owned()),
            source_dirty: git_output(&["status", "--porcelain"]).map(|status| !status.is_empty()),
            config,
            durability_scope: "running_process_fast_local_disk",
            status: "running",
            failure: None,
            cleanup_status: "not_started",
            cleanup_failure: None,
            local_storage_path: None,
            stages: Vec::new(),
            current: None,
            process_rss_bytes: None,
            recovery_probe_passed: false,
            path: directory.join(format!("{stamp}.json")),
        })
    }

    pub fn save(&self) -> Result<(), BenchFailure> {
        let bytes = serde_json::to_vec_pretty(self).map_err(BenchFailure::transport)?;
        let temporary = self.path.with_extension("tmp");
        std::fs::write(&temporary, bytes).map_err(BenchFailure::transport)?;
        std::fs::rename(temporary, &self.path).map_err(BenchFailure::transport)
    }
}

pub(super) fn git_output(arguments: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git")
        .args(arguments)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8(output.stdout).ok())
        .flatten()
}
