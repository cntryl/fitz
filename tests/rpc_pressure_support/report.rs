use serde::Serialize;
use std::path::PathBuf;

#[derive(Default, Serialize)]
pub struct Stage {
    pub offered: usize,
    pub attempted: usize,
    pub sent: usize,
    pub indeterminate_writes: usize,
    pub completed: usize,
    pub backpressure: usize,
    pub worker_dispatches: usize,
    pub unresolved: usize,
    pub elapsed_ns: u128,
    pub failure: Option<String>,
    pub worker_failure: Option<String>,
}

#[derive(Serialize)]
pub struct Report {
    pub schema: &'static str,
    pub source_sha: Option<String>,
    pub source_dirty: Option<bool>,
    pub scope: &'static str,
    pub transport: &'static str,
    pub storage_mode: &'static str,
    pub status: &'static str,
    pub producer_batch: usize,
    pub payload_bytes: usize,
    pub stage_deadline_seconds: u64,
    pub lifecycle_deadline_seconds: u64,
    pub startup_shutdown_deadline_seconds: u64,
    pub planned_bursts: Vec<usize>,
    pub admin_snapshot_authority: &'static str,
    pub advisory_workers_after_sessions_closed: Option<usize>,
    pub advisory_pending_after_sessions_closed: Option<usize>,
    pub stages: Vec<Stage>,
    pub worker_credit: u32,
    pub hold_ms: u64,
    pub service_delay_ms: u64,
    pub request_budget_ms: u32,
    pub recovery_passed: bool,
    pub lifecycle_probes_passed: bool,
    pub cleanup_passed: bool,
    pub failure: Option<String>,
    pub cleanup_failure: Option<String>,
    pub artifact_failure: Option<String>,
    #[serde(skip)]
    pub path: PathBuf,
}

impl Report {
    pub fn new() -> Result<Self, String> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_nanos();
        let directory = PathBuf::from("target/fitz-stress/rpc-pressure");
        std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
        Ok(Self {
            schema: "fitz.rpc-pressure.v1",
            source_sha: git(&["rev-parse", "HEAD"]),
            source_dirty: git(&["status", "--porcelain"]).map(|s| !s.is_empty()),
            scope: "running_process_live_rpc_no_retry_or_durability",
            transport: "tcp",
            storage_mode: "memory",
            status: "running",
            producer_batch: 128,
            payload_bytes: 1024,
            stage_deadline_seconds: 60,
            lifecycle_deadline_seconds: 10,
            startup_shutdown_deadline_seconds: 60,
            planned_bursts: Vec::new(),
            admin_snapshot_authority: "advisory_only_refresh_may_fail_or_be_stale",
            advisory_workers_after_sessions_closed: None,
            advisory_pending_after_sessions_closed: None,
            stages: Vec::new(),
            worker_credit: 1,
            hold_ms: 1000,
            service_delay_ms: 1,
            request_budget_ms: 30000,
            recovery_passed: false,
            lifecycle_probes_passed: false,
            cleanup_passed: false,
            failure: None,
            cleanup_failure: None,
            artifact_failure: None,
            path: directory.join(format!("{stamp}.json")),
        })
    }

    pub fn save(&self) -> Result<(), String> {
        let bytes = serde_json::to_vec_pretty(self).map_err(|e| e.to_string())?;
        let temporary = self.path.with_extension("tmp");
        std::fs::write(&temporary, bytes).map_err(|e| e.to_string())?;
        std::fs::rename(temporary, &self.path).map_err(|e| e.to_string())
    }
}

fn git(args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
