use super::super::histogram::Latencies;
use super::super::types::BenchFailure;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Default, Serialize)]
pub struct Stage {
    pub definitions: usize,
    pub mode: u8,
    pub recovery: bool,
    pub accepted_definitions: u64,
    pub completed_occurrences: u64,
    pub received: [u64; 2],
    pub create_elapsed_ns: u128,
    pub restart_elapsed_ns: u128,
    pub create_latencies: Latencies,
    pub fire_elapsed_ns: u128,
    pub restart_verified: bool,
    pub cancelled_verified: bool,
    pub pending_claims: usize,
    pub pending_ack_retries: usize,
    pub acknowledgement_failures: u64,
    pub process_rss_bytes: Option<u64>,
    pub process_peak_rss_bytes: Option<u64>,
    pub failure: Option<String>,
}

#[derive(Serialize)]
pub struct Report {
    pub schema: &'static str,
    pub source_sha: Option<String>,
    pub source_dirty: Option<bool>,
    pub workload_scope: &'static str,
    pub live_counter_snapshot_authoritative: bool,
    pub counts: Vec<usize>,
    pub fire_deadline_seconds: u64,
    pub status: &'static str,
    pub failure: Option<String>,
    pub cleanup_failure: Option<String>,
    pub local_storage_path: PathBuf,
    pub stages: Vec<Stage>,
    pub recovery_probe_passed: bool,
    #[serde(skip)]
    pub path: PathBuf,
}

impl Report {
    pub fn new(storage: PathBuf) -> Result<Self, BenchFailure> {
        let counts = std::env::var("FITZ_SCHEDULE_PRESSURE_COUNTS")
            .unwrap_or_else(|_| "1,32,128,512".into())
            .split(',')
            .map(|count| {
                count
                    .parse::<usize>()
                    .map_err(|error| BenchFailure::validation(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if counts.is_empty()
            || counts.len() > 10
            || counts.iter().any(|n| *n == 0 || *n > 4096)
            || counts.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(BenchFailure::validation(
                "counts must be 1 to 10 increasing values between 1 and 4096",
            ));
        }
        let fire_deadline_seconds = std::env::var("FITZ_SCHEDULE_PRESSURE_FIRE_SECS")
            .unwrap_or_else(|_| "120".into())
            .parse::<u64>()
            .map_err(|error| BenchFailure::validation(error.to_string()))?;
        if !(1..=600).contains(&fire_deadline_seconds) {
            return Err(BenchFailure::validation(
                "fire deadline must be between 1 and 600 seconds",
            ));
        }
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(BenchFailure::transport)?
            .as_nanos();
        let directory = PathBuf::from("target/fitz-stress/schedule-pressure");
        std::fs::create_dir_all(&directory).map_err(BenchFailure::transport)?;
        Ok(Self {
            schema: "fitz.schedule-pressure.v1", source_sha: git(&["rev-parse", "HEAD"]),
            source_dirty: git(&["status", "--porcelain"]).map(|s| !s.is_empty()),
            workload_scope: "forced_due_wave_local_sync_intent_clean_restart_observed_live_receipts_no_claim_ack_proof",
            live_counter_snapshot_authoritative: false,
            counts, fire_deadline_seconds, status: "running", failure: None, cleanup_failure: None,
            local_storage_path: storage, stages: Vec::new(), recovery_probe_passed: false,
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

fn git(args: &[&str]) -> Option<String> {
    let output = std::process::Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| {
            String::from_utf8(output.stdout)
                .ok()
                .map(|s| s.trim().into())
        })
        .flatten()
}
