use super::super::{ledger::Counts, report::git_output};
use crate::stress_support::types::BenchFailure;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Serialize)]
pub(super) struct PairTiming {
    pub sequence: usize,
    pub message_id: u64,
    pub reserve_ns: u128,
    pub ack_ns: u128,
    pub pause_ns: u128,
    pub cycle_ns: u128,
}

#[derive(Serialize)]
pub(super) struct Report {
    schema: &'static str,
    source_sha: Option<String>,
    source_dirty: Option<bool>,
    build_mode: &'static str,
    measurement_scope: &'static str,
    workload_deadline_seconds: u64,
    requested_pairs: usize,
    requested_pause_ms: u64,
    durability_scope: &'static str,
    metric_bucket_upper_ms: [Option<u64>; 9],
    metrics_before: BTreeMap<String, [u64; 9]>,
    metrics_after: BTreeMap<String, [u64; 9]>,
    pub status: &'static str,
    pub phase: &'static str,
    pub failure: Option<String>,
    pub cleanup_status: &'static str,
    pub cleanup_failure: Option<String>,
    pub local_storage_path: Option<PathBuf>,
    pub setup_elapsed_ns: u128,
    pub drain_elapsed_ns: u128,
    pub workload_elapsed_ns: u128,
    pub accounting: Counts,
    pub empty_verified: bool,
    pub process_rss_bytes: Option<u64>,
    pub process_peak_rss_bytes: Option<u64>,
    pub samples: Vec<PairTiming>,
    pub accepted_messages: Vec<(usize, u64)>,
    #[serde(skip)]
    path: PathBuf,
}

impl Report {
    pub fn new(pairs: usize) -> Result<Self, BenchFailure> {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(BenchFailure::transport)?
            .as_nanos();
        let directory = PathBuf::from("target/fitz-stress/queue-drain-latency");
        std::fs::create_dir_all(&directory).map_err(BenchFailure::transport)?;
        Ok(Self {
            schema: "fitz.queue-drain-latency.v1",
            source_sha: git_output(&["rev-parse", "HEAD"]).map(|sha| sha.trim().to_owned()),
            source_dirty: git_output(&["status", "--porcelain"]).map(|status| !status.is_empty()),
            build_mode: if cfg!(debug_assertions) { "debug_assertions" } else { "optimized" },
            measurement_scope: "client send, receive and validation; cycle includes ledger checks and requested sleep, excludes periodic artifact writes; no performance baseline or crash-recovery claim",
            workload_deadline_seconds: 90,
            requested_pairs: pairs,
            requested_pause_ms: 5,
            durability_scope: "running_process_fast_local_disk",
            metric_bucket_upper_ms: [Some(1), Some(5), Some(10), Some(50), Some(100), Some(500), Some(1000), Some(5000), None],
            metrics_before: BTreeMap::new(),
            metrics_after: BTreeMap::new(),
            status: "running",
            phase: "startup",
            failure: None,
            cleanup_status: "not_started",
            cleanup_failure: None,
            local_storage_path: None,
            setup_elapsed_ns: 0,
            drain_elapsed_ns: 0,
            workload_elapsed_ns: 0,
            accounting: Counts::default(),
            empty_verified: false,
            process_rss_bytes: None,
            process_peak_rss_bytes: None,
            samples: Vec::with_capacity(pairs),
            accepted_messages: Vec::with_capacity(pairs),
            path: directory.join(format!("{stamp}.json")),
        })
    }

    pub fn capture_metrics_before(&mut self) {
        self.metrics_before = queue_histograms();
    }

    pub fn capture_metrics_after(&mut self) {
        self.metrics_after = queue_histograms();
    }

    pub fn fail(&mut self, error: &BenchFailure) {
        self.status = "failed";
        self.failure = Some(error.to_string());
    }

    pub fn save(&self) -> Result<(), BenchFailure> {
        let bytes = serde_json::to_vec_pretty(self).map_err(BenchFailure::transport)?;
        let temporary = self.path.with_extension("tmp");
        std::fs::write(&temporary, bytes).map_err(BenchFailure::transport)?;
        std::fs::rename(temporary, &self.path).map_err(BenchFailure::transport)
    }
}

fn queue_histograms() -> BTreeMap<String, [u64; 9]> {
    fitz::observability::metrics()
        .export_histograms()
        .into_iter()
        .filter(|(name, _)| name.starts_with("fitz_queue_"))
        .collect()
}
