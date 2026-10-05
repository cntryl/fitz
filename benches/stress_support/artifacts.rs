use crate::stress_support::fixture::StorageProfile;
use crate::stress_support::histogram::Latencies;
use crate::stress_support::types::BenchFailure;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct Phase {
    pub(crate) name: String,
    pub(crate) clients: usize,
    pub(crate) configured_ns: u128,
    /// Time spent executing workload batches, excluding separate verification.
    pub(crate) elapsed_ns: u128,
    pub(crate) wall_elapsed_ns: u128,
    pub(crate) verification_elapsed_ns: u128,
    pub(crate) no_progress_timeout_ns: u128,
    pub(crate) termination: &'static str,
    pub(crate) attempted: u64,
    pub(crate) completed: u64,
    pub(crate) capacity_rejections: BTreeMap<u32, u64>,
    pub(crate) contentions: u64,
    pub(crate) delivery_window_misses: u64,
    pub(crate) failures: u64,
    pub(crate) timeouts: u64,
    pub(crate) validation_errors: u64,
    pub(crate) verification_checks: u64,
    pub(crate) lane_completions: Vec<u64>,
    pub(crate) lane_capacity_rejections: Vec<u64>,
    pub(crate) lane_last_completion_ns: Vec<u128>,
    pub(crate) capacity_boundary: bool,
    pub(crate) latencies: Latencies,
    pub(crate) process_rss_bytes: Option<u64>,
    pub(crate) process_peak_rss_bytes: Option<u64>,
    pub(crate) failure: Option<String>,
    pub(crate) failure_kind: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct Artifacts {
    pub(crate) schema: &'static str,
    pub(crate) tier: u8,
    pub(crate) domain: &'static str,
    pub(crate) configured_seconds: u64,
    pub(crate) storage_profile: &'static str,
    /// Historical fixture path; successful shutdown removes this directory.
    pub(crate) local_storage_path: Option<PathBuf>,
    pub(crate) queue_write_policy: &'static str,
    pub(crate) transport: &'static str,
    pub(crate) status: &'static str,
    pub(crate) phases: Vec<Phase>,
    pub(crate) current: Option<Phase>,
    pub(crate) failure: Option<String>,
    pub(crate) failure_kind: Option<String>,
    #[serde(skip)]
    directory: PathBuf,
}

impl Artifacts {
    pub(crate) fn new(
        tier: u8,
        domain: &'static str,
        duration: Duration,
        storage_profile: StorageProfile,
    ) -> Result<Self, BenchFailure> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(BenchFailure::transport)?
            .as_nanos();
        let directory = PathBuf::from("target/fitz-stress").join(format!(
            "tier{tier}-{domain}-{}-{timestamp}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).map_err(BenchFailure::transport)?;
        Ok(Self {
            schema: "fitz-stress.v1",
            tier,
            domain,
            configured_seconds: duration.as_secs(),
            storage_profile: storage_profile.label(),
            local_storage_path: None,
            queue_write_policy: "fast",
            transport: "tcp",
            status: "running",
            phases: Vec::with_capacity(9),
            current: None,
            failure: None,
            failure_kind: None,
            directory,
        })
    }

    pub(crate) fn save(&self) -> Result<(), BenchFailure> {
        let content = serde_json::to_vec_pretty(self).map_err(BenchFailure::transport)?;
        let temporary = self.directory.join("progress.tmp");
        std::fs::write(&temporary, content).map_err(BenchFailure::transport)?;
        std::fs::rename(temporary, self.directory.join("progress.json"))
            .map_err(BenchFailure::transport)
    }
}

/// Linux reports live RSS and high-water RSS; other Unix hosts expose peak only.
pub(crate) fn memory_sample() -> (Option<u64>, Option<u64>) {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok();
        let read = |field: &str| {
            status.as_ref()?.lines().find_map(|line| {
                let rest = line.strip_prefix(field)?;
                rest.split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()?
                    .checked_mul(1_024)
            })
        };
        (read("VmRSS:"), read("VmHWM:"))
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        // SAFETY: getrusage initializes the correctly sized output on success.
        let peak = unsafe {
            if libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) == 0 {
                u64::try_from(usage.assume_init().ru_maxrss).ok()
            } else {
                None
            }
        };
        #[cfg(not(target_os = "macos"))]
        let peak = peak.and_then(|value| value.checked_mul(1_024));
        (None, peak)
    }
    #[cfg(not(unix))]
    {
        (None, None)
    }
}
