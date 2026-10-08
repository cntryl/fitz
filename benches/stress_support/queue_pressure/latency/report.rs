use super::super::{ledger::Counts, report::git_output};
use crate::stress_support::types::BenchFailure;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static REPORT_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Serialize)]
pub(super) struct PairTiming {
    pub sequence: usize,
    pub message_id: u64,
    pub reserve_ns: u128,
    pub ack_ns: u128,
    pub pause_ns: u128,
    pub overshoot_ns: u128,
    pub cycle_ns: u128,
}

#[derive(Serialize)]
pub(super) struct CurrentReservation {
    pub sequence: usize,
    pub message_id: u64,
    pub token: u64,
    pub ack_state: Option<super::super::io::AckProgress>,
    pub ack_application_effect: &'static str,
    pub ledger_acknowledged: bool,
    pub pause_completed: bool,
    pub reserve_ns: u128,
    pub ack_ns: Option<u128>,
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
    pacing_implementation: &'static str,
    comparison_label: &'static str,
    durability_scope: &'static str,
    metric_bucket_upper_ms: [Option<u64>; 9],
    metrics_before: BTreeMap<String, [u64; 9]>,
    metrics_after: BTreeMap<String, [u64; 9]>,
    pub storage_before: Option<super::super::attribution::Snapshot>,
    pub storage_after: Option<super::super::attribution::Snapshot>,
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
    pub current_reserved: Option<CurrentReservation>,
    #[serde(skip)]
    path: PathBuf,
}

impl Report {
    pub fn observe_ack(
        &mut self,
        state: super::super::io::AckProgress,
    ) -> Result<(), BenchFailure> {
        let current = self
            .current_reserved
            .as_mut()
            .ok_or_else(|| BenchFailure::validation("missing current reservation"))?;
        current.ack_state = Some(state);
        current.ack_application_effect =
            if matches!(state, super::super::io::AckProgress::SuccessValidated) {
                "confirmed_ack"
            } else {
                "unconfirmed"
            };
        Ok(())
    }

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
            measurement_scope: "client send, receive and validation; cycle includes identity/ledger checks and requested sleep, excludes periodic report writes; counters count validated ACK success, samples additionally require completed pause; no performance baseline or crash-recovery claim",
            workload_deadline_seconds: 90,
            requested_pairs: pairs,
            requested_pause_ms: 5,
            pacing_implementation: super::super::pacing::IMPLEMENTATION,
            comparison_label: "corrected_pacing",
            durability_scope: "running_process_fast_local_disk",
            metric_bucket_upper_ms: [Some(1), Some(5), Some(10), Some(50), Some(100), Some(500), Some(1000), Some(5000), None],
            metrics_before: BTreeMap::new(),
            metrics_after: BTreeMap::new(),
            storage_before: None,
            storage_after: None,
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
            current_reserved: None,
            path: directory.join(format!("{stamp}-{}-{}.json", std::process::id(), REPORT_SEQUENCE.fetch_add(1, Ordering::Relaxed))),
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

#[cfg(test)]
mod tests {
    use super::{CurrentReservation, Report};

    fn reserved_report() -> Report {
        let mut report = Report::new(1).unwrap();
        report.current_reserved = Some(CurrentReservation {
            sequence: 0,
            message_id: 42,
            token: 7,
            ack_state: None,
            ack_application_effect: "not_dispatched",
            ledger_acknowledged: false,
            pause_completed: false,
            reserve_ns: 1,
            ack_ns: None,
        });
        report
    }

    fn failure_artifact(report: &mut Report) -> serde_json::Value {
        report.fail(&crate::stress_support::types::BenchFailure::transport(
            "injected cancellation",
        ));
        report.save().unwrap();
        serde_json::from_slice(&std::fs::read(&report.path).unwrap()).unwrap()
    }

    #[test]
    fn should_retain_identity_without_claiming_success_after_ack_dispatch() {
        // Arrange
        let mut report = reserved_report();
        // Act
        report
            .observe_ack(super::super::super::io::AckProgress::DispatchStarted)
            .unwrap();
        report
            .observe_ack(super::super::super::io::AckProgress::FrameSent)
            .unwrap();
        let artifact = failure_artifact(&mut report);
        let state = &artifact["current_reserved"];
        // Assert
        assert_eq!(state["message_id"], 42);
        assert_eq!(state["token"], 7);
        assert_eq!(state["ack_state"], "frame_sent");
        assert_eq!(state["ledger_acknowledged"], false);
    }

    #[test]
    fn should_preserve_unknown_ack_effect_after_coded_backend_error() {
        // Arrange
        let mut report = reserved_report();
        let body = fitz::protocol::error_codes::encode_error_body(
            fitz::protocol::error_codes::queue::ERR_BACKEND_ERROR,
            "storage commit outcome unknown",
        );
        // Act
        report
            .observe_ack(super::super::super::io::AckProgress::FrameReceived)
            .unwrap();
        let result =
            super::super::super::io::validate_ack_payload(&body, |state| report.observe_ack(state));
        let artifact = failure_artifact(&mut report);
        let state = &artifact["current_reserved"];
        // Assert
        assert_eq!(state["ack_state"], "error_response_validated");
        assert!(result.is_err());
        assert_eq!(state["ack_application_effect"], "unconfirmed");
        assert_eq!(state["ledger_acknowledged"], false);
    }

    #[test]
    fn should_preserve_unknown_ack_effect_after_plain_commit_error() {
        // Arrange
        let mut report = reserved_report();
        let mut encoder = fitz::protocol::payload_codec::PayloadEncoder::new();
        encoder.put_u8(1);
        encoder.put_string("storage commit outcome unknown");
        let body = encoder.finish();
        // Act
        report
            .observe_ack(super::super::super::io::AckProgress::FrameReceived)
            .unwrap();
        let result =
            super::super::super::io::validate_ack_payload(&body, |state| report.observe_ack(state));
        let artifact = failure_artifact(&mut report);
        // Assert
        assert!(result.is_err());
        assert_eq!(
            artifact["current_reserved"]["ack_state"],
            "error_response_validated"
        );
        assert_eq!(
            artifact["current_reserved"]["ack_application_effect"],
            "unconfirmed"
        );
        assert_eq!(artifact["accounting"]["acknowledged"], 0);
    }

    #[test]
    fn should_not_claim_a_valid_ack_response_after_malformed_frame() {
        // Arrange
        let mut report = reserved_report();
        let bytes = [204, 0, 2, 0];
        // Act
        let result = super::super::super::io::validate_ack_response(&bytes, |state| {
            report.observe_ack(state)
        });
        let artifact = failure_artifact(&mut report);
        // Assert
        assert!(result.is_err());
        assert_eq!(artifact["current_reserved"]["ack_state"], "frame_received");
        assert_eq!(
            artifact["current_reserved"]["ack_application_effect"],
            "unconfirmed"
        );
        assert_eq!(artifact["accounting"]["acknowledged"], 0);
    }

    #[test]
    fn should_preserve_known_ack_while_pause_has_no_sample() {
        // Arrange
        let mut report = reserved_report();
        report
            .observe_ack(super::super::super::io::AckProgress::SuccessValidated)
            .unwrap();
        report
            .current_reserved
            .as_mut()
            .unwrap()
            .ledger_acknowledged = true;
        report.phase = "acknowledged_pause_pending_sample";
        report.accounting.acknowledged = 1;
        // Act
        let artifact = failure_artifact(&mut report);
        let state = &artifact["current_reserved"];
        // Assert
        assert_eq!(state["ack_state"], "success_validated");
        assert_eq!(state["ledger_acknowledged"], true);
        assert_eq!(state["pause_completed"], false);
        assert_eq!(artifact["accounting"]["acknowledged"], 1);
        assert_eq!(artifact["samples"], serde_json::json!([]));
    }
}
