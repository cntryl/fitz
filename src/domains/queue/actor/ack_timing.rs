//! Benchmark-only cumulative timing; these observations never control retries.

#[derive(Clone, Copy)]
pub(super) enum Phase {
    Admission,
    Commit,
}

pub(super) fn measure<T>(ack: bool, phase: Phase, operation: impl FnOnce() -> T) -> T {
    #[cfg(feature = "benchkit")]
    let started = ack.then(std::time::Instant::now);
    #[cfg(not(feature = "benchkit"))]
    let _ = (ack, phase);
    let result = operation();
    #[cfg(feature = "benchkit")]
    if let Some(started) = started {
        let (histogram, total) = match phase {
            Phase::Admission => (
                "fitz_queue_diagnostic_ack_admission_us",
                "fitz_queue_diagnostic_ack_admission_total_ns",
            ),
            Phase::Commit => (
                "fitz_queue_diagnostic_ack_commit_us",
                "fitz_queue_diagnostic_ack_commit_total_ns",
            ),
        };
        let elapsed = started.elapsed();
        let metrics = crate::observability::metrics();
        metrics.histogram_observe_us(
            histogram,
            u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
        );
        metrics.counter_add(total, u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
    }
    result
}
