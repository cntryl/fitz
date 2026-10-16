//! Benchmark-only cumulative timing; these observations never control retries.

#[derive(Clone, Copy)]
pub(in crate::domains::queue) enum Phase {
    Admission,
    Commit,
    Preparation,
    ReserveHydration,
    #[cfg(feature = "benchkit")]
    AckDispatchWaiting,
    #[cfg(feature = "benchkit")]
    ReserveDispatchWaiting,
    #[cfg(feature = "benchkit")]
    AckReplyDelivery,
    #[cfg(feature = "benchkit")]
    ReserveReplyDelivery,
}

pub(in crate::domains::queue) fn measure<T>(
    enabled: bool,
    phase: Phase,
    operation: impl FnOnce() -> T,
) -> T {
    #[cfg(feature = "benchkit")]
    let started = enabled.then(std::time::Instant::now);
    #[cfg(not(feature = "benchkit"))]
    let _ = (enabled, phase);
    let result = operation();
    #[cfg(feature = "benchkit")]
    if let Some(started) = started {
        observe(phase, started.elapsed());
    }
    result
}

#[cfg(feature = "benchkit")]
pub(in crate::domains::queue) fn observe(phase: Phase, elapsed: std::time::Duration) {
    let (histogram, total) = match phase {
        Phase::Admission => (
            "fitz_queue_diagnostic_ack_admission_us",
            "fitz_queue_diagnostic_ack_admission_total_ns",
        ),
        Phase::Commit => (
            "fitz_queue_diagnostic_ack_commit_us",
            "fitz_queue_diagnostic_ack_commit_total_ns",
        ),
        Phase::Preparation => (
            "fitz_queue_diagnostic_ack_preparation_us",
            "fitz_queue_diagnostic_ack_preparation_total_ns",
        ),
        Phase::ReserveHydration => (
            "fitz_queue_diagnostic_reserve_hydration_us",
            "fitz_queue_diagnostic_reserve_hydration_total_ns",
        ),
        Phase::AckDispatchWaiting => (
            "fitz_queue_diagnostic_ack_dispatch_waiting_us",
            "fitz_queue_diagnostic_ack_dispatch_waiting_total_ns",
        ),
        Phase::ReserveDispatchWaiting => (
            "fitz_queue_diagnostic_reserve_dispatch_waiting_us",
            "fitz_queue_diagnostic_reserve_dispatch_waiting_total_ns",
        ),
        Phase::AckReplyDelivery => (
            "fitz_queue_diagnostic_ack_reply_delivery_us",
            "fitz_queue_diagnostic_ack_reply_delivery_total_ns",
        ),
        Phase::ReserveReplyDelivery => (
            "fitz_queue_diagnostic_reserve_reply_delivery_us",
            "fitz_queue_diagnostic_reserve_reply_delivery_total_ns",
        ),
    };
    let metrics = crate::observability::metrics();
    metrics.histogram_observe_us(
        histogram,
        u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
    );
    metrics.counter_add(total, u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
}
