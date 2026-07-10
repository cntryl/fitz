#![allow(dead_code)] // Helper API is shared selectively by benchmark targets.

/// Stress helpers for Fitz tier3 and tier4 benchmarks
///
/// Tier 3: System-level (domain + plumbing, single family/concurrent access patterns)
/// Tier 4: Integration-level (full TCP/WS to domain, complete pipeline)
///
/// The stress harness owns profile defaults, sample counts, warmups, fixed-operation Tier 2
/// samples, and fixed-duration Tier 3+ windows. Fitz benchmarks should use
/// named `cntryl-stress` measurements directly instead of carrying a second profile-default layer.
///
/// **`record_completed(N)`**: N is the explicit completed-operation count for one sample.
/// It must match the logical number of meaningful operations performed in the timed body so
/// throughput reported by `cntryl-tools summarize-benchmarks` is interpretable.
///
/// If a scenario has a natural transport or fanout scope, add tags like `measurement_scope`
/// and `batch_size` so the report can distinguish direct, transport, and delivery cost.
pub trait StressContextExt {
    fn measure_workload<F, R>(&mut self, name: &str, f: F) -> u64
    where
        F: FnMut() -> R;
}

impl StressContextExt for cntryl_stress::StressContext {
    fn measure_workload<F, R>(&mut self, name: &str, f: F) -> u64
    where
        F: FnMut() -> R,
    {
        self.measure_batch(name, 1, f)
    }
}

pub fn record_completed(ctx: &mut cntryl_stress::StressContext, completed: u64) {
    let _ = ctx.correctness().attempted(completed).completed(completed);
}
