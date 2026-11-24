// Other shared domain drivers are used by the seven-domain tier binaries.
#[allow(dead_code, unused_imports)]
mod stress_support;

use cntryl_stress::{stress, StressContext, StressResult};
use stress_support::{run_queue_envelope, QueueBacklog, QueueEnvelope};

#[stress(tier = 5, role = "diagnostic")]
fn should_complete_queue_full_window_10k(ctx: &mut StressContext) -> StressResult {
    run_queue_envelope(ctx, QueueEnvelope::FullWindow(QueueBacklog::TEN_THOUSAND))
}

#[stress(tier = 5, role = "diagnostic")]
fn should_complete_queue_full_window_25k(ctx: &mut StressContext) -> StressResult {
    run_queue_envelope(
        ctx,
        QueueEnvelope::FullWindow(QueueBacklog::TWENTY_FIVE_THOUSAND),
    )
}

#[stress(tier = 5, role = "diagnostic")]
fn should_complete_queue_full_window_100k(ctx: &mut StressContext) -> StressResult {
    run_queue_envelope(
        ctx,
        QueueEnvelope::FullWindow(QueueBacklog::HUNDRED_THOUSAND),
    )
}

#[stress(tier = 5, role = "diagnostic")]
fn should_drain_queue_finite_10k(ctx: &mut StressContext) -> StressResult {
    run_queue_envelope(ctx, QueueEnvelope::FiniteDrain(QueueBacklog::TEN_THOUSAND))
}

#[stress(tier = 5, role = "diagnostic")]
fn should_drain_queue_finite_25k(ctx: &mut StressContext) -> StressResult {
    run_queue_envelope(
        ctx,
        QueueEnvelope::FiniteDrain(QueueBacklog::TWENTY_FIVE_THOUSAND),
    )
}

#[stress(tier = 5, role = "diagnostic")]
fn should_drain_queue_finite_100k(ctx: &mut StressContext) -> StressResult {
    run_queue_envelope(
        ctx,
        QueueEnvelope::FiniteDrain(QueueBacklog::HUNDRED_THOUSAND),
    )
}

fn main() {
    stress_support::main();
}
