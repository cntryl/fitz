#[allow(dead_code, unused_imports)]
mod stress_support;
use cntryl_stress::{stress, StressContext, StressResult};

#[stress(tier = 5, role = "diagnostic")]
fn should_separate_queue_reserve_ack_and_pause_latency(ctx: &mut StressContext) -> StressResult {
    stress_support::run_queue_drain_latency(ctx)
}

fn main() {
    stress_support::main();
}
