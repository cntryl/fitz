#[allow(dead_code, unused_imports)]
mod stress_support;
use cntryl_stress::{stress, StressContext, StressResult};
#[stress(tier = 5, role = "diagnostic")]
fn should_measure_lease_contender_pressure(ctx: &mut StressContext) -> StressResult {
    stress_support::run_lease_pressure(ctx)
}
fn main() {
    stress_support::main();
}
