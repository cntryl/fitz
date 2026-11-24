mod stress_support;
use cntryl_stress::{stress, StressContext, StressResult};
use stress_support::Domain;

#[stress(tier = 6, role = "diagnostic")]
fn should_soak_queue(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Queue, 6)
}

#[stress(tier = 6, role = "diagnostic")]
fn should_soak_kv(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Kv, 6)
}

#[stress(tier = 6, role = "diagnostic")]
fn should_soak_stream(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Stream, 6)
}

#[stress(tier = 6, role = "diagnostic")]
fn should_soak_schedule(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Schedule, 6)
}

#[stress(tier = 6, role = "diagnostic")]
fn should_soak_rpc(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Rpc, 6)
}

#[stress(tier = 6, role = "diagnostic")]
fn should_soak_notice(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Notice, 6)
}

#[stress(tier = 6, role = "diagnostic")]
fn should_soak_lease(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Lease, 6)
}

fn main() {
    stress_support::main();
}
