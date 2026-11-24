mod stress_support;
use cntryl_stress::{stress, StressContext, StressResult};
use stress_support::Domain;

#[stress(tier = 5, role = "diagnostic")]
fn should_measure_queue_concurrency(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Queue, 5)
}

#[stress(tier = 5, role = "diagnostic")]
fn should_measure_kv_concurrency(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Kv, 5)
}

#[stress(tier = 5, role = "diagnostic")]
fn should_measure_stream_concurrency(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Stream, 5)
}

#[stress(tier = 5, role = "diagnostic")]
fn should_measure_schedule_concurrency(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Schedule, 5)
}

#[stress(tier = 5, role = "diagnostic")]
fn should_measure_rpc_concurrency(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Rpc, 5)
}

#[stress(tier = 5, role = "diagnostic")]
fn should_measure_notice_concurrency(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Notice, 5)
}

#[stress(tier = 5, role = "diagnostic")]
fn should_measure_lease_concurrency(ctx: &mut StressContext) -> StressResult {
    stress_support::run(ctx, Domain::Lease, 5)
}

fn main() {
    stress_support::main();
}
