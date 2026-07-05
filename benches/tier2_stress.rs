#![allow(dead_code, clippy::pedantic)]

use cntryl_stress::StressContext;
use std::time::Duration;

pub fn measure_iterations<F>(ctx: &mut StressContext, logical_operations_per_iteration: u64, f: F)
where
    F: FnMut(),
{
    let _ = ctx.measure_batch(logical_operations_per_iteration.max(1), f);
}

pub fn measure_once<F, R>(ctx: &mut StressContext, logical_operations: u64, f: F) -> R
where
    F: FnOnce() -> R,
{
    let result = ctx.measure(f);
    record_completed(ctx, logical_operations.max(1));
    result
}

pub fn record_duration(ctx: &mut StressContext, duration: Duration, completed: u64) {
    let _ = ctx.record_external(duration, completed.max(1));
}

pub fn record_completed(ctx: &mut StressContext, completed: u64) {
    let _ = ctx.correctness().attempted(completed).completed(completed);
}
