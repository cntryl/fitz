#![allow(dead_code, clippy::pedantic)]

use cntryl_stress::StressContext;
use std::hint::black_box;
use std::time::{Duration, Instant};

pub fn measure_iterations<F>(
    ctx: &mut StressContext,
    name: &str,
    logical_operations_per_iteration: u64,
    f: F,
) where
    F: FnMut(),
{
    let _ = ctx.measure_batch(name, logical_operations_per_iteration.max(1), f);
}

pub fn measure_once<F, R>(ctx: &mut StressContext, name: &str, logical_operations: u64, f: F) -> R
where
    F: FnOnce() -> R,
{
    let started = Instant::now();
    let result = black_box(f());
    record_duration(ctx, name, started.elapsed(), logical_operations);
    black_box(result)
}

pub fn record_duration(ctx: &mut StressContext, name: &str, duration: Duration, completed: u64) {
    let _ = ctx.record_external(name, duration, completed.max(1));
}

pub fn record_completed(ctx: &mut StressContext, completed: u64) {
    let _ = ctx.correctness().attempted(completed).completed(completed);
}
