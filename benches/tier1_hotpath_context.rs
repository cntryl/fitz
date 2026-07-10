use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::runtime::context::TimerManager;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

stress_allocator!();

const FAR_FUTURE_DELAY: Duration = Duration::from_hours(1);
const SHORT_DELAY: Duration = Duration::from_secs(30);
const REPEAT_INTERVAL: Duration = Duration::from_mins(1);
const BENCH_BASE_OFFSET: Duration = Duration::from_secs(1);
const SCHEDULE_ONCE_CANCEL_BATCH_OPS: u64 = 32;
const SCHEDULE_REPEAT_CANCEL_BATCH_OPS: u64 = 32;

fn bench_start_instant() -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now)
}

fn timer_manager_with_active_timers(count: usize, delay: Duration) -> (TimerManager, Instant) {
    let start = bench_start_instant();
    let mut manager = TimerManager::new_at(start);
    let now = start + BENCH_BASE_OFFSET;
    for _ in 0..count {
        manager.schedule_once_at(now, delay);
    }

    if count > 0 {
        let spare_timer = manager.schedule_once_at(now, delay);
        let _ = manager.cancel(spare_timer);
    }

    (manager, now)
}

fn record_group(ctx: &mut StressContext, operation: &str) {
    ctx.parameter("group", "hotpath_context");
    ctx.parameter("operation", operation);
}

#[stress(tier = 1, name = "schedule_once_cancel_empty")]
fn should_schedule_once_cancel_empty(ctx: &mut StressContext) {
    record_group(ctx, "schedule_once");
    ctx.parameter("completed_unit", "once_cancel_batches");
    ctx.parameter("logical_unit", "once_cancel_batch");
    ctx.parameter(
        "once_cancels_per_logical_operation",
        SCHEDULE_ONCE_CANCEL_BATCH_OPS.to_string(),
    );
    ctx.parameter(
        "batch_size",
        format!("{SCHEDULE_ONCE_CANCEL_BATCH_OPS}_once_cancels"),
    );
    let start = bench_start_instant();
    let now = start + BENCH_BASE_OFFSET;
    let mut manager = TimerManager::new_at(start);
    let warmup_timer_id = manager.schedule_once_at(now, SHORT_DELAY);
    let _ = manager.cancel(warmup_timer_id);

    let _ = ctx.measure_batch(
        "schedule_once_cancel_empty",
        SCHEDULE_ONCE_CANCEL_BATCH_OPS,
        || {
            for _ in 0..SCHEDULE_ONCE_CANCEL_BATCH_OPS {
                let timer_id = manager.schedule_once_at(black_box(now), black_box(SHORT_DELAY));
                black_box(manager.cancel(timer_id));
            }
        },
    );
}

#[stress(tier = 1, name = "schedule_repeat_cancel_empty")]
fn should_schedule_repeat_cancel_empty(ctx: &mut StressContext) {
    record_group(ctx, "schedule_repeat");
    ctx.parameter("completed_unit", "repeat_cancel_batches");
    ctx.parameter("logical_unit", "repeat_cancel_batch");
    ctx.parameter(
        "repeat_cancels_per_logical_operation",
        SCHEDULE_REPEAT_CANCEL_BATCH_OPS.to_string(),
    );
    ctx.parameter(
        "batch_size",
        format!("{SCHEDULE_REPEAT_CANCEL_BATCH_OPS}_repeat_cancels"),
    );
    let start = bench_start_instant();
    let now = start + BENCH_BASE_OFFSET;
    let mut manager = TimerManager::new_at(start);
    let warmup_timer_id = manager.schedule_repeat_at(now, SHORT_DELAY, REPEAT_INTERVAL);
    let _ = manager.cancel(warmup_timer_id);

    let _ = ctx.measure_batch(
        "schedule_repeat_cancel_empty",
        SCHEDULE_REPEAT_CANCEL_BATCH_OPS,
        || {
            for _ in 0..SCHEDULE_REPEAT_CANCEL_BATCH_OPS {
                let timer_id = manager.schedule_repeat_at(
                    black_box(now),
                    black_box(SHORT_DELAY),
                    black_box(REPEAT_INTERVAL),
                );
                black_box(manager.cancel(timer_id));
            }
        },
    );
}

macro_rules! schedule_once_with_active_bench {
    ($fn_name:ident, $bench_name:literal, $count:expr) => {
        #[stress(tier = 1)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "schedule_once_with_active");
            ctx.parameter("active_timers", $count);
            let (mut manager, now) = timer_manager_with_active_timers($count, FAR_FUTURE_DELAY);

            ctx.measure($bench_name, || {
                let timer_id = manager.schedule_once_at(black_box(now), black_box(SHORT_DELAY));
                black_box(manager.cancel(timer_id));
            });
        }
    };
}

schedule_once_with_active_bench!(
    should_schedule_once_cancel_with_10_active,
    "schedule_once_cancel_with_10_active",
    10
);
schedule_once_with_active_bench!(
    should_schedule_once_cancel_with_1000_active,
    "schedule_once_cancel_with_1000_active",
    1000
);

#[stress(tier = 1, name = "cancel_restore_present_1001_timers")]
fn should_cancel_restore_present_1001_timers(ctx: &mut StressContext) {
    record_group(ctx, "cancel_present");
    let (mut manager, now) = timer_manager_with_active_timers(1000, FAR_FUTURE_DELAY);
    let mut timer_id = manager.schedule_once_at(now, FAR_FUTURE_DELAY);

    ctx.measure("cancel_restore_present_1001_timers", || {
        black_box(manager.cancel(black_box(timer_id)));
        timer_id = manager.schedule_once_at(now, FAR_FUTURE_DELAY);
    });
}

stress_main!();
