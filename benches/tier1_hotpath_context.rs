use cntryl_stress::{black_box, stress, stress_allocator, stress_main, StressContext};
use fitz::runtime::context::TimerManager;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

stress_allocator!();

const FAR_FUTURE_DELAY: Duration = Duration::from_hours(1);
const SHORT_DELAY: Duration = Duration::from_secs(30);
const REPEAT_INTERVAL: Duration = Duration::from_mins(1);
const BENCH_BASE_OFFSET: Duration = Duration::from_secs(1);

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

fn timer_manager_with_due_once_timers(count: usize) -> (TimerManager, Instant) {
    let start = bench_start_instant();
    let mut manager = TimerManager::new_at(start);
    let now = start + BENCH_BASE_OFFSET;
    for _ in 0..count {
        manager.schedule_once_at(now, Duration::ZERO);
    }
    (manager, now)
}

fn timer_manager_with_due_repeating_timer() -> (TimerManager, Instant) {
    let start = bench_start_instant();
    let mut manager = TimerManager::new_at(start);
    let now = start + BENCH_BASE_OFFSET;
    manager.schedule_repeat_at(now, Duration::ZERO, REPEAT_INTERVAL);
    (manager, now)
}

fn record_group(ctx: &mut StressContext, operation: &str) {
    ctx.parameter("group", "hotpath_context");
    ctx.parameter("operation", operation);
}

#[stress(tier = 1, name = "schedule_once_empty")]
fn should_schedule_once_empty(ctx: &mut StressContext) {
    record_group(ctx, "schedule_once");
    let start = bench_start_instant();
    let now = start + BENCH_BASE_OFFSET;

    ctx.measure("operation", || {
        let mut manager = TimerManager::new_at(start);
        black_box(manager.schedule_once_at(black_box(now), black_box(SHORT_DELAY)));
    });
}

#[stress(tier = 1, name = "schedule_repeat_empty")]
fn should_schedule_repeat_empty(ctx: &mut StressContext) {
    record_group(ctx, "schedule_repeat");
    let start = bench_start_instant();
    let now = start + BENCH_BASE_OFFSET;

    ctx.measure("operation", || {
        let mut manager = TimerManager::new_at(start);
        black_box(manager.schedule_repeat_at(
            black_box(now),
            black_box(SHORT_DELAY),
            black_box(REPEAT_INTERVAL),
        ));
    });
}

macro_rules! schedule_once_with_active_bench {
    ($fn_name:ident, $bench_name:literal, $count:expr) => {
        #[stress(tier = 1, name = $bench_name)]
        fn $fn_name(ctx: &mut StressContext) {
            record_group(ctx, "schedule_once_with_active");
            ctx.parameter("active_timers", $count);

            ctx.measure("operation", || {
                let (mut manager, now) = timer_manager_with_active_timers($count, FAR_FUTURE_DELAY);
                black_box(manager.schedule_once_at(black_box(now), black_box(SHORT_DELAY)));
            });
        }
    };
}

schedule_once_with_active_bench!(
    should_schedule_once_with_10_active,
    "schedule_once_with_10_active",
    10
);
schedule_once_with_active_bench!(
    should_schedule_once_with_1000_active,
    "schedule_once_with_1000_active",
    1000
);

#[stress(tier = 1, name = "cancel_present_1001_timers")]
fn should_cancel_present_1001_timers(ctx: &mut StressContext) {
    record_group(ctx, "cancel_present");

    ctx.measure("operation", || {
        let (mut manager, now) = timer_manager_with_active_timers(1000, FAR_FUTURE_DELAY);
        let timer_id = manager.schedule_once_at(now, FAR_FUTURE_DELAY);
        black_box(manager.cancel(black_box(timer_id)));
    });
}

#[stress(tier = 1, name = "fired_timers_10_ready")]
fn should_collect_fired_timers_10_ready(ctx: &mut StressContext) {
    record_group(ctx, "fired_timers");
    ctx.parameter("ready_timers", 10);

    ctx.measure("operation", || {
        let (mut manager, now) = timer_manager_with_due_once_timers(10);
        black_box(manager.fired_timers_at(now));
    });
}

#[stress(tier = 1, name = "fired_timers_repeat_reschedule")]
fn should_collect_fired_timers_repeat_reschedule(ctx: &mut StressContext) {
    record_group(ctx, "fired_timers_repeat");

    ctx.measure("operation", || {
        let (mut manager, now) = timer_manager_with_due_repeating_timer();
        black_box(manager.fired_timers_at(now));
    });
}

#[stress(tier = 1, name = "clear_all_timers")]
fn should_clear_all_timers(ctx: &mut StressContext) {
    record_group(ctx, "clear");

    ctx.measure("operation", || {
        let (mut manager, _now) = timer_manager_with_active_timers(100, FAR_FUTURE_DELAY);
        manager.clear();
    });
}

stress_main!();
