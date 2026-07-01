#![allow(deprecated)]
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::runtime::context::{TimerId, TimerManager};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

#[path = "criterion_config.rs"]
mod criterion_config;

const FAR_FUTURE_DELAY: Duration = Duration::from_hours(1);
const SHORT_DELAY: Duration = Duration::from_secs(30);
const REPEAT_INTERVAL: Duration = Duration::from_mins(1);
const ACTIVE_TIMER_COUNTS: [usize; 3] = [10, 100, 1000];
const BENCH_BASE_OFFSET: Duration = Duration::from_secs(1);

fn bench_start_instant() -> Instant {
    static START: OnceLock<Instant> = OnceLock::new();
    *START.get_or_init(Instant::now)
}

fn timer_manager_with_active_timers(count: usize, delay: Duration) -> (TimerManager, Instant) {
    let start = bench_start_instant();
    let mut tm = TimerManager::new_at(start);
    let now = start + BENCH_BASE_OFFSET;
    for _ in 0..count {
        tm.schedule_once_at(now, delay);
    }

    if count > 0 {
        let spare_timer = tm.schedule_once_at(now, delay);
        let _ = tm.cancel(spare_timer);
    }

    (tm, now)
}

fn timer_manager_with_due_once_timers(count: usize) -> (TimerManager, Instant) {
    let start = bench_start_instant();
    let mut tm = TimerManager::new_at(start);
    let now = start + BENCH_BASE_OFFSET;
    for _ in 0..count {
        tm.schedule_once_at(now, Duration::ZERO);
    }
    (tm, now)
}

fn timer_manager_with_due_repeating_timer() -> (TimerManager, Instant) {
    let start = bench_start_instant();
    let mut tm = TimerManager::new_at(start);
    let now = start + BENCH_BASE_OFFSET;
    tm.schedule_repeat_at(now, Duration::ZERO, REPEAT_INTERVAL);
    (tm, now)
}

fn bench_timer_manager_schedule_once_empty(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("schedule_once_empty", |b| {
        b.iter_batched(
            || {
                let start = bench_start_instant();
                (TimerManager::new_at(start), start + BENCH_BASE_OFFSET)
            },
            |(mut tm, now)| {
                black_box(tm.schedule_once_at(black_box(now), black_box(SHORT_DELAY)));
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_timer_manager_schedule_repeat_empty(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("schedule_repeat_empty", |b| {
        b.iter_batched(
            || {
                let start = bench_start_instant();
                (TimerManager::new_at(start), start + BENCH_BASE_OFFSET)
            },
            |(mut tm, now)| {
                black_box(tm.schedule_repeat_at(
                    black_box(now),
                    black_box(SHORT_DELAY),
                    black_box(REPEAT_INTERVAL),
                ));
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_timer_manager_schedule_once_with_active_timers(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);

    for count in ACTIVE_TIMER_COUNTS {
        group.throughput(Throughput::Elements(1));
        group.bench_function(format!("schedule_once_with_{count}_active"), |b| {
            b.iter_batched(
                || timer_manager_with_active_timers(count, FAR_FUTURE_DELAY),
                |(mut tm, now)| {
                    black_box(tm.schedule_once_at(black_box(now), black_box(SHORT_DELAY)));
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

fn bench_timer_manager_cancel_present(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("cancel_present_1001_timers", |b| {
        b.iter_batched(
            || {
                let (mut tm, now) = timer_manager_with_active_timers(1000, FAR_FUTURE_DELAY);
                let timer_id = tm.schedule_once_at(now, FAR_FUTURE_DELAY);
                (tm, timer_id)
            },
            |(mut tm, timer_id): (TimerManager, TimerId)| {
                black_box(tm.cancel(black_box(timer_id)));
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn bench_timer_manager_fired_timers_empty(c: &mut Criterion) {
    let (mut tm, now) = timer_manager_with_active_timers(10, FAR_FUTURE_DELAY);

    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("fired_timers_none_ready", |b| {
        b.iter(|| {
            black_box(tm.fired_timers_at(now));
        });
    });

    group.finish();
}

fn bench_timer_manager_fired_timers_ready(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10));

    group.bench_function("fired_timers_10_ready", |b| {
        b.iter_batched(
            || timer_manager_with_due_once_timers(10),
            |(mut tm, now)| {
                black_box(tm.fired_timers_at(now));
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn bench_timer_manager_fired_timers_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);

    for count in ACTIVE_TIMER_COUNTS {
        let (mut tm, now) = timer_manager_with_active_timers(count, FAR_FUTURE_DELAY);
        group.throughput(Throughput::Elements(count as u64));
        group.bench_function(format!("fired_timers_none_ready_{count}"), |b| {
            b.iter(|| {
                black_box(tm.fired_timers_at(now));
            });
        });
    }

    group.finish();
}

fn bench_timer_manager_next_deadline(c: &mut Criterion) {
    let mut tm = TimerManager::new();
    for i in 0..20 {
        tm.schedule_once(Duration::from_secs(i * 10));
    }

    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("next_deadline", |b| {
        b.iter(|| {
            black_box(tm.next_deadline());
        });
    });

    group.finish();
}

fn bench_timer_manager_repeating_reschedule(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("fired_timers_repeat_reschedule", |b| {
        b.iter_batched(
            timer_manager_with_due_repeating_timer,
            |(mut tm, now)| {
                black_box(tm.fired_timers_at(now));
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn bench_timer_manager_clear(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    group.bench_function("clear_all_timers", |b| {
        b.iter_batched(
            || timer_manager_with_active_timers(100, FAR_FUTURE_DELAY),
            |(mut tm, _)| {
                tm.clear();
            },
            BatchSize::LargeInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier1();
    targets =
        bench_timer_manager_schedule_once_empty,
        bench_timer_manager_schedule_repeat_empty,
        bench_timer_manager_schedule_once_with_active_timers,
        bench_timer_manager_cancel_present,
        bench_timer_manager_fired_timers_empty,
        bench_timer_manager_fired_timers_ready,
        bench_timer_manager_fired_timers_scaling,
        bench_timer_manager_next_deadline,
        bench_timer_manager_repeating_reschedule,
        bench_timer_manager_clear
}
criterion_main!(benches);
