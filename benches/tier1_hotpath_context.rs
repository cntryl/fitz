use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::runtime::context::{TimerId, TimerManager};
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn bench_timer_id_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("timer_id_new", |b| {
        let mut counter = 0u64;
        b.iter(|| {
            // ONLY hot path - TimerId creation
            counter += 1;
            let _id = black_box(TimerId::new(counter));
        })
    });

    group.finish();
}

fn bench_timer_manager_schedule_once(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let delay = Duration::from_secs(30);

    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("schedule_once", |b| {
        let mut tm = TimerManager::new();
        b.iter(|| {
            // ONLY hot path - schedule one-time timer
            let _timer_id = tm.schedule_once(black_box(delay));
        })
    });

    group.finish();
}

fn bench_timer_manager_schedule_repeat(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let delay = Duration::from_secs(30);
    let interval = Duration::from_secs(60);

    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("schedule_repeat", |b| {
        let mut tm = TimerManager::new();
        b.iter(|| {
            // ONLY hot path - schedule repeating timer
            let _timer_id = tm.schedule_repeat(black_box(delay), black_box(interval));
        })
    });

    group.finish();
}

fn bench_timer_manager_cancel(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let mut tm = TimerManager::new();

    // Precompute timer IDs
    let timer_ids: Vec<TimerId> = (0..1000)
        .map(|_| tm.schedule_once(Duration::from_secs(3600)))
        .collect();

    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("cancel_timer", |b| {
        let mut idx = 0;
        b.iter(|| {
            // ONLY hot path - timer cancellation
            let timer_id = timer_ids[idx % timer_ids.len()];
            let _cancelled = tm.cancel(black_box(timer_id));
            idx += 1;
        })
    });

    group.finish();
}

fn bench_timer_manager_fired_timers_empty(c: &mut Criterion) {
    // Setup OUTSIDE benchmark - timers that won't fire immediately
    let mut tm = TimerManager::new();
    for _ in 0..10 {
        tm.schedule_once(Duration::from_secs(3600));
    }

    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("fired_timers_none_fired", |b| {
        b.iter(|| {
            // ONLY hot path - check fired timers (none should fire)
            let _fired = black_box(tm.fired_timers());
        })
    });

    group.finish();
}

fn bench_timer_manager_fired_timers_with_fired(c: &mut Criterion) {
    // Setup OUTSIDE benchmark - timers that have already fired
    let mut tm = TimerManager::new();
    for _ in 0..10 {
        tm.schedule_once(Duration::from_millis(1)); // Will fire immediately
    }

    // Let timers expire before benchmark
    std::thread::sleep(Duration::from_millis(10));

    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("fired_timers_all_fired", |b| {
        b.iter(|| {
            // ONLY hot path - check fired timers (all should fire)
            let _fired = black_box(tm.fired_timers());
        })
    });

    group.finish();
}

fn bench_timer_manager_next_deadline(c: &mut Criterion) {
    // Setup OUTSIDE benchmark
    let mut tm = TimerManager::new();
    for i in 0..20 {
        tm.schedule_once(Duration::from_secs(i * 10));
    }

    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("next_deadline", |b| {
        b.iter(|| {
            // ONLY hot path - find next deadline (min operation over timers)
            let _deadline = black_box(tm.next_deadline());
        })
    });

    group.finish();
}

fn bench_timer_manager_scaling(c: &mut Criterion) {
    // Test performance with different numbers of active timers
    let timer_counts = [10, 50, 100, 200];

    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);

    for count in timer_counts.iter() {
        group.throughput(Throughput::Elements(1));

        // Schedule_once scaling
        group.bench_function(format!("schedule_once_{}_timers", count), |b| {
            let mut tm = TimerManager::new();
            // Pre-populate with timers
            for _ in 0..*count {
                tm.schedule_once(Duration::from_secs(3600));
            }

            b.iter(|| {
                // ONLY hot path - schedule into populated manager
                let _timer_id = tm.schedule_once(black_box(Duration::from_secs(30)));
            })
        });

        // Fired_timers scaling
        group.bench_function(format!("fired_timers_{}_timers", count), |b| {
            let mut tm = TimerManager::new();
            for _ in 0..*count {
                tm.schedule_once(Duration::from_secs(3600));
            }

            b.iter(|| {
                // ONLY hot path - check fired timers with many active
                let _fired = black_box(tm.fired_timers());
            })
        });
    }

    group.finish();
}

fn bench_timer_manager_repeating_reschedule(c: &mut Criterion) {
    // Setup OUTSIDE benchmark - repeating timer that fires
    let mut tm = TimerManager::new();
    let timer_id = tm.schedule_repeat(Duration::from_millis(1), Duration::from_millis(1000));

    // Let timer fire once
    std::thread::sleep(Duration::from_millis(10));

    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("fired_timers_with_reschedule", |b| {
        b.iter(|| {
            // ONLY hot path - fired_timers that reschedules repeating timer
            let _fired = black_box(tm.fired_timers());
        })
    });

    group.finish();
}

fn bench_timer_manager_clear(c: &mut Criterion) {
    let mut group = c.benchmark_group("hotpath_context");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("clear_all_timers", |b| {
        b.iter_batched(
            || {
                // Setup per iteration - create manager with timers
                let mut tm = TimerManager::new();
                for _ in 0..100 {
                    tm.schedule_once(Duration::from_secs(3600));
                }
                tm
            },
            |mut tm| {
                // ONLY hot path - clear all timers
                tm.clear();
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_timer_id_creation,
        bench_timer_manager_schedule_once,
        bench_timer_manager_schedule_repeat,
        bench_timer_manager_cancel,
        bench_timer_manager_fired_timers_empty,
        bench_timer_manager_fired_timers_with_fired,
        bench_timer_manager_next_deadline,
        bench_timer_manager_scaling,
        bench_timer_manager_repeating_reschedule,
        bench_timer_manager_clear
}
criterion_main!(benches);
