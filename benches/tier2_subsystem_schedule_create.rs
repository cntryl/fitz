#![allow(deprecated)]
use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::benchkit::create_bench_store;
use fitz::domains::schedule::actor::ScheduleActor;
use fitz::domains::schedule::protocol::{
    validate_concrete_schedule_route, CronSchedule, ScheduleCreateEntry,
};
use fitz::domains::schedule::store::{ScheduleBatchInsert, ScheduleInsert, ScheduleStore};
use fitz::runtime::routing::RouteFamily;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[path = "criterion_config.rs"]
mod criterion_config;

const CREATE_BATCH_SIZE: usize = 32;
const ROUTE_RING_SIZE: usize = 1024;
const PAYLOAD_SIZE: usize = 32;
const ACTOR_CREATE_REPEAT_COUNT: u64 = 8;

#[inline]
fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[inline]
fn usize_to_u8_saturating(value: usize) -> u8 {
    u8::try_from(value).unwrap_or(u8::MAX)
}

#[inline]
fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

struct ScheduleCreateFixtures {
    routes: Vec<String>,
    payloads: Vec<Bytes>,
    hourly_cron: String,
    hourly_schedule: CronSchedule,
    daily_schedule: CronSchedule,
}

struct StoreInsertCase {
    store: ScheduleStore,
    routes: Vec<String>,
    payloads: Vec<Bytes>,
    next_fire_ms: u64,
    cron: String,
}

struct ActorCreateCase {
    actor: ScheduleActor,
    routes: Vec<String>,
    payloads: Vec<Bytes>,
    cron: String,
}

fn instant_to_epoch_ms(instant: Instant) -> u64 {
    let now_instant = Instant::now();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            (elapsed.as_secs() * 1000) + u64::from(elapsed.subsec_millis())
        });

    if instant >= now_instant {
        now_ms.saturating_add(u128_to_u64_saturating(
            instant.duration_since(now_instant).as_millis(),
        ))
    } else {
        now_ms.saturating_sub(u128_to_u64_saturating(
            now_instant.duration_since(instant).as_millis(),
        ))
    }
}

fn build_route(index: usize) -> String {
    format!("schedule://bench/subsystem/resource-{index}/run")
}

fn create_fixtures() -> ScheduleCreateFixtures {
    let routes = (0..ROUTE_RING_SIZE).map(build_route).collect();
    let payloads = (0..ROUTE_RING_SIZE)
        .map(|index| Bytes::from(vec![usize_to_u8_saturating(index % 251); PAYLOAD_SIZE]))
        .collect();
    let hourly_cron = "0 * * * *".to_string();

    ScheduleCreateFixtures {
        routes,
        payloads,
        hourly_schedule: CronSchedule::parse(&hourly_cron).expect("valid hourly cron"),
        daily_schedule: CronSchedule::parse("15 6 * * *").expect("valid daily cron"),
        hourly_cron,
    }
}

fn create_store_insert_case(fixtures: &ScheduleCreateFixtures) -> StoreInsertCase {
    let store = ScheduleStore::new(create_bench_store());
    let next_fire_time = Instant::now() + Duration::from_hours(1);

    StoreInsertCase {
        store,
        routes: fixtures.routes[..CREATE_BATCH_SIZE].to_vec(),
        payloads: fixtures.payloads[..CREATE_BATCH_SIZE].to_vec(),
        next_fire_ms: instant_to_epoch_ms(next_fire_time),
        cron: fixtures.hourly_cron.clone(),
    }
}

fn create_actor_case(fixtures: &ScheduleCreateFixtures) -> ActorCreateCase {
    ActorCreateCase {
        actor: ScheduleActor::new(
            RouteFamily::new(1),
            create_bench_store(),
            cntryl_midge::WriteOptions::buffered(),
        ),
        routes: fixtures.routes[..CREATE_BATCH_SIZE].to_vec(),
        payloads: fixtures.payloads[..CREATE_BATCH_SIZE].to_vec(),
        cron: fixtures.hourly_cron.clone(),
    }
}

fn time_actor_create_cases(
    iters: u64,
    fixtures: &ScheduleCreateFixtures,
    mut measure: impl FnMut(&mut ActorCreateCase),
) -> Duration {
    let mut remaining = iters.saturating_mul(ACTOR_CREATE_REPEAT_COUNT);
    let mut total = Duration::ZERO;

    while remaining > 0 {
        let mut case = create_actor_case(fixtures);
        let start = Instant::now();
        measure(&mut case);
        total += start.elapsed();
        remaining -= 1;
    }

    total / u32::try_from(ACTOR_CREATE_REPEAT_COUNT).expect("actor create repeat count fits u32")
}

fn bench_validate_route(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    fixtures: &ScheduleCreateFixtures,
) {
    group.throughput(Throughput::Elements(usize_to_u64_saturating(
        ROUTE_RING_SIZE,
    )));
    group.bench_function("validate_route_1024_unique", |b| {
        b.iter(|| {
            for route in &fixtures.routes {
                black_box(validate_concrete_schedule_route(black_box(route)))
                    .expect("valid schedule route");
            }
        });
    });
}

fn bench_next_fire(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    fixtures: &ScheduleCreateFixtures,
) {
    group.throughput(Throughput::Elements(usize_to_u64_saturating(
        CREATE_BATCH_SIZE,
    )));
    group.bench_function("next_fire_hourly_32", |b| {
        b.iter(|| {
            let start = Instant::now();
            for offset in 0..CREATE_BATCH_SIZE {
                black_box(
                    fixtures.hourly_schedule.next_fire_time(
                        start + Duration::from_secs(usize_to_u64_saturating(offset)),
                    ),
                );
            }
        });
    });

    group.bench_function("next_fire_daily_32", |b| {
        b.iter(|| {
            let start = Instant::now();
            for offset in 0..CREATE_BATCH_SIZE {
                black_box(fixtures.daily_schedule.next_fire_time(
                    start + Duration::from_secs(usize_to_u64_saturating(offset) * 60),
                ));
            }
        });
    });
}

fn bench_store_create(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    fixtures: &ScheduleCreateFixtures,
) {
    group.bench_function("store_insert_unique_inmemory_32", |b| {
        b.iter_batched(
            || create_store_insert_case(fixtures),
            |case| {
                for index in 0..CREATE_BATCH_SIZE {
                    black_box(
                        case.store
                            .insert(
                                1,
                                ScheduleInsert {
                                    route: &case.routes[index],
                                    cron: &case.cron,
                                    payload: &case.payloads[index],
                                    next_fire_ms: case.next_fire_ms,
                                    previous_fire_ms: None,
                                    last_fire_ms: None,
                                    executions_total: 0,
                                },
                                cntryl_midge::WriteOptions::buffered(),
                            )
                            .expect("schedule insert"),
                    );
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("store_insert_batch_unique_inmemory_32", |b| {
        b.iter_batched(
            || create_store_insert_case(fixtures),
            |case| {
                let items: Vec<_> = (0..CREATE_BATCH_SIZE)
                    .map(|index| ScheduleBatchInsert {
                        route: case.routes[index].clone(),
                        cron: case.cron.clone(),
                        payload: case.payloads[index].clone(),
                        next_fire_ms: case.next_fire_ms,
                        previous_fire_ms: None,
                        last_fire_ms: None,
                        executions_total: 0,
                    })
                    .collect();
                case.store
                    .insert_batch(1, &items, cntryl_midge::WriteOptions::buffered())
                    .expect("schedule insert batch");
                black_box(());
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_actor_create(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    fixtures: &ScheduleCreateFixtures,
) {
    group.bench_function("actor_create_unique_inmemory_32", |b| {
        b.iter_custom(|iters| {
            time_actor_create_cases(iters, fixtures, |case| {
                for index in 0..CREATE_BATCH_SIZE {
                    black_box(
                        case.actor
                            .create_schedule(
                                case.routes[index].clone(),
                                case.cron.clone(),
                                case.payloads[index].clone(),
                            )
                            .expect("actor create schedule"),
                    );
                }
            })
        });
    });

    group.bench_function("actor_create_batch_unique_inmemory_32", |b| {
        b.iter_custom(|iters| {
            time_actor_create_cases(iters, fixtures, |case| {
                let entries: Vec<_> = (0..CREATE_BATCH_SIZE)
                    .map(|index| ScheduleCreateEntry {
                        route: case.routes[index].clone(),
                        cron: case.cron.clone(),
                        payload: case.payloads[index].clone(),
                    })
                    .collect();
                black_box(
                    case.actor
                        .create_schedules(entries)
                        .expect("actor create schedule batch"),
                );
            })
        });
    });
}

fn bench_schedule_create_breakdown(c: &mut Criterion) {
    let fixtures = create_fixtures();
    let mut group = c.benchmark_group("subsystem_schedule_create");
    group.sampling_mode(SamplingMode::Flat);

    bench_validate_route(&mut group, &fixtures);
    bench_next_fire(&mut group, &fixtures);
    bench_store_create(&mut group, &fixtures);
    bench_actor_create(&mut group, &fixtures);

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_schedule_create_breakdown
}
criterion_main!(benches);
