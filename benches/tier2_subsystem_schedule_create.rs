use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use fitz::benchkit::create_bench_store;
use fitz::domains::schedule::actor::ScheduleActor;
use fitz::domains::schedule::protocol::{
    validate_concrete_schedule_route, CronSchedule, ScheduleCreateEntry,
};
use fitz::domains::schedule::store::{ScheduleInsert, ScheduleStore};
use fitz::runtime::routing::RouteFamily;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[path = "criterion_config.rs"]
mod criterion_config;

const CREATE_BATCH_SIZE: usize = 32;
const ROUTE_RING_SIZE: usize = 1024;
const PAYLOAD_SIZE: usize = 32;

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
        .map(|elapsed| (elapsed.as_secs() * 1000) + elapsed.subsec_millis() as u64)
        .unwrap_or(0);

    if instant >= now_instant {
        now_ms.saturating_add(instant.duration_since(now_instant).as_millis() as u64)
    } else {
        now_ms.saturating_sub(now_instant.duration_since(instant).as_millis() as u64)
    }
}

fn build_route(index: usize) -> String {
    format!("schedule://bench/subsystem/resource-{index}/run")
}

fn create_fixtures() -> ScheduleCreateFixtures {
    let routes = (0..ROUTE_RING_SIZE).map(build_route).collect();
    let payloads = (0..ROUTE_RING_SIZE)
        .map(|index| Bytes::from(vec![(index % 251) as u8; PAYLOAD_SIZE]))
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
    let next_fire_time = Instant::now() + Duration::from_secs(3600);

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

fn bench_schedule_create_breakdown(c: &mut Criterion) {
    let fixtures = create_fixtures();
    let mut group = c.benchmark_group("subsystem_schedule_create");
    group.sampling_mode(SamplingMode::Flat);

    group.throughput(Throughput::Elements(ROUTE_RING_SIZE as u64));
    group.bench_function("validate_route_1024_unique", |b| {
        b.iter(|| {
            for route in &fixtures.routes {
                black_box(validate_concrete_schedule_route(black_box(route)))
                    .expect("valid schedule route");
            }
        })
    });

    group.throughput(Throughput::Elements(CREATE_BATCH_SIZE as u64));
    group.bench_function("next_fire_hourly_32", |b| {
        b.iter(|| {
            let start = Instant::now();
            for offset in 0..CREATE_BATCH_SIZE {
                black_box(
                    fixtures
                        .hourly_schedule
                        .next_fire_time(start + Duration::from_secs(offset as u64)),
                );
            }
        })
    });

    group.bench_function("next_fire_daily_32", |b| {
        b.iter(|| {
            let start = Instant::now();
            for offset in 0..CREATE_BATCH_SIZE {
                black_box(
                    fixtures
                        .daily_schedule
                        .next_fire_time(start + Duration::from_secs(offset as u64 * 60)),
                );
            }
        })
    });

    group.bench_function("store_insert_unique_inmemory_32", |b| {
        b.iter_batched(
            || create_store_insert_case(&fixtures),
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
                                },
                                cntryl_midge::WriteOptions::buffered(),
                            )
                            .expect("schedule insert"),
                    );
                }
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("store_insert_batch_unique_inmemory_32", |b| {
        b.iter_batched(
            || create_store_insert_case(&fixtures),
            |case| {
                let items: Vec<_> = (0..CREATE_BATCH_SIZE)
                    .map(|index| {
                        (
                            case.routes[index].clone(),
                            case.cron.clone(),
                            case.payloads[index].clone(),
                            case.next_fire_ms,
                            None,
                        )
                    })
                    .collect();
                case.store
                    .insert_batch(1, &items, cntryl_midge::WriteOptions::buffered())
                    .expect("schedule insert batch");
                black_box(());
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("actor_create_unique_inmemory_32", |b| {
        b.iter_batched(
            || create_actor_case(&fixtures),
            |mut case| {
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
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("actor_create_batch_unique_inmemory_32", |b| {
        b.iter_batched(
            || create_actor_case(&fixtures),
            |mut case| {
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
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = criterion_config::criterion_config_for_tier2();
    targets = bench_schedule_create_breakdown
}
criterion_main!(benches);
