#![allow(deprecated)]
use bytes::Bytes;
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{black_box, stress, stress_main, StressContext};
use fitz::benchkit::create_bench_store;
use fitz::domains::schedule::actor::ScheduleActor;
use fitz::domains::schedule::protocol::{
    validate_concrete_schedule_route, CronSchedule, ScheduleCreateEntry,
};
use fitz::domains::schedule::store::{ScheduleBatchInsert, ScheduleInsert, ScheduleStore};
use fitz::runtime::routing::RouteFamily;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CREATE_BATCH_SIZE: usize = 32;
const STORE_INSERT_CASE_COUNT: usize = 16;
const STORE_BATCH_CASE_COUNT: usize = 16;
const ROUTE_RING_SIZE: usize = 1024;
const PAYLOAD_SIZE: usize = 32;
const ACTOR_CREATE_REPEAT_COUNT: u64 = 32;

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
    next_fire_start: Instant,
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
        next_fire_start: Instant::now() + Duration::from_mins(1),
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

#[stress(tier = 2, name = "validate_route_1024_unique")]
fn should_validate_route_1024_unique(ctx: &mut StressContext) {
    let fixtures = create_fixtures();

    tier2_stress::measure_iterations(
        ctx,
        "validate_route_1024_unique",
        usize_to_u64_saturating(ROUTE_RING_SIZE),
        || {
            for route in &fixtures.routes {
                black_box(validate_concrete_schedule_route(black_box(route)))
                    .expect("valid schedule route");
            }
        },
    );
}

#[stress(tier = 2, name = "next_fire_hourly_32")]
fn should_next_fire_hourly_32(ctx: &mut StressContext) {
    let fixtures = create_fixtures();

    tier2_stress::measure_iterations(
        ctx,
        "next_fire_hourly_32",
        usize_to_u64_saturating(CREATE_BATCH_SIZE),
        || {
            for offset in 0..CREATE_BATCH_SIZE {
                black_box(fixtures.hourly_schedule.next_fire_time(
                    fixtures.next_fire_start + Duration::from_secs(usize_to_u64_saturating(offset)),
                ));
            }
        },
    );
}

#[stress(tier = 2, name = "next_fire_daily_32")]
fn should_next_fire_daily_32(ctx: &mut StressContext) {
    let fixtures = create_fixtures();

    tier2_stress::measure_iterations(
        ctx,
        "next_fire_daily_32",
        usize_to_u64_saturating(CREATE_BATCH_SIZE),
        || {
            for offset in 0..CREATE_BATCH_SIZE {
                black_box(fixtures.daily_schedule.next_fire_time(
                    fixtures.next_fire_start
                        + Duration::from_secs(usize_to_u64_saturating(offset) * 60),
                ));
            }
        },
    );
}

#[stress(tier = 2, name = "store_insert_unique_inmemory_32")]
fn should_store_insert_unique_inmemory_32(ctx: &mut StressContext) {
    let fixtures = create_fixtures();
    let cases = (0..STORE_INSERT_CASE_COUNT)
        .map(|_| create_store_insert_case(&fixtures))
        .collect::<Vec<_>>();

    tier2_stress::measure_once(
        ctx,
        "store_insert_unique_inmemory_32",
        usize_to_u64_saturating(CREATE_BATCH_SIZE * STORE_INSERT_CASE_COUNT),
        || {
            for case in &cases {
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
            }
        },
    );
}

#[stress(tier = 2, name = "store_insert_batch_unique_inmemory_32")]
fn should_store_insert_batch_unique_inmemory_32(ctx: &mut StressContext) {
    let fixtures = create_fixtures();
    let cases = (0..STORE_BATCH_CASE_COUNT)
        .map(|_| create_store_insert_case(&fixtures))
        .collect::<Vec<_>>();
    let items_by_case = cases
        .iter()
        .map(|case| {
            (0..CREATE_BATCH_SIZE)
                .map(|index| ScheduleBatchInsert {
                    route: case.routes[index].clone(),
                    cron: case.cron.clone(),
                    payload: case.payloads[index].clone(),
                    next_fire_ms: case.next_fire_ms,
                    previous_fire_ms: None,
                    last_fire_ms: None,
                    executions_total: 0,
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    tier2_stress::measure_once(
        ctx,
        "store_insert_batch_unique_inmemory_32",
        usize_to_u64_saturating(CREATE_BATCH_SIZE * STORE_BATCH_CASE_COUNT),
        || {
            for (case, items) in cases.iter().zip(&items_by_case) {
                case.store
                    .insert_batch(1, items, cntryl_midge::WriteOptions::buffered())
                    .expect("schedule insert batch");
                black_box(());
            }
        },
    );
}

#[stress(tier = 2, name = "actor_create_unique_inmemory_32")]
fn should_actor_create_unique_inmemory_32(ctx: &mut StressContext) {
    let fixtures = create_fixtures();
    let mut cases = (0..ACTOR_CREATE_REPEAT_COUNT)
        .map(|_| create_actor_case(&fixtures))
        .collect::<Vec<_>>();

    let start = Instant::now();
    for case in &mut cases {
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
    }
    tier2_stress::record_duration(
        ctx,
        "actor_create_unique_inmemory_32",
        start.elapsed(),
        usize_to_u64_saturating(CREATE_BATCH_SIZE).saturating_mul(ACTOR_CREATE_REPEAT_COUNT),
    );
}

#[stress(tier = 2, name = "actor_create_batch_unique_inmemory_32")]
fn should_actor_create_batch_unique_inmemory_32(ctx: &mut StressContext) {
    let fixtures = create_fixtures();
    let mut cases = (0..ACTOR_CREATE_REPEAT_COUNT)
        .map(|_| create_actor_case(&fixtures))
        .collect::<Vec<_>>();
    let entries_by_case = cases
        .iter()
        .map(|case| {
            (0..CREATE_BATCH_SIZE)
                .map(|index| ScheduleCreateEntry {
                    route: case.routes[index].clone(),
                    cron: case.cron.clone(),
                    payload: case.payloads[index].clone(),
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let start = Instant::now();
    for (case, entries) in cases.iter_mut().zip(entries_by_case) {
        black_box(
            case.actor
                .create_schedules(entries)
                .expect("actor create schedule batch"),
        );
    }
    tier2_stress::record_duration(
        ctx,
        "actor_create_batch_unique_inmemory_32",
        start.elapsed(),
        usize_to_u64_saturating(CREATE_BATCH_SIZE).saturating_mul(ACTOR_CREATE_REPEAT_COUNT),
    );
}

stress_main!();
