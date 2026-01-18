use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::schedule::actor::ScheduleActor;
use fitz::domains::schedule::protocol::SchedulePayload;
use fitz::runtime::routing::RouteFamily;
use std::sync::Arc;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 4: INTEGRATION BENCHMARKS
//
// Target: Measure FULL SCHEDULE LIFECYCLE including persistence and recovery
// Goal: <100µs p50 for create + persist, <50µs for delete
// Throughput: 10k+ schedules/sec
//
// These benchmarks measure realistic schedule management with storage.
// ============================================================================

fn make_store() -> Arc<cntryl_midge::Engine> {
    Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    )
}

fn make_cron_payload(cron: &str) -> Bytes {
    let sp = SchedulePayload {
        cron: cron.to_string(),
    };
    Bytes::from(sp.encode())
}

fn bench_create_schedule_persist(c: &mut Criterion) {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    let payload = make_cron_payload("0 9 * * *");

    let mut group = c.benchmark_group("schedule_integration_create");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("create_and_persist_schedule", |b| {
        b.iter(|| {
            actor.create_schedule(
                black_box(fitz::runtime::routing::Route::new(
                    "notice://test/schedule/bench".to_string(),
                )),
                black_box(payload.clone()),
            )
        })
    });

    group.finish();
}

fn bench_delete_schedule_persist(c: &mut Criterion) {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    let payload = make_cron_payload("0 9 * * *");

    // Pre-create schedules to delete
    let mut ids = Vec::new();
    for i in 0..100 {
        let id = actor
            .create_schedule(
                fitz::runtime::routing::Route::new(format!("notice://test/schedule/bench{}", i)),
                payload.clone(),
            )
            .unwrap();
        ids.push(id);
    }

    let mut group = c.benchmark_group("schedule_integration_delete");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut idx = 0;

    group.bench_function("delete_and_persist_schedule", |b| {
        b.iter(|| {
            let id = black_box(ids[idx % ids.len()]);
            idx = (idx + 1) % ids.len();

            actor.delete_schedule(id)
        })
    });

    group.finish();
}

fn bench_recovery_from_storage(c: &mut Criterion) {
    // Arrange: Pre-populate storage with schedules
    let family = RouteFamily::new(0);
    let store = make_store();

    let payload = make_cron_payload("0 9 * * *");

    {
        let mut actor = ScheduleActor::new(family, store.clone(), cntryl_midge::WriteOptions::sync());

        for i in 0..50 {
            actor
                .create_schedule(
                    fitz::runtime::routing::Route::new(format!("notice://test/schedule/job{}", i)),
                    payload.clone(),
                )
                .unwrap();
        }
    }

    let mut group = c.benchmark_group("schedule_integration_recovery");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("recover_50_schedules_from_storage", |b| {
        b.iter(|| {
            ScheduleActor::new(
                black_box(family),
                black_box(store.clone()),
                black_box(cntryl_midge::WriteOptions::sync()),
            )
        })
    });

    group.finish();
}

fn bench_multiple_families_isolation(c: &mut Criterion) {
    // Arrange: Two families with schedules
    let family_a = RouteFamily::new(100);
    let family_b = RouteFamily::new(200);
    let store = make_store();

    let payload = make_cron_payload("0 9 * * *");

    {
        let mut actor_a = ScheduleActor::new(family_a, store.clone(), cntryl_midge::WriteOptions::sync());
        let mut actor_b = ScheduleActor::new(family_b, store.clone(), cntryl_midge::WriteOptions::sync());

        for i in 0..25 {
            actor_a
                .create_schedule(
                    fitz::runtime::routing::Route::new(format!("notice://test/schedule/a{}", i)),
                    payload.clone(),
                )
                .unwrap();

            actor_b
                .create_schedule(
                    fitz::runtime::routing::Route::new(format!("notice://test/schedule/b{}", i)),
                    payload.clone(),
                )
                .unwrap();
        }
    }

    let mut group = c.benchmark_group("schedule_integration_isolation");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("create_in_isolated_family", |b| {
        b.iter(|| {
            let mut actor = ScheduleActor::new(
                black_box(family_a),
                black_box(store.clone()),
                black_box(cntryl_midge::WriteOptions::default()),
            );

            actor.create_schedule(
                black_box(fitz::runtime::routing::Route::new(
                    "notice://test/schedule/isolated".to_string(),
                )),
                black_box(payload.clone()),
            )
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = bench_create_schedule_persist, bench_delete_schedule_persist, bench_recovery_from_storage, bench_multiple_families_isolation
}
criterion_main!(benches);
