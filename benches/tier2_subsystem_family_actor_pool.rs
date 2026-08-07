#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{black_box, stress, stress_main, StressContext};
use fitz::runtime::family_actor_pool::{FamilyActorLane, FamilyActorPool, FamilyActorPoolRuntime};
use fitz::runtime::routing::RouteFamily;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

const FAMILY_COUNT: usize = 8;
const BATCH_SIZE: usize = 1024;
const BURST_WAVE_COUNT: usize = 1024;
const IDLE_WAKE_SAMPLES: usize = 128;

fn provisioned_families() -> Vec<RouteFamily> {
    (1..=FAMILY_COUNT)
        .map(|id| RouteFamily::new(u32::try_from(id).expect("benchmark family fits")))
        .collect()
}

#[stress(tier = 2, name = "enqueue_family_affine_1024")]
fn should_enqueue_family_affine_work(ctx: &mut StressContext) {
    let families = provisioned_families();
    let pool = FamilyActorPool::<u64>::new(&families).expect("family actor pool");
    let ingress = pool.ingress();

    tier2_stress::measure_once(ctx, "enqueue_family_affine_1024", BATCH_SIZE as u64, || {
        for value in 0..BATCH_SIZE {
            let family = families[value % families.len()];
            ingress
                .try_enqueue(family, FamilyActorLane::Normal, black_box(value as u64))
                .expect("normal lane has room");
        }
    });
}

#[stress(tier = 2, name = "drain_family_actor_round_robin_1024")]
fn should_drain_family_actor_work_fairly(ctx: &mut StressContext) {
    let families = provisioned_families();
    let mut pool = FamilyActorPool::<u64>::new(&families).expect("family actor pool");
    let ingress = pool.ingress();
    for family in &families {
        for value in 0..(BATCH_SIZE / FAMILY_COUNT) {
            ingress
                .try_enqueue(*family, FamilyActorLane::Normal, value as u64)
                .expect("normal lane has room");
        }
    }
    let shard_count = pool.shard_count();

    tier2_stress::measure_once(
        ctx,
        "drain_family_actor_round_robin_1024",
        BATCH_SIZE as u64,
        || {
            let mut completed = 0_u64;
            for shard_index in 0..shard_count {
                let Some(mut shard) = pool.take_shard(shard_index) else {
                    continue;
                };
                while shard.try_next().is_some() {
                    completed = completed.saturating_add(1);
                }
            }
            assert_eq!(completed, BATCH_SIZE as u64);
        },
    );
}

#[stress(tier = 2, name = "dispatch_idle_wake_roundtrip_128")]
fn should_dispatch_after_each_idle_worker_wake(ctx: &mut StressContext) {
    let family = RouteFamily::new(1);
    let pool = FamilyActorPool::<u64>::new(&[family]).expect("family actor pool");
    let active = Arc::new(AtomicBool::new(true));
    let (observed_tx, observed_rx) = crossbeam_channel::bounded(1);
    let runtime = FamilyActorPoolRuntime::spawn(
        pool,
        active,
        |_| (),
        move |(), _family, _lane, message| {
            observed_tx.send(message).expect("dispatch observer");
        },
    );

    let mut measured = Duration::ZERO;
    for value in 0..IDLE_WAKE_SAMPLES {
        std::thread::sleep(Duration::from_millis(2));
        let started = Instant::now();
        runtime
            .try_enqueue(family, FamilyActorLane::Normal, value as u64)
            .expect("idle wake enqueue");
        assert_eq!(
            observed_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("idle worker dispatch"),
            value as u64
        );
        measured += started.elapsed();
    }
    tier2_stress::record_duration(
        ctx,
        "dispatch_idle_wake_roundtrip_128",
        measured,
        IDLE_WAKE_SAMPLES as u64,
    );
}

#[stress(tier = 2, name = "dispatch_coalesced_burst_1024")]
fn should_dispatch_coalesced_burst(ctx: &mut StressContext) {
    let family = RouteFamily::new(1);
    let pool = FamilyActorPool::<u64>::new(&[family]).expect("family actor pool");
    let active = Arc::new(AtomicBool::new(true));
    let (completed_tx, completed_rx) = crossbeam_channel::bounded(1);
    let runtime = FamilyActorPoolRuntime::spawn(
        pool,
        active,
        |_| 0_usize,
        move |completed, _family, _lane, _message| {
            *completed += 1;
            if *completed % BATCH_SIZE == 0 {
                completed_tx.send(()).expect("burst completion observer");
            }
        },
    );

    std::thread::sleep(Duration::from_millis(2));
    tier2_stress::measure_once(
        ctx,
        "dispatch_coalesced_burst_1024",
        (BATCH_SIZE * BURST_WAVE_COUNT) as u64,
        || {
            for wave in 0..BURST_WAVE_COUNT {
                for value in 0..BATCH_SIZE {
                    runtime
                        .try_enqueue(
                            family,
                            FamilyActorLane::Normal,
                            (wave * BATCH_SIZE + value) as u64,
                        )
                        .expect("burst enqueue");
                }
                completed_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("coalesced burst dispatch");
            }
        },
    );
}

stress_main!();
