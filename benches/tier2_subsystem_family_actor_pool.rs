#![allow(deprecated)]
#[path = "tier2_stress.rs"]
mod tier2_stress;

use cntryl_stress::{black_box, stress, stress_main, StressContext};
use fitz::runtime::family_actor_pool::{FamilyActorLane, FamilyActorPool};
use fitz::runtime::routing::RouteFamily;

const FAMILY_COUNT: usize = 8;
const BATCH_SIZE: usize = 1024;

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

stress_main!();
