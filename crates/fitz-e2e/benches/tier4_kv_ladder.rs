#[path = "tier4_kv_support.rs"]
mod tier4_kv_support;
use crate::tier4_kv_support::{measure_direct, measure_encoded};
use crate::tier4_support::StorageProfile;
use cntryl_stress::{stress, StressContext};
use fitz_core::testkit::operations as tier4_support;

#[stress(tier = 4)]
fn should_characterize_memory_direct_rollback(ctx: &mut StressContext) {
    measure_direct(ctx, StorageProfile::Memory, false, "memory_direct_rollback");
}

#[stress(tier = 4)]
fn should_characterize_local_disk_encoded_rollback(ctx: &mut StressContext) {
    measure_encoded(
        ctx,
        StorageProfile::LocalDisk,
        false,
        "local_disk_encoded_rollback",
    );
}

cntryl_stress::stress_main!();
