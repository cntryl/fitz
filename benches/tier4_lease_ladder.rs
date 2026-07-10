#[path = "tier4_lease_support.rs"]
mod tier4_lease_support;
#[path = "tier4_support.rs"]
mod tier4_support;

use crate::tier4_lease_support::{measure_direct, measure_encoded};
use cntryl_stress::{stress, StressContext};

#[stress(tier = 4)]
fn should_characterize_direct_acquire_release_lifecycle(ctx: &mut StressContext) {
    measure_direct(ctx, "direct_acquire_release_lifecycle");
}

#[stress(tier = 4)]
fn should_characterize_encoded_acquire_release_lifecycle(ctx: &mut StressContext) {
    measure_encoded(ctx, "encoded_acquire_release_lifecycle");
}

cntryl_stress::stress_main!();
