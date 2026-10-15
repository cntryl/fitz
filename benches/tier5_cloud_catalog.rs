#[path = "cloud_catalog/mod.rs"]
mod cloud_catalog;
#[allow(dead_code, unused_imports)]
mod stress_support;

use cntryl_stress::{stress, StressContext, StressResult};

#[stress(tier = 5, role = "diagnostic")]
fn should_recover_after_s3_catalog_pressure(ctx: &mut StressContext) -> StressResult {
    cloud_catalog::run(ctx)
}

#[stress(tier = 5, role = "diagnostic")]
fn should_recover_gib_stream_history_after_complete_cache_loss(
    ctx: &mut StressContext,
) -> StressResult {
    cloud_catalog::run_stream_history(ctx)
}

fn main() {
    // One readiness wait may include the 59-second crashed-writer lease and
    // replay. Its own 180-second deadline remains stricter than this watchdog.
    if std::env::var_os("STRESS_NO_PROGRESS_TIMEOUT_SECS").is_none() {
        std::env::set_var("STRESS_NO_PROGRESS_TIMEOUT_SECS", "240");
    }
    stress_support::main();
}
