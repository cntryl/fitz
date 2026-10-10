// Decides whether an early Tier 5 capacity stop carries enough work to pass.
use std::time::Duration;

/// Active batch time a stage must accumulate before a capacity stop qualifies,
/// capped at the stage budget so short diagnostic stages can still qualify.
pub(crate) const MINIMUM_ACTIVE: Duration = Duration::from_secs(30);
/// Validated domain cycles every active lane must complete before a capacity
/// stop qualifies.
pub(crate) const MINIMUM_LANE_COMPLETIONS: u64 = 10;

/// Returns why a capacity stop does not qualify as a pass, or `None` when the
/// stage accumulated the minimum active time and per-lane validated cycles.
pub(crate) fn shortfall(active: Duration, budget: Duration, lanes: &[u64]) -> Option<String> {
    let required = MINIMUM_ACTIVE.min(budget);
    let fewest = lanes.iter().copied().min().unwrap_or(0);
    if active >= required && fewest >= MINIMUM_LANE_COMPLETIONS {
        return None;
    }
    Some(format!(
        "capacity boundary after {} ms active with {fewest} validated cycles on the least \
         productive lane; a qualifying boundary requires {} ms active and {MINIMUM_LANE_COMPLETIONS} validated \
         cycles per lane",
        active.as_millis(),
        required.as_millis(),
    ))
}
