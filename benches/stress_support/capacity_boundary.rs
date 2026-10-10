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

#[cfg(test)]
mod tests {
    use super::{shortfall, MINIMUM_ACTIVE, MINIMUM_LANE_COMPLETIONS};
    use std::time::Duration;

    const STAGE_BUDGET: Duration = Duration::from_secs(514);

    #[test]
    fn should_fail_a_boundary_on_the_first_batch() {
        // Arrange
        let active = Duration::from_millis(3);
        let lanes = [1, 0, 1, 1];
        // Act
        let result = shortfall(active, STAGE_BUDGET, &lanes);
        // Assert
        assert!(result.is_some());
    }

    #[test]
    fn should_pass_a_boundary_after_the_minimum_work() {
        // Arrange
        let lanes = [MINIMUM_LANE_COMPLETIONS; 4];
        // Act
        let result = shortfall(MINIMUM_ACTIVE, STAGE_BUDGET, &lanes);
        // Assert
        assert_eq!(result, None);
    }

    #[test]
    fn should_fail_a_boundary_before_the_minimum_active_time() {
        // Arrange
        let active = MINIMUM_ACTIVE.saturating_sub(Duration::from_millis(1));
        let lanes = [MINIMUM_LANE_COMPLETIONS * 100; 4];
        // Act
        let result = shortfall(active, STAGE_BUDGET, &lanes);
        // Assert
        assert!(result.is_some());
    }

    #[test]
    fn should_fail_a_boundary_when_any_lane_lacks_the_minimum_completions() {
        // Arrange
        let lanes = [MINIMUM_LANE_COMPLETIONS * 100, MINIMUM_LANE_COMPLETIONS - 1];
        // Act
        let result = shortfall(MINIMUM_ACTIVE * 2, STAGE_BUDGET, &lanes);
        // Assert
        assert!(result.is_some());
    }

    #[test]
    fn should_cap_the_active_minimum_at_a_short_stage_budget() {
        // Arrange
        let budget = Duration::from_secs(1);
        let lanes = [MINIMUM_LANE_COMPLETIONS; 2];
        // Act
        let result = shortfall(budget, budget, &lanes);
        // Assert
        assert_eq!(result, None);
    }
}
