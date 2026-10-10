// Compiles the Tier 5 capacity-boundary decision under the test harness; the
// harness-free bench targets never build its unit tests.
#[path = "../benches/stress_support/capacity_boundary.rs"]
mod capacity_boundary;

use capacity_boundary::{shortfall, MINIMUM_ACTIVE, MINIMUM_LANE_COMPLETIONS};
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
