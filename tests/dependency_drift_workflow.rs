const WORKFLOW: &str = include_str!("../.github/workflows/dependency-drift.yml");

#[test]
fn should_check_each_git_revision_pin_on_a_weekly_schedule() {
    // Arrange
    let dependency_names = ["cntryl-lexkey", "cntryl-midge"];

    // Act
    let missing = dependency_names
        .into_iter()
        .filter(|name| !WORKFLOW.contains(name))
        .collect::<Vec<_>>();

    // Assert
    assert!(WORKFLOW.contains("cron:"));
    assert!(missing.is_empty(), "missing dependency checks: {missing:?}");
}

#[test]
fn should_have_permission_and_logic_to_report_drift_once() {
    // Arrange
    let required_contract = ["issues: write", "ahead_by", "total_count === 0"];

    // Act
    let missing = required_contract
        .into_iter()
        .filter(|fragment| !WORKFLOW.contains(fragment))
        .collect::<Vec<_>>();

    // Assert
    assert!(
        missing.is_empty(),
        "missing drift-reporting contract: {missing:?}"
    );
}
