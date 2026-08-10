const WORKFLOW: &str = include_str!("../.github/workflows/dependency-drift.yml");
const CARGO_MANIFEST: &str = include_str!("../Cargo.toml");

#[test]
fn should_check_each_git_main_dependency_on_a_weekly_schedule() {
    // Arrange
    let dependency_names = ["cntryl-lexkey", "cntryl-midge", "cntryl-stress"];

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
fn should_track_internal_git_dependencies_on_main_until_published_releases_exist() {
    // Arrange
    let expected_dependencies = [
        r#"cntryl-lexkey = { git = "https://github.com/cntryl/lexkey-rs", branch = "main" }"#,
        r#"cntryl-midge = { git = "https://github.com/cntryl/midge", branch = "main" }"#,
        r#"cntryl-stress = { git = "https://github.com/cntryl/stress", branch = "main" }"#,
    ];

    // Act
    let missing = expected_dependencies
        .into_iter()
        .filter(|dependency| !CARGO_MANIFEST.contains(dependency))
        .collect::<Vec<_>>();

    // Assert
    assert!(
        missing.is_empty(),
        "dependencies not tracking main: {missing:?}"
    );
}

#[test]
fn should_have_permission_plus_logic_to_report_drift_once() {
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
