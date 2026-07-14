use std::path::PathBuf;

#[test]
fn should_not_contain_a_top_level_scripts_directory() {
    // Arrange
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // Act
    let scripts_directory_exists = repo_root.join("scripts").exists();

    // Assert
    assert!(
        !scripts_directory_exists,
        "repository automation belongs in tests, proper tools, package scripts, or explicit workflow steps"
    );
}
