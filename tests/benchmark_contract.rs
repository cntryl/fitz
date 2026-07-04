use std::fs;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read_repo_file(path: &str) -> String {
    let absolute = repo_root().join(path);
    fs::read_to_string(&absolute)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", absolute.display()))
}

fn section_between<'a>(contents: &'a str, start: &str, end: &str) -> &'a str {
    let start_index = contents
        .find(start)
        .unwrap_or_else(|| panic!("missing section start: {start}"));
    let tail = &contents[start_index..];
    let end_index = tail
        .find(end)
        .unwrap_or_else(|| panic!("missing section end: {end}"));
    &tail[..end_index]
}

#[test]
fn should_prebuild_lease_tier3_destinations_before_measurement() {
    // Arrange
    let source = read_repo_file("benches/tier3_system_lease.rs");
    let request_helper = section_between(&source, "fn request(", "\nfn acquire_token(");

    // Act
    let accepts_prebuilt_destination = request_helper.contains("destination: &RouteAddress");
    let uses_prebuilt_route_helper = request_helper.contains("route_frame_to_address(");
    let builds_destination_inside_request = request_helper.contains("route_frame(");

    // Assert
    assert!(
        accepts_prebuilt_destination
            && uses_prebuilt_route_helper
            && !builds_destination_inside_request,
        "Tier 3 lease request helper must use prebuilt RouteAddress values"
    );
}
