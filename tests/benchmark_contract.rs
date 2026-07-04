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

#[test]
fn should_batch_notice_tier3_fanout_delivery_confirmation() {
    // Arrange
    let source = read_repo_file("benches/tier3_system_notice.rs");
    let fanout_helper = section_between(
        &source,
        "fn measure_notice_fanout",
        "\nfn single_star_scaling_case",
    );

    // Act
    let defines_batch_size = source.contains("const NOTICE_FANOUT_CONFIRM_BATCH_SIZE");
    let routes_batched_publishes =
        fanout_helper.contains("for _ in 0..NOTICE_FANOUT_CONFIRM_BATCH_SIZE");
    let waits_once_per_batch =
        fanout_helper.contains("expected_per_subscriber += NOTICE_FANOUT_CONFIRM_BATCH_SIZE");
    let counts_batched_elements =
        fanout_helper.contains("ctx.set_elements(iterations as u64 * batch_size)");

    // Assert
    assert!(
        defines_batch_size
            && routes_batched_publishes
            && waits_once_per_batch
            && counts_batched_elements,
        "Tier 3 notice fanout must confirm real delivery in bounded batches instead of serializing every publish behind one wait loop"
    );
}
