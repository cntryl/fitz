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
    let request_helper = section_between(&source, "fn send_request(", "\nfn drain_responses(");

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
fn should_batch_lease_tier3_query_response_confirmation() {
    // Arrange
    let source = read_repo_file("benches/tier3_system_lease.rs");
    let query_benchmark = section_between(
        &source,
        "fn should_complete_round_robin_query_operations",
        "\n#[stress_test]\nfn should_complete_cycling_query_renew_operations",
    );

    // Act
    let defines_batch_size = source.contains("const LEASE_QUERY_CONFIRM_BATCH_SIZE");
    let sends_batched_queries =
        query_benchmark.contains("for _ in 0..LEASE_QUERY_CONFIRM_BATCH_SIZE");
    let drains_once_per_batch =
        query_benchmark.contains("drain_responses(&inbox, LEASE_QUERY_CONFIRM_BATCH_SIZE)");
    let counts_batched_elements =
        query_benchmark.contains("ctx.set_elements(iterations as u64 * batch_size)");

    // Assert
    assert!(
        defines_batch_size
            && sends_batched_queries
            && drains_once_per_batch
            && counts_batched_elements,
        "Tier 3 lease query must confirm real routed responses in bounded batches instead of serializing every query behind one wait loop"
    );
}

#[test]
fn should_batch_lease_tier3_mixed_response_confirmation() {
    // Arrange
    let source = read_repo_file("benches/tier3_system_lease.rs");
    let mixed_benchmark = section_between(
        &source,
        "fn should_complete_cycling_query_renew_operations",
        "\nstress_main!();",
    );

    // Act
    let defines_batch_size = source.contains("const LEASE_MIXED_CONFIRM_BATCH_SIZE: usize = 3");
    let sends_query_then_renew_then_query =
        mixed_benchmark.contains("for msg_type in [403, 401, 403]");
    let drains_once_per_cycle =
        mixed_benchmark.contains("drain_responses(&inbox, LEASE_MIXED_CONFIRM_BATCH_SIZE)");
    let parses_renew_response = mixed_benchmark.contains("parse_renew_token(&responses)");
    let counts_batched_elements =
        mixed_benchmark.contains("ctx.set_elements(iterations as u64 * batch_size)");

    // Assert
    assert!(
        defines_batch_size
            && sends_query_then_renew_then_query
            && drains_once_per_cycle
            && parses_renew_response
            && counts_batched_elements,
        "Tier 3 lease mixed workload must confirm the real query/renew/query cycle in one bounded response batch"
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

#[test]
fn should_drain_schedule_tier3_cleanup_writes_outside_measurement() {
    // Arrange
    let source = read_repo_file("benches/tier3_system_schedule.rs");
    let due_collection_helper = section_between(
        &source,
        "fn measure_prepared_due_collection",
        "\nfn precompute_data",
    );

    // Act
    let confirms_real_ack =
        due_collection_helper.contains(".bench_ack_pending_fire_claims(&delivered)");
    let drains_after_ack = due_collection_helper.contains(".bench_drain_storage()");
    let measures_only_claim_path =
        due_collection_helper.contains("measured += started_at.elapsed();");

    // Assert
    assert!(
        confirms_real_ack && drains_after_ack && measures_only_claim_path,
        "Tier 3 schedule due collection must confirm real ack delivery and drain out-of-timer cleanup writes before the next measured claim"
    );
}
