use super::*;

#[test]
fn should_return_all_realm_records_given_two_resource_batches_on_direct_sink_path() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(
        &context,
        "stream://bench/events/orders",
        50,
        b"realm read event",
    );
    seed_committed_stream_route(
        &context,
        "stream://bench/audit/ledger",
        50,
        b"realm read event",
    );
    // Realm watermark convergence across two areas is asynchronous; wait for
    // it before reading, since a realm-scoped read only surfaces data up to
    // the converged watermark.
    wait_for_realm_watermark(&context, "bench", 99, std::time::Duration::from_secs(2));

    // Act
    let realm_records = context
        .sink
        .read_realm_records_for_tests(context.family, "bench", 0, 1000)
        .expect("read realm from store");

    let read_frame = build_stream_read("stream://bench/*/*", 0);
    let (read_msg_type, read_payload) = extract_single_tlv_field(&read_frame);
    let response = request(&context, "stream://bench/*/*", read_msg_type, read_payload);
    let response_count = count_stream_read_records_from_payload(response.as_ref())
        .expect("count wildcard response records");

    // Assert
    assert_eq!(realm_records.len(), 100);
    assert_eq!(response_count, 100);
}

#[test]
fn should_read_realm_through_canonical_double_wildcard_alias() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(
        &context,
        "stream://bench/events/orders",
        1,
        b"realm alias event",
    );
    wait_for_realm_watermark(&context, "bench", 0, std::time::Duration::from_secs(2));
    let read_frame = build_stream_read("stream://bench/**", 0);
    let (read_msg_type, read_payload) = extract_single_tlv_field(&read_frame);

    // Act
    let response = request(&context, "stream://bench/**", read_msg_type, read_payload);

    // Assert
    assert_eq!(
        count_stream_read_records_from_payload(response.as_ref())
            .expect("count canonical realm alias response records"),
        1
    );
}
