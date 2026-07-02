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
