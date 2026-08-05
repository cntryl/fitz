use super::*;
use crate::benchkit::{
    build_stream_read_with_cursor, build_stream_read_with_limit,
    build_stream_read_with_snapshot_cursor,
};

fn global_read(context: &TestContext, from_offset: u64, limit: u64) -> DecodedStreamReadPayload {
    let frame = build_stream_read_with_cursor("stream://**", from_offset, limit, None);
    let (msg_type, payload) = extract_single_tlv_field(&frame);
    let response = request(context, "stream://**", msg_type, payload);
    let data = decode_stream_success_data(response.as_ref());
    decode_global_stream_read_payload(&data)
}

/// Resume a paginated global read, echoing back the prior page's cursor
/// fingerprint as routing-design.md §11.2 requires.
fn global_read_resume(
    context: &TestContext,
    from_offset: u64,
    limit: u64,
    cursor_fingerprint: Option<u64>,
    captured_watermark: Option<u64>,
) -> DecodedStreamReadPayload {
    let frame = build_stream_read_with_snapshot_cursor(
        "stream://**",
        from_offset,
        limit,
        cursor_fingerprint,
        captured_watermark,
    );
    let (msg_type, payload) = extract_single_tlv_field(&frame);
    let response = request(context, "stream://**", msg_type, payload);
    let data = decode_stream_success_data(response.as_ref());
    decode_global_stream_read_payload(&data)
}

#[test]
fn should_return_items_across_multiple_realms_in_global_commit_order() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://zeta/events/orders", 3, b"zeta-event");
    seed_committed_stream_route(&context, "stream://alpha/events/orders", 3, b"alpha-event");

    // Act
    let payload = global_read(&context, 0, 1000);

    // Assert
    assert_eq!(payload.routes.len(), 6);
    assert!(!payload.has_more);
    assert_eq!(
        payload
            .records
            .iter()
            .map(|record| record.global_offset)
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)]
    );
    assert!(payload.routes[..3]
        .iter()
        .all(|route| route.starts_with("stream://zeta/")));
    assert!(payload.routes[3..]
        .iter()
        .all(|route| route.starts_with("stream://alpha/")));
}

#[test]
fn should_paginate_across_realm_boundary() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://alpha/events/orders", 3, b"alpha-event");
    seed_committed_stream_route(&context, "stream://zeta/events/orders", 3, b"zeta-event");

    // Act
    let first_page = global_read(&context, 0, 2);
    let resume_offset = first_page
        .last_global_offset
        .expect("global read cursor offset");
    let second_page = global_read_resume(
        &context,
        resume_offset + 1,
        1000,
        first_page.cursor_fingerprint,
        first_page.captured_watermark,
    );

    // Assert
    assert_eq!(first_page.routes.len(), 2);
    assert!(first_page.has_more);
    assert!(first_page
        .routes
        .iter()
        .all(|route| route.starts_with("stream://alpha/")));

    assert_eq!(second_page.routes.len(), 4);
    assert!(!second_page.has_more);
    assert!(second_page.routes[..1]
        .iter()
        .all(|route| route.starts_with("stream://alpha/")));
    assert!(second_page.routes[1..]
        .iter()
        .all(|route| route.starts_with("stream://zeta/")));
}

#[test]
fn should_hold_captured_global_watermark_while_new_commits_arrive() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://alpha/events/orders", 3, b"alpha-event");
    let first_page = global_read(&context, 0, 1);
    seed_committed_stream_route(&context, "stream://zeta/events/orders", 1, b"later-event");
    let resume_offset = first_page
        .last_global_offset
        .expect("global read cursor offset")
        .saturating_add(1);

    // Act
    let second_page = global_read_resume(
        &context,
        resume_offset,
        100,
        first_page.cursor_fingerprint,
        first_page.captured_watermark,
    );

    // Assert
    assert_eq!(first_page.captured_watermark, Some(3));
    assert_eq!(second_page.routes.len(), 2);
    assert!(second_page
        .routes
        .iter()
        .all(|route| route.starts_with("stream://alpha/")));
    assert!(!second_page.has_more);
}

#[test]
fn should_allow_initial_read_from_nonzero_offset_without_cursor_metadata() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://alpha/events/orders", 3, b"alpha-event");
    seed_committed_stream_route(&context, "stream://zeta/events/orders", 3, b"zeta-event");
    let first_page = global_read(&context, 0, 2);
    let resume_offset = first_page
        .last_global_offset
        .expect("global read cursor offset");

    // Act: issue a new inclusive read from a nonzero offset without cursor metadata.
    let frame = build_stream_read_with_cursor("stream://**", resume_offset + 1, 1000, None);
    let (msg_type, payload) = extract_single_tlv_field(&frame);
    let response = request(&context, "stream://**", msg_type, payload);

    // Assert
    let page = decode_global_stream_read_payload(&decode_stream_success_data(response.as_ref()));
    assert_eq!(page.routes.len(), 4);
}

#[test]
fn should_reject_resumed_read_with_cursor_fingerprint_from_a_different_selector() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://alpha/events/orders", 3, b"alpha-event");
    seed_committed_stream_route(&context, "stream://zeta/events/orders", 3, b"zeta-event");
    let global_first_page = global_read(&context, 0, 2);
    let resume_offset = global_first_page
        .last_global_offset
        .expect("global read cursor offset");

    // A fingerprint issued for a *different* global selector.
    let filtered_frame = build_stream_read_with_limit("stream://*/events/*", 0, 1000);
    let (filtered_msg_type, filtered_payload) = extract_single_tlv_field(&filtered_frame);
    let filtered_response = request(
        &context,
        "stream://*/events/*",
        filtered_msg_type,
        filtered_payload,
    );
    let filtered_page =
        decode_global_stream_read_payload(&decode_stream_success_data(filtered_response.as_ref()));
    let foreign_fingerprint = filtered_page
        .cursor_fingerprint
        .expect("filtered global read cursor fingerprint");

    // Act: resume the global read using the realm read's fingerprint.
    let frame = build_stream_read_with_cursor(
        "stream://**",
        resume_offset + 1,
        1000,
        Some(foreign_fingerprint),
    );
    let (msg_type, payload) = extract_single_tlv_field(&frame);
    let response = request(&context, "stream://**", msg_type, payload);

    // Assert
    let error =
        decode_stream_error_message(response.as_ref()).expect("expected cursor mismatch error");
    assert!(
        error.contains("ERR_CURSOR_SELECTOR_MISMATCH"),
        "unexpected error: {error}"
    );
}

#[test]
fn should_return_empty_global_read_given_no_committed_realms() {
    // Arrange
    let context = setup_test_context();

    // Act
    let payload = global_read(&context, 0, 1000);

    // Assert
    assert_eq!(payload.routes.len(), 0);
    assert_eq!(payload.last_global_offset, None);
    assert!(!payload.has_more);
}

#[test]
fn should_resume_global_read_after_zero_limit_page() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://alpha/events/orders", 2, b"alpha-event");
    let empty_page = global_read(&context, 0, 0);

    // Act
    let resumed = global_read_resume(
        &context,
        0,
        10,
        empty_page.cursor_fingerprint,
        empty_page.captured_watermark,
    );

    // Assert
    assert!(empty_page.routes.is_empty());
    assert_eq!(empty_page.last_global_offset, None);
    assert!(empty_page.has_more);
    assert_eq!(resumed.routes.len(), 2);
    assert_eq!(resumed.records[0].global_offset, Some(0));
    assert_eq!(resumed.records[1].global_offset, Some(1));
}

#[test]
fn should_not_advance_global_cursor_past_an_empty_snapshot_tail() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(
        &context,
        "stream://alpha/events/orders",
        2,
        b"initial-event",
    );
    let tail_page = global_read(&context, 2, 10);
    seed_committed_stream_route(&context, "stream://beta/events/orders", 1, b"later-event");

    // Act: start a new snapshot at the offset following the last examined item.
    let next_offset = tail_page
        .last_global_offset
        .map_or(0, |offset| offset.saturating_add(1));
    let next_page = global_read(&context, next_offset, 10);

    // Assert
    assert!(tail_page.routes.is_empty());
    assert_eq!(tail_page.last_global_offset, Some(1));
    assert_eq!(next_page.routes.len(), 1);
    assert_eq!(next_page.records[0].global_offset, Some(2));
}
