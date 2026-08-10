use super::*;
use crate::benchkit::live_sink::CountingSink;
use std::time::Duration;

fn register_watermark_notice_sink(
    context: &TestContext,
    route: &str,
) -> std::sync::Arc<CountingSink> {
    let sink = std::sync::Arc::new(CountingSink::new());
    context.router.register(
        RouteAddress::new(context.family, Route::new(route)),
        sink.clone(),
    );
    sink
}

#[test]
fn should_advance_plus_persist_area_plus_realm_watermarks_after_commit() {
    // Arrange
    let context = setup_test_context();
    let area_notice = register_watermark_notice_sink(&context, "stream://bench/events/*/watermark");
    let realm_notice = register_watermark_notice_sink(&context, "stream://bench/*/*/watermark");

    // Act: commit 5 events to a single resource (area+realm offsets 0..4)
    seed_committed_stream_route(
        &context,
        "stream://bench/events/orders",
        5,
        b"watermark-event",
    );

    let area_notified = area_notice.wait_for_count(1, Duration::from_secs(2));
    let realm_notified = realm_notice.wait_for_count(1, Duration::from_secs(2));

    // Assert: the AreaActor/RealmActor pipeline actually ran (nothing else in
    // the system delivers to these literal internal notice routes) and
    // persisted the correct watermark.
    assert!(area_notified >= 1, "expected an area watermark notice");
    assert!(realm_notified >= 1, "expected a realm watermark notice");
    assert_eq!(
        context
            .sink
            .get_watermark_for_tests(context.family, "bench", "events")
            .expect("area watermark"),
        4
    );
    assert_eq!(
        wait_for_realm_watermark(&context, "bench", 4, Duration::from_secs(2)),
        4
    );
}

#[test]
fn should_track_independent_watermarks_per_area_within_a_realm() {
    // Arrange
    let context = setup_test_context();
    let events_notice =
        register_watermark_notice_sink(&context, "stream://bench/events/*/watermark");
    let audit_notice = register_watermark_notice_sink(&context, "stream://bench/audit/*/watermark");

    // Act
    seed_committed_stream_route(&context, "stream://bench/events/orders", 3, b"events-event");
    seed_committed_stream_route(&context, "stream://bench/audit/ledger", 7, b"audit-event");

    events_notice.wait_for_count(1, Duration::from_secs(2));
    audit_notice.wait_for_count(1, Duration::from_secs(2));

    // Assert
    assert_eq!(
        context
            .sink
            .get_watermark_for_tests(context.family, "bench", "events")
            .expect("events area watermark"),
        2
    );
    assert_eq!(
        context
            .sink
            .get_watermark_for_tests(context.family, "bench", "audit")
            .expect("audit area watermark"),
        6
    );
    // Realm watermark tracks realm-wide offset contiguity directly (not an
    // aggregate of area-local watermarks): "events" occupies realm-wide
    // offsets 0-2 and "audit" occupies 3-9 (the realm-wide counter is shared
    // across areas in commit order), so the realm is fully contiguous up
    // through 9 even though "events"' own area-local watermark is only 2.
    assert_eq!(
        wait_for_realm_watermark(&context, "bench", 9, Duration::from_secs(2)),
        9
    );
}

#[test]
fn should_reject_client_routes_using_the_reserved_area_watermark_segment() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/__area__";
    let begin_frame = build_stream_begin(route);
    let (begin_msg_type, begin_payload) = extract_single_tlv_field(&begin_frame);

    // Act
    let response = request(&context, route, begin_msg_type, begin_payload);

    // Assert
    assert_eq!(
        decode_stream_error_message(response.as_ref()).expect("reserved resource error"),
        "resource '__area__' is reserved for internal broker use"
    );
}
