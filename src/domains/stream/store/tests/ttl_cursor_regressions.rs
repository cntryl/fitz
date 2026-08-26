use super::*;

fn commit_ttl_event(store: &StreamStore, expected_next_offset: u64) {
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            expected_resource_next_offset: expected_next_offset,
            events: &single_event(b"ttl"),
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit TTL event");
}

#[test]
fn should_not_regress_area_read_cursor_past_expired_records_before_from_offset() {
    // Arrange
    // The per-fragment expiry pre-pass walks the whole 64-record page,
    // including records BELOW `from_offset`. Advancing the cursor for those
    // hands a tailing client a resume point behind where it already was, so
    // it re-reads events it has consumed - forever, once the stream is idle.
    let db = create_test_engine_with_cfs(vec![1]);
    let clock = Arc::new(TestStreamClock::new(1_000));
    let store = StreamStore::with_config(db, BatchLimits::default(), StreamTTL::with_seconds(10))
        .with_clock_for_tests(clock.clone());
    for offset in 0..3 {
        commit_ttl_event(&store, offset);
    }
    clock.set(5_000);
    for offset in 3..5 {
        commit_ttl_event(&store, offset);
    }
    // Offsets 0..2 have expired; 3 and 4 are still live.
    clock.set(12_000);
    let (first_items, first_cursor) = store
        .read_area(1, "north", "orders", 0, 64, None)
        .expect("read the live area page");
    let resume_from = first_cursor
        .last_area_offset
        .expect("area cursor")
        .saturating_add(1);

    // Act
    let (tail_items, tail_cursor) = store
        .read_area(1, "north", "orders", resume_from, 64, None)
        .expect("tail the area from the resume point");

    // Assert
    assert_eq!(event_records(first_items).len(), 2);
    assert!(tail_items.is_empty());
    assert_eq!(
        tail_cursor.last_area_offset,
        Some(resume_from),
        "an empty page must leave the cursor at the requested offset, not behind it"
    );
}

#[test]
fn should_not_regress_realm_read_cursor_past_expired_records_before_from_offset() {
    // Arrange
    // Same defect as the area plane: `read_realm_with_filter` runs the same
    // expiry pre-pass over records the caller already paged past.
    let db = create_test_engine_with_cfs(vec![1]);
    let clock = Arc::new(TestStreamClock::new(1_000));
    let store = StreamStore::with_config(db, BatchLimits::default(), StreamTTL::with_seconds(10))
        .with_clock_for_tests(clock.clone());
    for offset in 0..3 {
        commit_ttl_event(&store, offset);
    }
    clock.set(5_000);
    for offset in 3..5 {
        commit_ttl_event(&store, offset);
    }
    clock.set(12_000);
    let (_, first_cursor) = store
        .read_realm(1, "north", 0, 64, None)
        .expect("read the live realm page");
    let resume_from = first_cursor
        .last_realm_offset
        .expect("realm cursor")
        .saturating_add(1);

    // Act
    let (tail_items, tail_cursor) = store
        .read_realm(1, "north", resume_from, 64, None)
        .expect("tail the realm from the resume point");

    // Assert
    assert!(tail_items.is_empty());
    assert_eq!(
        tail_cursor.last_realm_offset,
        Some(resume_from),
        "an empty page must leave the cursor at the requested offset, not behind it"
    );
}

#[test]
fn should_not_regress_resource_read_cursor_past_expired_records_before_from_offset() {
    // Arrange
    // `StreamActor::read_with_filter` short-circuits reads at or past its live
    // next-offset, but the admin surface calls `read_resource_with_filter`
    // straight through with a caller-supplied offset.
    let db = create_test_engine_with_cfs(vec![1]);
    let clock = Arc::new(TestStreamClock::new(1_000));
    let store = StreamStore::with_config(db, BatchLimits::default(), StreamTTL::with_seconds(10))
        .with_clock_for_tests(clock.clone());
    for offset in 0..3 {
        commit_ttl_event(&store, offset);
    }
    clock.set(5_000);
    for offset in 3..5 {
        commit_ttl_event(&store, offset);
    }
    clock.set(12_000);

    // Act
    let (items, cursor) = store
        .read_resource(&ReadResourceParams {
            family: 1,
            realm: "north",
            area: "orders",
            resource: "created",
            from_offset: 5,
            limit: 64,
            max_bytes: None,
        })
        .expect("read the resource past its last committed offset");

    // Assert
    assert!(items.is_empty());
    assert_eq!(
        cursor.last_resource_offset, 5,
        "an empty page must not move the cursor behind the requested offset"
    );
}
