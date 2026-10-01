use super::*;

fn commit_events(
    store: &crate::domains::stream::store::StreamStore,
    family: RouteFamily,
    realm: &str,
    area: &str,
    resource: &str,
    bodies: &[&'static [u8]],
) {
    let events = bodies
        .iter()
        .map(|body| crate::domains::stream::store::EventPayload {
            body: Bytes::from_static(body),
            metadata: Some(Bytes::from_static(b"event metadata")),
            discriminator: None,
        })
        .collect::<Vec<_>>();
    store
        .commit_records(crate::domains::stream::store::CommitRecordsParams {
            family: family.as_u64(),
            realm,
            area,
            resource,
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
        })
        .expect("commit Stream fixture events");
}

fn append_event_at(
    store: &crate::domains::stream::store::StreamStore,
    family: RouteFamily,
    expected_offset: u64,
) {
    let body = Bytes::copy_from_slice(&expected_offset.to_be_bytes());
    let event = crate::domains::stream::store::EventPayload {
        body,
        metadata: None,
        discriminator: None,
    };
    store
        .commit_records(crate::domains::stream::store::CommitRecordsParams {
            family: family.as_u64(),
            realm: "acme",
            area: "jobs",
            resource: "orders",
            expected_resource_next_offset: expected_offset,
            events: std::slice::from_ref(&event),
            ingest_metadata: None,
            mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
        })
        .expect("commit concurrent Stream event");
}

#[test]
fn should_round_trip_readable_stream_history_for_selected_family_and_pattern() {
    // Arrange
    let context = setup_test_context();
    let family = context.family;
    commit_events(
        &context.sink.config.stream_store,
        family,
        "acme",
        "jobs",
        "orders",
        &[b"first", b"second"],
    );
    commit_events(
        &context.sink.config.stream_store,
        family,
        "other",
        "jobs",
        "orders",
        &[b"other realm"],
    );
    commit_events(
        &context.sink.config.stream_store,
        RouteFamily::new(2),
        "acme",
        "jobs",
        "orders",
        &[b"other family"],
    );
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Stream,
        family,
        "stream://acme/**",
    )
    .expect("valid Stream realm selector");

    // Act
    let artifact = context
        .sink
        .capture_stream_snapshot(&selector)
        .expect("capture Stream history");
    let decoded = crate::snapshot::SnapshotArtifact::from_bytes(
        &artifact.to_bytes().expect("encode snapshot artifact"),
    )
    .expect("decode snapshot artifact");
    let destination = setup_test_context();

    let completed = destination
        .sink
        .restore_stream_snapshot(&decoded.to_bytes().expect("persisted artifact bytes"))
        .expect("restore into empty destination");
    let (restored, _) = destination
        .sink
        .config
        .stream_store
        .read_resource(&crate::domains::stream::store::ReadResourceParams {
            family: family.as_u64(),
            realm: "acme",
            area: "jobs",
            resource: "orders",
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        })
        .expect("read restored Stream history");

    // Assert
    assert_eq!(decoded.domain(), crate::snapshot::SnapshotDomain::Stream);
    assert_eq!(decoded.route_family(), family.as_u64());
    assert_eq!(decoded.record_count(), 2);
    assert_eq!(decoded.stream_resources().len(), 1);
    assert_eq!(decoded.stream_resources()[0].records[0].body, b"first");
    assert_eq!(decoded.stream_resources()[0].records[1].body, b"second");
    assert_eq!(decoded.stream_resources()[0].captured_watermark, 2);
    assert_eq!(completed, ["stream://acme/jobs/orders"]);
    assert_eq!(restored.len(), 2);
    let crate::domains::stream::protocol::StreamReadItem::Event(first) = &restored[0] else {
        panic!("first restored item is an event");
    };
    let crate::domains::stream::protocol::StreamReadItem::Event(second) = &restored[1] else {
        panic!("second restored item is an event");
    };
    assert_eq!(first.resource_offset, 0);
    assert_eq!(second.resource_offset, 1);
    assert_eq!(first.body, Bytes::from_static(b"first"));
    assert_eq!(second.body, Bytes::from_static(b"second"));
    assert_eq!(first.metadata.as_deref(), Some(&b"event metadata"[..]));
    let (area_records, _) = destination
        .sink
        .config
        .stream_store
        .read_area(family.as_u64(), "acme", "jobs", 0, 10, None)
        .expect("read restored area history");
    assert_eq!(area_records.len(), 2);
}

#[test]
fn should_reject_restore_when_any_selected_stream_history_exists() {
    // Arrange
    let source = setup_test_context();
    commit_events(
        &source.sink.config.stream_store,
        source.family,
        "acme",
        "jobs",
        "orders",
        &[b"source"],
    );
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Stream,
        source.family,
        "stream://acme/**",
    )
    .expect("valid Stream selector");
    let artifact = source
        .sink
        .capture_stream_snapshot(&selector)
        .expect("capture Stream history");
    let destination = setup_test_context();
    commit_events(
        &destination.sink.config.stream_store,
        destination.family,
        "acme",
        "jobs",
        "shipments",
        &[b"already here"],
    );

    // Act
    let result = destination
        .sink
        .restore_stream_snapshot(&artifact.to_bytes().expect("encode snapshot"));
    let (orders, _) = destination
        .sink
        .config
        .stream_store
        .read_resource(&crate::domains::stream::store::ReadResourceParams {
            family: destination.family.as_u64(),
            realm: "acme",
            area: "jobs",
            resource: "orders",
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        })
        .expect("read orders destination");

    // Assert
    assert!(result.is_err());
    assert!(orders.is_empty());
    let (shipments, _) = destination
        .sink
        .config
        .stream_store
        .read_resource(&crate::domains::stream::store::ReadResourceParams {
            family: destination.family.as_u64(),
            realm: "acme",
            area: "jobs",
            resource: "shipments",
            from_offset: 0,
            limit: 10,
            max_bytes: None,
        })
        .expect("read existing shipments history");
    assert_eq!(shipments.len(), 1);
}

#[test]
fn should_capture_an_empty_exact_stream_resource() {
    // Arrange
    let context = setup_test_context();
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Stream,
        context.family,
        "stream://acme/jobs/orders",
    )
    .expect("valid exact Stream selector");

    // Act
    let artifact = context
        .sink
        .capture_stream_snapshot(&selector)
        .expect("capture empty Stream resource");

    // Assert
    assert_eq!(artifact.stream_resources().len(), 1);
    assert_eq!(
        artifact.stream_resources()[0].route,
        "stream://acme/jobs/orders"
    );
    assert_eq!(artifact.stream_resources()[0].captured_watermark, 0);
    assert_eq!(artifact.stream_resources()[0].records, []);
}

#[test]
fn should_capture_empty_stream_realm_pattern_without_resources() {
    // Arrange
    let context = setup_test_context();
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Stream,
        context.family,
        "stream://empty/**",
    )
    .expect("valid empty Stream realm selector");

    // Act
    let artifact = context
        .sink
        .capture_stream_snapshot(&selector)
        .expect("capture empty Stream realm selector");

    // Assert
    assert_eq!(artifact.stream_resources(), []);
    assert_eq!(artifact.record_count(), 0);
}

#[test]
fn should_capture_a_consistent_prefix_while_stream_writes_continue() {
    // Arrange
    let context = setup_test_context();
    let family = context.family;
    for offset in 0..20 {
        append_event_at(&context.sink.config.stream_store, family, offset);
    }
    let selector = crate::snapshot::SnapshotSelector::new(
        crate::snapshot::SnapshotDomain::Stream,
        family,
        "stream://acme/jobs/orders",
    )
    .expect("valid exact Stream selector");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let store = context.sink.config.stream_store.clone();

    // Act
    std::thread::scope(|scope| {
        let writer_barrier = barrier.clone();
        let writer = scope.spawn(move || {
            writer_barrier.wait();
            for offset in 20..60 {
                append_event_at(&store, family, offset);
            }
        });
        barrier.wait();
        for _ in 0..20 {
            let artifact = context
                .sink
                .capture_stream_snapshot(&selector)
                .expect("capture while writer continues");
            let resource = &artifact.stream_resources()[0];
            assert_eq!(
                u64::try_from(resource.records.len()).expect("record count"),
                resource.captured_watermark
            );
            for (offset, record) in resource.records.iter().enumerate() {
                assert_eq!(
                    record.body,
                    u64::try_from(offset).expect("offset").to_be_bytes()
                );
            }
        }
        writer.join().expect("finish concurrent writer");
    });

    // Assert
    assert_eq!(
        context
            .sink
            .config
            .stream_store
            .get_next_resource_offset(family.as_u64(), "acme", "jobs", "orders")
            .expect("read final resource offset"),
        60
    );
}
