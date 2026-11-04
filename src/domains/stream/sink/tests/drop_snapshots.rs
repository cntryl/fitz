use super::snapshot::commit_events;
use super::*;
use crate::snapshot::{SnapshotDomain, SnapshotSelector};

#[test]
fn should_capture_an_empty_resource_after_stream_drop() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/jobs/orders";
    commit_events(
        &context.sink.config.stream_store,
        context.family,
        "acme",
        "jobs",
        "orders",
        &[b"deleted body"],
    );
    assert_eq!(super::drop::drop_stream(&context, route).as_ref(), &[0]);
    let selector = SnapshotSelector::new(SnapshotDomain::Stream, context.family, route)
        .expect("exact Stream selector");

    // Act
    let artifact = context
        .sink
        .capture_stream_snapshot(&selector)
        .expect("capture dropped resource");

    // Assert
    assert_eq!(artifact.record_count(), 0);
    assert_eq!(artifact.stream_resources().len(), 1);
    assert_eq!(artifact.stream_resources()[0].route, route);
    assert_eq!(artifact.stream_resources()[0].captured_watermark, 0);
    assert_eq!(artifact.stream_resources()[0].records, []);
}

#[test]
fn should_restore_stream_history_into_a_dropped_realm_from_zero() {
    // Arrange
    let source = setup_test_context();
    let route = "stream://acme/jobs/orders";
    commit_events(
        &source.sink.config.stream_store,
        source.family,
        "acme",
        "jobs",
        "orders",
        &[b"restored body"],
    );
    let selector = SnapshotSelector::new(SnapshotDomain::Stream, source.family, "stream://acme/**")
        .expect("Stream realm selector");
    let artifact = source
        .sink
        .capture_stream_snapshot(&selector)
        .expect("capture source history")
        .to_bytes()
        .expect("encode source snapshot");
    let destination = setup_test_context();
    commit_events(
        &destination.sink.config.stream_store,
        destination.family,
        "acme",
        "jobs",
        "orders",
        &[b"deleted body", b"deleted tail"],
    );
    assert_eq!(
        super::drop::drop_stream(&destination, "stream://acme/*").as_ref(),
        &[0]
    );

    // Act
    let restored = destination
        .sink
        .restore_stream_snapshot(&artifact)
        .expect("restore into dropped namespace");

    // Assert
    assert_eq!(restored, [route]);
    let (items, _) = destination
        .sink
        .config
        .stream_store
        .read_global(destination.family.as_u64(), 0, 64, None, None)
        .expect("read restored global history");
    let [crate::domains::stream::protocol::StreamReadItem::Event(record)] = items.as_slice() else {
        panic!("restore must expose exactly one new event: {items:?}");
    };
    assert_eq!(record.body.as_ref(), b"restored body");
    assert_eq!(
        record.metadata.as_deref(),
        Some(b"event metadata".as_slice())
    );
    assert_eq!(record.resource_offset, 0);
    assert_eq!(record.area_offset, Some(0));
    assert_eq!(record.realm_offset, Some(0));
    assert_eq!(record.global_offset, Some(2));
}
