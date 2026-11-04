use super::*;
use crate::domains::stream::StreamDropScope;
use crate::runtime::routing::RouteFamily;

fn realm_scope() -> StreamDropScope {
    StreamDropScope::parse(RouteFamily::new(1), "stream://acme/*").unwrap()
}

fn resource_scope(resource: &str) -> StreamDropScope {
    StreamDropScope::parse(
        RouteFamily::new(1),
        &format!("stream://acme/logs/{resource}"),
    )
    .unwrap()
}

fn append_blobs(store: &StreamStore, resource: &str, count: usize) {
    let events = vec![
        EventPayload {
            body: Bytes::from(vec![0xA5; INLINE_PAYLOAD_LIMIT + 1]),
            metadata: None,
            discriminator: None,
        };
        count
    ];
    store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "acme",
            area: "logs",
            resource,
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .unwrap();
}

fn reclaim_after_retained_prefix(store: &StreamStore) {
    append_blobs(store, "retained", 1);
    append_blobs(store, "deleted", 129);
    store
        .drop_scope(&resource_scope("deleted"), StreamWriteMode::Sync)
        .unwrap();
    store.run_maintenance(1).unwrap();
}

#[test]
fn should_preserve_drop_reader_fencing_after_a_stale_cold_layout_activation() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
    let during_activation = store.clone();
    super::super::config_and_layout::before_next_layout_commit_for_tests(Box::new(move || {
        during_activation
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "acme",
                area: "logs",
                resource: "events",
                expected_resource_next_offset: 0,
                events: &single_event(b"deleted history"),
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .unwrap();
        during_activation
            .drop_scope(&realm_scope(), StreamWriteMode::Sync)
            .unwrap();
    }));

    // Act
    let activation = store.inspect_and_activate_layout_for_family(1);

    // Assert
    assert!(activation.is_ok(), "{activation:?}");
    let transaction = store.db.read_tx(1);
    let marker = transaction
        .get(&encode_stream_layout_marker_key())
        .unwrap()
        .unwrap();
    assert_eq!(marker.as_ref(), [0, 0xD5, 2]);
    assert_eq!(
        store
            .resource_drop_cutoff(1, "acme", "logs", "events")
            .unwrap(),
        1
    );
}

#[test]
fn should_accept_a_concurrent_compatible_cold_layout_activation() {
    // Arrange
    let store = Arc::new(StreamStore::new(create_test_engine_with_cfs(vec![1])));
    let during_activation = store.clone();
    super::super::config_and_layout::before_next_layout_commit_for_tests(Box::new(move || {
        during_activation
            .inspect_and_activate_layout_for_family(1)
            .unwrap();
    }));

    // Act
    let activation = store.inspect_and_activate_layout_for_family(1);

    // Assert
    assert!(activation.is_ok(), "{activation:?}");
    assert_eq!(
        store
            .db
            .read_tx(1)
            .get(&encode_stream_layout_marker_key())
            .unwrap()
            .unwrap()
            .as_ref(),
        [0, 0xD4, 2]
    );
}

#[test]
fn should_finish_payload_reclamation_while_the_same_empty_scope_is_dropped_again() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    append_blobs(&store, "events", 130);
    let scope = realm_scope();
    store.drop_scope(&scope, StreamWriteMode::Sync).unwrap();
    store.run_maintenance(1).unwrap();
    let mut restarts = Vec::new();

    // Act
    for _ in 0..3 {
        store.drop_scope(&scope, StreamWriteMode::Sync).unwrap();
        let progress = store
            .db
            .read_tx(1)
            .get(&super::super::drop_reclamation::progress_key())
            .unwrap()
            .map(|bytes| super::super::drop_reclamation::decode_progress(&bytes).unwrap());
        restarts.push(progress);
        store.run_maintenance(1).unwrap();
    }

    // Assert
    let retained_payload = store
        .db
        .read_tx(1)
        .get(&encode_payload_blob_key(128))
        .unwrap();
    assert!(
        retained_payload.is_none(),
        "repeated empty DROP restarted reclamation at {restarts:?}"
    );
}

#[test]
fn should_revisit_scanned_payloads_when_a_new_scope_is_dropped() {
    // Arrange
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    reclaim_after_retained_prefix(&store);

    // Act
    store
        .drop_scope(&resource_scope("retained"), StreamWriteMode::Sync)
        .unwrap();
    drain_maintenance(&store, 1);

    // Assert
    assert!(store
        .db
        .read_tx(1)
        .get(&encode_payload_blob_key(0))
        .unwrap()
        .is_none());
}

#[test]
fn should_resume_pending_reclamation_rescans_after_an_engine_restart() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(directory.path())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    engine.create_column_family("family-one").unwrap();
    let store = StreamStore::new(engine.clone());
    reclaim_after_retained_prefix(&store);
    store
        .drop_scope(&resource_scope("retained"), StreamWriteMode::Sync)
        .unwrap();
    drop(store);
    crate::testkit::midge::shutdown_test_engine(engine);

    // Act
    let reopened_engine = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(directory.path())
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    let reopened = StreamStore::new(reopened_engine.clone());
    reopened
        .validate_persisted_state_for_existing_families()
        .unwrap();
    drain_maintenance(&reopened, 1);

    // Assert
    assert!(reopened
        .db
        .read_tx(1)
        .get(&encode_payload_blob_key(0))
        .unwrap()
        .is_none());
    drop(reopened);
    crate::testkit::midge::shutdown_test_engine(reopened_engine);
}
