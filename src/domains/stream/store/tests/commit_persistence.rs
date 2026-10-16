use super::*;
use crate::domains::WritePolicy;

#[test]
fn should_acknowledge_cloud_wal_before_sync_stream_commit_returns_in_background_mode() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let options = cntryl_midge::OpenOptions::cloud_simulated(directory.path(), "stream", "commit")
        .build()
        .unwrap();
    let shutdown_budget = options.runtime_response_timeout();
    let engine = Arc::new(cntryl_midge::Engine::open(options).unwrap());
    engine.create_column_family("tenant_default").unwrap();
    let store = StreamDomainStorage::from(engine.clone()).into_store(
        StreamStorageLayout::PromotionFrontier,
        WritePolicy::CloudAsync,
        WritePolicy::CloudAsync,
    );
    store
        .ensure_layout_activation_for_existing_families()
        .unwrap();
    let events = single_event(b"cloud-sync-event");
    let before = engine.metrics().get_runtime_metrics().unwrap();

    // Act
    let result = store.commit_records(CommitRecordsParams {
        family: 1,
        realm: "test",
        area: "commit",
        resource: "policy",
        expected_resource_next_offset: 0,
        events: &events,
        ingest_metadata: None,
        mode: StreamWriteMode::Sync,
    });
    let after = engine.metrics().get_runtime_metrics().unwrap();

    // Assert
    assert!(result.is_ok(), "Stream commit failed: {result:?}");
    assert!(after.cloud_async_wal_segments_sealed > before.cloud_async_wal_segments_sealed);
    assert!(after.cloud_async_wal_uploads_completed > before.cloud_async_wal_uploads_completed);
    drop(store);
    crate::testkit::midge::shutdown_test_engine_with_timeout(engine, shutdown_budget);
}
