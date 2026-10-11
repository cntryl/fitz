use super::*;
use crate::domains::stream::protocol::StreamWriteMode;
use crate::domains::stream::store::{CommitRecordsParams, StreamStore};
use crate::testkit::domain_internals::stream::EventPayload;

#[test]
fn should_keep_stream_usable_after_fast_queue_sst_publication() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::local(directory.path())
                .with_memtable_size_limit(8 * 1024 * 1024)
                .build()
                .unwrap(),
        )
        .unwrap(),
    );
    let family = engine.create_column_family("family").unwrap();
    let stream = StreamStore::new(engine.clone());
    let events = vec![EventPayload {
        body: Bytes::from_static(b"probe"),
        metadata: None,
        discriminator: None,
    }];
    stream
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "test",
            area: "events",
            resource: "before",
            expected_resource_next_offset: 0,
            events: &events,
            ingest_metadata: None,
            mode: StreamWriteMode::Buffered,
        })
        .unwrap();
    let store = QueueStore::new(engine.clone()).with_local_fast_wal(true);
    let mut queue = QueueActor::new_with_write_policy(
        RouteFamily::new(1),
        queue_key(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
        crate::domains::WritePolicy::BestEffort,
    );
    for index in 0..2048 {
        assert!(matches!(
            queue.handle_send(Bytes::from(vec![0x5a; 4096]), None),
            QueueResponse::Sent { .. }
        ));
        if index % 256 == 0 {
            store.flush_family(1).unwrap();
        }
    }
    store.flush_family(1).unwrap();
    // Complete SST publication before probing the Stream write path.
    engine.flush_cf(&family).unwrap();
    assert!(std::fs::read_dir(directory.path().join("sst"))
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .path()
            .extension()
            .is_some_and(|suffix| suffix == "sst")));

    // Act
    let result = stream.commit_records(CommitRecordsParams {
        family: 1,
        realm: "test",
        area: "events",
        resource: "after",
        expected_resource_next_offset: 0,
        events: &events,
        ingest_metadata: None,
        mode: StreamWriteMode::Buffered,
    });

    // Assert
    assert!(
        result.is_ok(),
        "Stream after Queue SST publication: {result:?}"
    );
    drop(queue);
    drop(store);
    drop(stream);
    crate::testkit::midge::shutdown_test_engine(engine);
}
