use super::*;
use crate::domains::stream::protocol::StreamWriteMode;
use crate::domains::stream::storage::encode_payload_blob_key;
use crate::domains::stream::store::{CommitRecordsParams, EventPayload};
use crate::domains::stream::StreamDropScope;

fn domain(engine: Arc<cntryl_midge::Engine>) -> StreamDomain {
    StreamDomain::try_new(
        engine,
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageWriteOptions::local(),
    )
    .expect("create Stream domain without optional metrics")
}

fn seed_large_payload(sink: &StreamDomain) {
    sink.config
        .stream_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "acme",
            area: "jobs",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &[EventPayload {
                body: Bytes::from(vec![0xA5; 32 * 1024]),
                metadata: Some(Bytes::from_static(b"private metadata")),
                discriminator: None,
            }],
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit large payload");
}

fn drop_history(sink: &StreamDomain) {
    let scope = StreamDropScope::parse(RouteFamily::new(1), "stream://acme/jobs/orders")
        .expect("valid DROP scope");
    sink.config
        .stream_store
        .drop_scope(&scope, StreamWriteMode::Sync)
        .expect("persist DROP intent");
}

fn payload_exists(engine: &cntryl_midge::Engine) -> bool {
    engine
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin payload read")
        .get(&encode_payload_blob_key(0))
        .expect("read payload blob")
        .is_some()
}

#[test]
fn should_schedule_idle_stream_reclamation_from_its_persisted_drop_intent() {
    // Arrange
    let engine = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let sink = domain(engine.clone());
    let family = RouteFamily::new(1);
    seed_large_payload(&sink);
    for _ in 0..32 {
        if !sink.config.stream_store.has_pending_maintenance(1) {
            break;
        }
        sink.run_maintenance_slice_for_tests(family);
    }
    assert!(!sink.config.stream_store.has_pending_maintenance(1));
    drop_history(&sink);
    assert!(payload_exists(&engine));

    // Act
    sink.run_maintenance_slice();
    sink.inspect_family_for_tests(family, |_| ());

    // Assert
    assert!(!payload_exists(&engine));
    assert!(!sink.config.stream_store.has_pending_maintenance(1));
}

#[test]
fn should_schedule_reclamation_after_restart_without_new_stream_traffic() {
    // Arrange
    let engine = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let sink = domain(engine.clone());
    seed_large_payload(&sink);
    drop_history(&sink);
    sink.stop();
    drop(sink);
    let restarted = domain(engine.clone());
    assert!(payload_exists(&engine));

    // Act
    restarted.run_maintenance_slice();
    restarted.inspect_family_for_tests(RouteFamily::new(1), |_| ());

    // Assert
    assert!(!payload_exists(&engine));
}

#[test]
fn should_refresh_stream_inventory_when_a_late_drop_has_no_payloads_to_reclaim() {
    // Arrange
    let engine = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let admin = crate::control::admin::read_model::AdminReadModel::new();
    let sink = StreamDomain::try_new(
        engine.clone(),
        Arc::new(Router::new()),
        admin.clone(),
        StreamStorageWriteOptions::local(),
    )
    .expect("create Stream domain");
    let family = RouteFamily::new(1);
    seed_large_payload(&sink);
    for _ in 0..32 {
        if !sink.config.stream_store.has_pending_maintenance(1) {
            break;
        }
        sink.run_maintenance_slice_for_tests(family);
    }
    sink.sync_admin_snapshot();
    assert_eq!(admin.streams(None).len(), 1);
    let mut transaction = engine
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin expired global fragment removal");
    transaction
        .delete(crate::domains::stream::storage::encode_compact_global_page_key(0))
        .expect("remove expired global fragment");
    transaction
        .delete(encode_payload_blob_key(0))
        .expect("remove expired payload");
    transaction
        .commit(cntryl_midge::WriteOptions::sync())
        .expect("commit expired history removal");
    // A late storage application runs after the request handler has returned.
    drop_history(&sink);

    // Act
    sink.run_maintenance_slice();
    sink.inspect_family_for_tests(family, |_| ());
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    assert_eq!(admin.streams(None).len(), 0);
}
