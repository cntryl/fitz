use super::common::*;

#[tokio::test]
pub(crate) async fn should_rebuild_stream_admin_from_durable_metadata_after_restart() {
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-admin");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut actor = make_stream_actor(store.clone(), "test", "events", "admin");
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"persisted"));
    commit_for_owner(&mut actor, 10, 100);
    drop(actor);
    drop(store);
    drop(engine);
    wait_for_stream_storage_release().await;

    let server = TestServer::start_with_local_storage(db_path)
        .await
        .expect("restart stream server");

    let streams = server.runtime.stream_list_streams(None);
    let stream = streams
        .iter()
        .find(|item| item.realm == "test" && item.area == "events" && item.resource == "admin")
        .expect("stream should be visible from durable admin rebuild");
    assert_eq!(stream.offset, 0);
    assert_eq!(stream.watermark, 0);
    assert_eq!(stream.size_bytes, b"persisted".len() as u64);
    assert_eq!(stream.sessions_active, 0);

    server.shutdown().await.expect("shutdown test server");
}

#[tokio::test]
pub(crate) async fn should_preserve_monotonic_stream_resource_offsets_after_restart() {
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-resource");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut actor = make_stream_actor(store.clone(), "test", "events", "orders");
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"one"));
    commit_for_owner(&mut actor, 10, 100);
    drop(actor);
    drop(store);
    drop(engine);
    wait_for_stream_storage_release().await;

    let engine = open_local_stream_engine(db_path)
        .await
        .expect("reopen local stream engine");
    let store = Arc::new(StreamStore::new(engine));
    let mut actor = make_stream_actor(store, "test", "events", "orders");
    actor.begin_append_session(10, 101, None).unwrap();
    append_for_owner(&mut actor, 10, 101, 1, Bytes::from_static(b"two"));
    commit_for_owner(&mut actor, 10, 101);

    let records = actor
        .read(0, 10, None)
        .expect("read restarted stream")
        .items;
    let records = event_records(&records);
    let resource_offsets: Vec<u64> = records
        .iter()
        .map(|record| record.resource_offset)
        .collect();
    let bodies: Vec<Vec<u8>> = records.iter().map(|record| record.body.to_vec()).collect();
    assert_eq!(resource_offsets, vec![0, 1]);
    assert_eq!(bodies, vec![b"one".to_vec(), b"two".to_vec()]);
}

#[tokio::test]
pub(crate) async fn should_preserve_monotonic_stream_area_offsets_after_restart() {
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-area");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut orders = make_stream_actor(store.clone(), "test", "events", "orders");
    let mut audits = make_stream_actor(store.clone(), "test", "events", "audits");
    orders.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut orders, 10, 100, 0, Bytes::from_static(b"one"));
    commit_for_owner(&mut orders, 10, 100);
    audits.begin_append_session(20, 200, None).unwrap();
    append_for_owner(&mut audits, 20, 200, 0, Bytes::from_static(b"two"));
    commit_for_owner(&mut audits, 20, 200);
    drop(orders);
    drop(audits);
    drop(store);
    drop(engine);
    wait_for_stream_storage_release().await;

    let engine = open_local_stream_engine(db_path)
        .await
        .expect("reopen local stream engine");
    let store = Arc::new(StreamStore::new(engine));
    let mut orders = make_stream_actor(store.clone(), "test", "events", "orders");
    orders.begin_append_session(10, 101, None).unwrap();
    append_for_owner(&mut orders, 10, 101, 1, Bytes::from_static(b"three"));
    commit_for_owner(&mut orders, 10, 101);

    let records = store
        .read_area(1, "test", "events", 0, 10, None)
        .expect("read area stream")
        .0;
    let records = event_records(&records);
    let area_offsets: Vec<u64> = records
        .iter()
        .map(|record| record.area_offset.expect("area offset"))
        .collect();
    assert_eq!(area_offsets, vec![0, 1, 2]);
}

#[tokio::test]
pub(crate) async fn should_preserve_monotonic_stream_realm_offsets_after_restart() {
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-realm");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut area_a = make_stream_actor(store.clone(), "test", "area-a", "orders");
    let mut area_b = make_stream_actor(store.clone(), "test", "area-b", "orders");
    area_a.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut area_a, 10, 100, 0, Bytes::from_static(b"one"));
    commit_for_owner(&mut area_a, 10, 100);
    area_b.begin_append_session(20, 200, None).unwrap();
    append_for_owner(&mut area_b, 20, 200, 0, Bytes::from_static(b"two"));
    commit_for_owner(&mut area_b, 20, 200);
    drop(area_a);
    drop(area_b);
    drop(store);
    drop(engine);
    wait_for_stream_storage_release().await;

    let engine = open_local_stream_engine(db_path)
        .await
        .expect("reopen local stream engine");
    let store = Arc::new(StreamStore::new(engine));
    let mut area_a = make_stream_actor(store.clone(), "test", "area-a", "orders");
    area_a.begin_append_session(10, 101, None).unwrap();
    append_for_owner(&mut area_a, 10, 101, 1, Bytes::from_static(b"three"));
    commit_for_owner(&mut area_a, 10, 101);

    let records = store
        .read_realm(1, "test", 0, 10, None)
        .expect("read realm stream")
        .0;
    let records = event_records(&records);
    let realm_offsets: Vec<u64> = records
        .iter()
        .map(|record| record.realm_offset.expect("realm offset"))
        .collect();
    assert_eq!(realm_offsets, vec![0, 1, 2]);
}

#[tokio::test]
pub(crate) async fn should_not_expose_uncommitted_values_given_restart() {
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-uncommitted");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut actor = make_stream_actor(store.clone(), "test", "events", "restart-loss");
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"staged"));
    drop(actor);
    drop(store);
    drop(engine);
    wait_for_stream_storage_release().await;

    let engine = open_local_stream_engine(db_path)
        .await
        .expect("reopen local stream engine");
    let store = Arc::new(StreamStore::new(engine));
    let mut actor = make_stream_actor(store, "test", "events", "restart-loss");
    actor.begin_append_session(10, 101, None).unwrap();
    append_for_owner(&mut actor, 10, 101, 0, Bytes::from_static(b"committed"));
    commit_for_owner(&mut actor, 10, 101);

    let records = actor
        .read(0, 10, None)
        .expect("read restarted stream")
        .items;
    let records = event_records(&records);
    let parsed: Vec<(u64, Vec<u8>)> = records
        .iter()
        .map(|record| (record.resource_offset, record.body.to_vec()))
        .collect();
    assert_eq!(parsed, vec![(0, b"committed".to_vec())]);
}

#[tokio::test]
pub(crate) async fn should_rebuild_event_sourced_aggregate_after_restart() {
    // Arrange
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-event-sourced");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut actor = make_stream_actor(store.clone(), "event-sourced", "orders", "order-restart");
    actor.begin_append_session(10, 100, None).unwrap();
    append_for_owner(&mut actor, 10, 100, 0, Bytes::from_static(b"opened"));
    append_for_owner(&mut actor, 10, 100, 1, Bytes::from_static(b"line-added"));
    commit_for_owner(&mut actor, 10, 100);
    drop(actor);
    drop(store);
    drop(engine);
    wait_for_stream_storage_release().await;

    // Act
    let engine = open_local_stream_engine(db_path)
        .await
        .expect("reopen local stream engine");
    let store = Arc::new(StreamStore::new(engine));
    let actor = make_stream_actor(store, "event-sourced", "orders", "order-restart");
    let records = actor
        .read(0, 10, None)
        .expect("read restarted aggregate stream")
        .items;
    let records = event_records(&records);

    // Assert
    let resource_offsets: Vec<u64> = records
        .iter()
        .map(|record| record.resource_offset)
        .collect();
    let bodies: Vec<Vec<u8>> = records.iter().map(|record| record.body.to_vec()).collect();
    assert_eq!(resource_offsets, vec![0, 1]);
    assert_eq!(bodies, vec![b"opened".to_vec(), b"line-added".to_vec()]);
}

#[tokio::test]
pub(crate) async fn should_start_local_stream_boot_given_promotion_frontier_layout() {
    // Arrange
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-frontier-empty");
    let db_path = db_path.to_string_lossy().to_string();

    // Act
    let server = TestServer::start_with_local_storage_and_stream_layout(
        db_path,
        StreamStorageLayout::PromotionFrontier,
    )
    .await
    .expect("start promotion-frontier local stream server");

    // Assert
    assert_eq!(server.runtime.stream_subscriptions_active(), 0);
}

#[tokio::test]
pub(crate) async fn should_fail_local_stream_restart_given_mismatched_layout_marker() {
    // Arrange
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-frontier-mismatch");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let mut txn = engine
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin write tx");
    txn.put(
        encode_stream_layout_marker_key(),
        StreamLayoutMarkerValue::new(StreamStorageLayout::LegacyCovering).encode(),
        None,
    )
    .expect("write legacy layout marker");
    txn.commit(cntryl_midge::WriteOptions::sync())
        .expect("commit legacy layout marker");
    drop(engine);
    wait_for_stream_storage_release().await;

    // Act
    let result = TestServer::start_with_local_storage_and_stream_layout(
        db_path,
        StreamStorageLayout::PromotionFrontier,
    )
    .await;

    // Assert
    match result {
        Ok(_) => panic!("mismatched layout marker should fail stream restart"),
        Err(error) => {
            assert!(error
                .to_string()
                .contains("ERR_STREAM_STORAGE_LAYOUT_MISMATCH"));
        }
    }
}
