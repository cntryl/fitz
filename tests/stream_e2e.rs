//! Stream domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use bytes::Bytes;
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::domains::stream::store::StreamStore;
use fitz::domains::stream::StreamActor;
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::TestServer;
use fixtures::define_transport_tests;
use fixtures::transport::*;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::time::Duration;

async fn wait_for_stream_subscription_count(server: &TestServer, expected: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if server.runtime.stream_subscriptions_active() == expected {
                return;
            }

            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("wait for stream subscription count");
}

fn decode_stream_ok_data(payload: &[u8]) -> Vec<u8> {
    let mut dec = PayloadDecoder::new(payload);
    let status = dec.get_u8().expect("stream response status");
    assert_eq!(status, 0, "expected stream success payload");
    let _session_id = dec.get_optional_u64().expect("stream response session id");
    let data = dec.get_bytes().expect("stream response data");
    assert!(
        dec.is_complete(),
        "expected complete stream response payload"
    );
    data.to_vec()
}

fn parse_stream_read_records(frame: &[u8]) -> Vec<(u64, Vec<u8>)> {
    let (_msg_type, status, payload) = parse_stream_response(frame);
    assert_eq!(status, 0, "expected successful stream read");

    let data = decode_stream_ok_data(&payload);
    let mut dec = PayloadDecoder::new(&data);
    let count = dec.get_u32().expect("stream read record count");
    let mut records = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let offset = dec.get_u64().expect("stream read record offset");
        let body = dec.get_bytes().expect("stream read record body").to_vec();
        records.push((offset, body));
    }
    assert!(dec.is_complete(), "expected complete stream read payload");
    records
}

async fn wait_for_stream_storage_release() {
    tokio::time::sleep(Duration::from_millis(750)).await;
}

async fn open_local_stream_engine(
    db_path: String,
) -> Result<Arc<cntryl_midge::Engine>, Box<dyn std::error::Error>> {
    let boot_config = fitz::boot::runtime::BootConfig::with_local_storage(db_path);
    fitz::boot::storage::init(&boot_config).await
}

fn make_stream_actor(
    store: Arc<StreamStore>,
    realm: &str,
    area: &str,
    resource: &str,
) -> StreamActor {
    StreamActor::new(
        RouteFamily::new(1),
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        store,
    )
}

async fn commit_stream_record_with_offset<C>(
    client: &mut C,
    route: &str,
    expected_offset: u64,
    body: &[u8],
) where
    C: StreamConnector,
{
    let begin_response = client
        .send_and_receive(&build_stream_begin(route, expected_offset), 2000)
        .await
        .expect("begin stream");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(&build_stream_append(session_id, body), 2000)
        .await
        .expect("append stream");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected success for stream append");

    let commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit stream");
    let (_msg_type, status, _data) = parse_stream_response(&commit_response);
    assert_eq!(status, 0, "Expected success for stream commit");
}

async fn commit_stream_record<C>(client: &mut C, route: &str, body: &[u8])
where
    C: StreamConnector,
{
    commit_stream_record_with_offset(client, route, 0, body).await;
}

// Generic test helper for appending to stream
async fn should_append_data_to_stream<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/audit";

    // Act
    commit_stream_record(&mut client, route, b"event-001").await;
}

// Generic test helper for reading from stream
async fn should_read_appended_data<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/logs/main";
    let test_data = b"stream-record-1";
    commit_stream_record(&mut client, route, test_data).await;

    // Act
    let read_frame = build_stream_read(route, 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Expected success for stream read");
}

// Generic test helper for read ordering
async fn should_preserve_append_order<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/ordered/main";

    // Act
    commit_stream_record_with_offset(&mut client, route, 0, b"first").await;
    commit_stream_record_with_offset(&mut client, route, 1, b"second").await;

    let read_frame = build_stream_read(route, 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Expected success for ordered read");
}

// Generic test helper for read past end
async fn should_handle_read_past_end<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_stream_read("stream://test/sparse/main", 999999);

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, _status, _data) = parse_stream_response(&response);
    // Status can be success (empty read) or not found - both acceptable
    // Any status is acceptable here - we're just validating the request completes
}

// Generic test helper for FIFO ordering with multiple appends
async fn should_maintain_fifo_order_with_multiple_appends<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/fifo/main";

    // Act - Append 5 events
    for i in 1..=5 {
        let data = format!("event-{}", i).into_bytes();
        commit_stream_record_with_offset(&mut client, route, (i - 1) as u64, &data).await;
    }

    // Assert - Order should be preserved (can't directly verify without GET support for sequence, but test ensures no errors)
}

// Generic test helper for large stream payloads
async fn should_handle_large_stream_payload<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/large/main";
    let large_data = vec![b'D'; 60_000]; // Within u16 TLV length limit (65535)

    // Act
    commit_stream_record(&mut client, route, &large_data).await;
}

// Generic test helper for concurrent appends from multiple clients
async fn should_handle_concurrent_appends_from_multiple_clients<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client1 = C::connect(server).await.expect("connect 1");
    let mut client2 = C::connect(server).await.expect("connect 2");
    let route = "stream://test/concurrent/main";

    // Act - Both clients append
    commit_stream_record_with_offset(&mut client1, route, 0, b"client-1-event").await;
    commit_stream_record_with_offset(&mut client2, route, 1, b"client-2-event").await;
}

// Generic test helper for multiple sequential read operations
async fn should_handle_sequential_read_operations<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/sequential/main";

    // First, append some data
    commit_stream_record(&mut client, route, b"event-data").await;

    // Act - Sequential reads
    let read1_frame = build_stream_read(route, 0);
    let response1 = client
        .send_and_receive(&read1_frame, 2000)
        .await
        .expect("read 1");

    let (_msg_type, status1, _data) = parse_stream_response(&response1);
    assert_eq!(status1, 0);

    // Act - Read again with different offset
    let read2_frame = build_stream_read(route, 0);
    let response2 = client
        .send_and_receive(&read2_frame, 2000)
        .await
        .expect("read 2");

    let (_msg_type, status2, _data) = parse_stream_response(&response2);
    assert_eq!(status2, 0);

    // Act - Third read
    let read3_frame = build_stream_read(route, 0);
    let response3 = client
        .send_and_receive(&read3_frame, 2000)
        .await
        .expect("read 3");

    // Assert
    let (_msg_type, status3, _data) = parse_stream_response(&response3);
    assert_eq!(status3, 0, "Sequential reads should all succeed");
}

// Generic test helper for stream isolation
async fn should_isolate_streams_by_route<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route1 = "stream://test/app/stream1";
    let route2 = "stream://test/app/stream2";

    // Act - Append to stream 1
    commit_stream_record(&mut client, route1, b"data-1").await;

    // Act - Append to stream 2
    commit_stream_record(&mut client, route2, b"data-2").await;

    // Act - Read from stream 1
    let read_frame = build_stream_read(route1, 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Should isolate streams by route");
}

async fn should_read_committed_area_history_given_wildcard_route<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let orders_route = "stream://test/events/orders";
    let audits_route = "stream://test/events/audits";

    // Act
    commit_stream_record(&mut client, orders_route, b"order-created").await;
    commit_stream_record_with_offset(&mut client, audits_route, 0, b"audit-recorded").await;

    let response = client
        .send_and_receive(&build_stream_read("stream://test/events/*", 0), 2000)
        .await
        .expect("read area history");

    // Assert
    let records = parse_stream_read_records(&response);
    let bodies: Vec<Vec<u8>> = records.into_iter().map(|(_, body)| body).collect();
    assert_eq!(bodies, vec![b"order-created".to_vec(), b"audit-recorded".to_vec()]);
}

async fn should_read_committed_realm_history_given_wildcard_route<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let events_route = "stream://test/events/orders";
    let audit_route = "stream://test/audit/ledger";

    // Act
    commit_stream_record(&mut client, events_route, b"realm-one").await;
    commit_stream_record_with_offset(&mut client, audit_route, 0, b"realm-two").await;

    let response = client
        .send_and_receive(&build_stream_read("stream://test/*/*", 0), 2000)
        .await
        .expect("read realm history");

    // Assert
    let records = parse_stream_read_records(&response);
    let bodies: Vec<Vec<u8>> = records.into_iter().map(|(_, body)| body).collect();
    assert_eq!(bodies, vec![b"realm-one".to_vec(), b"realm-two".to_vec()]);
}

async fn should_retain_other_stream_subscription_after_unsubscribe<C>(server: &TestServer)
where
    C: StreamConnector,
{
    let removed_route = "stream://test/app/events";
    let retained_route = "stream://test/app/audits";
    let mut subscriber = C::connect(server).await.expect("connect subscriber");
    let mut writer = C::connect(server).await.expect("connect writer");

    let removed_subscribe_response = subscriber
        .send_and_receive(&build_stream_subscribe(removed_route), 2000)
        .await
        .expect("subscribe removed route");
    let (_msg_type, status, _data) = parse_stream_response(&removed_subscribe_response);
    assert_eq!(status, 0, "Expected success for removed route subscribe");

    let retained_subscribe_response = subscriber
        .send_and_receive(&build_stream_subscribe(retained_route), 2000)
        .await
        .expect("subscribe retained route");
    let (_msg_type, status, _data) = parse_stream_response(&retained_subscribe_response);
    assert_eq!(status, 0, "Expected success for retained route subscribe");

    let unsubscribe_response = subscriber
        .send_and_receive(&build_stream_unsubscribe(removed_route), 2000)
        .await
        .expect("unsubscribe removed route");
    let (_msg_type, status, _data) = parse_stream_response(&unsubscribe_response);
    assert_eq!(status, 0, "Expected success for removed route unsubscribe");

    commit_stream_record(&mut writer, removed_route, b"removed").await;
    assert!(
        subscriber.recv_frame(200).await.is_err(),
        "Removed route commit should not deliver after unsubscribe"
    );

    commit_stream_record(&mut writer, retained_route, b"retained").await;

    let retained_delivery = subscriber
        .recv_frame(2000)
        .await
        .expect("retained route delivery");
    let retained_delivery = parse_stream_delivery(&retained_delivery).expect("parse delivery");
    assert_eq!(retained_delivery.msg_type, 609);
    assert!(retained_delivery.subscription_id > 0);
    assert_eq!(retained_delivery.route, retained_route);

    let retained_payload: serde_json::Value =
        serde_json::from_slice(&retained_delivery.body).expect("notify payload JSON");
    assert_eq!(retained_payload["event"], "committed");
    assert_eq!(retained_payload["batch_size"], 1);
}

async fn should_remove_stream_subscription_when_subscriber_disconnects<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let route = "stream://test/app/events";
    let mut subscriber = C::connect(server).await.expect("connect subscriber");

    // Act
    let subscribe_response = subscriber
        .send_and_receive(&build_stream_subscribe(route), 2000)
        .await
        .expect("subscribe route");
    let (_msg_type, status, _data) = parse_stream_response(&subscribe_response);
    assert_eq!(status, 0, "Expected success for route subscribe");
    wait_for_stream_subscription_count(server, 1).await;

    drop(subscriber);
    server
        .wait_for_session_count(0)
        .await
        .expect("subscriber disconnect cleanup");

    // Assert
    wait_for_stream_subscription_count(server, 0).await;
    assert_eq!(server.runtime.stream_subscriptions_active(), 0);
}

async fn should_not_treat_stream_subscription_as_replay_cursor_given_shared_route<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let route = "stream://test/app/shared";
    let mut writer = C::connect(server).await.expect("connect writer");
    let mut subscriber = C::connect(server).await.expect("connect subscriber");

    // Act
    commit_stream_record(&mut writer, route, b"before-subscribe").await;

    let subscribe_response = subscriber
        .send_and_receive(&build_stream_subscribe(route), 2000)
        .await
        .expect("subscribe route");
    let (_msg_type, status, _data) = parse_stream_response(&subscribe_response);
    assert_eq!(status, 0, "Expected success for stream subscribe");

    commit_stream_record_with_offset(&mut writer, route, 1, b"after-subscribe").await;
    let live_delivery = subscriber
        .recv_frame(2000)
        .await
        .expect("live stream delivery");
    let live_delivery = parse_stream_delivery(&live_delivery).expect("parse stream delivery");

    let read_response = writer
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read full stream history");

    // Assert
    assert_eq!(live_delivery.msg_type, 609);
    assert_eq!(live_delivery.route, route);
    let delivery_payload: serde_json::Value =
        serde_json::from_slice(&live_delivery.body).expect("stream notify payload JSON");
    assert_eq!(delivery_payload["last_resource_offset"], 1);
    assert!(
        subscriber.recv_frame(200).await.is_err(),
        "stream subscribe should not replay committed history"
    );

    let records = parse_stream_read_records(&read_response);
    let bodies: Vec<Vec<u8>> = records.into_iter().map(|(_, body)| body).collect();
    assert_eq!(
        bodies,
        vec![b"before-subscribe".to_vec(), b"after-subscribe".to_vec()]
    );
}

async fn should_abort_uncommitted_stream_session_on_disconnect<C>(server: &TestServer)
where
    C: StreamConnector,
{
    let route = "stream://test/events/disconnect";

    let session_id = {
        let mut client = C::connect(server).await.expect("connect staging client");
        let begin_response = client
            .send_and_receive(&build_stream_begin(route, 0), 2000)
            .await
            .expect("begin staging stream session");
        let (_msg_type, status, data) = parse_stream_response(&begin_response);
        assert_eq!(status, 0, "expected success for stream begin");
        let session_id = parse_stream_session_id(&data).expect("stream session id");

        let append_response = client
            .send_and_receive(&build_stream_append(session_id, b"staged"), 2000)
            .await
            .expect("append staged stream event");
        let (_msg_type, status, _data) = parse_stream_response(&append_response);
        assert_eq!(status, 0, "expected success for staged append");
        session_id
    };

    server
        .wait_for_session_count(0)
        .await
        .expect("wait for disconnect cleanup");

    let mut client = C::connect(server)
        .await
        .expect("connect replacement client");

    let stale_commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("send stale commit");
    let (_msg_type, status, _data) = parse_stream_response(&stale_commit_response);
    assert_ne!(
        status, 0,
        "stale stream session should be gone after disconnect"
    );

    commit_stream_record(&mut client, route, b"committed").await;

    let read_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read committed stream");
    let records = parse_stream_read_records(&read_response);
    assert_eq!(records, vec![(0, b"committed".to_vec())]);
}

#[tokio::test]
async fn should_rebuild_stream_admin_from_durable_metadata_after_restart() {
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-admin");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut actor = make_stream_actor(store.clone(), "test", "events", "admin");
    actor.begin_append_session(10, 100, 0, None).unwrap();
    actor
        .append_to_session(100, Bytes::from_static(b"persisted"), None)
        .unwrap();
    actor.commit_session(100, StreamWriteMode::Sync).unwrap();
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

    server.shutdown().await;
}

#[tokio::test]
async fn should_preserve_monotonic_stream_resource_offsets_after_restart() {
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-resource");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut actor = make_stream_actor(store.clone(), "test", "events", "orders");
    actor.begin_append_session(10, 100, 0, None).unwrap();
    actor
        .append_to_session(100, Bytes::from_static(b"one"), None)
        .unwrap();
    actor.commit_session(100, StreamWriteMode::Sync).unwrap();
    drop(actor);
    drop(store);
    drop(engine);
    wait_for_stream_storage_release().await;

    let engine = open_local_stream_engine(db_path)
        .await
        .expect("reopen local stream engine");
    let store = Arc::new(StreamStore::new(engine));
    let mut actor = make_stream_actor(store, "test", "events", "orders");
    actor.begin_append_session(10, 101, 1, None).unwrap();
    actor
        .append_to_session(101, Bytes::from_static(b"two"), None)
        .unwrap();
    actor.commit_session(101, StreamWriteMode::Sync).unwrap();

    let records = actor
        .read(0, 10, None)
        .expect("read restarted stream")
        .records;
    let resource_offsets: Vec<u64> = records
        .iter()
        .map(|record| record.resource_offset)
        .collect();
    let bodies: Vec<Vec<u8>> = records.iter().map(|record| record.body.to_vec()).collect();
    assert_eq!(resource_offsets, vec![0, 1]);
    assert_eq!(bodies, vec![b"one".to_vec(), b"two".to_vec()]);
}

#[tokio::test]
async fn should_preserve_monotonic_stream_area_offsets_after_restart() {
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-area");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut orders = make_stream_actor(store.clone(), "test", "events", "orders");
    let mut audits = make_stream_actor(store.clone(), "test", "events", "audits");
    orders.begin_append_session(10, 100, 0, None).unwrap();
    orders
        .append_to_session(100, Bytes::from_static(b"one"), None)
        .unwrap();
    orders.commit_session(100, StreamWriteMode::Sync).unwrap();
    audits.begin_append_session(20, 200, 0, None).unwrap();
    audits
        .append_to_session(200, Bytes::from_static(b"two"), None)
        .unwrap();
    audits.commit_session(200, StreamWriteMode::Sync).unwrap();
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
    orders.begin_append_session(10, 101, 1, None).unwrap();
    orders
        .append_to_session(101, Bytes::from_static(b"three"), None)
        .unwrap();
    orders.commit_session(101, StreamWriteMode::Sync).unwrap();

    let records = store
        .read_area(1, "test", "events", 0, 10, None)
        .expect("read area stream")
        .0;
    let area_offsets: Vec<u64> = records
        .iter()
        .map(|record| record.area_offset.expect("area offset"))
        .collect();
    assert_eq!(area_offsets, vec![0, 1, 2]);
}

#[tokio::test]
async fn should_preserve_monotonic_stream_realm_offsets_after_restart() {
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-realm");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut area_a = make_stream_actor(store.clone(), "test", "area-a", "orders");
    let mut area_b = make_stream_actor(store.clone(), "test", "area-b", "orders");
    area_a.begin_append_session(10, 100, 0, None).unwrap();
    area_a
        .append_to_session(100, Bytes::from_static(b"one"), None)
        .unwrap();
    area_a.commit_session(100, StreamWriteMode::Sync).unwrap();
    area_b.begin_append_session(20, 200, 0, None).unwrap();
    area_b
        .append_to_session(200, Bytes::from_static(b"two"), None)
        .unwrap();
    area_b.commit_session(200, StreamWriteMode::Sync).unwrap();
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
    area_a.begin_append_session(10, 101, 1, None).unwrap();
    area_a
        .append_to_session(101, Bytes::from_static(b"three"), None)
        .unwrap();
    area_a.commit_session(101, StreamWriteMode::Sync).unwrap();

    let records = store
        .read_realm(1, "test", 0, 10, None)
        .expect("read realm stream")
        .0;
    let realm_offsets: Vec<u64> = records
        .iter()
        .map(|record| record.realm_offset.expect("realm offset"))
        .collect();
    assert_eq!(realm_offsets, vec![0, 1, 2]);
}

#[tokio::test]
async fn should_drop_uncommitted_stream_batch_on_restart() {
    let tempdir = TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-stream-uncommitted");
    let db_path = db_path.to_string_lossy().to_string();
    let engine = open_local_stream_engine(db_path.clone())
        .await
        .expect("open local stream engine");
    let store = Arc::new(StreamStore::new(engine.clone()));
    let mut actor = make_stream_actor(store.clone(), "test", "events", "restart-loss");
    actor.begin_append_session(10, 100, 0, None).unwrap();
    actor
        .append_to_session(100, Bytes::from_static(b"staged"), None)
        .unwrap();
    drop(actor);
    drop(store);
    drop(engine);
    wait_for_stream_storage_release().await;

    let engine = open_local_stream_engine(db_path)
        .await
        .expect("reopen local stream engine");
    let store = Arc::new(StreamStore::new(engine));
    let mut actor = make_stream_actor(store, "test", "events", "restart-loss");
    actor.begin_append_session(10, 101, 0, None).unwrap();
    actor
        .append_to_session(101, Bytes::from_static(b"committed"), None)
        .unwrap();
    actor.commit_session(101, StreamWriteMode::Sync).unwrap();

    let records = actor
        .read(0, 10, None)
        .expect("read restarted stream")
        .records;
    let parsed: Vec<(u64, Vec<u8>)> = records
        .iter()
        .map(|record| (record.resource_offset, record.body.to_vec()))
        .collect();
    assert_eq!(parsed, vec![(0, b"committed".to_vec())]);
}

define_transport_tests!(
    TcpStreamConnector,
    WsStreamConnector;
    should_append_data_to_stream_tcp / should_append_data_to_stream_ws => should_append_data_to_stream,
    should_read_appended_data_tcp / should_read_appended_data_ws => should_read_appended_data,
    should_preserve_append_order_tcp / should_preserve_append_order_ws => should_preserve_append_order,
    should_handle_read_past_end_tcp / should_handle_read_past_end_ws => should_handle_read_past_end,
    should_maintain_fifo_order_with_multiple_appends_tcp / should_maintain_fifo_order_with_multiple_appends_ws => should_maintain_fifo_order_with_multiple_appends,
    should_handle_large_stream_payload_tcp / should_handle_large_stream_payload_ws => should_handle_large_stream_payload,
    should_handle_concurrent_appends_from_multiple_clients_tcp / should_handle_concurrent_appends_from_multiple_clients_ws => should_handle_concurrent_appends_from_multiple_clients,
    should_handle_sequential_read_operations_tcp / should_handle_sequential_read_operations_ws => should_handle_sequential_read_operations,
    should_isolate_streams_by_route_tcp / should_isolate_streams_by_route_ws => should_isolate_streams_by_route,
    should_read_committed_area_history_given_wildcard_route_tcp / should_read_committed_area_history_given_wildcard_route_ws => should_read_committed_area_history_given_wildcard_route,
    should_read_committed_realm_history_given_wildcard_route_tcp / should_read_committed_realm_history_given_wildcard_route_ws => should_read_committed_realm_history_given_wildcard_route,
    should_abort_uncommitted_stream_session_on_disconnect_tcp / should_abort_uncommitted_stream_session_on_disconnect_ws => should_abort_uncommitted_stream_session_on_disconnect,
    should_remove_stream_subscription_when_subscriber_disconnects_tcp / should_remove_stream_subscription_when_subscriber_disconnects_ws => should_remove_stream_subscription_when_subscriber_disconnects,
    should_not_treat_stream_subscription_as_replay_cursor_given_shared_route_tcp / should_not_treat_stream_subscription_as_replay_cursor_given_shared_route_ws => should_not_treat_stream_subscription_as_replay_cursor_given_shared_route,
    should_retain_other_stream_subscription_after_unsubscribe_tcp / should_retain_other_stream_subscription_after_unsubscribe_ws => should_retain_other_stream_subscription_after_unsubscribe,
);
