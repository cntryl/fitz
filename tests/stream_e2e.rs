//! Stream domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use bytes::{BufMut, Bytes};
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::domains::stream::storage::{encode_stream_layout_marker_key, StreamLayoutMarkerValue};
use fitz::domains::stream::store::StreamStore;
use fitz::domains::stream::{StreamActor, StreamReadItem, StreamRecord, StreamStorageLayout};
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

fn parse_stream_ok_data(frame: &[u8]) -> Vec<u8> {
    let (_msg_type, status, payload) = parse_stream_response(frame);
    assert_eq!(status, 0, "expected successful stream response");
    decode_stream_ok_data(&payload)
}

fn parse_stream_error_message(frame: &[u8]) -> String {
    let (_msg_type, status, payload) = parse_stream_response(frame);
    assert_eq!(status, 1, "expected failing stream response");

    let (_code, message) =
        fitz::protocol::error_codes::decode_error_body(&payload).expect("stream error envelope");
    message
}

fn event_records(items: &[StreamReadItem]) -> Vec<StreamRecord> {
    items
        .iter()
        .filter_map(|item| match item {
            StreamReadItem::Event(record) => Some(record.clone()),
            _ => None,
        })
        .collect()
}

fn append_for_owner(
    actor: &mut StreamActor,
    owner_session_id: u64,
    stream_session_id: u64,
    expected_offset: u64,
    body: Bytes,
) {
    actor
        .append_to_session_with_discriminator_for_owner(
            owner_session_id,
            stream_session_id,
            expected_offset,
            body,
            None,
            None,
        )
        .unwrap();
}

fn commit_for_owner(actor: &mut StreamActor, owner_session_id: u64, stream_session_id: u64) {
    actor
        .commit_session_for_owner(owner_session_id, stream_session_id, StreamWriteMode::Sync)
        .unwrap();
}

fn build_stream_last(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(605, &buf);
    builder.build()
}

fn build_stream_get_metadata(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(606, &buf);
    builder.build()
}

fn build_stream_rollback(session_id: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u64(session_id);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(603, &buf);
    builder.build()
}

fn build_stream_read_with_options(
    route: &str,
    start_offset: u64,
    limit: u64,
    max_bytes: Option<u64>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());
    buf.put_u64(start_offset);
    buf.put_u64(limit);
    match max_bytes {
        Some(value) => {
            buf.put_u8(1);
            buf.put_u64(value);
        }
        None => buf.put_u8(0),
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

fn build_stream_read_with_raw_filter(
    route: &str,
    start_offset: u64,
    limit: u64,
    max_bytes: Option<u64>,
    filter: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(route.len() as u32);
    buf.put_slice(route.as_bytes());
    buf.put_u64(start_offset);
    buf.put_u64(limit);
    match max_bytes {
        Some(value) => {
            buf.put_u8(1);
            buf.put_u64(value);
        }
        None => buf.put_u8(0),
    }
    buf.put_u8(1);
    buf.put_u32(filter.len() as u32);
    buf.put_slice(filter);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WireStreamRecord {
    resource_offset: u64,
    area_offset: Option<u64>,
    realm_offset: Option<u64>,
    body: Vec<u8>,
    metadata: Option<Vec<u8>>,
    created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WireReadCursor {
    last_resource_offset: u64,
    last_area_offset: Option<u64>,
    last_realm_offset: Option<u64>,
    has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WireReadResponse {
    records: Vec<WireStreamRecord>,
    cursor: WireReadCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WireStreamMetadata {
    first_resource_offset: Option<u64>,
    last_resource_offset: Option<u64>,
    resource_count: u64,
    max_batch_events: u64,
    max_batch_bytes: u64,
    ttl_seconds: Option<u64>,
    area_watermark: u64,
    realm_watermark: u64,
}

fn decode_wire_stream_record(dec: &mut PayloadDecoder<'_>) -> WireStreamRecord {
    let resource_offset = dec.get_u64().expect("stream read record offset");
    let area_offset = dec.get_optional_u64().expect("stream area offset");
    let realm_offset = dec.get_optional_u64().expect("stream realm offset");
    let body = dec.get_bytes().expect("stream read record body").to_vec();
    let metadata = dec
        .get_optional_bytes()
        .expect("stream read record metadata")
        .map(|bytes| bytes.to_vec());
    let created_at = dec.get_u64().expect("stream read record created_at");

    WireStreamRecord {
        resource_offset,
        area_offset,
        realm_offset,
        body,
        metadata,
        created_at,
    }
}

fn parse_stream_read_response(frame: &[u8]) -> WireReadResponse {
    let (_msg_type, status, payload) = parse_stream_response(frame);
    assert_eq!(status, 0, "expected successful stream read");

    let data = decode_stream_ok_data(&payload);
    let mut dec = PayloadDecoder::new(&data);
    let count = dec.get_u32().expect("stream read record count") as usize;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        match dec.get_u8().expect("stream read item tag") {
            0 => records.push(decode_wire_stream_record(&mut dec)),
            1 => {
                let _offset = dec.get_u64().expect("stream filtered offset");
                let _reason = dec.get_u8().expect("stream filtered reason");
            }
            2 => {
                let _from_offset = dec.get_u64().expect("stream filtered range from offset");
                let _to_offset = dec.get_u64().expect("stream filtered range to offset");
                let _reason = dec.get_u8().expect("stream filtered range reason");
            }
            other => panic!("unexpected stream read item tag: {}", other),
        }
    }

    let cursor = WireReadCursor {
        last_resource_offset: dec.get_u64().expect("stream cursor resource offset"),
        last_area_offset: dec.get_optional_u64().expect("stream cursor area offset"),
        last_realm_offset: dec.get_optional_u64().expect("stream cursor realm offset"),
        has_more: dec.get_u8().expect("stream cursor has_more") == 1,
    };
    assert!(dec.is_complete(), "expected complete stream read payload");

    WireReadResponse { records, cursor }
}

fn parse_stream_last_response(frame: &[u8]) -> Option<WireStreamRecord> {
    let (_msg_type, status, payload) = parse_stream_response(frame);
    assert_eq!(status, 0, "expected successful stream last response");

    let data = decode_stream_ok_data(&payload);
    if data.is_empty() {
        return None;
    }

    let mut dec = PayloadDecoder::new(&data);
    let record = decode_wire_stream_record(&mut dec);
    assert!(dec.is_complete(), "expected complete stream last payload");
    Some(record)
}

fn parse_stream_metadata_response(frame: &[u8]) -> WireStreamMetadata {
    let (_msg_type, status, payload) = parse_stream_response(frame);
    assert_eq!(status, 0, "expected successful stream metadata response");

    let data = decode_stream_ok_data(&payload);
    let mut dec = PayloadDecoder::new(&data);
    let metadata = WireStreamMetadata {
        first_resource_offset: dec
            .get_optional_u64()
            .expect("first resource metadata offset"),
        last_resource_offset: dec
            .get_optional_u64()
            .expect("last resource metadata offset"),
        resource_count: dec.get_u64().expect("resource metadata count"),
        max_batch_events: dec.get_u64().expect("resource max_batch_events"),
        max_batch_bytes: dec.get_u64().expect("resource max_batch_bytes"),
        ttl_seconds: dec.get_optional_u64().expect("resource ttl seconds"),
        area_watermark: dec.get_u64().expect("resource area watermark"),
        realm_watermark: dec.get_u64().expect("resource realm watermark"),
    };
    assert!(
        dec.is_complete(),
        "expected complete stream metadata payload"
    );
    metadata
}

fn parse_stream_read_records(frame: &[u8]) -> Vec<(u64, Vec<u8>)> {
    parse_stream_read_response(frame)
        .records
        .into_iter()
        .map(|record| (record.resource_offset, record.body))
        .collect()
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
    .expect("create stream actor")
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
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin stream");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(
            &build_stream_append(session_id, expected_offset, body),
            2000,
        )
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
    let read = parse_stream_read_response(&response);
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].resource_offset, 0);
    assert_eq!(read.records[0].body, test_data.to_vec());
    assert_eq!(read.cursor.last_resource_offset, 0);
    assert_eq!(read.cursor.last_area_offset, Some(0));
    assert_eq!(read.cursor.last_realm_offset, Some(0));
    assert!(!read.cursor.has_more);
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
    let read = parse_stream_read_response(&response);
    let offsets: Vec<u64> = read
        .records
        .iter()
        .map(|record| record.resource_offset)
        .collect();
    let bodies: Vec<Vec<u8>> = read
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    assert_eq!(offsets, vec![0, 1]);
    assert_eq!(bodies, vec![b"first".to_vec(), b"second".to_vec()]);
    assert_eq!(read.cursor.last_resource_offset, 1);
    assert_eq!(read.cursor.last_area_offset, Some(1));
    assert_eq!(read.cursor.last_realm_offset, Some(1));
    assert!(!read.cursor.has_more);
}

// Generic test helper for read past end
async fn should_handle_read_past_end<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/sparse/main";
    commit_stream_record(&mut client, route, b"present").await;
    let frame = build_stream_read(route, 1);

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let read = parse_stream_read_response(&response);
    assert!(read.records.is_empty());
    assert_eq!(read.cursor.last_resource_offset, 1);
    assert_eq!(read.cursor.last_area_offset, None);
    assert_eq!(read.cursor.last_realm_offset, None);
    assert!(!read.cursor.has_more);
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

    // Act
    let response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read stream history");

    // Assert
    let read = parse_stream_read_response(&response);
    let offsets: Vec<u64> = read
        .records
        .iter()
        .map(|record| record.resource_offset)
        .collect();
    let bodies: Vec<Vec<u8>> = read
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    assert_eq!(offsets, vec![0, 1, 2, 3, 4]);
    assert_eq!(
        bodies,
        vec![
            b"event-1".to_vec(),
            b"event-2".to_vec(),
            b"event-3".to_vec(),
            b"event-4".to_vec(),
            b"event-5".to_vec(),
        ]
    );
    assert_eq!(read.cursor.last_resource_offset, 4);
    assert!(!read.cursor.has_more);
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

    // Act
    commit_stream_record_with_offset(&mut client1, route, 0, b"client-1-event").await;

    let begin_response = client2
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin stale stream write");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client2
        .send_and_receive(&build_stream_append(session_id, 0, b"client-2-event"), 2000)
        .await
        .expect("append stale stream write");
    let error = parse_stream_error_message(&append_response);

    let read_response = client1
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read committed stream history");

    // Assert
    assert!(error.contains("concurrency conflict"));
    let read = parse_stream_read_response(&read_response);
    let bodies: Vec<Vec<u8>> = read.records.into_iter().map(|record| record.body).collect();
    assert_eq!(bodies, vec![b"client-1-event".to_vec()]);
}

async fn should_reject_future_expected_offset_given_gap<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/concurrent/future-offset";
    commit_stream_record(&mut client, route, b"event-0").await;

    // Act
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin future-offset stream write");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(&build_stream_append(session_id, 2, b"gap"), 2000)
        .await
        .expect("append future-offset stream write");
    let error = parse_stream_error_message(&append_response);

    // Assert
    assert!(error.contains("concurrency conflict"));
}

async fn should_return_session_not_found_given_append_to_unknown_session<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act
    let response = client
        .send_and_receive(&build_stream_append(999, 0, b"ghost"), 2000)
        .await
        .expect("append unknown stream session");
    let error = parse_stream_error_message(&response);

    // Assert
    assert!(error.contains("session not found"));
}

async fn should_return_session_not_found_given_commit_to_unknown_session<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act
    let response = client
        .send_and_receive(&build_stream_commit(999), 2000)
        .await
        .expect("commit unknown stream session");
    let error = parse_stream_error_message(&response);

    // Assert
    assert!(error.contains("session not found"));
}

async fn should_return_session_not_found_given_rollback_to_unknown_session<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act
    let response = client
        .send_and_receive(&build_stream_rollback(999), 2000)
        .await
        .expect("rollback unknown stream session");
    let error = parse_stream_error_message(&response);

    // Assert
    assert!(error.contains("session not found"));
}

async fn should_return_error_given_empty_batch_commit<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/errors/empty-batch";
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin empty-batch stream session");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    // Act
    let response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit empty stream session");
    let error = parse_stream_error_message(&response);

    // Assert
    assert!(error.contains("empty batch"));
}

async fn should_return_empty_success_given_zero_limit_read<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/limits/zero-read";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let response = client
        .send_and_receive(&build_stream_read_with_options(route, 0, 0, None), 2000)
        .await
        .expect("read zero-limit stream history");
    let read = parse_stream_read_response(&response);

    // Assert
    assert!(read.records.is_empty());
    assert_eq!(read.cursor.last_resource_offset, 0);
    assert_eq!(read.cursor.last_area_offset, None);
    assert_eq!(read.cursor.last_realm_offset, None);
    assert!(!read.cursor.has_more);
}

async fn should_keep_connection_open_given_malformed_stream_filter_read<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/filters/malformed";
    commit_stream_record(&mut client, route, b"payload").await;
    let malformed_filter = [0, 0xF2, 0, 0, 0, 0];

    // Act
    let malformed_response = client
        .send_and_receive(
            &build_stream_read_with_raw_filter(route, 0, 10, None, &malformed_filter),
            2000,
        )
        .await
        .expect("read malformed stream filter payload");
    let malformed_error = parse_stream_error_message(&malformed_response);

    let valid_read_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read valid stream history after malformed filter");
    let valid_read = parse_stream_read_response(&valid_read_response);

    // Assert
    assert!(malformed_error.contains("ERR_STREAM_FILTER_UNSUPPORTED_VERSION"));
    assert_eq!(valid_read.records.len(), 1);
    assert_eq!(valid_read.records[0].body, b"payload".to_vec());
}

async fn should_keep_connection_open_given_invalid_stream_filter_payload<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/filters/invalid-payload";
    commit_stream_record(&mut client, route, b"payload").await;
    let invalid_filter = [0, 0xF1, 0, 0, 0, 1, 9];

    // Act
    let invalid_response = client
        .send_and_receive(
            &build_stream_read_with_raw_filter(route, 0, 10, None, &invalid_filter),
            2000,
        )
        .await
        .expect("read invalid stream filter payload");
    let invalid_error = parse_stream_error_message(&invalid_response);

    let valid_read_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read valid stream history after invalid filter payload");
    let valid_read = parse_stream_read_response(&valid_read_response);

    // Assert
    assert!(invalid_error.contains("ERR_STREAM_FILTER_INVALID_PAYLOAD"));
    assert_eq!(valid_read.records.len(), 1);
    assert_eq!(valid_read.records[0].body, b"payload".to_vec());
}

async fn should_return_none_given_empty_resource_last<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/empty/last";

    // Act
    let response = client
        .send_and_receive(&build_stream_last(route), 2000)
        .await
        .expect("read empty resource tail");

    // Assert
    assert_eq!(parse_stream_ok_data(&response), Vec::<u8>::new());
    assert!(parse_stream_last_response(&response).is_none());
}

async fn should_return_empty_metadata_given_empty_resource<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/empty/meta";

    // Act
    let response = client
        .send_and_receive(&build_stream_get_metadata(route), 2000)
        .await
        .expect("read empty resource metadata");
    let metadata = parse_stream_metadata_response(&response);

    // Assert
    assert_eq!(metadata.first_resource_offset, None);
    assert_eq!(metadata.last_resource_offset, None);
    assert_eq!(metadata.resource_count, 0);
    assert_eq!(metadata.area_watermark, 0);
    assert_eq!(metadata.realm_watermark, 0);
}

async fn should_allow_append_given_empty_body<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/payloads/empty-body";
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin empty-body stream session");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(&build_stream_append(session_id, 0, b""), 2000)
        .await
        .expect("append empty-body stream event");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected success for stream append");

    let commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit empty-body stream session");
    let (_msg_type, status, _data) = parse_stream_response(&commit_response);
    assert_eq!(status, 0, "Expected success for stream commit");

    // Act
    let response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read empty-body stream record");
    let read = parse_stream_read_response(&response);

    // Assert
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].body, Vec::<u8>::new());
}

async fn should_allow_append_given_metadata_only<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/payloads/metadata-only";
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin metadata-only stream session");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(
            &build_stream_append_with_metadata(session_id, 0, b"", Some(b"meta")),
            2000,
        )
        .await
        .expect("append metadata-only stream event");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected success for stream append");

    let commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit metadata-only stream session");
    let (_msg_type, status, _data) = parse_stream_response(&commit_response);
    assert_eq!(status, 0, "Expected success for stream commit");

    // Act
    let response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read metadata-only stream record");
    let read = parse_stream_read_response(&response);

    // Assert
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].body, Vec::<u8>::new());
    assert_eq!(read.records[0].metadata, Some(b"meta".to_vec()));
}

// Generic test helper for multiple sequential read operations
async fn should_handle_sequential_read_operations<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/sequential/main";

    commit_stream_record_with_offset(&mut client, route, 0, b"event-1").await;
    commit_stream_record_with_offset(&mut client, route, 1, b"event-2").await;
    commit_stream_record_with_offset(&mut client, route, 2, b"event-3").await;

    // Act
    let read1_frame = build_stream_read_with_options(route, 0, 1, None);
    let response1 = client
        .send_and_receive(&read1_frame, 2000)
        .await
        .expect("read 1");

    let read2_frame = build_stream_read_with_options(route, 1, 1, None);
    let response2 = client
        .send_and_receive(&read2_frame, 2000)
        .await
        .expect("read 2");

    let read3_frame = build_stream_read_with_options(route, 2, 1, None);
    let response3 = client
        .send_and_receive(&read3_frame, 2000)
        .await
        .expect("read 3");

    // Assert
    let read1 = parse_stream_read_response(&response1);
    let read2 = parse_stream_read_response(&response2);
    let read3 = parse_stream_read_response(&response3);
    assert_eq!(read1.records.len(), 1);
    assert_eq!(read1.records[0].body, b"event-1".to_vec());
    assert_eq!(read1.cursor.last_resource_offset, 0);
    assert!(read1.cursor.has_more);
    assert_eq!(read2.records.len(), 1);
    assert_eq!(read2.records[0].body, b"event-2".to_vec());
    assert_eq!(read2.cursor.last_resource_offset, 1);
    assert!(read2.cursor.has_more);
    assert_eq!(read3.records.len(), 1);
    assert_eq!(read3.records[0].body, b"event-3".to_vec());
    assert_eq!(read3.cursor.last_resource_offset, 2);
    assert!(!read3.cursor.has_more);
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
    commit_stream_record_with_offset(&mut client, orders_route, 1, b"order-shipped").await;

    let first_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/events/*", 0, 2, None),
            2000,
        )
        .await
        .expect("read area history");
    let second_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/events/*", 2, 2, None),
            2000,
        )
        .await
        .expect("resume area history");

    // Assert
    let first = parse_stream_read_response(&first_response);
    let second = parse_stream_read_response(&second_response);
    let bodies: Vec<Vec<u8>> = first
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    let area_offsets: Vec<Option<u64>> = first
        .records
        .iter()
        .map(|record| record.area_offset)
        .collect();
    assert_eq!(
        bodies,
        vec![b"order-created".to_vec(), b"audit-recorded".to_vec()]
    );
    assert_eq!(area_offsets, vec![Some(0), Some(1)]);
    assert_eq!(first.cursor.last_area_offset, Some(1));
    assert!(first.cursor.has_more);

    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].body, b"order-shipped".to_vec());
    assert_eq!(second.records[0].area_offset, Some(2));
    assert_eq!(second.cursor.last_area_offset, Some(2));
    assert!(!second.cursor.has_more);
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
    commit_stream_record_with_offset(&mut client, events_route, 1, b"realm-three").await;

    let first_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/*/*", 0, 2, None),
            2000,
        )
        .await
        .expect("read realm history");
    let second_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/*/*", 2, 2, None),
            2000,
        )
        .await
        .expect("resume realm history");

    // Assert
    let first = parse_stream_read_response(&first_response);
    let second = parse_stream_read_response(&second_response);
    let bodies: Vec<Vec<u8>> = first
        .records
        .iter()
        .map(|record| record.body.clone())
        .collect();
    let realm_offsets: Vec<Option<u64>> = first
        .records
        .iter()
        .map(|record| record.realm_offset)
        .collect();
    assert_eq!(bodies, vec![b"realm-one".to_vec(), b"realm-two".to_vec()]);
    assert_eq!(realm_offsets, vec![Some(0), Some(1)]);
    assert_eq!(first.cursor.last_realm_offset, Some(1));
    assert!(first.cursor.has_more);

    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].body, b"realm-three".to_vec());
    assert_eq!(second.records[0].realm_offset, Some(2));
    assert_eq!(second.cursor.last_realm_offset, Some(2));
    assert!(!second.cursor.has_more);
}

async fn should_stop_area_wildcard_read_given_max_bytes<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let orders_route = "stream://test/events/orders";
    let audits_route = "stream://test/events/audits";

    commit_stream_record(&mut client, orders_route, b"abcd").await;
    commit_stream_record_with_offset(&mut client, audits_route, 0, b"efgh").await;

    // Act
    let first_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/events/*", 0, 10, Some(4)),
            2000,
        )
        .await
        .expect("read first area wildcard page");
    let second_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/events/*", 1, 10, Some(4)),
            2000,
        )
        .await
        .expect("read second area wildcard page");

    // Assert
    let first = parse_stream_read_response(&first_response);
    let second = parse_stream_read_response(&second_response);
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.records[0].body, b"abcd".to_vec());
    assert_eq!(first.records[0].area_offset, Some(0));
    assert_eq!(first.cursor.last_area_offset, Some(0));
    assert!(first.cursor.has_more);

    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].body, b"efgh".to_vec());
    assert_eq!(second.records[0].area_offset, Some(1));
    assert_eq!(second.cursor.last_area_offset, Some(1));
    assert!(!second.cursor.has_more);
}

async fn should_stop_realm_wildcard_read_given_max_bytes<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let events_route = "stream://test/events/orders";
    let audit_route = "stream://test/audit/ledger";

    commit_stream_record(&mut client, events_route, b"abcd").await;
    commit_stream_record_with_offset(&mut client, audit_route, 0, b"efgh").await;

    // Act
    let first_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/*/*", 0, 10, Some(4)),
            2000,
        )
        .await
        .expect("read first realm wildcard page");
    let second_response = client
        .send_and_receive(
            &build_stream_read_with_options("stream://test/*/*", 1, 10, Some(4)),
            2000,
        )
        .await
        .expect("read second realm wildcard page");

    // Assert
    let first = parse_stream_read_response(&first_response);
    let second = parse_stream_read_response(&second_response);
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.records[0].body, b"abcd".to_vec());
    assert_eq!(first.records[0].realm_offset, Some(0));
    assert_eq!(first.cursor.last_realm_offset, Some(0));
    assert!(first.cursor.has_more);

    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].body, b"efgh".to_vec());
    assert_eq!(second.records[0].realm_offset, Some(1));
    assert_eq!(second.cursor.last_realm_offset, Some(1));
    assert!(!second.cursor.has_more);
}

async fn should_expose_exact_resource_record_metadata_on_read<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/rich";
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin stream session");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(
            &build_stream_append_with_metadata(session_id, 0, b"payload", Some(b"meta")),
            2000,
        )
        .await
        .expect("append rich stream event");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected success for stream append");

    let commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit rich stream session");
    let (_msg_type, status, _data) = parse_stream_response(&commit_response);
    assert_eq!(status, 0, "Expected success for stream commit");

    // Act
    let read_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read rich stream event");
    let read = parse_stream_read_response(&read_response);

    // Assert
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].resource_offset, 0);
    assert_eq!(read.records[0].area_offset, Some(0));
    assert_eq!(read.records[0].realm_offset, Some(0));
    assert_eq!(read.records[0].body, b"payload".to_vec());
    assert_eq!(read.records[0].metadata, Some(b"meta".to_vec()));
    assert!(read.records[0].created_at > 0);
    assert_eq!(read.cursor.last_resource_offset, 0);
    assert_eq!(read.cursor.last_area_offset, Some(0));
    assert_eq!(read.cursor.last_realm_offset, Some(0));
    assert!(!read.cursor.has_more);
}

async fn should_expose_exact_resource_record_metadata_on_last<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/last-rich";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let last_response = client
        .send_and_receive(&build_stream_last(route), 2000)
        .await
        .expect("read stream tail");
    let record = parse_stream_last_response(&last_response).expect("expected tail record");

    // Assert
    assert_eq!(record.resource_offset, 0);
    assert_eq!(record.area_offset, Some(0));
    assert_eq!(record.realm_offset, Some(0));
    assert_eq!(record.body, b"payload".to_vec());
    assert_eq!(record.metadata, None);
    assert!(record.created_at > 0);
}

async fn should_expose_exact_resource_metadata_on_get_metadata<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/meta";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let metadata_response = client
        .send_and_receive(&build_stream_get_metadata(route), 2000)
        .await
        .expect("read stream metadata");
    let metadata = parse_stream_metadata_response(&metadata_response);

    // Assert
    assert_eq!(metadata.first_resource_offset, Some(0));
    assert_eq!(metadata.last_resource_offset, Some(0));
    assert_eq!(metadata.resource_count, 1);
    assert_eq!(metadata.max_batch_events, 10_000);
    assert_eq!(metadata.max_batch_bytes, 10 * 1024 * 1024);
    assert_eq!(metadata.ttl_seconds, None);
    assert_eq!(metadata.area_watermark, 0);
    assert_eq!(metadata.realm_watermark, 0);
}

async fn should_stop_exact_resource_read_given_max_bytes<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/max-bytes";
    commit_stream_record_with_offset(&mut client, route, 0, b"abcd").await;
    commit_stream_record_with_offset(&mut client, route, 1, b"efgh").await;

    // Act
    let first_response = client
        .send_and_receive(&build_stream_read_with_options(route, 0, 10, Some(4)), 2000)
        .await
        .expect("read first byte-limited page");
    let second_response = client
        .send_and_receive(&build_stream_read_with_options(route, 1, 10, Some(4)), 2000)
        .await
        .expect("read resumed byte-limited page");

    // Assert
    let first = parse_stream_read_response(&first_response);
    let second = parse_stream_read_response(&second_response);
    assert_eq!(first.records.len(), 1);
    assert_eq!(first.records[0].body, b"abcd".to_vec());
    assert_eq!(first.cursor.last_resource_offset, 0);
    assert!(first.cursor.has_more);
    assert_eq!(second.records.len(), 1);
    assert_eq!(second.records[0].body, b"efgh".to_vec());
    assert_eq!(second.cursor.last_resource_offset, 1);
    assert!(!second.cursor.has_more);
}

async fn should_return_empty_success_given_area_wildcard_last<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/orders";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let response = client
        .send_and_receive(&build_stream_last("stream://test/events/*"), 2000)
        .await
        .expect("read area wildcard last");

    // Assert
    assert_eq!(parse_stream_ok_data(&response), Vec::<u8>::new());
    assert!(parse_stream_last_response(&response).is_none());
}

async fn should_return_empty_success_given_realm_wildcard_last<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/orders";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let response = client
        .send_and_receive(&build_stream_last("stream://test/*/*"), 2000)
        .await
        .expect("read realm wildcard last");

    // Assert
    assert_eq!(parse_stream_ok_data(&response), Vec::<u8>::new());
    assert!(parse_stream_last_response(&response).is_none());
}

async fn should_return_empty_success_given_area_wildcard_get_metadata<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/orders";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let response = client
        .send_and_receive(&build_stream_get_metadata("stream://test/events/*"), 2000)
        .await
        .expect("read area wildcard metadata");

    // Assert
    assert_eq!(parse_stream_ok_data(&response), Vec::<u8>::new());
}

async fn should_return_empty_success_given_realm_wildcard_get_metadata<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/orders";
    commit_stream_record(&mut client, route, b"payload").await;

    // Act
    let response = client
        .send_and_receive(&build_stream_get_metadata("stream://test/*/*"), 2000)
        .await
        .expect("read realm wildcard metadata");

    // Assert
    assert_eq!(parse_stream_ok_data(&response), Vec::<u8>::new());
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
            .send_and_receive(&build_stream_begin(route), 2000)
            .await
            .expect("begin staging stream session");
        let (_msg_type, status, data) = parse_stream_response(&begin_response);
        assert_eq!(status, 0, "expected success for stream begin");
        let session_id = parse_stream_session_id(&data).expect("stream session id");

        let append_response = client
            .send_and_receive(&build_stream_append(session_id, 0, b"staged"), 2000)
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
async fn should_preserve_monotonic_stream_resource_offsets_after_restart() {
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
async fn should_drop_uncommitted_stream_batch_on_restart() {
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
async fn should_start_local_stream_boot_given_promotion_frontier_layout() {
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
async fn should_fail_local_stream_restart_given_mismatched_layout_marker() {
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
    should_reject_future_expected_offset_given_gap_tcp / should_reject_future_expected_offset_given_gap_ws => should_reject_future_expected_offset_given_gap,
    should_return_session_not_found_given_append_to_unknown_session_tcp / should_return_session_not_found_given_append_to_unknown_session_ws => should_return_session_not_found_given_append_to_unknown_session,
    should_return_session_not_found_given_commit_to_unknown_session_tcp / should_return_session_not_found_given_commit_to_unknown_session_ws => should_return_session_not_found_given_commit_to_unknown_session,
    should_return_session_not_found_given_rollback_to_unknown_session_tcp / should_return_session_not_found_given_rollback_to_unknown_session_ws => should_return_session_not_found_given_rollback_to_unknown_session,
    should_return_error_given_empty_batch_commit_tcp / should_return_error_given_empty_batch_commit_ws => should_return_error_given_empty_batch_commit,
    should_return_empty_success_given_zero_limit_read_tcp / should_return_empty_success_given_zero_limit_read_ws => should_return_empty_success_given_zero_limit_read,
    should_keep_connection_open_given_malformed_stream_filter_read_tcp / should_keep_connection_open_given_malformed_stream_filter_read_ws => should_keep_connection_open_given_malformed_stream_filter_read,
    should_keep_connection_open_given_invalid_stream_filter_payload_tcp / should_keep_connection_open_given_invalid_stream_filter_payload_ws => should_keep_connection_open_given_invalid_stream_filter_payload,
    should_return_none_given_empty_resource_last_tcp / should_return_none_given_empty_resource_last_ws => should_return_none_given_empty_resource_last,
    should_return_empty_metadata_given_empty_resource_tcp / should_return_empty_metadata_given_empty_resource_ws => should_return_empty_metadata_given_empty_resource,
    should_allow_append_given_empty_body_tcp / should_allow_append_given_empty_body_ws => should_allow_append_given_empty_body,
    should_allow_append_given_metadata_only_tcp / should_allow_append_given_metadata_only_ws => should_allow_append_given_metadata_only,
    should_handle_sequential_read_operations_tcp / should_handle_sequential_read_operations_ws => should_handle_sequential_read_operations,
    should_isolate_streams_by_route_tcp / should_isolate_streams_by_route_ws => should_isolate_streams_by_route,
    should_read_committed_area_history_given_wildcard_route_tcp / should_read_committed_area_history_given_wildcard_route_ws => should_read_committed_area_history_given_wildcard_route,
    should_read_committed_realm_history_given_wildcard_route_tcp / should_read_committed_realm_history_given_wildcard_route_ws => should_read_committed_realm_history_given_wildcard_route,
    should_stop_area_wildcard_read_given_max_bytes_tcp / should_stop_area_wildcard_read_given_max_bytes_ws => should_stop_area_wildcard_read_given_max_bytes,
    should_stop_realm_wildcard_read_given_max_bytes_tcp / should_stop_realm_wildcard_read_given_max_bytes_ws => should_stop_realm_wildcard_read_given_max_bytes,
    should_expose_exact_resource_record_metadata_on_read_tcp / should_expose_exact_resource_record_metadata_on_read_ws => should_expose_exact_resource_record_metadata_on_read,
    should_expose_exact_resource_record_metadata_on_last_tcp / should_expose_exact_resource_record_metadata_on_last_ws => should_expose_exact_resource_record_metadata_on_last,
    should_expose_exact_resource_metadata_on_get_metadata_tcp / should_expose_exact_resource_metadata_on_get_metadata_ws => should_expose_exact_resource_metadata_on_get_metadata,
    should_stop_exact_resource_read_given_max_bytes_tcp / should_stop_exact_resource_read_given_max_bytes_ws => should_stop_exact_resource_read_given_max_bytes,
    should_return_empty_success_given_area_wildcard_last_tcp / should_return_empty_success_given_area_wildcard_last_ws => should_return_empty_success_given_area_wildcard_last,
    should_return_empty_success_given_realm_wildcard_last_tcp / should_return_empty_success_given_realm_wildcard_last_ws => should_return_empty_success_given_realm_wildcard_last,
    should_return_empty_success_given_area_wildcard_get_metadata_tcp / should_return_empty_success_given_area_wildcard_get_metadata_ws => should_return_empty_success_given_area_wildcard_get_metadata,
    should_return_empty_success_given_realm_wildcard_get_metadata_tcp / should_return_empty_success_given_realm_wildcard_get_metadata_ws => should_return_empty_success_given_realm_wildcard_get_metadata,
    should_abort_uncommitted_stream_session_on_disconnect_tcp / should_abort_uncommitted_stream_session_on_disconnect_ws => should_abort_uncommitted_stream_session_on_disconnect,
    should_remove_stream_subscription_when_subscriber_disconnects_tcp / should_remove_stream_subscription_when_subscriber_disconnects_ws => should_remove_stream_subscription_when_subscriber_disconnects,
    should_not_treat_stream_subscription_as_replay_cursor_given_shared_route_tcp / should_not_treat_stream_subscription_as_replay_cursor_given_shared_route_ws => should_not_treat_stream_subscription_as_replay_cursor_given_shared_route,
    should_retain_other_stream_subscription_after_unsubscribe_tcp / should_retain_other_stream_subscription_after_unsubscribe_ws => should_retain_other_stream_subscription_after_unsubscribe,
);
