//! Stream domain end-to-end tests
//! Tests both TCP and WebSocket transports

pub(crate) use crate::fixtures::define_transport_tests;
pub(crate) use crate::fixtures::transport::*;
pub(crate) use bytes::{BufMut, Bytes};
pub(crate) use fitz::domains::stream::protocol::StreamWriteMode;
pub(crate) use fitz::domains::stream::storage::{
    encode_stream_layout_marker_key, StreamLayoutMarkerValue,
};
pub(crate) use fitz::domains::stream::store::StreamStore;
pub(crate) use fitz::domains::stream::{
    StreamActor, StreamReadItem, StreamRecord, StreamStorageLayout,
};
pub(crate) use fitz::protocol::payload_codec::PayloadDecoder;
pub(crate) use fitz::runtime::routing::RouteFamily;
pub(crate) use fitz::testkit::TestServer;
pub(crate) use std::sync::Arc;
pub(crate) use tempfile::TempDir;
pub(crate) use tokio::time::Duration;

pub(crate) async fn wait_for_stream_subscription_count(server: &TestServer, expected: usize) {
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

pub(crate) fn decode_stream_ok_data(payload: &[u8]) -> Vec<u8> {
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

pub(crate) fn parse_stream_ok_data(frame: &[u8]) -> Vec<u8> {
    let (_msg_type, status, payload) = parse_stream_response(frame);
    assert_eq!(status, 0, "expected successful stream response");
    decode_stream_ok_data(&payload)
}

pub(crate) fn parse_stream_error_message(frame: &[u8]) -> String {
    let (_msg_type, status, payload) = parse_stream_response(frame);
    assert_eq!(status, 1, "expected failing stream response");

    let (_code, message) =
        fitz::protocol::error_codes::decode_error_body(&payload).expect("stream error envelope");
    message
}

pub(crate) fn event_records(items: &[StreamReadItem]) -> Vec<StreamRecord> {
    items
        .iter()
        .filter_map(|item| match item {
            StreamReadItem::Event(record) => Some(record.clone()),
            _ => None,
        })
        .collect()
}

pub(crate) fn append_for_owner(
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

pub(crate) fn commit_for_owner(
    actor: &mut StreamActor,
    owner_session_id: u64,
    stream_session_id: u64,
) {
    actor
        .commit_session_for_owner(owner_session_id, stream_session_id, StreamWriteMode::Sync)
        .unwrap();
}

pub(crate) fn build_stream_last(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(usize_to_u32_saturating(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(605, &buf);
    builder.build()
}

pub(crate) fn build_stream_get_metadata(route: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(usize_to_u32_saturating(route.len()));
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(606, &buf);
    builder.build()
}

pub(crate) fn build_stream_rollback(session_id: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u64(session_id);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(603, &buf);
    builder.build()
}

pub(crate) fn build_stream_read_with_options(
    route: &str,
    start_offset: u64,
    limit: u64,
    max_bytes: Option<u64>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(usize_to_u32_saturating(route.len()));
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

pub(crate) fn build_stream_read_with_raw_filter(
    route: &str,
    start_offset: u64,
    limit: u64,
    max_bytes: Option<u64>,
    filter: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.put_u32(usize_to_u32_saturating(route.len()));
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
    buf.put_u32(usize_to_u32_saturating(filter.len()));
    buf.put_slice(filter);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WireStreamRecord {
    pub(crate) resource_offset: u64,
    pub(crate) area_offset: Option<u64>,
    pub(crate) realm_offset: Option<u64>,
    pub(crate) body: Vec<u8>,
    pub(crate) metadata: Option<Vec<u8>>,
    pub(crate) created_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WireReadCursor {
    pub(crate) last_resource_offset: u64,
    pub(crate) last_area_offset: Option<u64>,
    pub(crate) last_realm_offset: Option<u64>,
    pub(crate) has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WireReadResponse {
    pub(crate) routes: Vec<String>,
    pub(crate) records: Vec<WireStreamRecord>,
    pub(crate) cursor: WireReadCursor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WireStreamMetadata {
    pub(crate) first_resource_offset: Option<u64>,
    pub(crate) last_resource_offset: Option<u64>,
    pub(crate) resource_count: u64,
    pub(crate) max_batch_events: u64,
    pub(crate) max_batch_bytes: u64,
    pub(crate) ttl_seconds: Option<u64>,
    pub(crate) area_watermark: u64,
    pub(crate) realm_watermark: u64,
}

pub(crate) fn decode_wire_stream_record(dec: &mut PayloadDecoder<'_>) -> WireStreamRecord {
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

pub(crate) fn parse_stream_read_response(frame: &[u8]) -> WireReadResponse {
    let (_msg_type, status, payload) = parse_stream_response(frame);
    assert_eq!(status, 0, "expected successful stream read");

    let data = decode_stream_ok_data(&payload);
    let mut dec = PayloadDecoder::new(&data);
    let count = dec.get_u32().expect("stream read record count") as usize;
    let mut routes = Vec::with_capacity(count);
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        routes.push(dec.get_string().expect("stream read item route"));
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
            other => panic!("unexpected stream read item tag: {other}"),
        }
    }

    let cursor = WireReadCursor {
        last_resource_offset: dec.get_u64().expect("stream cursor resource offset"),
        last_area_offset: dec.get_optional_u64().expect("stream cursor area offset"),
        last_realm_offset: dec.get_optional_u64().expect("stream cursor realm offset"),
        has_more: dec.get_u8().expect("stream cursor has_more") == 1,
    };
    assert!(dec.is_complete(), "expected complete stream read payload");

    WireReadResponse {
        routes,
        records,
        cursor,
    }
}

pub(crate) fn parse_stream_last_response(frame: &[u8]) -> Option<WireStreamRecord> {
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

pub(crate) fn parse_stream_metadata_response(frame: &[u8]) -> WireStreamMetadata {
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

pub(crate) fn parse_stream_read_records(frame: &[u8]) -> Vec<(u64, Vec<u8>)> {
    parse_stream_read_response(frame)
        .records
        .into_iter()
        .map(|record| (record.resource_offset, record.body))
        .collect()
}

pub(crate) async fn wait_for_stream_storage_release() {
    tokio::time::sleep(Duration::from_millis(750)).await;
}

pub(crate) async fn open_local_stream_engine(
    db_path: String,
) -> Result<Arc<cntryl_midge::Engine>, Box<dyn std::error::Error>> {
    let boot_config = fitz::boot::runtime::BootConfig::with_local_storage(db_path);
    fitz::boot::storage::init(&boot_config).await
}

pub(crate) fn make_stream_actor(
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

pub(crate) async fn begin_stream_session<C>(client: &mut C, route: &str) -> u64
where
    C: StreamConnector,
{
    let begin_response = client
        .send_and_receive(&build_stream_begin(route), 2000)
        .await
        .expect("begin stream");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    parse_stream_session_id(&data).expect("stream session id")
}

pub(crate) async fn append_stream_record_with_metadata<C>(
    client: &mut C,
    session_id: u64,
    expected_offset: u64,
    body: &[u8],
    metadata: Option<&[u8]>,
) where
    C: StreamConnector,
{
    let append_response = client
        .send_and_receive(
            &build_stream_append_with_metadata(session_id, expected_offset, body, metadata),
            2000,
        )
        .await
        .expect("append stream");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected success for stream append");
}

pub(crate) async fn commit_stream_session<C>(client: &mut C, session_id: u64)
where
    C: StreamConnector,
{
    let commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit stream");
    let (_msg_type, status, _data) = parse_stream_response(&commit_response);
    assert_eq!(status, 0, "Expected success for stream commit");
}

pub(crate) async fn rollback_stream_session<C>(client: &mut C, session_id: u64)
where
    C: StreamConnector,
{
    let rollback_response = client
        .send_and_receive(&build_stream_rollback(session_id), 2000)
        .await
        .expect("rollback stream");
    let (_msg_type, status, _data) = parse_stream_response(&rollback_response);
    assert_eq!(status, 0, "Expected success for stream rollback");
}

pub(crate) async fn commit_stream_record_with_offset<C>(
    client: &mut C,
    route: &str,
    expected_offset: u64,
    body: &[u8],
) where
    C: StreamConnector,
{
    let session_id = begin_stream_session(client, route).await;
    append_stream_record_with_metadata(client, session_id, expected_offset, body, None).await;
    commit_stream_session(client, session_id).await;
}

pub(crate) async fn commit_stream_record<C>(client: &mut C, route: &str, body: &[u8])
where
    C: StreamConnector,
{
    commit_stream_record_with_offset(client, route, 0, body).await;
}

#[inline]
pub(crate) fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[inline]
pub(crate) fn i32_to_u64_nonnegative(value: i32) -> u64 {
    u64::try_from(value).unwrap_or_default()
}
