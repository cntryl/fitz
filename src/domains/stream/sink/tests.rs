use super::*;
use crate::benchkit::{
    build_stream_append, build_stream_append_with_metadata, build_stream_begin,
    build_stream_commit, build_stream_read, build_stream_read_with_limit, build_stream_subscribe,
    count_stream_read_records_from_payload, extract_single_tlv_field, register_session_queue_sink,
    route_frame, FrameQueueSink,
};
use crate::dispatch::protocol::frame::ChannelId;
use bytes::Bytes;

const TEST_CLIENT_SESSION_ID: u64 = 1;

struct TestContext {
    router: Arc<Router>,
    family: RouteFamily,
    source: RouteAddress,
    inbox: Arc<FrameQueueSink>,
    sink: Arc<StreamDomainSink>,
    admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
}

fn setup_test_context() -> TestContext {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(StreamDomainSink::new(
        crate::benchkit::create_bench_store(),
        router.clone(),
        admin_read_model.clone(),
        StreamStorageWriteOptions::local(),
    ));
    router.register_domain_pattern("stream", sink.clone() as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_queue_sink(&router, family, TEST_CLIENT_SESSION_ID);

    TestContext {
        router,
        family,
        source,
        inbox,
        sink,
        admin_read_model,
    }
}

fn request(context: &TestContext, destination: &str, msg_type: u16, payload: Bytes) -> Bytes {
    route_frame(
        context.router.as_ref(),
        &context.source,
        destination,
        TEST_CLIENT_SESSION_ID,
        ChannelId::Pub,
        msg_type,
        payload,
        context.family,
    )
    .expect("stream route");

    let responses = context.inbox.drain();
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("stream response")
}

fn request_from_session(
    context: &TestContext,
    source: &RouteAddress,
    inbox: &FrameQueueSink,
    session_id: u64,
    destination: &str,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_frame(
        context.router.as_ref(),
        source,
        destination,
        session_id,
        ChannelId::Pub,
        msg_type,
        payload,
        context.family,
    )
    .expect("stream route");

    let responses = inbox.drain();
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("stream response")
}

#[derive(Debug)]
struct DecodedStreamWireRecord {
    resource_offset: u64,
    area_offset: Option<u64>,
    realm_offset: Option<u64>,
    global_offset: Option<u64>,
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

#[derive(Debug)]
struct DecodedStreamReadPayload {
    routes: Vec<String>,
    records: Vec<DecodedStreamWireRecord>,
    last_resource_offset: u64,
    last_area_offset: Option<u64>,
    last_realm_offset: Option<u64>,
    last_global_offset: Option<u64>,
    has_more: bool,
    cursor_fingerprint: Option<u64>,
    captured_watermark: Option<u64>,
}

#[derive(Debug)]
struct DecodedStreamMetadataPayload {
    first_resource_offset: Option<u64>,
    last_resource_offset: Option<u64>,
    resource_count: u64,
    max_batch_events: u64,
    max_batch_bytes: u64,
    ttl_seconds: Option<u64>,
    area_watermark: u64,
    realm_watermark: u64,
}

fn decode_stream_wire_record(
    decoder: &mut crate::dispatch::protocol::payload_codec::PayloadDecoder<'_>,
    extended: bool,
) -> DecodedStreamWireRecord {
    let resource_offset = decoder.get_u64().expect("stream resource offset");
    let area_offset = decoder.get_optional_u64().expect("stream area offset");
    let realm_offset = decoder.get_optional_u64().expect("stream realm offset");
    let global_offset = extended
        .then(|| decoder.get_optional_u64().expect("stream global offset"))
        .flatten();
    let body = decoder.get_bytes().expect("stream body");
    let metadata = decoder.get_optional_bytes().expect("stream metadata");
    let created_at = decoder.get_u64().expect("stream created_at");

    DecodedStreamWireRecord {
        resource_offset,
        area_offset,
        realm_offset,
        global_offset,
        body,
        metadata,
        created_at,
    }
}

fn decode_stream_read_payload_with_format(data: &[u8], extended: bool) -> DecodedStreamReadPayload {
    let mut decoder = crate::dispatch::protocol::payload_codec::PayloadDecoder::new(data);
    let count = decoder.get_u32().expect("stream read record count") as usize;
    let mut routes = Vec::with_capacity(count);
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        routes.push(decoder.get_string().expect("concrete stream route"));
        match decoder.get_u8().expect("stream read item kind") {
            0 => records.push(decode_stream_wire_record(&mut decoder, extended)),
            1 => {
                decoder.get_u64().expect("stream filtered offset");
                decoder.get_u8().expect("stream filtered reason");
            }
            2 => {
                decoder.get_u64().expect("stream filtered range start");
                decoder.get_u64().expect("stream filtered range end");
                decoder.get_u8().expect("stream filtered reason");
            }
            kind => panic!("unexpected stream read item kind: {kind}"),
        }
    }

    let last_resource_offset = decoder.get_u64().expect("stream cursor resource offset");
    let last_area_offset = decoder
        .get_optional_u64()
        .expect("stream cursor area offset");
    let last_realm_offset = decoder
        .get_optional_u64()
        .expect("stream cursor realm offset");
    let last_global_offset = extended
        .then(|| {
            decoder
                .get_optional_u64()
                .expect("stream cursor global offset")
        })
        .flatten();
    let has_more = decoder.get_u8().expect("stream cursor has_more") == 1;
    let cursor_fingerprint = extended
        .then(|| {
            decoder
                .get_optional_u64()
                .expect("stream cursor fingerprint")
        })
        .flatten();
    let captured_watermark = extended
        .then(|| {
            decoder
                .get_optional_u64()
                .expect("stream cursor captured watermark")
        })
        .flatten();
    assert!(
        decoder.is_complete(),
        "expected complete stream read payload"
    );

    DecodedStreamReadPayload {
        routes,
        records,
        last_resource_offset,
        last_area_offset,
        last_realm_offset,
        last_global_offset,
        has_more,
        cursor_fingerprint,
        captured_watermark,
    }
}

fn decode_stream_read_payload(data: &[u8]) -> DecodedStreamReadPayload {
    decode_stream_read_payload_with_format(data, false)
}

fn decode_global_stream_read_payload(data: &[u8]) -> DecodedStreamReadPayload {
    decode_stream_read_payload_with_format(data, true)
}

fn decode_routed_stream_read_routes(data: &[u8]) -> Vec<String> {
    let mut decoder = crate::dispatch::protocol::payload_codec::PayloadDecoder::new(data);
    let count = decoder.get_u32().expect("stream read record count") as usize;
    let mut routes = Vec::with_capacity(count);
    for _ in 0..count {
        routes.push(decoder.get_string().expect("concrete stream route"));
        match decoder.get_u8().expect("stream read item kind") {
            0 => {
                let _record = decode_stream_wire_record(&mut decoder, false);
            }
            1 => {
                decoder.get_u64().expect("stream filtered offset");
                decoder.get_u8().expect("stream filtered reason");
            }
            2 => {
                decoder.get_u64().expect("stream filtered range start");
                decoder.get_u64().expect("stream filtered range end");
                decoder.get_u8().expect("stream filtered reason");
            }
            kind => panic!("unexpected stream read item kind: {kind}"),
        }
    }
    decoder.get_u64().expect("stream cursor resource offset");
    decoder
        .get_optional_u64()
        .expect("stream cursor area offset");
    decoder
        .get_optional_u64()
        .expect("stream cursor realm offset");
    decoder.get_u8().expect("stream cursor has_more");
    assert!(
        decoder.is_complete(),
        "expected complete routed stream read"
    );
    routes
}

fn decode_stream_success_data(payload: &[u8]) -> Bytes {
    let mut decoder = crate::dispatch::protocol::payload_codec::PayloadDecoder::new(payload);
    let status = decoder.get_u8().expect("stream status");
    assert_eq!(status, 0, "expected stream success response");
    decoder
        .get_optional_u64()
        .expect("stream response session id");
    let data = decoder.get_bytes().expect("stream response data");
    assert!(
        decoder.is_complete(),
        "expected complete stream success response"
    );
    data
}

fn decode_stream_metadata_payload(data: &[u8]) -> DecodedStreamMetadataPayload {
    let mut decoder = crate::dispatch::protocol::payload_codec::PayloadDecoder::new(data);
    let first_resource_offset = decoder
        .get_optional_u64()
        .expect("first stream metadata offset");
    let last_resource_offset = decoder
        .get_optional_u64()
        .expect("last stream metadata offset");
    let resource_count = decoder.get_u64().expect("stream metadata count");
    let max_batch_events = decoder.get_u64().expect("stream max_batch_events");
    let max_batch_bytes = decoder.get_u64().expect("stream max_batch_bytes");
    let ttl_seconds = decoder.get_optional_u64().expect("stream ttl seconds");
    let area_watermark = decoder.get_u64().expect("stream area watermark");
    let realm_watermark = decoder.get_u64().expect("stream realm watermark");
    assert!(
        decoder.is_complete(),
        "expected complete stream metadata payload"
    );

    DecodedStreamMetadataPayload {
        first_resource_offset,
        last_resource_offset,
        resource_count,
        max_batch_events,
        max_batch_bytes,
        ttl_seconds,
        area_watermark,
        realm_watermark,
    }
}

fn begin_stream(context: &TestContext, route: &str) -> u64 {
    let begin_frame = build_stream_begin(route);
    let (msg_type, payload) = extract_single_tlv_field(&begin_frame);
    let response = request(context, route, msg_type, payload);
    crate::benchkit::parse_stream_session_id(response.as_ref()).expect("stream session id")
}

fn decode_stream_error_message(payload: &[u8]) -> Result<String, String> {
    if let Ok((_, message)) = crate::dispatch::protocol::error_codes::decode_error_body(payload) {
        return Ok(message);
    }
    let mut decoder = crate::dispatch::protocol::payload_codec::PayloadDecoder::new(payload);
    if decoder.get_u8()? != 2 {
        return Err("stream response is not an error".to_string());
    }
    decoder.get_u32()?;
    decoder.get_string()
}

fn seed_committed_stream_route(
    context: &TestContext,
    route: &str,
    event_count: usize,
    body: &'static [u8],
) {
    let session_id = begin_stream(context, route);

    for expected_offset in 0..event_count as u64 {
        let append_frame = build_stream_append(session_id, expected_offset, body);
        let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
        let _ = request(context, route, append_msg_type, append_payload);
    }

    let commit_frame = build_stream_commit(session_id, 1);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
    let _ = request(context, route, commit_msg_type, commit_payload);
}

/// Poll `get_realm_watermark_for_tests` until it reaches `expected` or the
/// timeout elapses, returning the last observed value. Realm watermark
/// convergence is asynchronous (a separately spawned `RealmActor` processes
/// `BatchCommitted` off the request path), so a single immediate read after
/// commit can race the actor pipeline and observe a stale value.
fn wait_for_realm_watermark(
    context: &TestContext,
    realm: &str,
    expected: u64,
    timeout: std::time::Duration,
) -> u64 {
    let deadline = std::time::Instant::now() + timeout;
    let mut last = 0;
    while std::time::Instant::now() < deadline {
        last = context
            .sink
            .get_realm_watermark_for_tests(context.family, realm)
            .expect("realm watermark");
        if last == expected {
            return last;
        }
        std::thread::yield_now();
    }
    last
}

/// Poll `get_watermark_for_tests` until it reaches `expected` or the timeout
/// elapses, returning the last observed value. Area watermark convergence is
/// asynchronous (a separately spawned `AreaActor` processes `BatchCommitted`
/// off the request path), so a single immediate read after commit can race
/// the actor pipeline and observe a stale value.
fn wait_for_area_watermark(
    context: &TestContext,
    realm: &str,
    area: &str,
    expected: u64,
    timeout: std::time::Duration,
) -> u64 {
    let deadline = std::time::Instant::now() + timeout;
    let mut last = 0;
    while std::time::Instant::now() < deadline {
        last = context
            .sink
            .get_watermark_for_tests(context.family, realm, area)
            .expect("area watermark");
        if last == expected {
            return last;
        }
        std::thread::yield_now();
    }
    last
}

fn stream_read_response(
    context: &TestContext,
    route: &str,
    from_offset: u64,
    limit: u64,
) -> DecodedStreamReadPayload {
    let frame = build_stream_read_with_limit(route, from_offset, limit);
    let (msg_type, payload) = extract_single_tlv_field(&frame);
    let response = request(context, route, msg_type, payload);
    let data = decode_stream_success_data(response.as_ref());
    decode_stream_read_payload(&data)
}

mod correctness;
mod global_reads;
mod realm_reads;
mod sink_dispatch;
mod watermark_wiring;
