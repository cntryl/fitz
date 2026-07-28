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
    body: Bytes,
    metadata: Option<Bytes>,
    created_at: u64,
}

#[derive(Debug)]
struct DecodedStreamReadPayload {
    records: Vec<DecodedStreamWireRecord>,
    last_resource_offset: u64,
    last_area_offset: Option<u64>,
    last_realm_offset: Option<u64>,
    has_more: bool,
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
) -> DecodedStreamWireRecord {
    let resource_offset = decoder.get_u64().expect("stream resource offset");
    let area_offset = decoder.get_optional_u64().expect("stream area offset");
    let realm_offset = decoder.get_optional_u64().expect("stream realm offset");
    let body = decoder.get_bytes().expect("stream body");
    let metadata = decoder.get_optional_bytes().expect("stream metadata");
    let created_at = decoder.get_u64().expect("stream created_at");

    DecodedStreamWireRecord {
        resource_offset,
        area_offset,
        realm_offset,
        body,
        metadata,
        created_at,
    }
}

fn decode_stream_read_payload(data: &[u8]) -> DecodedStreamReadPayload {
    let mut decoder = crate::dispatch::protocol::payload_codec::PayloadDecoder::new(data);
    let count = decoder.get_u32().expect("stream read record count") as usize;
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        match decoder.get_u8().expect("stream read item kind") {
            0 => records.push(decode_stream_wire_record(&mut decoder)),
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
    let has_more = decoder.get_u8().expect("stream cursor has_more") == 1;
    assert!(
        decoder.is_complete(),
        "expected complete stream read payload"
    );

    DecodedStreamReadPayload {
        records,
        last_resource_offset,
        last_area_offset,
        last_realm_offset,
        has_more,
    }
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
    crate::dispatch::protocol::error_codes::decode_error_body(payload).map(|(_, message)| message)
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

#[test]
fn should_reject_stream_delivery_when_managed_actor_is_stopped() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = StreamDomainSink::new(
        crate::benchkit::create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageWriteOptions::local(),
    );
    let destination = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("stream://bench/events/orders"),
    );
    let envelope = Envelope::new(destination, 42_u64);

    // Act
    sink.stop_actor_for_tests();
    let result = sink.deliver(envelope);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(matches!(result, Err(DeliveryError::ActorStopped)));
}

#[test]
fn should_route_stream_live_count_queries_through_managed_actor() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/orders";
    let subscribe_frame = build_stream_subscribe(route);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);
    let _subscribe_response = request(&context, route, subscribe_msg_type, subscribe_payload);
    let _stream_session_id = begin_stream(&context, route);
    assert_eq!(context.sink.stream_count(), 1);
    assert_eq!(context.sink.append_session_count(), 1);
    assert_eq!(context.sink.subscription_count(), 1);

    // Act
    context.sink.stop_actor_for_tests();
    let stream_count = context.sink.stream_count();
    let append_session_count = context.sink.append_session_count();
    let subscription_count = context.sink.subscription_count();

    // Assert
    assert!(!context.sink.is_actor_running());
    assert_eq!(stream_count, 0);
    assert_eq!(append_session_count, 0);
    assert_eq!(subscription_count, 0);
}

#[test]
fn should_route_stream_admin_snapshot_sync_through_managed_actor() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://bench/events/orders", 1, b"persisted");

    // Act
    context.sink.stop_actor_for_tests();
    context.sink.sync_admin_snapshot();
    let streams = context.admin_read_model.streams(None);
    let events_total = context.admin_read_model.stream_events_total();

    // Assert
    assert!(!context.sink.is_actor_running());
    assert!(streams.is_empty());
    assert_eq!(events_total, 0);
}

#[test]
fn should_route_stream_admin_dirty_refresh_through_managed_actor() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://bench/events/orders", 1, b"persisted");

    // Act
    context.sink.stop_actor_for_tests();
    context.sink.refresh_admin_snapshot_if_dirty();
    let streams = context.admin_read_model.streams(None);
    let events_total = context.admin_read_model.stream_events_total();

    // Assert
    assert!(!context.sink.is_actor_running());
    assert!(streams.is_empty());
    assert_eq!(events_total, 0);
}

#[test]
fn should_reject_append_session_followups_from_non_owner_session() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/orders";
    let stream_session_id = begin_stream(&context, route);
    let (other_source, other_inbox) =
        register_session_queue_sink(&context.router, context.family, 2);

    // Act
    let other_append = build_stream_append(stream_session_id, 0, b"stolen");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&other_append);
    let other_append_response = request_from_session(
        &context,
        &other_source,
        &other_inbox,
        2,
        route,
        append_msg_type,
        append_payload,
    );

    let owner_append = build_stream_append(stream_session_id, 0, b"owner");
    let (owner_append_type, owner_append_payload) = extract_single_tlv_field(&owner_append);
    let owner_append_response = request(&context, route, owner_append_type, owner_append_payload);

    let other_commit = build_stream_commit(stream_session_id, 1);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&other_commit);
    let other_commit_response = request_from_session(
        &context,
        &other_source,
        &other_inbox,
        2,
        route,
        commit_msg_type,
        commit_payload,
    );

    let owner_commit = build_stream_commit(stream_session_id, 1);
    let (owner_commit_type, owner_commit_payload) = extract_single_tlv_field(&owner_commit);
    let owner_commit_response = request(&context, route, owner_commit_type, owner_commit_payload);

    let rollback_session_id = begin_stream(&context, route);
    let mut rollback_encoder = PayloadEncoder::new();
    rollback_encoder.put_u64(rollback_session_id);
    let rollback_payload = Bytes::from(rollback_encoder.finish());
    let other_rollback_response = request_from_session(
        &context,
        &other_source,
        &other_inbox,
        2,
        route,
        603,
        rollback_payload,
    );

    // Assert
    assert_eq!(
        decode_stream_error_message(other_append_response.as_ref())
            .expect("non-owner append error"),
        "session not found"
    );
    assert!(decode_stream_error_message(owner_append_response.as_ref()).is_err());
    assert_eq!(
        decode_stream_error_message(other_commit_response.as_ref())
            .expect("non-owner commit error"),
        "session not found"
    );
    assert!(decode_stream_error_message(owner_commit_response.as_ref()).is_err());
    assert_eq!(
        decode_stream_error_message(other_rollback_response.as_ref())
            .expect("non-owner rollback error"),
        "session not found"
    );
    assert_eq!(context.sink.append_session_count(), 1);
}

#[test]
fn should_return_all_area_records_given_two_resource_batches_on_direct_sink_path() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(
        &context,
        "stream://bench/area/orders",
        50,
        b"area read event",
    );
    seed_committed_stream_route(
        &context,
        "stream://bench/area/audits",
        50,
        b"area read event",
    );

    // Act
    let area_records = context
        .sink
        .read_area_records_for_tests(context.family, "bench", "area", 0, 1000)
        .expect("read area from store");

    let read_frame = build_stream_read("stream://bench/area/*", 0);
    let (read_msg_type, read_payload) = extract_single_tlv_field(&read_frame);
    let response = request(
        &context,
        "stream://bench/area/*",
        read_msg_type,
        read_payload,
    );
    let response_count = count_stream_read_records_from_payload(response.as_ref())
        .expect("count wildcard response records");

    // Assert
    assert_eq!(area_records.len(), 100);
    assert_eq!(response_count, 100);
}

#[test]
fn should_return_area_cursor_given_zero_limit_area_wildcard_read() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://bench/events/orders", 1, b"orders");

    // Act
    let read_payload = stream_read_response(&context, "stream://bench/events/*", 0, 0);

    // Assert
    assert!(read_payload.records.is_empty());
    assert_eq!(read_payload.last_resource_offset, 0);
    assert_eq!(read_payload.last_area_offset, Some(0));
    assert_eq!(read_payload.last_realm_offset, None);
    assert!(!read_payload.has_more);
}

#[test]
fn should_return_realm_cursor_given_zero_limit_realm_wildcard_read() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://bench/events/orders", 1, b"orders");

    // Act
    let read_payload = stream_read_response(&context, "stream://bench/*/*", 0, 0);

    // Assert
    assert!(read_payload.records.is_empty());
    assert_eq!(read_payload.last_resource_offset, 0);
    assert_eq!(read_payload.last_area_offset, None);
    assert_eq!(read_payload.last_realm_offset, Some(0));
    assert!(!read_payload.has_more);
}

#[test]
fn should_keep_sink_alive_given_malformed_filter_payload_on_stream_read() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/orders";
    seed_committed_stream_route(&context, route, 1, b"persisted");

    let mut malformed_read_payload =
        crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
    malformed_read_payload.put_string(route);
    malformed_read_payload.put_u64(0);
    malformed_read_payload.put_u64(10);
    malformed_read_payload.put_optional_u64(None);
    malformed_read_payload.put_u8(1);
    malformed_read_payload.put_bytes(&[0, 0xF2, 0, 0, 0, 0]);

    // Act
    let malformed_response = request(
        &context,
        route,
        604,
        Bytes::from(malformed_read_payload.finish()),
    );
    let malformed_error = decode_stream_error_message(malformed_response.as_ref())
        .expect("decode malformed stream filter error");

    let valid_read_frame = build_stream_read(route, 0);
    let (valid_read_msg_type, valid_read_payload) = extract_single_tlv_field(&valid_read_frame);
    let valid_read_response = request(&context, route, valid_read_msg_type, valid_read_payload);
    let valid_read_count = count_stream_read_records_from_payload(valid_read_response.as_ref())
        .expect("count valid stream read records");

    // Assert
    assert!(
        malformed_error.contains("ERR_STREAM_FILTER_UNSUPPORTED_VERSION"),
        "unexpected malformed filter error: {malformed_error}"
    );
    assert_eq!(valid_read_count, 1);
}

#[test]
fn should_preserve_append_session_without_notify_given_commit_failure() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/orders";
    let (subscriber_source, subscriber_inbox) =
        register_session_queue_sink(&context.router, context.family, 2);
    let subscribe_frame = build_stream_subscribe(route);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);
    let _subscribe_response = request_from_session(
        &context,
        &subscriber_source,
        &subscriber_inbox,
        2,
        route,
        subscribe_msg_type,
        subscribe_payload,
    );
    subscriber_inbox.clear();

    let session_id = begin_stream(&context, route);
    let append_frame = build_stream_append(session_id, 0, b"retryable");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    let _append_response = request(&context, route, append_msg_type, append_payload);
    context.sink.fail_next_promotion_frontier_commit_for_tests();

    // Act
    let failed_commit_frame = build_stream_commit(session_id, 1);
    let (failed_commit_type, failed_commit_payload) =
        extract_single_tlv_field(&failed_commit_frame);
    let failed_commit_response =
        request(&context, route, failed_commit_type, failed_commit_payload);
    let failed_commit_error = decode_stream_error_message(failed_commit_response.as_ref())
        .expect("decode failed stream commit error");
    let append_sessions_after_failure = context.sink.append_session_count();
    let notifications_after_failure = subscriber_inbox.count();
    let read_after_failure = stream_read_response(&context, route, 0, 10);

    let retry_commit_frame = build_stream_commit(session_id, 1);
    let (retry_commit_type, retry_commit_payload) = extract_single_tlv_field(&retry_commit_frame);
    let retry_commit_response = request(&context, route, retry_commit_type, retry_commit_payload);
    let read_after_retry = stream_read_response(&context, route, 0, 10);
    context.sink.sync_admin_snapshot();
    let stream = context
        .admin_read_model
        .streams(None)
        .into_iter()
        .find(|item| item.realm == "bench" && item.area == "events" && item.resource == "orders")
        .expect("committed stream after retry");

    // Assert
    assert_eq!(failed_commit_error, "Injected stream commit failure");
    assert_eq!(append_sessions_after_failure, 1);
    assert_eq!(notifications_after_failure, 0);
    assert_eq!(context.sink.append_session_count(), 0);
    assert_eq!(subscriber_inbox.count(), 1);
    assert!(decode_stream_error_message(retry_commit_response.as_ref()).is_err());
    assert!(read_after_failure.records.is_empty());
    assert_eq!(read_after_retry.records.len(), 1);
    assert_eq!(
        read_after_retry.records[0].body,
        Bytes::from_static(b"retryable")
    );
    assert_eq!(stream.offset, 0);
    assert_eq!(stream.sessions_active, 0);
    assert_eq!(context.admin_read_model.stream_events_total(), 1);
}

#[test]
fn should_drop_uncommitted_append_session_given_session_cleanup() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/pending";
    let session_id = begin_stream(&context, route);
    let append_frame = build_stream_append(session_id, 0, b"staged");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    let _append_response = request(&context, route, append_msg_type, append_payload);
    let destination = RouteAddress::new(context.family, Route::new(route));

    // Act
    context
        .sink
        .deliver(Envelope::new(
            destination,
            crate::runtime::SessionCleanup {
                session_id: TEST_CLIENT_SESSION_ID,
            },
        ))
        .expect("deliver session cleanup");
    let read_after_cleanup = stream_read_response(&context, route, 0, 10);
    context.sink.sync_admin_snapshot();

    // Assert
    assert_eq!(context.sink.append_session_count(), 0);
    assert!(read_after_cleanup.records.is_empty());
    assert!(context.admin_read_model.streams(None).is_empty());
    assert_eq!(context.admin_read_model.stream_events_total(), 0);
}

#[test]
fn should_create_stream_sink_given_promotion_frontier_layout() {
    // Arrange
    let router = Arc::new(Router::new());

    // Act
    let sink = StreamDomainSink::new_with_layout(
        crate::benchkit::create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageLayout::PromotionFrontier,
        StreamStorageWriteOptions::local(),
    )
    .expect("create stream sink");

    // Assert
    assert_eq!(
        sink.storage_layout(),
        StreamStorageLayout::PromotionFrontier
    );
}

#[test]
fn should_create_stream_sink_with_background_cloud_policy_through_public_api() {
    // Arrange
    let tempdir = tempfile::TempDir::new().expect("create cloud simulation directory");
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::cloud_simulated(
                tempdir.path(),
                "fitz-stream-sink",
                "background",
            )
            .build()
            .expect("build cloud-simulated options"),
        )
        .expect("open cloud-simulated engine"),
    );
    store
        .create_column_family("tenant_default")
        .expect("create route-family column family");
    let router = Arc::new(Router::new());

    // Act
    let result = StreamDomainSink::new_with_layout(
        store,
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageLayout::PromotionFrontier,
        StreamStorageWriteOptions::cloud_background(),
    );

    // Assert
    if let Err(error) = result {
        panic!("cloud Stream sink creation failed: {error}");
    }
}

#[test]
fn should_configure_strict_cloud_writes_before_stream_initialization() {
    // Arrange
    let tempdir = tempfile::TempDir::new().expect("create cloud simulation directory");
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::cloud_simulated(
                tempdir.path(),
                "fitz-stream-sink",
                "strict",
            )
            .build()
            .expect("build cloud-simulated options"),
        )
        .expect("open cloud-simulated engine"),
    );
    store
        .create_column_family("tenant_default")
        .expect("create route-family column family");

    // Act
    let result = StreamDomainSink::new_with_layout(
        store,
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageLayout::PromotionFrontier,
        StreamStorageWriteOptions::cloud_strict(),
    );

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_keep_sync_commits_local_given_local_sync_policy() {
    // Arrange
    let router = Arc::new(Router::new());

    // Act
    let sink = StreamDomainSink::new(
        crate::benchkit::create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageWriteOptions::local(),
    );

    // Assert
    assert_eq!(
        sink.sync_write_mode_for_tests(),
        crate::domains::stream::protocol::StreamWriteMode::Sync
    );
}

#[test]
fn should_exclude_uncommitted_stream_from_admin_snapshot_given_active_session() {
    // Arrange
    let context = setup_test_context();
    let _session_id = begin_stream(&context, "stream://bench/events/pending");

    // Act
    context.sink.sync_admin_snapshot();
    let streams = context.admin_read_model.streams(None);
    let events_total = context.admin_read_model.stream_events_total();

    // Assert
    assert!(streams.is_empty());
    assert_eq!(events_total, 0);
}

#[test]
fn should_preserve_committed_snapshot_given_active_session_overlay() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://bench/events/orders", 1, b"persisted");
    let _session_id = begin_stream(&context, "stream://bench/events/orders");

    // Act
    context.sink.sync_admin_snapshot();
    let streams = context.admin_read_model.streams(None);
    let stream = streams
        .iter()
        .find(|item| item.realm == "bench" && item.area == "events" && item.resource == "orders")
        .expect("committed stream should remain visible");
    let events_total = context.admin_read_model.stream_events_total();

    // Assert
    assert_eq!(stream.offset, 0);
    assert_eq!(stream.watermark, 0);
    assert_eq!(stream.size_bytes, b"persisted".len() as u64);
    assert_eq!(stream.sessions_active, 1);
    assert_eq!(events_total, 1);
}

#[test]
fn should_preserve_committed_watermarks_given_uncommitted_append_overlay() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/orders";
    seed_committed_stream_route(&context, route, 1, b"persisted");
    let session_id = begin_stream(&context, route);
    let append_frame = build_stream_append(session_id, 0, b"staged");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    let _ = request(&context, route, append_msg_type, append_payload);

    // Act
    context.sink.sync_admin_snapshot();
    let stream = context
        .admin_read_model
        .streams(None)
        .into_iter()
        .find(|item| item.realm == "bench" && item.area == "events" && item.resource == "orders")
        .expect("committed stream should remain visible");
    let area_watermark = context
        .admin_read_model
        .stream_area_watermark("bench", "events")
        .expect("area watermark detail");
    let realm_watermark = context
        .admin_read_model
        .stream_realm_watermark("bench")
        .expect("realm watermark detail");
    let events_total = context.admin_read_model.stream_events_total();

    // Assert
    assert_eq!(stream.offset, 0);
    assert_eq!(stream.watermark, 0);
    assert_eq!(stream.size_bytes, b"persisted".len() as u64);
    assert_eq!(stream.sessions_active, 1);
    assert_eq!(area_watermark.resource_count, 1);
    assert_eq!(area_watermark.family_watermarks.len(), 1);
    assert_eq!(area_watermark.family_watermarks[0].family, 1);
    assert_eq!(area_watermark.family_watermarks[0].watermark, 0);
    assert_eq!(realm_watermark.area_count, 1);
    assert_eq!(realm_watermark.resource_count, 1);
    assert_eq!(realm_watermark.family_watermarks.len(), 1);
    assert_eq!(realm_watermark.family_watermarks[0].family, 1);
    assert_eq!(realm_watermark.family_watermarks[0].watermark, 0);
    assert_eq!(events_total, 1);
}

#[test]
fn should_not_inflate_admin_watermark_counts_given_uncommitted_new_resource_overlay() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://bench/events/orders", 1, b"persisted");
    let session_id = begin_stream(&context, "stream://bench/events/audits");
    let append_frame = build_stream_append(session_id, 0, b"staged");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    let _ = request(
        &context,
        "stream://bench/events/audits",
        append_msg_type,
        append_payload,
    );

    // Act
    context.sink.sync_admin_snapshot();
    let streams = context.admin_read_model.streams(None);
    let area_watermark = context
        .admin_read_model
        .stream_area_watermark("bench", "events")
        .expect("area watermark detail");
    let realm_watermark = context
        .admin_read_model
        .stream_realm_watermark("bench")
        .expect("realm watermark detail");
    let events_total = context.admin_read_model.stream_events_total();

    // Assert
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].resource, "orders");
    assert!(streams.iter().all(|item| item.resource != "audits"));
    assert_eq!(area_watermark.resource_count, 1);
    assert_eq!(area_watermark.family_watermarks.len(), 1);
    assert_eq!(area_watermark.family_watermarks[0].family, 1);
    assert_eq!(area_watermark.family_watermarks[0].watermark, 0);
    assert_eq!(realm_watermark.area_count, 1);
    assert_eq!(realm_watermark.resource_count, 1);
    assert_eq!(realm_watermark.family_watermarks.len(), 1);
    assert_eq!(realm_watermark.family_watermarks[0].family, 1);
    assert_eq!(realm_watermark.family_watermarks[0].watermark, 0);
    assert_eq!(events_total, 1);
}

#[test]
fn should_return_trimmed_head_metadata_summary_given_missing_first_resource_page() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(
        &context,
        "stream://bench/events/orders",
        crate::domains::stream::storage::REALM_PAGE_RECORD_LIMIT + 1,
        b"persisted",
    );
    context
        .sink
        .delete_compact_resource_page_for_tests(context.family, "bench", "events", "orders", 0)
        .expect("delete first resource page");

    // Act
    let payload = context
        .sink
        .encode_metadata_response_data_for_tests(
            context.family,
            &Route::new("stream://bench/events/orders"),
        )
        .expect("encode stream metadata summary");
    let metadata = decode_stream_metadata_payload(&payload);

    // Assert
    assert_eq!(
        metadata.first_resource_offset,
        Some(crate::domains::stream::storage::REALM_PAGE_RECORD_LIMIT as u64)
    );
    assert_eq!(
        metadata.last_resource_offset,
        Some(crate::domains::stream::storage::REALM_PAGE_RECORD_LIMIT as u64)
    );
    assert_eq!(metadata.resource_count, 1);
}

#[test]
fn should_encode_exact_resource_read_payload_given_committed_record_with_metadata() {
    // Arrange
    let context = setup_test_context();
    let session_id = begin_stream(&context, "stream://bench/events/orders");
    let append_frame = build_stream_append_with_metadata(session_id, 0, b"payload", Some(b"meta"));
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);
    let _ = request(
        &context,
        "stream://bench/events/orders",
        append_msg_type,
        append_payload,
    );
    let commit_frame = build_stream_commit(session_id, 1);
    let (commit_msg_type, commit_payload) = extract_single_tlv_field(&commit_frame);
    let _ = request(
        &context,
        "stream://bench/events/orders",
        commit_msg_type,
        commit_payload,
    );

    // Act
    let payload = context
        .sink
        .encode_read_response_data_for_tests(
            context.family,
            &Route::new("stream://bench/events/orders"),
            0,
            10,
            None,
            None,
        )
        .expect("encode exact stream read payload");
    let read_payload = decode_stream_read_payload(&payload);

    // Assert
    assert_eq!(read_payload.records.len(), 1);
    assert_eq!(read_payload.records[0].resource_offset, 0);
    assert_eq!(read_payload.records[0].area_offset, Some(0));
    assert_eq!(read_payload.records[0].realm_offset, Some(0));
    assert_eq!(read_payload.records[0].body, Bytes::from_static(b"payload"));
    assert_eq!(
        read_payload.records[0].metadata,
        Some(Bytes::from_static(b"meta"))
    );
    assert!(read_payload.records[0].created_at > 0);
    assert_eq!(read_payload.last_resource_offset, 0);
    assert_eq!(read_payload.last_area_offset, Some(0));
    assert_eq!(read_payload.last_realm_offset, Some(0));
    assert!(!read_payload.has_more);
}

#[test]
fn should_encode_exact_resource_metadata_payload_given_empty_stream() {
    // Arrange
    let context = setup_test_context();

    // Act
    let payload = context
        .sink
        .encode_metadata_response_data_for_tests(
            context.family,
            &Route::new("stream://bench/events/empty"),
        )
        .expect("encode empty stream metadata payload");
    let metadata = decode_stream_metadata_payload(&payload);

    // Assert
    assert_eq!(metadata.first_resource_offset, None);
    assert_eq!(metadata.last_resource_offset, None);
    assert_eq!(metadata.resource_count, 0);
    assert_eq!(metadata.max_batch_events, 10_000);
    assert_eq!(metadata.max_batch_bytes, 10 * 1024 * 1024);
    assert_eq!(metadata.ttl_seconds, None);
    assert_eq!(metadata.area_watermark, 0);
    assert_eq!(metadata.realm_watermark, 0);
}

mod correctness;
mod realm_reads;
