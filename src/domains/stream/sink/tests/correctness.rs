use super::*;
use crate::benchkit::{
    build_stream_append, build_stream_begin, build_stream_subscribe, extract_single_tlv_field,
    route_frame_to_address, FrameQueueSink,
};
use crate::dispatch::protocol::frame::ChannelId;
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::Mailbox;
use bytes::Bytes;

fn request_from_session_to_address(
    context: &TestContext,
    source: &RouteAddress,
    inbox: &FrameQueueSink,
    session_id: u64,
    destination: &RouteAddress,
    msg_type: u16,
    payload: Bytes,
) -> Bytes {
    route_frame_to_address(
        context.router.as_ref(),
        source,
        destination,
        session_id,
        ChannelId::Pub,
        msg_type,
        payload,
    )
    .expect("stream route");

    let responses = inbox.drain();
    responses
        .last()
        .map(|frame| frame.payload.clone())
        .expect("stream response")
}

fn context_for_family(context: &TestContext, family: RouteFamily) -> TestContext {
    let (source, inbox) =
        register_session_queue_sink(&context.router, family, TEST_CLIENT_SESSION_ID);
    TestContext {
        router: Arc::clone(&context.router),
        family,
        source,
        inbox,
        sink: Arc::clone(&context.sink),
        admin_read_model: Arc::clone(&context.admin_read_model),
    }
}

#[test]
fn should_fail_closed_after_stream_actor_panic() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = StreamDomain::try_new(
        crate::benchkit::create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageWriteOptions::local(),
    )
    .expect("create Stream test sink");
    sink.panic_actor_for_failpoint();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);

    // Act
    while !sink.family_health_snapshot().healthy_families.is_empty()
        && std::time::Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    let destination = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("stream://bench/events/orders"),
    );
    let result = sink.deliver(Envelope::new(destination, 42_u64));
    let health = sink.family_health_snapshot();

    // Assert
    assert!(health.healthy_families.is_empty());
    assert_eq!(health.panic_count, 1);
    assert_eq!(health.failed_families, vec![RouteFamily::new(1)]);
    assert!(matches!(result, Err(DeliveryError::ActorStopped)));
}

#[test]
fn should_not_retain_subscription_when_subscribe_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "stream://bench/events/undeliverable";
    let source = RouteAddress::new(family, Route::new("inbox://session/1"));
    let destination = RouteAddress::new(family, Route::new(route));
    let mailbox = Arc::new(Mailbox::new(1));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    mailbox
        .sender()
        .try_send(Envelope::new(source.clone(), 1_u8))
        .expect("fill subscriber mailbox");
    let sink = StreamDomain::try_new(
        crate::benchkit::create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageWriteOptions::local(),
    )
    .expect("create Stream test sink");
    let frame = build_stream_subscribe(route);
    let (message_type, payload) = extract_single_tlv_field(&frame);

    // Act
    sink.deliver(Envelope::from_route(
        source,
        destination,
        FrameContext::new(
            1,
            ChannelId::Pub,
            crate::dispatch::protocol::tlv::MessageType::new(message_type),
            payload,
            family,
        ),
    ))
    .expect("deliver stream subscription");

    // Assert
    assert_eq!(sink.subscription_count(), 0);
}

#[test]
fn should_not_retain_append_session_when_begin_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "stream://bench/events/undeliverable-begin";
    let source = RouteAddress::new(family, Route::new("inbox://session/1"));
    let destination = RouteAddress::new(family, Route::new(route));
    let mailbox = Arc::new(Mailbox::new(1));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    mailbox
        .sender()
        .try_send(Envelope::new(source.clone(), 1_u8))
        .expect("fill response mailbox");
    let sink = StreamDomain::try_new(
        crate::benchkit::create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        StreamStorageWriteOptions::local(),
    )
    .expect("create Stream test sink");
    let frame = build_stream_begin(route);
    let (message_type, payload) = extract_single_tlv_field(&frame);

    // Act
    sink.deliver(Envelope::from_route(
        source,
        destination,
        FrameContext::new(
            1,
            ChannelId::Pub,
            crate::dispatch::protocol::tlv::MessageType::new(message_type),
            payload,
            family,
        ),
    ))
    .expect("deliver stream begin");

    // Assert
    assert_eq!(sink.append_session_count(), 0);
}

#[test]
fn should_fail_closed_given_malformed_stream_commit_notification() {
    // Arrange
    let context = setup_test_context();
    let pattern = "stream://bench/events/*";
    let (subscriber, inbox) = register_session_queue_sink(&context.router, context.family, 2);
    let subscribe = build_stream_subscribe(pattern);
    let (msg_type, payload) = extract_single_tlv_field(&subscribe);
    let _response =
        request_from_session(&context, &subscriber, &inbox, 2, pattern, msg_type, payload);
    inbox.clear();
    let malformed = crate::runtime::DomainPublishEvent::new(
        context.family,
        Route::new("stream://bench/events/orders"),
        Bytes::from_static(b"not-json"),
    );

    // Act
    let family = context.family;
    let pending_is_empty = context.sink.inspect_family_for_tests(family, move |state| {
        state.core.handle_domain_publish(&malformed);
        state.core.subscriptions.pending.is_empty()
    });

    // Assert
    assert_eq!(inbox.count(), 0);
    assert!(pending_is_empty);
}

#[test]
fn should_reject_stream_request_when_source_and_destination_families_differ() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/family-boundary";
    let stream_session_id = begin_stream(&context, route);
    let append_frame = build_stream_append(stream_session_id, 0, b"blocked");
    let (append_msg_type, append_payload) = extract_single_tlv_field(&append_frame);

    // Act
    let mismatched_destination = RouteAddress::new(RouteFamily::new(2), Route::new(route));
    let mismatched_response = request_from_session_to_address(
        &context,
        &context.source,
        &context.inbox,
        TEST_CLIENT_SESSION_ID,
        &mismatched_destination,
        append_msg_type,
        append_payload,
    );
    let owner_append_frame = build_stream_append(stream_session_id, 0, b"allowed");
    let (owner_append_type, owner_append_payload) = extract_single_tlv_field(&owner_append_frame);
    let owner_append_response = request(&context, route, owner_append_type, owner_append_payload);

    // Assert
    assert_eq!(
        decode_stream_error_message(mismatched_response.as_ref()).expect("mismatched family error"),
        "route family mismatch"
    );
    assert!(decode_stream_error_message(owner_append_response.as_ref()).is_err());
}

#[test]
fn should_reject_stream_subscription_given_empty_pattern() {
    // Arrange
    let context = setup_test_context();
    let subscribe_frame = build_stream_subscribe("");
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);

    // Act
    let response = request(
        &context,
        "stream://bench/events/orders",
        subscribe_msg_type,
        subscribe_payload,
    );

    // Assert
    assert_eq!(
        decode_stream_error_message(response.as_ref()).expect("empty pattern error"),
        "subscription pattern must use stream://"
    );
    assert_eq!(context.sink.subscription_count(), 0);
}

#[test]
fn should_reject_stream_begin_given_wildcard_resource_route() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/*";
    let begin_frame = build_stream_begin(route);
    let (begin_msg_type, begin_payload) = extract_single_tlv_field(&begin_frame);

    // Act
    let response = request(&context, route, begin_msg_type, begin_payload);

    // Assert
    assert_eq!(
        decode_stream_error_message(response.as_ref()).expect("wildcard begin error"),
        "stream append routes require concrete realm/area/resource"
    );
    assert_eq!(context.sink.append_session_count(), 0);
}

#[test]
fn should_reject_noncanonical_stream_read_selectors() {
    // Arrange
    let context = setup_test_context();
    let selectors = [
        "stream://bench/**/*",
        "stream://bench/events/**",
        "stream://*/**",
        "stream://**/orders",
    ];

    // Act
    let errors = selectors.map(|selector| {
        let frame = build_stream_read(selector, 0);
        let (msg_type, payload) = extract_single_tlv_field(&frame);
        let response = request(&context, selector, msg_type, payload);
        decode_stream_error_message(response.as_ref()).expect("invalid read selector error")
    });

    // Assert
    assert!(errors
        .into_iter()
        .all(|error| error.contains("stream route selector must be one of the 10 shapes")));
}

#[test]
fn should_accept_realm_plus_global_filtered_stream_read_selectors() {
    // Arrange
    let context = setup_test_context();
    let selectors = [
        "stream://bench/*/orders",
        "stream://*/events/*",
        "stream://*/events/orders",
        "stream://*/*/orders",
    ];

    // Act
    for selector in selectors {
        // Assert
        let frame = build_stream_read(selector, 0);
        let (msg_type, payload) = extract_single_tlv_field(&frame);
        let response = request(&context, selector, msg_type, payload);
        assert!(
            decode_stream_error_message(response.as_ref()).is_err(),
            "expected {selector} to be accepted"
        );
    }
}

#[test]
fn should_accept_global_catch_all_stream_read_selector() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://**";
    let frame = build_stream_read(route, 0);
    let (msg_type, payload) = extract_single_tlv_field(&frame);

    // Act
    let response = request(&context, route, msg_type, payload);

    // Assert
    assert!(decode_stream_error_message(response.as_ref()).is_err());
}

#[test]
fn should_accept_global_catch_all_stream_subscribe_pattern() {
    // Arrange
    let context = setup_test_context();
    let pattern = "stream://**";
    let subscribe_frame = build_stream_subscribe(pattern);
    let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);

    // Act
    let response = request(
        &context,
        "stream://bench/events/orders",
        subscribe_msg_type,
        subscribe_payload,
    );

    // Assert
    assert!(decode_stream_error_message(response.as_ref()).is_err());
    assert_eq!(context.sink.subscription_count(), 1);
}

#[test]
fn should_reject_stream_subscribe_patterns_wider_than_read_grammar() {
    // Arrange
    let context = setup_test_context();
    let rejected_patterns = ["stream://bench/events/**", "stream://bench/**/orders"];

    // Act
    let errors = rejected_patterns.map(|pattern| {
        let subscribe_frame = build_stream_subscribe(pattern);
        let (subscribe_msg_type, subscribe_payload) = extract_single_tlv_field(&subscribe_frame);
        let response = request(
            &context,
            "stream://bench/events/orders",
            subscribe_msg_type,
            subscribe_payload,
        );
        decode_stream_error_message(response.as_ref())
    });

    // Assert
    assert!(errors.into_iter().all(|error| error.is_ok()));
}

#[test]
fn should_reject_global_realm_wildcard_for_concrete_stream_lookups() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://*/events/orders";

    // Act
    let errors = [
        crate::benchkit::build_stream_last(route),
        crate::benchkit::build_stream_get_metadata(route),
    ]
    .map(|frame| {
        let (msg_type, payload) = extract_single_tlv_field(&frame);
        let response = request(&context, route, msg_type, payload);
        decode_stream_error_message(response.as_ref())
            .expect("global-realm wildcard lookup should return an error")
    });

    // Assert
    assert!(errors.iter().all(|error| error.contains("concrete")));
}

#[test]
fn should_refresh_admin_snapshot_after_second_family_commit() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/orders";
    seed_committed_stream_route(&context, route, 1, b"first-family");
    context.sink.refresh_admin_snapshot_if_dirty();
    assert_eq!(context.admin_read_model.streams(None).len(), 1);
    let second_family = context_for_family(&context, RouteFamily::new(2));

    // Act
    seed_committed_stream_route(&second_family, route, 1, b"second-family");
    assert_eq!(
        stream_read_response(&second_family, route, 0, 1)
            .records
            .len(),
        1
    );
    context.sink.refresh_admin_snapshot_if_dirty();
    let mut families = context
        .admin_read_model
        .streams(None)
        .into_iter()
        .map(|stream| stream.route_family)
        .collect::<Vec<_>>();
    families.sort_unstable();
    let area_watermark = context
        .admin_read_model
        .stream_area_watermark("bench", "events")
        .expect("shared area watermark");
    let realm_watermark = context
        .admin_read_model
        .stream_realm_watermark("bench")
        .expect("shared realm watermark");

    // Assert
    assert_eq!(families, [1, 2]);
    assert_eq!(context.admin_read_model.stream_events_total(), 2);
    assert_eq!(area_watermark.resource_count, 2);
    assert_eq!(area_watermark.family_watermarks.len(), 2);
    assert_eq!(realm_watermark.area_count, 1);
    assert_eq!(realm_watermark.resource_count, 2);
    assert_eq!(realm_watermark.family_watermarks.len(), 2);
}

#[test]
fn should_refresh_healthy_family_admin_snapshot_after_first_family_fails() {
    // Arrange
    let context = setup_test_context();
    seed_committed_stream_route(&context, "stream://bench/events/first", 1, b"first-family");
    context.sink.refresh_admin_snapshot_if_dirty();
    assert_eq!(context.admin_read_model.streams(None).len(), 1);
    context
        .sink
        .panic_family_actor_for_failpoint(context.family);
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while !context
        .sink
        .family_health_snapshot()
        .failed_families
        .contains(&context.family)
        && std::time::Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert_eq!(
        context.sink.family_health_snapshot().failed_families,
        vec![context.family]
    );
    let second_family = context_for_family(&context, RouteFamily::new(2));

    // Act
    seed_committed_stream_route(
        &second_family,
        "stream://bench/events/orders",
        1,
        b"second-family",
    );
    assert_eq!(
        stream_read_response(&second_family, "stream://bench/events/orders", 0, 1)
            .records
            .len(),
        1
    );
    context.sink.refresh_admin_snapshot_if_dirty();
    let streams = context.admin_read_model.streams(None);

    // Assert
    assert_eq!(streams.len(), 2);
    assert!(streams.iter().any(|stream| stream.route_family == 1));
    assert!(streams.iter().any(|stream| stream.route_family == 2));
    assert_eq!(context.admin_read_model.stream_events_total(), 2);
    assert_eq!(
        context.sink.family_health_snapshot().healthy_families,
        vec![RouteFamily::new(2)]
    );
}

#[test]
fn should_include_second_family_live_session_in_admin_projection() {
    // Arrange
    let context = setup_test_context();
    let second_family = context_for_family(&context, RouteFamily::new(2));
    let route = "stream://bench/events/orders";
    seed_committed_stream_route(&second_family, route, 1, b"second-family");
    let _active_session_id = begin_stream(&second_family, route);

    // Act
    context.sink.refresh_admin_snapshot_if_dirty();
    let stream = context
        .admin_read_model
        .streams(None)
        .into_iter()
        .find(|stream| stream.route_family == 2)
        .expect("committed second-family stream");
    let area_watermark = context
        .admin_read_model
        .stream_area_watermark("bench", "events")
        .expect("second-family area watermark");
    let realm_watermark = context
        .admin_read_model
        .stream_realm_watermark("bench")
        .expect("second-family realm watermark");

    // Assert
    assert_eq!(stream.sessions_active, 1);
    assert_eq!(area_watermark.family_watermarks.len(), 1);
    assert_eq!(area_watermark.family_watermarks[0].family, 2);
    assert_eq!(realm_watermark.family_watermarks.len(), 1);
    assert_eq!(realm_watermark.family_watermarks[0].family, 2);
}

#[test]
fn should_project_persisted_unprovisioned_family_into_admin_snapshot() {
    // Arrange
    let engine = crate::testkit::create_test_engine_with_cfs(vec![1, 2]);
    let store = crate::domains::stream::StreamStore::new(Arc::clone(&engine));
    store
        .commit_records(crate::domains::stream::store::CommitRecordsParams {
            family: 2,
            realm: "bench",
            area: "events",
            resource: "orders",
            expected_resource_next_offset: 0,
            events: &[crate::domains::stream::store::EventPayload {
                body: Bytes::from_static(b"persisted"),
                metadata: None,
                discriminator: None,
            }],
            ingest_metadata: None,
            mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
        })
        .expect("commit family-2 history");
    let read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = StreamDomain::new_with_storage_layout_and_families(
        crate::storage::FitzStorageEngine::new(engine),
        Arc::new(Router::new()),
        Arc::clone(&read_model),
        StreamStorageLayout::default(),
        Some(&[RouteFamily::new(1)]),
        StreamStorageWriteOptions::local(),
    )
    .expect("create family-1-only Stream domain");

    // Act
    sink.refresh_admin_snapshot_if_dirty();
    let streams = read_model.streams(None);
    let area_watermark = read_model.stream_area_watermark("bench", "events");
    let realm_watermark = read_model.stream_realm_watermark("bench");

    // Assert
    assert_eq!(streams.len(), 1);
    assert_eq!(streams[0].route_family, 2);
    assert_eq!(read_model.stream_events_total(), 1);
    let area_watermark = area_watermark.expect("family-2 area watermark");
    assert_eq!(area_watermark.resource_count, 1);
    assert_eq!(area_watermark.family_watermarks[0].family, 2);
    let realm_watermark = realm_watermark.expect("family-2 realm watermark");
    assert_eq!(realm_watermark.resource_count, 1);
    assert_eq!(realm_watermark.family_watermarks[0].family, 2);
}

#[test]
fn should_clear_failed_family_live_sessions_without_losing_committed_admin_row() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/orders";
    seed_committed_stream_route(&context, route, 1, b"persisted");
    let _active_session_id = begin_stream(&context, route);
    context.sink.refresh_admin_snapshot_if_dirty();
    assert_eq!(context.admin_read_model.streams(None)[0].sessions_active, 1);
    context
        .sink
        .panic_family_actor_for_failpoint(context.family);
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while !context
        .sink
        .family_health_snapshot()
        .failed_families
        .contains(&context.family)
        && std::time::Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert_eq!(
        context.sink.family_health_snapshot().failed_families,
        vec![context.family]
    );
    let second_family = context_for_family(&context, RouteFamily::new(2));

    // Act
    seed_committed_stream_route(
        &second_family,
        "stream://bench/events/second",
        1,
        b"healthy-family",
    );
    context.sink.refresh_admin_snapshot_if_dirty();
    let first_family_stream = context
        .admin_read_model
        .streams(None)
        .into_iter()
        .find(|stream| stream.route_family == 1)
        .expect("durable row from failed family");

    // Assert
    assert_eq!(first_family_stream.resource, "orders");
    assert_eq!(first_family_stream.sessions_active, 0);
    assert_eq!(context.admin_read_model.stream_events_total(), 2);
}

#[test]
fn should_refresh_distinct_shard_family_admin_snapshot_while_peer_is_blocked() {
    // Arrange
    let context = setup_test_context();
    let second_family_id = RouteFamily::new(2);
    let ingress = context.sink.family_runtime.ingress();
    if ingress.shard_for_family(context.family) == ingress.shard_for_family(second_family_id) {
        // One worker cannot process another family while blocked.
        return;
    }
    let second_family = context_for_family(&context, second_family_id);
    seed_committed_stream_route(
        &second_family,
        "stream://bench/events/orders",
        1,
        b"second-family",
    );
    assert!(context.admin_read_model.streams(None).is_empty());
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    context
        .sink
        .block_family_actor_for_tests(context.family, entered_tx, release_rx);
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first-family actor should block");
    let sink = Arc::clone(&context.sink);
    let (started_tx, started_rx) = crossbeam_channel::bounded(1);

    // Act
    let refresh = std::thread::spawn(move || {
        started_tx.send(()).expect("signal Stream admin refresh");
        sink.refresh_admin_snapshot_if_dirty();
    });
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("start Stream admin refresh");
    let deadline = std::time::Instant::now() + Duration::from_millis(750);
    let mut observed_early = false;
    while std::time::Instant::now() < deadline {
        if context
            .admin_read_model
            .streams(None)
            .iter()
            .any(|stream| stream.route_family == 2)
        {
            observed_early = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    release_tx.send(()).expect("release first-family actor");
    refresh.join().expect("finish Stream admin refresh");

    // Assert
    assert!(
        observed_early,
        "second-family snapshot waited for first family"
    );
    assert_eq!(context.admin_read_model.stream_events_total(), 1);
}
