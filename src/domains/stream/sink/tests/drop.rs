use super::*;

pub(super) fn drop_stream(context: &TestContext, selector: &str) -> Bytes {
    let (kind, payload) =
        extract_single_tlv_field(&crate::benchkit::build_stream_drop(selector, 1));
    request(context, selector, kind, payload)
}

#[test]
fn should_recreate_stream_history_from_offset_zero_after_drop() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/logs/events";
    seed_committed_stream_route(&context, route, 3, b"old");
    assert_eq!(drop_stream(&context, "stream://acme/*").as_ref(), &[0]);

    // Act
    seed_committed_stream_route(&context, route, 1, b"new");
    let (kind, payload) = extract_single_tlv_field(&build_stream_read(route, 0));
    let response = request(&context, route, kind, payload);

    // Assert
    let page = decode_stream_read_payload(&decode_stream_success_data(&response));
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].resource_offset, 0);
    assert_eq!(page.records[0].area_offset, Some(0));
    assert_eq!(page.records[0].realm_offset, Some(0));
    assert_eq!(page.records[0].body.as_ref(), b"new");
}

#[test]
fn should_reject_a_queued_drop_after_session_cleanup() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/logs/events";
    seed_committed_stream_route(&context, route, 1, b"retained");
    let scope = crate::domains::stream::StreamDropScope::parse(context.family, route).unwrap();
    let request = crate::domains::stream::StreamClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            TEST_CLIENT_SESSION_ID,
            crate::runtime::ClientChannel::Pub,
            610,
            context.family,
        ),
        Ok(crate::domains::stream::StreamClientFrame::Drop(
            crate::domains::stream::StreamDropRequest {
                scope,
                mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
            },
        )),
    );
    let destination = RouteAddress::new(context.family, Route::new(route));
    let queued = context.router.retain_session_work(Envelope::from_route(
        context.source.clone(),
        destination.clone(),
        request,
    ));
    context
        .sink
        .deliver(Envelope::new(
            destination,
            crate::runtime::SessionCleanup {
                session_id: TEST_CLIENT_SESSION_ID,
            },
        ))
        .unwrap();

    // Act
    context.sink.deliver(queued).unwrap();
    let response = context.inbox.drain().pop().unwrap().payload.clone();

    // Assert
    assert!(decode_stream_error_message(&response)
        .unwrap()
        .contains("cleaned up"));
    assert_eq!(
        context
            .sink
            .config
            .stream_store
            .get_next_resource_offset(context.family.as_u64(), "acme", "logs", "events")
            .unwrap(),
        1
    );
}

#[test]
fn should_reject_a_typed_drop_for_another_route_family() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/logs/events";
    seed_committed_stream_route(&context, route, 1, b"retained");
    let scope = crate::domains::stream::StreamDropScope::parse(RouteFamily::new(2), route).unwrap();
    let request = crate::domains::stream::StreamClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            TEST_CLIENT_SESSION_ID,
            crate::runtime::ClientChannel::Pub,
            610,
            context.family,
        ),
        Ok(crate::domains::stream::StreamClientFrame::Drop(
            crate::domains::stream::StreamDropRequest {
                scope,
                mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
            },
        )),
    );

    // Act
    context
        .sink
        .deliver(Envelope::from_route(
            context.source.clone(),
            RouteAddress::new(context.family, Route::new(route)),
            request,
        ))
        .unwrap();
    let response = context.inbox.drain().pop().unwrap().payload.clone();

    // Assert
    assert!(decode_stream_error_message(&response)
        .unwrap()
        .contains("route family mismatch"));
    assert_eq!(
        context
            .sink
            .config
            .stream_store
            .get_next_resource_offset(context.family.as_u64(), "acme", "logs", "events")
            .unwrap(),
        1
    );
}

#[test]
fn should_drop_committed_stream_history_without_removing_neighbors() {
    // Arrange
    let context = setup_test_context();
    let deleted = "stream://acme/logs/deleted";
    let neighbor = "stream://acme/logs/deleted2";
    seed_committed_stream_route(&context, deleted, 2, b"old");
    seed_committed_stream_route(&context, neighbor, 1, b"retained");

    // Act
    let response = drop_stream(&context, deleted);

    // Assert
    assert_eq!(response.as_ref(), &[0]);
    for (route, expected) in [(deleted, 0), (neighbor, 1)] {
        let (kind, payload) = extract_single_tlv_field(&build_stream_read(route, 0));
        let response = request(&context, route, kind, payload);
        let page = decode_stream_read_payload(&decode_stream_success_data(&response));
        assert_eq!(page.records.len(), expected, "{route}");
    }
}

#[test]
fn should_reject_stream_drop_while_the_callers_append_session_is_active() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/logs/events";
    let _session = begin_stream(&context, route);

    // Act
    let response = drop_stream(&context, "stream://acme/*");

    // Assert
    assert!(decode_stream_error_message(&response)
        .expect("DROP conflict")
        .contains("active append"));
    assert_eq!(context.sink.append_session_count(), 1);
}

#[test]
fn should_discard_an_append_session_when_a_pending_drop_applies() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/logs/events";
    seed_committed_stream_route(&context, route, 1, b"old");
    let session = begin_stream(&context, route);
    let scope = crate::domains::stream::StreamDropScope::parse(context.family, route).unwrap();
    context
        .sink
        .config
        .stream_store
        .drop_scope(
            &scope,
            crate::domains::stream::protocol::StreamWriteMode::Sync,
        )
        .unwrap();
    let (kind, payload) = extract_single_tlv_field(&build_stream_append(session, 1, b"staged"));

    // Act
    let response = request(&context, route, kind, payload);

    // Assert
    assert!(decode_stream_error_message(&response)
        .unwrap()
        .contains("ERR_STREAM_DROPPED"));
    assert_eq!(context.sink.append_session_count(), 0);
    assert_eq!(
        context
            .sink
            .config
            .stream_store
            .get_next_resource_offset(context.family.as_u64(), "acme", "logs", "events")
            .unwrap(),
        0
    );
}

#[test]
fn should_reject_expired_drop_before_storage_submission() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/logs/events";
    seed_committed_stream_route(&context, route, 1, b"retained");
    let scope = crate::domains::stream::StreamDropScope::parse(context.family, route).unwrap();
    let request = crate::domains::stream::StreamClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            TEST_CLIENT_SESSION_ID,
            crate::runtime::ClientChannel::Pub,
            610,
            context.family,
        ),
        Ok(crate::domains::stream::StreamClientFrame::Drop(
            crate::domains::stream::StreamDropRequest {
                scope,
                mode: crate::domains::stream::protocol::StreamWriteMode::Sync,
            },
        )),
    );
    let envelope = Envelope::from_route(
        context.source.clone(),
        RouteAddress::new(context.family, Route::new(route)),
        request,
    )
    .with_deadline(
        std::time::Instant::now()
            .checked_sub(Duration::from_secs(1))
            .unwrap(),
    );

    // Act
    context.sink.deliver(envelope).unwrap();
    let response = context.inbox.drain().pop().unwrap().payload.clone();

    // Assert
    assert!(decode_stream_error_message(&response)
        .unwrap()
        .contains("deadline expired before admission"));
    assert_eq!(
        context
            .sink
            .config
            .stream_store
            .get_next_resource_offset(context.family.as_u64(), "acme", "logs", "events")
            .unwrap(),
        1
    );
}

#[test]
fn should_preserve_live_subscriptions_when_stream_history_is_dropped() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://acme/logs/events";
    seed_committed_stream_route(&context, route, 1, b"old");
    let (kind, payload) = extract_single_tlv_field(&build_stream_subscribe("stream://acme/logs/*"));
    let response = request(&context, "stream://acme/logs/*", kind, payload);
    assert_eq!(response[0], 0);

    // Act
    let response = drop_stream(&context, "stream://acme/*");

    // Assert
    assert_eq!(response.as_ref(), &[0]);
    assert_eq!(context.sink.subscription_count(), 1);
}
