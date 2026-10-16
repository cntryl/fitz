use super::*;

#[test]
fn should_acknowledge_slow_successful_sync_commit_without_an_indeterminate_timeout() {
    // Arrange
    let context = setup_test_context();
    let route = "stream://bench/events/orders";
    let session = begin_stream(&context, route);
    let (kind, payload) = extract_single_tlv_field(&build_stream_append(session, 0, b"persisted"));
    let _ = request(&context, route, kind, payload);
    context
        .sink
        .config
        .stream_store
        .delay_next_promotion_frontier_commit_for_tests();
    let (kind, payload) = extract_single_tlv_field(&build_stream_commit(session, 1));

    // Act
    let delivered = route_frame(
        context.router.as_ref(),
        &context.source,
        route,
        TEST_CLIENT_SESSION_ID,
        ChannelId::Pub,
        kind,
        payload,
        context.family,
    );
    let responses = context.inbox.drain();
    let read = stream_read_response(&context, route, 0, 1);

    // Assert
    assert_eq!(delivered, Ok(()));
    assert_eq!(responses.len(), 1, "one terminal commit reply");
    assert_eq!(responses[0].payload.as_ref(), &[0, 0, 0, 0, 0]);
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].body, Bytes::from_static(b"persisted"));
    assert_eq!(read.records[0].resource_offset, 0);
}

#[test]
fn should_keep_stream_control_cleanup_deadline_bounded() {
    // Arrange
    let context = setup_test_context();
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    context
        .sink
        .block_family_actor_for_tests(context.family, entered_tx, release_rx);
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);

    // Act
    let result = std::thread::scope(|scope| {
        scope.spawn(|| {
            let result = context.sink.deliver_high_priority(Envelope::new(
                RouteAddress::new(context.family, Route::new("stream://cleanup")),
                crate::runtime::SessionCleanup { session_id: 7 },
            ));
            let _ = result_tx.send(result);
        });
        let result = result_rx.recv_timeout(Duration::from_secs(2));
        release_tx.send(()).unwrap();
        result
    });

    // Assert
    assert_eq!(result.unwrap(), Err(DeliveryError::Timeout));
}
