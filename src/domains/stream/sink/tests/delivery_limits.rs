use super::*;
use crate::domains::stream::sink::model::StreamDomainCommand;
use crate::runtime::{DomainPublishEvent, FamilyActorLane, FAMILY_ACTOR_NORMAL_LANE_CAPACITY};

const HOT_ROUTE: &str = "stream://bench/events/orders";

#[test]
fn should_deliver_exact_notification_when_parked_jobs_are_full() {
    // Arrange
    let context = setup_test_context();
    let _wildcard = subscribe_sink(&context, "stream://bench/events/*", 3);
    let family = context.family;
    context.sink.inspect_family_for_tests(family, move |state| {
        for offset in 1..=5_001 {
            state
                .core
                .handle_domain_publish(&commit_event(family, offset));
        }
        assert_eq!(state.core.delivery.parked_counts(), (5_000, 5_000));
    });
    let exact = subscribe_sink(&context, HOT_ROUTE, 2);

    // Act
    seed_committed_stream_route(&context, HOT_ROUTE, 1, b"durable");

    // Assert
    assert_eq!(exact.count(), 1);
}

#[test]
fn should_dispatch_at_most_one_slice_per_actor_turn() {
    // Arrange
    let context = setup_test_context();
    let exact = subscribe_sink(&context, HOT_ROUTE, 2);
    let inbox = exact.clone();
    let family = context.family;
    let first_turn = context.sink.inspect_family_for_tests(family, move |state| {
        for offset in 1..=65 {
            state
                .core
                .handle_domain_publish(&commit_event(family, offset));
        }

        // Act
        state.core.dispatch_ready_notifications();
        state.core.dispatch_ready_notifications();
        inbox.count()
    });

    // Assert
    assert_eq!(first_turn, 64);
    assert_eq!(
        exact.drain_after_count(65, Duration::from_secs(2)).len(),
        65
    );
}

#[test]
fn should_resume_delivery_after_the_normal_lane_rejects_continuation() {
    // Arrange
    let context = setup_test_context();
    let exact = subscribe_sink(&context, HOT_ROUTE, 2);
    let family = context.family;
    let ingress = context.sink.family_runtime.ingress();
    let result = context.sink.inspect_family_for_tests(family, move |state| {
        for offset in 1..=192 {
            state
                .core
                .handle_domain_publish(&commit_event(family, offset));
        }
        let (reply, _unread) = crossbeam_channel::unbounded();
        for _ in 0..FAMILY_ACTOR_NORMAL_LANE_CAPACITY {
            ingress
                .try_enqueue(
                    family,
                    FamilyActorLane::Normal,
                    StreamDomainCommand::ReadLiveCounts(reply.clone()),
                )
                .unwrap();
        }

        // Act
        state.offer_continuation(&ingress);
        state.continuation_queued
    });

    // Assert
    assert!(!result, "a full lane cannot mark a continuation as queued");
    assert_eq!(
        exact.drain_after_count(192, Duration::from_secs(2)).len(),
        192
    );
}

#[test]
fn should_release_parked_target_credits_when_recipients_disconnect() {
    // Arrange
    let context = setup_test_context();
    let pattern = "stream://bench/events/*";
    for session in 2..=17 {
        subscribe_sink(&context, pattern, session);
    }
    let family = context.family;
    context.sink.inspect_family_for_tests(family, move |state| {
        for offset in 1..=4_097 {
            state
                .core
                .handle_domain_publish(&commit_event(family, offset));
        }
        assert_eq!(state.core.delivery.parked_counts(), (4_096, 65_536));
    });

    // Act
    context
        .sink
        .deliver_high_priority(Envelope::new(
            RouteAddress::new(family, Route::new(HOT_ROUTE)),
            crate::runtime::SessionCleanup { session_id: 2 },
        ))
        .unwrap();

    // Assert
    let counts = context
        .sink
        .inspect_family_for_tests(family, |state| state.core.delivery.parked_counts());
    assert_eq!(counts, (4_096, 61_440));
}

#[test]
fn should_release_delivery_budget_when_family_panics() {
    // Arrange
    let context = setup_test_context();
    let _inbox = subscribe_sink(&context, "stream://bench/events/*", 2);
    let family = context.family;
    context.sink.inspect_family_for_tests(family, move |state| {
        state.core.handle_domain_publish(&commit_event(family, 1));
    });
    assert!(context.sink.config.delivery_budgets.parked_bytes() > 0);

    // Act
    context.sink.panic_family_actor_for_failpoint(family);
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while context.sink.family_runtime.is_family_running(family)
        && std::time::Instant::now() < deadline
    {
        std::thread::yield_now();
    }

    // Assert
    assert!(!context.sink.family_runtime.is_family_running(family));
    assert_eq!(context.sink.config.delivery_budgets.parked_bytes(), 0);
}

#[test]
fn should_exclude_new_registrations_from_already_parked_jobs() {
    // Arrange
    let context = setup_test_context();
    let pattern = "stream://bench/events/*";
    let original = subscribe_sink(&context, pattern, 2);
    let family = context.family;
    context.sink.inspect_family_for_tests(family, move |state| {
        state.core.handle_domain_publish(&commit_event(family, 1));
    });
    let newcomer = subscribe_sink(&context, pattern, 3);

    // Act
    context
        .sink
        .config
        .stream_store
        .set_watermark(1, "bench", "events", 1)
        .unwrap();
    context.sink.run_maintenance_slice_for_tests(family);

    // Assert
    assert_eq!(
        original.drain_after_count(1, Duration::from_secs(2)).len(),
        1
    );
    assert_eq!(newcomer.count(), 0);
}

#[test]
fn should_fence_deferred_notification_for_a_recreated_resource() {
    // Arrange
    let context = setup_test_context();
    let exact = subscribe_sink(&context, HOT_ROUTE, 2);
    seed_committed_stream_route(&context, HOT_ROUTE, 1, b"old");
    exact.clear();
    let family = context.family;
    let scope = crate::domains::stream::StreamDropScope::parse(family, HOT_ROUTE).unwrap();

    // Act
    context.sink.inspect_family_for_tests(family, move |state| {
        state.core.handle_domain_publish(&commit_event(family, 0));
        state
            .core
            .stream_store
            .drop_scope_with_deadline(
                &scope,
                crate::domains::stream::protocol::StreamWriteMode::Sync,
                None,
            )
            .unwrap();
        state.core.dispatch_ready_notifications();
    });
    seed_committed_stream_route(&context, HOT_ROUTE, 1, b"new");

    // Assert
    assert_eq!(exact.count(), 1);
    let (kind, payload) = extract_single_tlv_field(&build_stream_read(HOT_ROUTE, 0));
    let response = request(&context, HOT_ROUTE, kind, payload);
    let page = decode_stream_read_payload(&decode_stream_success_data(&response));
    assert_eq!(page.records[0].body.as_ref(), b"new");
}

fn subscribe_sink(context: &TestContext, pattern: &str, session: u64) -> Arc<FrameQueueSink> {
    let (source, inbox) = register_session_queue_sink(&context.router, context.family, session);
    let (kind, payload) = extract_single_tlv_field(&build_stream_subscribe(pattern));
    let response = request_from_session(context, &source, &inbox, session, pattern, kind, payload);
    assert_eq!(response[0], 0);
    inbox
}

fn commit_event(family: RouteFamily, offset: u64) -> DomainPublishEvent {
    DomainPublishEvent::new(
        family,
        Route::new(HOT_ROUTE),
        Bytes::from(
            serde_json::json!({
                "event": "committed",
                "first_global_offset": offset,
                "last_area_offset": offset,
                "last_realm_offset": offset,
                "last_global_offset": offset,
            })
            .to_string(),
        ),
    )
}

#[test]
fn should_replay_the_committed_record_after_notification_loss() {
    // Arrange
    let context = setup_test_context();
    let closed = subscribe_sink(&context, HOT_ROUTE, 2);
    context
        .router
        .unregister(&crate::benchkit::session_inbox_route(context.family, 2));
    seed_committed_stream_route(&context, HOT_ROUTE, 1, b"replayable");

    // Act
    let (kind, payload) = extract_single_tlv_field(&build_stream_read(HOT_ROUTE, 0));
    let response = request(&context, HOT_ROUTE, kind, payload);
    let page = decode_stream_read_payload(&decode_stream_success_data(&response));

    // Assert
    assert_eq!(closed.count(), 0);
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].body.as_ref(), b"replayable");
}

#[test]
fn should_hold_global_notification_until_the_watermark_passes_its_offset() {
    // Arrange
    let context = setup_test_context();
    let inbox = subscribe_sink(&context, "stream://**", 2);
    let family = context.family;
    let store = &context.sink.config.stream_store;
    store.set_global_watermark(1, 7).unwrap();
    context.sink.inspect_family_for_tests(family, move |state| {
        state.core.handle_domain_publish(&commit_event(family, 7));
    });
    assert_eq!(inbox.count(), 0);

    // Act
    store.set_global_watermark(1, 8).unwrap();
    context.sink.run_maintenance_slice_for_tests(family);

    // Assert
    assert_eq!(inbox.drain_after_count(1, Duration::from_secs(2)).len(), 1);
}
