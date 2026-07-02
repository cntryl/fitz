use super::*;

#[test]
fn should_retry_pending_claim_after_restart_given_initial_live_publish_failure() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let schedule_route = "schedule://acme/jobs/replay/run";
    let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let initial_sink = Arc::new(ScheduleDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model.clone(),
    ));

    initial_sink
        .deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(700),
                encode_schedule_create(schedule_route, "* * * * *", b"replay"),
                family,
            ),
        ))
        .expect("create schedule");
    wait_for_schedule_count(&initial_sink, 1);

    initial_sink.prepare_actor_scan_for_tests(family, 1);

    // Act
    initial_sink.scan_due_schedules();
    wait_for_pending_fire_count(&initial_sink, 1);

    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let restarted_sink = Arc::new(ScheduleDomainSink::new(
        store,
        router.clone(),
        admin_read_model,
    ));
    router.register_domain_pattern("schedule", restarted_sink.clone());
    restarted_sink
        .preload_persisted_families()
        .expect("preload persisted families");

    restarted_sink
        .deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(schedule_route),
                family,
            ),
        ))
        .expect("subscribe schedule");
    let subscribe_envelope = receive_envelope(&subscriber_mailbox, "subscribe ack envelope");
    let subscribe_frame = subscribe_envelope
        .into_payload::<FrameContext>()
        .expect("subscribe ack frame");
    let mut subscribe_decoder = PayloadDecoder::new(&subscribe_frame.payload);
    let _subscribe_status = subscribe_decoder.get_u8().expect("subscribe status");
    let subscription_id = subscribe_decoder
        .get_optional_u64()
        .expect("subscription id")
        .expect("subscription id present");

    restarted_sink.scan_due_schedules();

    // Assert
    let notify_envelope = receive_envelope(&subscriber_mailbox, "schedule notify envelope");
    let notify_frame = notify_envelope
        .into_payload::<FrameContext>()
        .expect("schedule notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 705);

    let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
    let notified_subscription_id = notify_decoder.get_u64().expect("notify subscription id");
    let notified_payload = notify_decoder.get_bytes().expect("notify payload");

    assert_eq!(notified_subscription_id, subscription_id);
    assert_eq!(notified_payload.as_ref(), b"replay");
    assert!(notify_decoder.is_complete());

    restarted_sink.scan_due_schedules();
    wait_for_pending_fire_count(&restarted_sink, 0);
    assert_no_envelope(&subscriber_mailbox);
}

#[test]
fn should_retry_ack_without_republishing_given_same_broker_ack_persist_failure() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let schedule_route = "schedule://acme/jobs/retry/run";
    let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(ScheduleDomainSink::new(
        store,
        router.clone(),
        admin_read_model,
    ));
    router.register_domain_pattern("schedule", sink.clone());

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        schedule_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(700),
            encode_schedule_create(schedule_route, "* * * * *", b"retry"),
            family,
        ),
    ))
    .expect("create schedule");
    let _create_ack = receive_envelope(&subscriber_mailbox, "create ack envelope");
    wait_for_schedule_count(&sink, 1);

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        schedule_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(703),
            encode_schedule_subscribe(schedule_route),
            family,
        ),
    ))
    .expect("subscribe schedule");
    let _subscribe_ack = receive_envelope(&subscriber_mailbox, "subscribe ack envelope");
    drain_mailbox(&subscriber_mailbox);

    let claimed_count = sink.prepare_actor_scan_claim_and_fail_next_commit_for_tests(family, 1);
    assert_eq!(claimed_count, 1);

    // Act
    sink.scan_due_schedules();
    let first_notify = receive_envelope(&subscriber_mailbox, "first schedule notify envelope");
    wait_for_ack_failure_count(&sink, 1);
    wait_for_pending_fire_count(&sink, 1);
    wait_for_pending_ack_retry_count(&sink, 1);
    let pending_after_failed_ack = sink.pending_fire_count();
    let pending_ack_retries = sink.pending_ack_retry_count();
    sink.scan_due_schedules();
    wait_for_pending_fire_count(&sink, 0);
    wait_for_pending_ack_retry_count(&sink, 0);

    // Assert
    let notify_frame = first_notify
        .into_payload::<FrameContext>()
        .expect("first schedule notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 705);
    let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
    let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
    let notified_payload = notify_decoder.get_bytes().expect("notify payload");
    assert_eq!(notified_payload.as_ref(), b"retry");
    assert!(notify_decoder.is_complete());
    assert_eq!(sink.ack_failure_count(), 1);
    assert_eq!(pending_after_failed_ack, 1);
    assert_eq!(pending_ack_retries, 1);
    assert_eq!(sink.pending_fire_count(), 0);
    assert_no_envelope(&subscriber_mailbox);
}

#[test]
fn should_increment_expired_pending_claim_metric_when_cleanup_removes_orphans() {
    // Arrange
    let family = RouteFamily::new(1);
    let clock = Arc::new(MockClock::new(1_700_000_000_000));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let metrics = MetricsCollector::new();
    let sink = ScheduleDomainSink::new(store.clone(), router, admin_read_model)
        .with_metrics(metrics.clone());
    let mut actor = crate::domains::schedule::ScheduleActor::new_with_clock(
        family,
        store,
        cntryl_midge::WriteOptions::buffered(),
        clock.clone(),
    );
    let create_response = actor.handle(crate::domains::schedule::ScheduleMessage::Create {
        route: "schedule://acme/jobs/cleanup/run".to_string(),
        cron: "* * * * *".to_string(),
        payload: Bytes::from_static(b"cleanup"),
    });
    assert!(matches!(
        create_response,
        crate::domains::schedule::ScheduleResponse::Ok
    ));
    actor.bench_prepare_scan(1);
    let claimed = actor.bench_claim_due_fires();
    assert_eq!(claimed.len(), 1);
    clock.advance(Duration::from_millis(11));
    sink.force_pending_claim_cleanup_due_for_tests(10);
    sink.insert_actor_for_tests(family, actor);

    // Act
    sink.scan_due_schedules();

    // Assert
    wait_for_metric_counter(&metrics, METRIC_PENDING_CLAIMS_EXPIRED_TOTAL, 1);
    assert_eq!(sink.actor_pending_fire_count_for_tests(family), 0);
}
