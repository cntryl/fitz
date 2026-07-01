use super::*;

struct UnsubscribeFixture {
    family: RouteFamily,
    session_id: u64,
    removed_route: &'static str,
    retained_route: &'static str,
    removed_address: RouteAddress,
    retained_address: RouteAddress,
    subscriber_address: RouteAddress,
    subscriber_mailbox: Arc<Mailbox>,
    sink: ScheduleDomainSink,
}

fn create_unsubscribe_fixture() -> UnsubscribeFixture {
    let family = RouteFamily::new(1);
    let session_id = 7;
    let removed_route = "schedule://acme/jobs/nightly/run";
    let retained_route = "schedule://acme/jobs/weekly/report";
    let removed_address = RouteAddress::new(family, Route::new(removed_route));
    let retained_address = RouteAddress::new(family, Route::new(retained_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(16));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = ScheduleDomainSink::new(store, router, admin_read_model);

    UnsubscribeFixture {
        family,
        session_id,
        removed_route,
        retained_route,
        removed_address,
        retained_address,
        subscriber_address,
        subscriber_mailbox,
        sink,
    }
}

fn subscribe_fixture_route(
    fixture: &UnsubscribeFixture,
    address: &RouteAddress,
    route: &str,
    label: &str,
) {
    fixture
        .sink
        .deliver(Envelope::from_route(
            fixture.subscriber_address.clone(),
            address.clone(),
            FrameContext::new(
                fixture.session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(route),
                fixture.family,
            ),
        ))
        .expect(label);
    let _subscribe_ack = receive_envelope(&fixture.subscriber_mailbox, label);
}

fn unsubscribe_removed_route(fixture: &UnsubscribeFixture) -> u8 {
    fixture
        .sink
        .deliver(Envelope::from_route(
            fixture.subscriber_address.clone(),
            fixture.removed_address.clone(),
            FrameContext::new(
                fixture.session_id,
                ChannelId::Sub,
                MessageType::new(704),
                encode_schedule_subscribe(fixture.removed_route),
                fixture.family,
            ),
        ))
        .expect("unsubscribe removed schedule route");
    let unsubscribe_envelope =
        receive_envelope(&fixture.subscriber_mailbox, "unsubscribe ack envelope");
    let unsubscribe_frame = unsubscribe_envelope
        .into_payload::<FrameContext>()
        .expect("unsubscribe ack frame");
    let mut unsubscribe_decoder = PayloadDecoder::new(&unsubscribe_frame.payload);
    let unsubscribe_status = unsubscribe_decoder.get_u8().expect("unsubscribe status");
    assert!(unsubscribe_decoder.is_complete());
    unsubscribe_status
}

fn publish_fixture_event(fixture: &UnsubscribeFixture, route: &str, payload: &'static [u8]) {
    fixture
        .sink
        .deliver(Envelope::new(
            RouteAddress::new(fixture.family, Route::new("schedule://events/test")),
            crate::runtime::DomainPublishEvent::new(
                fixture.family,
                Route::new(route),
                Bytes::from_static(payload),
            ),
        ))
        .expect("deliver schedule event");
}

fn receive_schedule_notify_payload(mailbox: &Mailbox, label: &str) -> Bytes {
    let notify_envelope = receive_envelope(mailbox, label);
    let notify_frame = notify_envelope
        .into_payload::<FrameContext>()
        .expect("schedule notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 705);
    let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
    let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
    let notified_payload = notify_decoder.get_bytes().expect("notify payload");
    assert!(notify_decoder.is_complete());
    notified_payload
}

#[test]
fn should_publish_schedule_notify_to_subscribers_when_due() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let schedule_route = "schedule://acme/jobs/nightly/run";
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

    let create_ctx = FrameContext::new(
        session_id,
        ChannelId::Sub,
        MessageType::new(700),
        encode_schedule_create(schedule_route, "* * * * *", b"nightly"),
        family,
    );
    let subscribe_ctx = FrameContext::new(
        session_id,
        ChannelId::Sub,
        MessageType::new(703),
        encode_schedule_subscribe(schedule_route),
        family,
    );

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        schedule_address.clone(),
        create_ctx,
    ))
    .expect("create schedule");
    let _create_ack = receive_envelope(&subscriber_mailbox, "create ack envelope");
    wait_for_schedule_count(&sink, 1);

    // Act
    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        schedule_address,
        subscribe_ctx,
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
    wait_for_subscription_count(&sink, 1);

    {
        let mut actors = sink.state.core.actors.lock();
        let actor = actors.get_mut(&family).expect("schedule actor");
        actor.bench_prepare_scan(1);
    }

    sink.scan_due_schedules();

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
    assert_eq!(notified_payload.as_ref(), b"nightly");
    assert!(notify_decoder.is_complete());
}

#[test]
fn should_remove_schedule_subscriptions_given_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let schedule_route = "schedule://acme/jobs/nightly/run";
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
            encode_schedule_create(schedule_route, "* * * * *", b"nightly"),
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
    wait_for_subscription_count(&sink, 1);

    // Act
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("schedule://cleanup")),
        crate::runtime::SessionCleanup { session_id },
    ))
    .expect("cleanup session");
    wait_for_subscription_count(&sink, 0);
    {
        let mut actors = sink.state.core.actors.lock();
        let actor = actors.get_mut(&family).expect("schedule actor");
        actor.bench_prepare_scan(1);
    }
    sink.scan_due_schedules();
    wait_for_pending_fire_count(&sink, 0);

    // Assert
    assert_eq!(sink.subscription_count(), 0);
    assert_no_envelope(&subscriber_mailbox);
    assert!(sink.state.core.sub_families.lock().is_empty());
}

#[test]
fn should_retain_other_schedule_subscription_given_unsubscribe_on_same_session() {
    // Arrange
    let fixture = create_unsubscribe_fixture();
    subscribe_fixture_route(
        &fixture,
        &fixture.removed_address,
        fixture.removed_route,
        "removed subscribe ack envelope",
    );
    subscribe_fixture_route(
        &fixture,
        &fixture.retained_address,
        fixture.retained_route,
        "retained subscribe ack envelope",
    );
    wait_for_subscription_count(&fixture.sink, 2);
    drain_mailbox(&fixture.subscriber_mailbox);

    // Act
    let unsubscribe_status = unsubscribe_removed_route(&fixture);
    wait_for_subscription_count(&fixture.sink, 1);
    publish_fixture_event(&fixture, fixture.removed_route, b"nightly");
    let removed_notification = fixture
        .subscriber_mailbox
        .receiver()
        .recv_timeout(Duration::from_millis(50));
    publish_fixture_event(&fixture, fixture.retained_route, b"weekly");
    let retained_payload = receive_schedule_notify_payload(
        &fixture.subscriber_mailbox,
        "retained schedule notify envelope",
    );

    // Assert
    assert_eq!(unsubscribe_status, 0);
    assert_eq!(fixture.sink.subscription_count(), 1);
    assert!(removed_notification.is_err());
    assert_eq!(retained_payload.as_ref(), b"weekly");
    assert_no_envelope(&fixture.subscriber_mailbox);
}

#[test]
fn should_count_live_publish_failure_given_domain_routing_error() {
    // Arrange
    let family = RouteFamily::new(1);
    let schedule_route = "schedule://acme/jobs/orphan/run";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(ScheduleDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model,
    ));

    {
        let mut actors = sink.state.core.actors.lock();
        let mut actor = crate::domains::schedule::ScheduleActor::new(
            family,
            store,
            cntryl_midge::WriteOptions::buffered(),
        );
        actor
            .create_schedule(
                schedule_route.to_string(),
                "* * * * *".to_string(),
                Bytes::from_static(b"orphan"),
            )
            .expect("create schedule");
        actor.bench_prepare_scan(1);
        actors.insert(family, actor);
    }

    assert_eq!(sink.notify_failure_count(), 0, "no failures before scan");

    // Act
    sink.scan_due_schedules();

    // Assert
    wait_for_notify_failure_count(&sink, 1);
    assert_eq!(
        sink.notify_failure_count(),
        1,
        "live publish handoff failure should be counted when domain routing returns an error"
    );
    assert_eq!(
        sink.ack_failure_count(),
        0,
        "ack failure counter must remain zero when the publish itself failed"
    );
}
