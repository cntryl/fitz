use super::*;

fn new_correctness_schedule_sink(router: Arc<Router>) -> ScheduleDomainSink {
    ScheduleDomainSink::new(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    )
}

fn schedule_error_message(mailbox: &Mailbox, label: &str) -> String {
    let frame = receive_envelope(mailbox, label)
        .into_payload::<FrameContext>()
        .expect("schedule response frame");
    let mut decoder = PayloadDecoder::new(&frame.payload);
    assert_eq!(decoder.get_u8().expect("schedule response status"), 1);
    decoder.get_string().expect("schedule error message")
}

#[test]
fn should_reject_schedule_request_when_source_and_destination_families_differ() {
    // Arrange
    let source_family = RouteFamily::new(2);
    let destination_family = RouteFamily::new(1);
    let source = RouteAddress::new(source_family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(
        destination_family,
        Route::new("schedule://acme/jobs/nightly/run"),
    );
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_schedule_sink(router);
    let request = crate::domains::schedule::ScheduleClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            7,
            crate::runtime::ClientChannel::Sub,
            700,
            destination_family,
        ),
        Ok(crate::domains::schedule::ScheduleMessage::List {
            offset: 0,
            limit: 10,
        }),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver mismatched schedule request");
    let error = schedule_error_message(&mailbox, "schedule family mismatch response");

    // Assert
    assert_eq!(error, "route family mismatch");
    assert_eq!(sink.actor_count_for_tests(), 0);
}

#[test]
fn should_reject_schedule_subscription_when_identity_does_not_match_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("schedule://acme/jobs/nightly/run"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_schedule_sink(router);
    let request = crate::domains::schedule::ScheduleClientRequest::new(
        crate::runtime::ClientFrameMeta::new(7, crate::runtime::ClientChannel::Sub, 703, family),
        Ok(crate::domains::schedule::ScheduleMessage::Subscribe {
            family_id: family,
            route: Route::new("schedule://acme/jobs/nightly/run"),
            session_id: 8,
            subscriber: source.clone(),
        }),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver mismatched schedule subscription");
    let error = schedule_error_message(&mailbox, "schedule identity mismatch response");

    // Assert
    assert_eq!(error, "route family mismatch");
    assert!(sink.subscriptions_are_empty_for_tests());
}

#[test]
fn should_reject_schedule_subscription_when_decoded_family_differs_from_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let other_family = RouteFamily::new(2);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("schedule://acme/jobs/nightly/run"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_schedule_sink(router);
    let request = crate::domains::schedule::ScheduleClientRequest::new(
        crate::runtime::ClientFrameMeta::new(7, crate::runtime::ClientChannel::Sub, 703, family),
        Ok(crate::domains::schedule::ScheduleMessage::Subscribe {
            family_id: other_family,
            route: Route::new("schedule://acme/jobs/nightly/run"),
            session_id: 7,
            subscriber: RouteAddress::new(other_family, Route::new("inbox://session/7")),
        }),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver decoded-family mismatch");
    let error = schedule_error_message(&mailbox, "schedule decoded family mismatch response");

    // Assert
    assert_eq!(error, "route family mismatch");
    assert!(sink.subscriptions_are_empty_for_tests());
}
