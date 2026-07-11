use super::routing_watch_and_admin::{bad_request_reason, new_queue_domain_sink};
use super::*;
use crate::domains::queue::{
    QueueClientFrame, QueueClientRequest, QueueMessage, QueueSubscriptionMessage,
};
use crate::protocol::frame_context::FrameContext;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::{ClientChannel, ClientFrameMeta, Mailbox, MailboxSink};

fn receive_response(mailbox: &Mailbox, label: &str) -> FrameContext {
    mailbox
        .receiver()
        .try_recv()
        .expect(label)
        .into_payload::<FrameContext>()
        .expect("queue response frame")
}

fn queue_send_frame(family: RouteFamily, route: &str) -> QueueClientFrame {
    QueueClientFrame::Op(QueueMessage::Send {
        family_id: family,
        route: Route::new(route),
        body: bytes::Bytes::from_static(b"body"),
        delay_seconds: None,
    })
}

#[test]
fn should_reject_queue_request_when_source_and_destination_families_differ() {
    // Arrange
    let source_family = RouteFamily::new(2);
    let destination_family = RouteFamily::new(1);
    let source = RouteAddress::new(source_family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(destination_family, Route::new("queue://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::best_effort(),
    );
    let request = QueueClientRequest::new(
        ClientFrameMeta::new(7, ClientChannel::Pub, 200, destination_family),
        Ok(queue_send_frame(
            destination_family,
            "queue://acme/jobs/email",
        )),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("reject mismatched queue request");
    let response = receive_response(&mailbox, "family mismatch response");

    // Assert
    assert_eq!(response.route_family, source_family);
    assert_eq!(bad_request_reason(&response), "route family mismatch");
    assert!(sink.actors_are_empty_for_tests());
}

#[test]
fn should_reject_queue_operation_when_decoded_family_differs_from_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("queue://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1, 2]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::best_effort(),
    );
    let request = QueueClientRequest::new(
        ClientFrameMeta::new(7, ClientChannel::Pub, 200, family),
        Ok(queue_send_frame(
            RouteFamily::new(2),
            "queue://acme/jobs/email",
        )),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("reject decoded family mismatch");
    let response = receive_response(&mailbox, "decoded family mismatch response");

    // Assert
    assert_eq!(bad_request_reason(&response), "route family mismatch");
    assert!(sink.actors_are_empty_for_tests());
}

#[test]
fn should_reject_queue_watch_when_subscription_identity_does_not_match_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("queue://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::best_effort(),
    );
    let request = QueueClientRequest::new(
        ClientFrameMeta::new(7, ClientChannel::Sub, 207, family),
        Ok(QueueClientFrame::Sub(QueueSubscriptionMessage::Watch {
            family_id: family,
            pattern: Route::new("queue://acme/jobs/*/ready"),
            session_id: 8,
            subscriber: source.clone(),
        })),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("reject mismatched queue watch");
    let response = receive_response(&mailbox, "watch identity mismatch response");

    // Assert
    assert_eq!(bad_request_reason(&response), "route family mismatch");
    assert!(sink.watch_families_are_empty_for_tests());
}

#[test]
fn should_reject_queue_watch_given_empty_pattern() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("queue://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::best_effort(),
    );
    let request = QueueClientRequest::new(
        ClientFrameMeta::new(7, ClientChannel::Sub, 207, family),
        Ok(QueueClientFrame::Sub(QueueSubscriptionMessage::Watch {
            family_id: family,
            pattern: Route::new(""),
            session_id: 7,
            subscriber: source.clone(),
        })),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("reject empty queue watch");
    let response = receive_response(&mailbox, "empty watch response");

    // Assert
    assert_eq!(bad_request_reason(&response), "empty pattern");
    assert!(sink.watch_families_are_empty_for_tests());
}

#[test]
fn should_reject_queue_operation_given_wildcard_queue_route() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("queue://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::best_effort(),
    );
    let request = QueueClientRequest::new(
        ClientFrameMeta::new(7, ClientChannel::Pub, 200, family),
        Ok(queue_send_frame(family, "queue://acme/jobs/*")),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("reject wildcard queue operation");
    let response = receive_response(&mailbox, "wildcard queue response");

    // Assert
    assert_eq!(
        bad_request_reason(&response),
        "invalid queue route: queue://acme/jobs/*"
    );
    assert!(sink.actors_are_empty_for_tests());
}
