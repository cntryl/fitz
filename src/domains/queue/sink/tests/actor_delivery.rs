use super::routing_watch_and_admin::{encode_queue_send, new_queue_domain_sink};
use super::*;

fn queue_send_envelope(family: RouteFamily, queue_route: &str) -> Envelope {
    let client_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let queue_address = RouteAddress::new(family, Route::new(queue_route));
    Envelope::from_route(
        client_address,
        queue_address,
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send(queue_route, b"email"),
            family,
        ),
    )
}

#[test]
fn should_route_queue_delivery_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/emails";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
    );
    let envelope = queue_send_envelope(family, queue_route);

    // Act
    sink.stop_actor_for_tests();
    let result = sink.deliver(envelope);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(matches!(result, Err(DeliveryError::ActorStopped)));
    assert!(sink.actors.lock().is_empty());
}

#[test]
fn should_route_queue_admin_refresh_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/emails";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model.clone(),
        cntryl_midge::WriteOptions::best_effort(),
    );
    sink.deliver(queue_send_envelope(family, queue_route))
        .expect("enqueue queue message");

    // Act
    sink.stop_actor_for_tests();
    sink.refresh_admin_snapshot_if_dirty();
    let queues = admin_read_model.queues(None);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(queues.is_empty());
}

#[test]
fn should_route_queue_live_counts_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/emails";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        cntryl_midge::WriteOptions::best_effort(),
    );
    sink.deliver(queue_send_envelope(family, queue_route))
        .expect("enqueue queue message");
    assert_eq!(sink.ready_message_count(), 1);

    // Act
    sink.stop_actor_for_tests();
    let ready_messages = sink.ready_message_count();

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(ready_messages, 0);
}
