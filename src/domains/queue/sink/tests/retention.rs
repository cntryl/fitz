use super::*;
use crate::domains::queue::{retention::QueueRetention, QueueActor};
use crate::testkit::clock::MockClock;

#[test]
fn should_wake_a_parked_reserve_after_expired_ready_heads_are_cleaned() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "queue://acme/jobs/expiry";
    let router = Arc::new(Router::new());
    let mailbox = Arc::new(Mailbox::new(4));
    let inbox = RouteAddress::new(family, Route::new("inbox://session/17"));
    router.register(inbox.clone(), mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    sink.inspect_family_for_tests(family, move |state| {
        let clock = MockClock::new();
        let key = QueueKey::from_route(family, &Route::new(route)).unwrap();
        let mut actor = QueueActor::with_clock(
            family,
            key.clone(),
            state.store.clone(),
            Box::new(clock.clone()),
            None,
            state.dedup_store.clone(),
        )
        .with_retention(QueueRetention {
            message_seconds: Some(1),
            dlq_seconds: None,
        });
        actor.handle_send_batch(&vec![(Bytes::new(), None); 128]);
        actor.handle_send_with_ttl(
            Bytes::from_static(b"live"),
            None,
            std::num::NonZeroU32::new(1),
        );
        clock.advance(Duration::from_secs(1));
        state.actor_registry.insert_for_tests(key.clone(), actor);
        state.ready_states.insert(key, true);
        let request = FrameContext::new(
            17,
            ChannelId::Sub,
            MessageType::new(202),
            encode_queue_reserve_wait(route, 30, 1, 30),
            family,
        );
        state
            .deliver_envelope(&Envelope::from_route(
                inbox,
                RouteAddress::new(family, Route::new("queue://inbound")),
                request,
            ))
            .unwrap();
        assert_eq!(state.reservation_book.pending_count(), 1);
    });

    // Act
    let pending = sink.inspect_family_for_tests(family, |state| {
        for pass in 1..=4 {
            state.sweep_idle_actors_at(Instant::now() + Duration::from_secs(pass));
        }
        state.reservation_book.pending_count()
    });

    // Assert
    assert_eq!(pending, 0);
    let response = receive_queue_frame(&mailbox, "woken reserve");
    assert_eq!(receive_response_first_message(&response).0, 129);
}
