use super::*;

const DUE_MESSAGE_COUNT: usize = 33;
const EXPECTED_FIRST_PASS: usize = 32;

fn actor_with_clock(resource: &str) -> (QueueActor, MockClock) {
    let clock = MockClock::new();
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("open test store"),
    );
    let actor = QueueActor::with_clock(
        RouteFamily::new(0),
        unique_queue_key(resource),
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    (actor, clock)
}

fn messages(delay_seconds: Option<u64>) -> Vec<(Bytes, Option<u64>)> {
    (0..DUE_MESSAGE_COUNT)
        .map(|index| (Bytes::from(format!("message-{index}")), delay_seconds))
        .collect()
}

#[test]
fn should_bound_delayed_promotions_per_due_work_pass() {
    // Arrange
    let (mut actor, clock) = actor_with_clock("bounded-delayed-promotions");
    let response = actor.handle_send_batch(&messages(Some(1)));
    assert!(
        matches!(response, QueueResponse::SentBatch { ref ids } if ids.len() == DUE_MESSAGE_COUNT)
    );
    clock.advance(Duration::from_secs(2));

    // Act
    actor.process_delayed_messages();

    // Assert
    assert_eq!(actor.ready_len(), EXPECTED_FIRST_PASS);
    assert_eq!(actor.delayed.len(), DUE_MESSAGE_COUNT - EXPECTED_FIRST_PASS);
    actor.process_delayed_messages();
    assert_eq!(actor.ready_len(), DUE_MESSAGE_COUNT);
    assert!(actor.delayed.is_empty());
}

#[test]
fn should_bound_inflight_expirations_per_due_work_pass() {
    // Arrange
    let (mut actor, clock) = actor_with_clock("bounded-inflight-expirations");
    let response = actor.handle_send_batch(&messages(None));
    assert!(
        matches!(response, QueueResponse::SentBatch { ref ids } if ids.len() == DUE_MESSAGE_COUNT)
    );
    let reserved = actor.handle_receive_for_session(TEST_SESSION_ID, 1, Some(DUE_MESSAGE_COUNT));
    assert!(
        matches!(reserved, QueueResponse::Received { ref messages } if messages.len() == DUE_MESSAGE_COUNT)
    );
    clock.advance(Duration::from_secs(2));

    // Act
    actor.process_expired_timers();

    // Assert
    assert_eq!(actor.ready_len(), EXPECTED_FIRST_PASS);
    assert_eq!(
        actor.inflight.len(),
        DUE_MESSAGE_COUNT - EXPECTED_FIRST_PASS
    );
    actor.process_expired_timers();
    assert_eq!(actor.ready_len(), DUE_MESSAGE_COUNT);
    assert!(actor.inflight.is_empty());
}
