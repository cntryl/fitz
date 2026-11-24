use super::*;

fn new_actor(resource_prefix: &str) -> QueueActor {
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    QueueActor::new(
        RouteFamily::new(0),
        unique_queue_key(resource_prefix),
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    )
}

#[test]
fn should_report_live_counts_after_immediate_enqueue() {
    // Arrange
    let mut actor = new_actor("live-counts-ready");

    // Act
    let response = actor.handle_send(Bytes::from_static(b"ready"), None);
    let counts = actor.live_counts();

    // Assert
    assert!(matches!(response, QueueResponse::Sent { .. }));
    assert_eq!(counts.ready, 1);
    assert_eq!(counts.delayed, 0);
    assert_eq!(counts.inflight, 0);
    assert_eq!(counts.dead_letters, 0);
}

#[test]
fn should_report_live_counts_after_delayed_enqueue() {
    // Arrange
    let mut actor = new_actor("live-counts-delayed");

    // Act
    let response = actor.handle_send(Bytes::from_static(b"delayed"), Some(30));
    let counts = actor.live_counts();

    // Assert
    assert!(matches!(response, QueueResponse::Sent { .. }));
    assert_eq!(counts.ready, 0);
    assert_eq!(counts.delayed, 1);
    assert_eq!(counts.inflight, 0);
    assert_eq!(counts.dead_letters, 0);
}

#[test]
fn should_report_live_counts_after_reserve() {
    // Arrange
    let mut actor = new_actor("live-counts-complete");
    let (_id, _token) = send_and_reserve_single_message(&mut actor, "work");

    // Act
    let counts = actor.live_counts();

    // Assert
    assert_eq!(counts.ready, 0);
    assert_eq!(counts.delayed, 0);
    assert_eq!(counts.inflight, 1);
    assert_eq!(counts.dead_letters, 0);
}

#[test]
fn should_report_live_counts_after_complete() {
    // Arrange
    let mut actor = new_actor("live-counts-complete");
    let (id, token) = send_and_reserve_single_message(&mut actor, "work");

    // Act
    let complete_response = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);
    let counts = actor.live_counts();

    // Assert
    assert_eq!(complete_response, QueueResponse::Acked);
    assert_eq!(counts.ready, 0);
    assert_eq!(counts.delayed, 0);
    assert_eq!(counts.inflight, 0);
    assert_eq!(counts.dead_letters, 0);
}

#[test]
fn should_report_live_counts_after_dead_letter_transition() {
    // Arrange
    let clock = MockClock::new();
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("live-counts-dlq");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key,
        store,
        Box::new(clock.clone()),
        Some(1),
        crate::utils::idempotency::default_dedup_store(),
    );
    let msg_id = send_and_dead_letter_single_message(&mut actor, &clock, "poison", 1);

    // Act
    let counts = actor.live_counts();

    // Assert
    assert!(!actor.ready_contains(msg_id));
    assert_eq!(counts.ready, 0);
    assert_eq!(counts.delayed, 0);
    assert_eq!(counts.inflight, 0);
    assert_eq!(counts.dead_letters, 1);
}
