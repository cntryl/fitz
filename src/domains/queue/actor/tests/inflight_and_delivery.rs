use super::*;

#[test]
fn should_reject_extend_with_invalid_token() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-extend-invalid");
    let mut actor = QueueActor::new(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    let body = Bytes::from("test message");
    actor.handle_send(body, None);
    let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    let msg_id = match reserve_response {
        QueueResponse::Received { messages } => messages[0].id,
        _ => panic!("Expected Received response"),
    };

    // Act
    let response = actor.handle_extend_for_session(TEST_SESSION_ID, msg_id, 99999, 60);

    // Assert
    assert_eq!(response, QueueResponse::InvalidToken);
}

#[test]
fn should_redeliver_message_when_inflight_expires() {
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
    let queue_key = unique_queue_key("jobs-redelivery");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    let body = Bytes::from("test message");
    actor.handle_send(body.clone(), None);
    let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    let msg_id = match reserve_response {
        QueueResponse::Received { messages } => messages[0].id,
        _ => panic!("Expected Received response"),
    };

    assert_eq!(actor.ready_len(), 0);
    assert_eq!(actor.inflight.len(), 1);

    // Act
    clock.advance(Duration::from_secs(31));
    actor.process_expired_timers();

    // Message should return to the ready queue before redelivery.
    assert_eq!(actor.ready_len(), 1);
    assert_eq!(actor.inflight.len(), 0);
    assert!(actor.ready_contains(msg_id));

    // Reserve again after expiration.
    let redelivery_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    match redelivery_response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, msg_id);
            assert_eq!(messages[0].attempts, 2); // Incremented from 1 to 2
        }
        _ => panic!("Expected Received response"),
    }
}

#[test]
fn should_reserve_multiple_messages_in_batch() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-reserve-batch");
    let mut actor = QueueActor::new(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Enqueue 5 messages
    for i in 0..5 {
        let body = Bytes::from(format!("message {i}"));
        actor.handle_send(body, None);
    }

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(3));

    // Assert
    match response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 3);
            assert_eq!(actor.ready_len(), 2); // 2 remaining
            assert_eq!(actor.inflight.len(), 3);
        }
        _ => panic!("Expected Received response"),
    }
}

#[test]
fn should_bound_reserve_batch_to_tlv_payload_capacity() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-reserve-wire-capacity");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    for _ in 0..100 {
        actor.handle_send(Bytes::from(vec![0x5a; 1024]), None);
    }

    // Act
    let mut response_bytes_remaining =
        crate::domains::queue::protocol::MAX_QUEUE_RESPONSE_PAYLOAD_BYTES
            - crate::domains::queue::protocol::RECEIVED_RESPONSE_HEADER_BYTES;
    let (response, wire_budget_exhausted) = actor.handle_receive_for_session_with_wire_budget(
        TEST_SESSION_ID,
        30,
        Some(100),
        &mut response_bytes_remaining,
        crate::domains::queue::protocol::RESERVED_MESSAGE_WIRE_OVERHEAD_BYTES,
    );
    let payload = crate::dispatch::protocol::queue_codec::encode_response(202, &response);

    // Assert
    assert!(u16::try_from(payload.len()).is_ok());
    let QueueResponse::Received { messages } = response else {
        panic!("Expected Received response");
    };
    assert_eq!(messages.len(), 62);
    assert_eq!(actor.ready_len(), 38);
    assert_eq!(actor.inflight.len(), 62);
    assert!(actor.admin_dead_letters().is_empty());
    assert!(wire_budget_exhausted);
}

#[test]
fn should_dead_letter_oversized_head_and_reserve_following_message() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-reserve-oversized-head");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let response_budget = crate::domains::queue::protocol::MAX_QUEUE_RESPONSE_PAYLOAD_BYTES
        - crate::domains::queue::protocol::RECEIVED_RESPONSE_HEADER_BYTES;
    let message_overhead = crate::domains::queue::protocol::RESERVED_MESSAGE_WIRE_OVERHEAD_BYTES;
    // Stands in for a record written before the SEND limit existed; the
    // reserve-time dead-letter path still has to cope with those.
    actor.handle_send_unvalidated_for_tests(
        Bytes::from(vec![0x5a; response_budget - message_overhead + 1]),
        None,
    );
    actor.handle_send(Bytes::from_static(b"deliverable"), None);

    // Act
    let mut response_bytes_remaining = response_budget;
    let (response, _) = actor.handle_receive_for_session_with_wire_budget(
        TEST_SESSION_ID,
        30,
        Some(1),
        &mut response_bytes_remaining,
        message_overhead,
    );

    // Assert
    let QueueResponse::Received { messages } = response else {
        panic!("Expected Received response");
    };
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].body, Bytes::from_static(b"deliverable"));
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(actor.inflight.len(), 1);
    let dead_letters = actor.admin_dead_letters();
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].reason, "reserve_response_too_large");
}

#[test]
fn should_ack_multiple_messages_in_batch() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-ack-batch");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    for i in 0..3 {
        actor.handle_send(Bytes::from(format!("message {i}")), None);
    }
    let reserved = match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(3)) {
        QueueResponse::Received { messages } => messages,
        other => panic!("Expected Received response, got {other:?}"),
    };
    let acknowledgements: Vec<_> = reserved
        .iter()
        .map(|message| (message.id, message.token))
        .collect();

    // Act
    let responses = actor.handle_ack_batch_for_session(TEST_SESSION_ID, &acknowledgements);

    // Assert
    assert_eq!(responses, vec![QueueResponse::Acked; 3]);
    assert_eq!(actor.inflight.len(), 0);
}

#[test]
fn should_dequeue_all_enqueued_messages() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("fifo-order");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Seed the queue in a known order.
    for i in 0..5 {
        let body = Bytes::from(format!("msg-{i}"));
        let _ = actor.handle_send(body, None);
    }

    // Act
    let mut reserved_all = Vec::new();
    loop {
        match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(2)) {
            QueueResponse::Received { messages } => {
                if messages.is_empty() {
                    // Queue is empty (actor returns empty Received, not NotFound)
                    break;
                }
                for m in messages {
                    reserved_all.push(m.body);
                    // Simulate immediate completion to prevent redelivery
                    let _ = actor.handle_ack_for_session(TEST_SESSION_ID, m.id, m.token);
                }
            }
            QueueResponse::NotFound => {
                // Queue is empty, we're done
                break;
            }
            _ => panic!("Expected Received or NotFound response"),
        }
    }

    // Assert
    let mut expected: Vec<Bytes> = (0..5).map(|i| Bytes::from(format!("msg-{i}"))).collect();
    reserved_all.sort();
    expected.sort();
    assert_eq!(reserved_all, expected);
}

#[test]
fn should_ignore_stale_timer_after_extend() {
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
    let queue_key = unique_queue_key("jobs-stale-timer");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    let body = Bytes::from("test message");
    actor.handle_send(body, None);
    let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    let (msg_id, token) = match reserve_response {
        QueueResponse::Received { messages } => (messages[0].id, messages[0].token),
        _ => panic!("Expected Received response"),
    };
    let initial_epoch = actor.inflight.get(&msg_id).unwrap().inflight_epoch;

    // Act - Extend before first timer expires
    clock.advance(Duration::from_secs(15));
    actor.handle_extend_for_session(TEST_SESSION_ID, msg_id, token, 60);
    let extended_epoch = actor.inflight.get(&msg_id).unwrap().inflight_epoch;

    // Advance to first timer expiration (30s total)
    clock.advance(Duration::from_secs(15));
    actor.process_expired_timers();

    // Assert - Message still inflight (stale timer ignored)
    assert!(extended_epoch > initial_epoch);
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(actor.inflight.len(), 1);
}

#[test]
fn should_reject_operations_on_expired_inflight() {
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
    let queue_key = unique_queue_key("jobs-expired-inflight");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    let body = Bytes::from("test message");
    actor.handle_send(body, None);
    let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    let (msg_id, token) = match reserve_response {
        QueueResponse::Received { messages } => (messages[0].id, messages[0].token),
        _ => panic!("Expected Received response"),
    };

    // Act - Advance past expiration
    clock.advance(Duration::from_secs(31));

    // Assert - Extend fails (entry would be cleaned up if timer processed)
    // Since we're calling directly without going through actor receive,
    // the entry still exists but is expired
    let _extend_response = actor.handle_extend_for_session(TEST_SESSION_ID, msg_id, token, 60);
    // However the logic checks expiration first, so it returns InflightExpired
    // before being removed, OR it could be NotFound if already cleaned up
    // Let's process timers explicitly to make this deterministic
    actor.process_expired_timers();

    // Now the entry is definitely gone
    let extend_response2 = actor.handle_extend_for_session(TEST_SESSION_ID, msg_id, token, 60);
    assert_eq!(extend_response2, QueueResponse::NotFound);

    let complete_response = actor.handle_ack_for_session(TEST_SESSION_ID, msg_id, token);
    assert_eq!(complete_response, QueueResponse::NotFound);
}

#[test]
fn should_return_message_to_ready_when_extend_observes_expired_inflight() {
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
    let queue_key = unique_queue_key("jobs-expired-extend-redelivery");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    actor.handle_send(Bytes::from("test message"), None);
    let reserved = match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
        QueueResponse::Received { messages } => messages[0].clone(),
        _ => panic!("Expected Received response"),
    };
    clock.advance(Duration::from_secs(31));

    // Act
    let response =
        actor.handle_extend_for_session(TEST_SESSION_ID, reserved.id, reserved.token, 60);

    // Assert
    assert_eq!(response, QueueResponse::InflightExpired);
    assert_eq!(actor.inflight.len(), 0);
    assert_eq!(actor.ready_len(), 1);
    let redelivered = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
    match redelivered {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, reserved.id);
            assert_eq!(messages[0].attempts, 2);
        }
        _ => panic!("Expected redelivered message"),
    }
}

#[test]
fn should_return_message_to_ready_when_complete_observes_expired_inflight() {
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
    let queue_key = unique_queue_key("jobs-expired-complete-redelivery");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    actor.handle_send(Bytes::from("test message"), None);
    let reserved = match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
        QueueResponse::Received { messages } => messages[0].clone(),
        _ => panic!("Expected Received response"),
    };
    clock.advance(Duration::from_secs(31));

    // Act
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, reserved.id, reserved.token);

    // Assert
    assert_eq!(response, QueueResponse::InflightExpired);
    assert_eq!(actor.inflight.len(), 0);
    assert_eq!(actor.ready_len(), 1);
    let redelivered = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
    match redelivered {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, reserved.id);
            assert_eq!(messages[0].attempts, 2);
        }
        _ => panic!("Expected redelivered message"),
    }
}

#[test]
fn should_return_not_found_for_nonexistent_message() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-not-found");
    let mut actor = QueueActor::new(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let fake_id = MessageId::new(99999);

    // Act
    let extend_response = actor.handle_extend_for_session(TEST_SESSION_ID, fake_id, 12345, 60);
    let complete_response = actor.handle_ack_for_session(TEST_SESSION_ID, fake_id, 12345);

    // Assert
    assert_eq!(extend_response, QueueResponse::NotFound);
    assert_eq!(complete_response, QueueResponse::NotFound);
}

#[test]
fn should_delay_message_visibility() {
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
    let queue_key = unique_queue_key("jobs-delay");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    let body = Bytes::from("delayed message");
    let response = actor.handle_send(body.clone(), Some(30));

    let QueueResponse::Sent { id: msg_id } = response else {
        panic!("Expected Enqueued response");
    };

    // Message should stay delayed until visibility expires.
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(actor.delayed.len(), 1);

    // Immediate reserve should still be empty.
    let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
    match reserve_response {
        QueueResponse::NotFound => {}
        QueueResponse::Received { messages } if messages.is_empty() => {}
        _ => panic!("Expected NotFound or empty Received response for delayed messages"),
    }

    // Advance time past the visibility delay.
    clock.advance(Duration::from_secs(31));
    actor.process_delayed_messages();

    // The message should now be ready.
    assert_eq!(actor.ready_len(), 1);
    assert_eq!(actor.delayed.len(), 0);

    // Final reserve should succeed.
    let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
    // Assert
    match reserve_response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, msg_id);
            assert_eq!(messages[0].body, body);
        }
        _ => panic!("Expected Received response"),
    }
}

#[test]
fn should_move_to_dlq_after_max_attempts() {
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
    let queue_key = unique_queue_key("jobs-dlq");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        Some(3), // max_attempts = 3
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act - Enqueue message
    let body = Bytes::from("test message");
    let enqueue_response = actor.handle_send(body.clone(), None);
    let QueueResponse::Sent { id: msg_id } = enqueue_response else {
        panic!("Expected Sent response");
    };

    // Simulate 3 failed delivery attempts
    for attempt in 1..=3 {
        // Reserve
        let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
        match reserve_response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].attempts, attempt);
            }
            _ => panic!("Expected Reserved response on attempt {attempt}"),
        }

        // Expire inflight entry (simulating failed processing)
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();

        if attempt < 3 {
            // Should be back in ready queue
            assert_eq!(actor.ready_len(), 1);
            assert_eq!(actor.inflight.len(), 0);
        }
    }

    // Assert
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(actor.inflight.len(), 0);
    assert_eq!(actor.dlq_count, 1);

    let record = actor
        .load_record_metadata_from_store(msg_id)
        .expect("dlq record should remain in storage");
    assert_eq!(record.state, QueueState::Dlq);
    assert_eq!(record.attempts, 3);
    assert!(record.dead_lettered_at_ms.is_some());
    assert_eq!(record.dlq_reason, Some(DlqReason::MaxAttemptsExceeded));

    let cf_id = queue_key.family.id();
    let txn = store
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadOnly)
        .expect("begin tx");
    let header_result = txn
        .get(&QueueActor::header_key(&queue_key, msg_id))
        .expect("midge get header");
    assert!(
        header_result.is_some(),
        "DLQ header should remain in storage"
    );
    let body_result = txn
        .get(&QueueActor::body_key(&queue_key, msg_id))
        .expect("midge get body");
    assert!(body_result.is_some(), "DLQ body should remain in storage");
}

include!("inflight_and_delivery_more.rs");
