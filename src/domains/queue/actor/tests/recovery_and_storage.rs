use super::*;

#[test]
fn should_persist_delayed_promotion_before_restart() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-delayed-promotion");
    let clock = MockClock::new();

    {
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );
        let response = actor.handle_send(Bytes::from("delayed"), Some(1));
        assert!(matches!(response, QueueResponse::Sent { .. }));
    }

    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    clock.advance(Duration::from_secs(2));

    // Act
    actor.process_delayed_messages();
    let recovered = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        Box::new(clock),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Assert
    assert_eq!(actor.ready_len(), 1);
    assert_eq!(actor.persisted_delayed.len(), 0);
    assert!(read_delayed_index_entries(&store, &queue_key).is_empty());
    assert_eq!(read_ready_index_ranges(&store, &queue_key).len(), 1);
    assert_eq!(recovered.ready_len(), 1);
    assert_eq!(recovered.persisted_delayed.len(), 0);
    assert_eq!(recovered.admin_snapshot().messages_delayed, 0);
    assert_eq!(recovered.admin_snapshot().messages_ready, 1);
}

#[test]
fn should_recover_mixed_batch_visibility_counts_after_restart() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-mixed-batch-restart");
    let clock = MockClock::new();
    {
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            Box::new(clock.clone()),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );
        let response = actor.handle_send_batch(&[
            (Bytes::from_static(b"ready-a"), None),
            (Bytes::from_static(b"delayed"), Some(60)),
            (Bytes::from_static(b"ready-b"), None),
        ]);
        assert!(matches!(response, QueueResponse::SentBatch { .. }));
    }

    // Act
    let mut recovered = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let recovered_snapshot = recovered.admin_snapshot();
    let recovered_ready_ranges = read_ready_index_ranges(&store, &queue_key).len();
    let recovered_delayed_entries = read_delayed_index_entries(&store, &queue_key).len();
    let ready_response = recovered.handle_receive_for_session(TEST_SESSION_ID, 120, Some(3));
    clock.advance(Duration::from_secs(61));
    recovered.process_delayed_messages();
    let delayed_response = recovered.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert_eq!(recovered_snapshot.messages_ready, 2);
    assert_eq!(recovered_snapshot.messages_delayed, 1);
    assert_eq!(recovered_ready_ranges, 2);
    assert_eq!(recovered_delayed_entries, 1);
    let ready_messages = match ready_response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 2);
            assert_eq!(messages[0].body, Bytes::from_static(b"ready-a"));
            assert_eq!(messages[1].body, Bytes::from_static(b"ready-b"));
            messages
        }
        other => panic!("Expected ready messages after restart, found {other:?}"),
    };
    let delayed_messages = match delayed_response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].body, Bytes::from_static(b"delayed"));
            messages
        }
        other => panic!("Expected delayed message after promotion, found {other:?}"),
    };
    for message in ready_messages.iter().chain(delayed_messages.iter()) {
        assert_eq!(
            recovered.handle_ack_for_session(TEST_SESSION_ID, message.id, message.token),
            QueueResponse::Acked
        );
    }
    assert!(read_ready_index_ranges(&store, &queue_key).is_empty());
    assert!(read_delayed_index_entries(&store, &queue_key).is_empty());
    assert_eq!(recovered.admin_snapshot().messages_total, 0);
}

#[test]
fn should_hydrate_oversized_body_from_store_without_caching() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-oversized-body");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    let oversized = Bytes::from(vec![b'x'; QueueActor::BODY_CACHE_LIMIT_BYTES + 1]);
    let response = actor.handle_send(oversized.clone(), None);
    assert!(matches!(response, QueueResponse::Sent { .. }));

    match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].body, oversized);
        }
        _ => panic!("Expected Received response"),
    }

    // Assert
    assert_eq!(actor.body_cache.len(), 0);
    assert_eq!(actor.body_cache_bytes, 0);
}

#[test]
fn should_preserve_ready_body_cache_when_receiving_uncached_message() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-receive-cache-preserve");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let mut ids = Vec::with_capacity(QueueActor::BODY_CACHE_LIMIT + 1);

    for i in 0..=QueueActor::BODY_CACHE_LIMIT {
        let body = Bytes::from(format!("message-{i}"));
        let response = actor.handle_send(body, None);
        let QueueResponse::Sent { id } = response else {
            panic!("Expected Sent response");
        };
        ids.push(id);
    }

    let first_id = ids[0];
    let second_id = ids[1];
    assert!(!actor.body_cache.contains_key(&first_id));
    assert!(actor.body_cache.contains_key(&second_id));
    assert_eq!(actor.body_cache.len(), QueueActor::BODY_CACHE_LIMIT);

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    match response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, first_id);
        }
        _ => panic!("Expected Received response"),
    }
    assert!(actor.body_cache.contains_key(&second_id));
    assert_eq!(actor.body_cache.len(), QueueActor::BODY_CACHE_LIMIT);
}

#[test]
fn should_evict_reserved_message_body_from_cache_on_receive() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-receive-cache-evict");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let QueueResponse::Sent { id: message_id } =
        actor.handle_send(Bytes::from("cached message"), None)
    else {
        panic!("Expected Sent response");
    };
    assert!(actor.body_cache.contains_key(&message_id));

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    match response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, message_id);
        }
        _ => panic!("Expected Received response"),
    }
    assert!(!actor.body_cache.contains_key(&message_id));
}

#[test]
fn should_compact_hot_body_fifo_under_cache_churn() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-body-cache-churn");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    for i in 0..(QueueActor::BODY_CACHE_LIMIT * 3) {
        let body = Bytes::from(format!("message-{i}"));
        let response = actor.handle_send(body, None);
        let QueueResponse::Sent { id } = response else {
            panic!("Expected Sent response");
        };
        let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
        let token = match response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                messages[0].token
            }
            _ => panic!("Expected Received response"),
        };
        assert_eq!(
            actor.handle_ack_for_session(TEST_SESSION_ID, id, token),
            QueueResponse::Acked
        );
    }

    // Assert
    let max_fifo_len = QueueActor::BODY_CACHE_LIMIT * QueueActor::BODY_CACHE_FIFO_SLACK_MULTIPLIER
        + actor.body_cache.len();
    assert!(actor.body_cache.is_empty());
    assert_eq!(actor.body_cache_bytes, 0);
    assert!(actor.body_cache_fifo.len() <= max_fifo_len);
}

#[test]
fn should_return_empty_when_reserving_empty_queue() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-empty");
    let mut actor = QueueActor::new(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(10));

    // Assert - empty queue returns Received with no messages (not NotFound)
    match response {
        QueueResponse::NotFound => {}
        QueueResponse::Received { messages } if messages.is_empty() => {}
        _ => panic!("Expected NotFound or empty Received response for empty queue"),
    }
}

#[test]
fn should_complete_message_with_valid_token() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-complete");
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

    let (msg_id, token) = match reserve_response {
        QueueResponse::Received { messages } => (messages[0].id, messages[0].token),
        _ => panic!("Expected Received response"),
    };

    // Act
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, msg_id, token);

    // Assert
    assert_eq!(response, QueueResponse::Acked);
    assert_eq!(actor.inflight.len(), 0);
}

#[test]
fn should_return_error_when_ack_commit_fails() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-ack-commit-failure");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    let sent = actor.handle_send(Bytes::from("test message"), None);
    let QueueResponse::Sent { id } = sent else {
        panic!("Expected Sent response");
    };
    let QueueResponse::Received { messages: reserved } =
        actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1))
    else {
        panic!("Expected Received response");
    };
    let token = reserved[0].token;
    QueueActor::fail_next_ack_commit_for_tests();

    // Act
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
}

#[test]
fn should_allow_ack_retry_after_ack_commit_fails() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-ack-commit-retry");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let (id, token) = send_and_reserve_single_message(&mut actor, "test message");
    QueueActor::fail_next_ack_commit_for_tests();

    // Act
    let failed = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);
    let retried = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);
    let duplicate = actor.handle_ack_for_session(TEST_SESSION_ID, id, token);

    // Assert
    assert!(matches!(failed, QueueResponse::Error { .. }));
    assert_eq!(retried, QueueResponse::Acked);
    assert_eq!(duplicate, QueueResponse::Acked);
    assert_eq!(actor.inflight.len(), 0);
    assert_eq!(actor.ready_len(), 0);
}

#[test]
fn should_recover_reserved_unacked_message_as_ready_after_restart() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-reserved-restart");
    let message_id = {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );
        let response = actor.handle_send(Bytes::from_static(b"unacked"), None);
        let message_id = match response {
            QueueResponse::Sent { id } => id,
            other => panic!("Expected Sent response, found {other:?}"),
        };
        let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
        match response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].id, message_id);
            }
            other => panic!("Expected Received response, found {other:?}"),
        }
        assert_eq!(read_ready_index_ranges(&store, &queue_key).len(), 1);
        message_id
    };

    // Act
    let mut recovered = QueueActor::new(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let response = recovered.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    match response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, message_id);
            assert_eq!(messages[0].body, Bytes::from_static(b"unacked"));
        }
        other => panic!("Expected unacked message after restart, found {other:?}"),
    }
}

#[test]
fn should_dead_letter_unhydratable_head_and_deliver_next_message() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-hydrate-failure");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let message_id = match actor.handle_send(Bytes::from("test message"), None) {
        QueueResponse::Sent { id } => id,
        other => panic!("Expected Sent response, found {other:?}"),
    };
    let next_message_id = match actor.handle_send(Bytes::from("next message"), None) {
        QueueResponse::Sent { id } => id,
        other => panic!("Expected Sent response, found {other:?}"),
    };
    actor.evict_cached_record(message_id);
    actor.evict_cached_body(message_id);
    let mut txn = store
        .begin_tx(
            queue_key.family.id(),
            cntryl_midge::TransactionMode::ReadWrite,
        )
        .expect("begin write tx");
    txn.delete(QueueActor::header_key(&queue_key, message_id))
        .expect("delete queue header");
    txn.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit queue header delete");

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    match response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, next_message_id);
        }
        other => panic!("Expected next Received response, found {other:?}"),
    }
    let dead_letters = actor.admin_dead_letters();
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].message_id, message_id.as_u64());
    assert_eq!(dead_letters[0].reason, "hydration_failed");
    assert!(actor
        .replay_dead_letter(message_id)
        .expect("replay hydration dead letter"));
}

#[test]
fn should_complete_message_when_cached_complete_response_is_invalid() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let dedup_store = crate::utils::idempotency::default_dedup_store();
    let queue_key = unique_queue_key("jobs-invalid-complete-cache");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key.clone(),
        store,
        None,
        dedup_store.clone(),
    );
    let (message_id, token) = send_and_reserve_single_message(&mut actor, "test message");
    dedup_store.record(
        crate::utils::idempotency::DedupKey {
            realm: queue_key.realm.clone(),
            domain: crate::utils::idempotency::Domain::Queue,
            identifier: crate::utils::idempotency::DedupIdentifier::QueueComplete {
                family: queue_key.family.as_u64(),
                area: queue_key.area.clone(),
                resource: queue_key.resource.clone(),
                owner_session_id: TEST_SESSION_ID,
                message_id: message_id.as_u64(),
                token,
            },
        },
        vec![0xFF, 0xAA, 0x55],
    );

    // Act
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, message_id, token);

    // Assert
    assert_eq!(response, QueueResponse::Acked);
    assert_eq!(actor.inflight.len(), 0);
}

#[test]
fn should_keep_inflight_message_when_redelivery_commit_fails() {
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
    let queue_key = unique_queue_key("jobs-redelivery-commit-fail");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let (msg_id, _) = send_and_reserve_single_message(&mut actor, "test message");
    clock.advance(Duration::from_secs(31));
    QueueActor::fail_next_redelivery_commit_for_tests();

    // Act
    actor.process_expired_timers();

    // Assert
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(actor.inflight.len(), 1);
    assert!(actor.inflight.contains_key(&msg_id));
    assert!(!actor.ready_contains(msg_id));
}

#[test]
fn should_redeliver_message_on_retry_sweep_after_redelivery_commit_failure() {
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
    let queue_key = unique_queue_key("jobs-redelivery-retry");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let (msg_id, _) = send_and_reserve_single_message(&mut actor, "test message");
    clock.advance(Duration::from_secs(31));
    QueueActor::fail_next_redelivery_commit_for_tests();
    actor.process_expired_timers();
    clock.advance(Duration::from_secs(1));

    // Act
    actor.process_expired_timers();

    // Assert
    assert_eq!(actor.ready_len(), 1);
    assert_eq!(actor.inflight.len(), 0);
    assert!(actor.ready_contains(msg_id));
}

#[test]
fn should_reject_complete_with_invalid_token() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-invalid-token");
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
    let response = actor.handle_ack_for_session(TEST_SESSION_ID, msg_id, 99999);

    // Assert
    assert_eq!(response, QueueResponse::InvalidToken);
    assert_eq!(actor.inflight.len(), 1);
}

#[test]
fn should_isolate_ack_dedup_given_different_queue_resources() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let shared_dedup_store = crate::utils::idempotency::default_dedup_store();
    let first_key = unique_queue_key("jobs-dedup-a");
    let second_key = unique_queue_key("jobs-dedup-b");
    let mut first_actor = QueueActor::new(
        RouteFamily::new(0),
        first_key,
        store.clone(),
        None,
        shared_dedup_store.clone(),
    );
    let mut second_actor = QueueActor::new(
        RouteFamily::new(0),
        second_key,
        store,
        None,
        shared_dedup_store,
    );
    let (first_id, first_token) = send_and_reserve_single_message(&mut first_actor, "first");
    let (second_id, second_token) = send_and_reserve_single_message(&mut second_actor, "second");
    if second_token == first_token {
        second_actor
            .inflight
            .get_mut(&second_id)
            .expect("second inflight message")
            .token = first_token.wrapping_add(1);
    }

    // Act
    let first_response = first_actor.handle_ack_for_session(TEST_SESSION_ID, first_id, first_token);
    let second_response =
        second_actor.handle_ack_for_session(TEST_SESSION_ID, second_id, first_token);

    // Assert
    assert_eq!(first_id, second_id);
    assert_eq!(first_response, QueueResponse::Acked);
    assert_eq!(second_response, QueueResponse::InvalidToken);
}

#[test]
fn should_isolate_ack_dedup_given_different_route_families() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1, 2]);
    let shared_dedup_store = crate::utils::idempotency::default_dedup_store();
    let first_key = QueueKey {
        family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "queue".to_string(),
        resource: format!("jobs-family-{}", Uuid::new_v4()),
    };
    let second_key = QueueKey {
        family: RouteFamily::new(2),
        realm: first_key.realm.clone(),
        area: first_key.area.clone(),
        resource: first_key.resource.clone(),
    };
    let mut first_actor = QueueActor::new(
        RouteFamily::new(1),
        first_key,
        store.clone(),
        None,
        shared_dedup_store.clone(),
    );
    let mut second_actor = QueueActor::new(
        RouteFamily::new(2),
        second_key,
        store,
        None,
        shared_dedup_store,
    );
    let (first_id, first_token) = send_and_reserve_single_message(&mut first_actor, "first");
    let (second_id, second_token) = send_and_reserve_single_message(&mut second_actor, "second");
    if second_token == first_token {
        second_actor
            .inflight
            .get_mut(&second_id)
            .expect("second inflight message")
            .token = first_token.wrapping_add(1);
    }

    // Act
    let first_response = first_actor.handle_ack_for_session(TEST_SESSION_ID, first_id, first_token);
    let second_response =
        second_actor.handle_ack_for_session(TEST_SESSION_ID, second_id, first_token);

    // Assert
    assert_eq!(first_id, second_id);
    assert_eq!(first_response, QueueResponse::Acked);
    assert_eq!(second_response, QueueResponse::InvalidToken);
}

#[test]
fn should_extend_inflight_with_valid_token() {
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
    let queue_key = unique_queue_key("jobs-extend");
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

    let old_expiry = actor.inflight.get(&msg_id).unwrap().expires_at;

    // Act
    clock.advance(Duration::from_secs(15));
    let response = actor.handle_extend_for_session(TEST_SESSION_ID, msg_id, token, 60);

    // Assert
    assert_eq!(response, QueueResponse::Extended);
    let new_expiry = actor.inflight.get(&msg_id).unwrap().expires_at;
    assert!(new_expiry > old_expiry);
}
