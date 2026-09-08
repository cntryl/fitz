#[test]
fn should_not_requeue_completed_dead_letter_given_restart() {
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
    let queue_key = unique_queue_key("jobs-dlq-restart");
    let msg_id = {
        let mut actor = QueueActor::with_clock(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            Box::new(clock.clone()),
            Some(2),
            crate::utils::idempotency::default_dedup_store(),
        );

        let enqueue_response = actor.handle_send(Bytes::from("test message"), None);
        let QueueResponse::Sent { id: msg_id } = enqueue_response else {
            panic!("Expected Sent response");
        };

        for _ in 0..2 {
            match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
                QueueResponse::Received { messages } => assert_eq!(messages.len(), 1),
                other => panic!("Expected Received response, found {other:?}"),
            }
            clock.advance(Duration::from_secs(31));
            actor.process_expired_timers();
        }

        assert_eq!(actor.dlq_count, 1);
        msg_id
    };

    // Act
    let mut recovered = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key,
        store,
        Box::new(clock),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    );
    let reserve_response = recovered.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert_eq!(recovered.ready_len(), 0);
    assert_eq!(recovered.dlq_count, 1);
    match reserve_response {
        QueueResponse::NotFound => {}
        QueueResponse::Received { messages } => assert!(messages.is_empty()),
        other => panic!("Expected empty queue after restart, found {other:?}"),
    }

    let record = recovered
        .load_record_metadata_from_store(msg_id)
        .expect("dlq record should remain in storage after restart");
    assert_eq!(record.state, QueueState::Dlq);
}

#[test]
fn should_report_admin_dead_letters_given_retained_dlq_messages() {
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
    let queue_key = unique_queue_key("jobs-admin-dlq");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key,
        store,
        Box::new(clock.clone()),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    );
    let msg_id = match actor.handle_send(Bytes::from("test message"), None) {
        QueueResponse::Sent { id } => id,
        other => panic!("Expected Sent response, found {other:?}"),
    };
    for _ in 0..2 {
        match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
            QueueResponse::Received { messages } => assert_eq!(messages.len(), 1),
            other => panic!("Expected Received response, found {other:?}"),
        }
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();
    }

    // Act
    let snapshot = actor.admin_snapshot();
    let dead_letters = actor.admin_dead_letters();

    // Assert
    assert_eq!(snapshot.messages_ready, 0);
    assert_eq!(snapshot.messages_delayed, 0);
    assert_eq!(snapshot.messages_inflight, 0);
    assert_eq!(snapshot.messages_dead_lettered, 1);
    assert_eq!(snapshot.messages_total, 1);
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(dead_letters[0].message_id, msg_id.as_u64());
    assert_eq!(dead_letters[0].attempts, 2);
    assert_eq!(dead_letters[0].reason, "max_attempts_exceeded");
    assert!(dead_letters[0].dead_lettered_at_epoch_ms > 0);
}

#[test]
fn should_replay_dead_letter_given_retained_dlq_message() {
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
    let queue_key = unique_queue_key("jobs-dlq-replay");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key,
        store,
        Box::new(clock.clone()),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    );
    let msg_id = match actor.handle_send(Bytes::from("test message"), None) {
        QueueResponse::Sent { id } => id,
        other => panic!("Expected Sent response, found {other:?}"),
    };
    for _ in 0..2 {
        match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
            QueueResponse::Received { messages } => assert_eq!(messages.len(), 1),
            other => panic!("Expected Received response, found {other:?}"),
        }
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();
    }

    // Act
    let replayed = actor
        .replay_dead_letter(msg_id)
        .expect("replay dead letter");
    let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert!(replayed);
    assert_eq!(actor.dlq_count, 0);
    match reserve_response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, msg_id);
            assert_eq!(messages[0].attempts, 1);
        }
        other => panic!("Expected Received response after replay, found {other:?}"),
    }
}

#[test]
fn should_recover_replayed_dead_letter_as_ready_after_restart() {
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
    let queue_key = unique_queue_key("jobs-dlq-replay-restart");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    );
    let msg_id = send_and_dead_letter_single_message(&mut actor, &clock, "test message", 2);
    assert_eq!(actor.dlq_count, 1);
    assert_eq!(read_dlq_index_entries(&store, &queue_key).len(), 1);

    // Act
    let replayed = actor
        .replay_dead_letter(msg_id)
        .expect("replay dead letter");
    let mut recovered = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        Box::new(clock),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    );
    let reserve_response = recovered.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert!(replayed);
    assert!(read_dlq_index_entries(&store, &queue_key).is_empty());
    assert_eq!(recovered.dlq_count, 0);
    assert_eq!(recovered.ready_len(), 0);
    match reserve_response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, msg_id);
            assert_eq!(messages[0].attempts, 1);
        }
        other => panic!("Expected Received response after replay restart, found {other:?}"),
    }
}

#[test]
fn should_purge_dead_letter_given_retained_dlq_message() {
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
    let queue_key = unique_queue_key("jobs-dlq-purge");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key,
        store,
        Box::new(clock.clone()),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    );
    let msg_id = match actor.handle_send(Bytes::from("test message"), None) {
        QueueResponse::Sent { id } => id,
        other => panic!("Expected Sent response, found {other:?}"),
    };
    for _ in 0..2 {
        match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
            QueueResponse::Received { messages } => assert_eq!(messages.len(), 1),
            other => panic!("Expected Received response, found {other:?}"),
        }
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();
    }

    // Act
    let purged = actor.purge_dead_letter(msg_id).expect("purge dead letter");
    let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert!(purged);
    assert_eq!(actor.dlq_count, 0);
    match reserve_response {
        QueueResponse::NotFound => {}
        QueueResponse::Received { messages } => assert!(messages.is_empty()),
        other => panic!("Expected empty queue after purge, found {other:?}"),
    }
}

#[test]
fn should_keep_purged_dead_letter_deleted_after_restart() {
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
    let queue_key = unique_queue_key("jobs-dlq-purge-restart");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    );
    let msg_id = send_and_dead_letter_single_message(&mut actor, &clock, "test message", 2);
    assert_eq!(actor.dlq_count, 1);
    assert_eq!(read_dlq_index_entries(&store, &queue_key).len(), 1);

    // Act
    let purged = actor.purge_dead_letter(msg_id).expect("purge dead letter");
    let mut recovered = QueueActor::with_clock(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        Box::new(clock),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    );
    let reserve_response = recovered.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert!(purged);
    assert!(read_dlq_index_entries(&store, &queue_key).is_empty());
    assert_eq!(recovered.dlq_count, 0);
    assert_eq!(recovered.ready_len(), 0);
    match reserve_response {
        QueueResponse::Received { messages } => assert!(messages.is_empty()),
        other => panic!("Expected empty queue after purge restart, found {other:?}"),
    }
    assert!(recovered.load_record_metadata_from_store(msg_id).is_err());
}

#[test]
fn should_allow_unlimited_retries_when_max_attempts_is_none() {
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
    let queue_key = unique_queue_key("jobs-unlimited");
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0), /* CF=0 for Midge test limitation */
        queue_key,
        store,
        Box::new(clock.clone()),
        None, // No max_attempts limit
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act - Enqueue message
    let body = Bytes::from("test message");
    actor.handle_send(body, None);

    // Simulate 10 failed delivery attempts
    for attempt in 1..=10 {
        // Reserve
        let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
        match reserve_response {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].attempts, attempt);
            }
            _ => panic!("Expected Reserved response on attempt {attempt}"),
        }

        // Expire inflight entry
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();

        // Should always be back in ready queue (unlimited retries)
        assert_eq!(actor.ready_len(), 1);
        assert_eq!(actor.inflight.len(), 0);
    }

    // Assert - Message still available after 10 attempts
    let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
    match reserve_response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].attempts, 11);
        }
        _ => panic!("Expected Received response"),
    }
}

#[test]
fn should_not_dead_letter_message_that_only_a_wildcard_reserve_cannot_carry() {
    // Arrange
    // A body that fits a concrete reserve response exactly, but not
    // once the wildcard routing envelope and route string are added.
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-reserve-wildcard-overhead");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let response_budget = crate::domains::queue::protocol::MAX_QUEUE_RESPONSE_PAYLOAD_BYTES
        - crate::domains::queue::protocol::RECEIVED_RESPONSE_HEADER_BYTES;
    let concrete_overhead = crate::domains::queue::protocol::RESERVED_MESSAGE_WIRE_OVERHEAD_BYTES;
    let route = "queue://acme/jobs/wildcard-overhead";
    let wildcard_overhead = concrete_overhead
        + crate::domains::queue::protocol::ROUTED_MESSAGE_WIRE_OVERHEAD_BYTES
        + route.len();
    // Above the new SEND limit (which uses the wildcard shape), so seed it as
    // a record written before that limit existed.
    actor.handle_send_unvalidated_for_tests(
        Bytes::from(vec![0x5a; response_budget - concrete_overhead]),
        None,
    );

    // Act
    // Reserve through the wildcard wire shape.
    let mut response_bytes_remaining = response_budget;
    let (response, _) = actor.handle_receive_for_session_with_wire_budget(
        TEST_SESSION_ID,
        30,
        Some(1),
        &mut response_bytes_remaining,
        wildcard_overhead,
    );

    // Assert
    // The wildcard caller gets nothing, but the message stays ready
    // for a concrete reserve rather than being permanently dead-lettered.
    let QueueResponse::Received { messages } = response else {
        panic!("Expected Received response");
    };
    assert!(messages.is_empty());
    assert!(
        actor.admin_dead_letters().is_empty(),
        "message deliverable by a concrete reserve must not be dead-lettered"
    );
    assert_eq!(actor.ready_len(), 1);

    // And a concrete reserve still delivers it.
    let mut concrete_bytes_remaining = response_budget;
    let (concrete_response, _) = actor.handle_receive_for_session_with_wire_budget(
        TEST_SESSION_ID,
        30,
        Some(1),
        &mut concrete_bytes_remaining,
        concrete_overhead,
    );
    let QueueResponse::Received { messages } = concrete_response else {
        panic!("Expected Received response");
    };
    assert_eq!(messages.len(), 1);
}

#[test]
fn should_reject_send_of_body_that_no_reserve_shape_could_return() {
    // Arrange
    // The inbound SEND ceiling and the outbound RESERVE ceiling are the same
    // 65_535-byte payload limit, but the response spends part of it on a
    // header and a per-message envelope. A body accepted on write above that
    // budget can never be handed back, so it must be refused at SEND rather
    // than accepted and dead-lettered later, after the producer was told it
    // was stored.
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-send-oversized");
    let route_len = format!(
        "queue://{}/{}/{}",
        queue_key.realm, queue_key.area, queue_key.resource
    )
    .len();
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    // The strictest shape is a wildcard reserve: header, message envelope,
    // routing envelope, and the route string.
    let max_deliverable_body = crate::domains::queue::protocol::MAX_QUEUE_RESPONSE_PAYLOAD_BYTES
        - crate::domains::queue::protocol::RECEIVED_RESPONSE_HEADER_BYTES
        - crate::domains::queue::protocol::RESERVED_MESSAGE_WIRE_OVERHEAD_BYTES
        - crate::domains::queue::protocol::ROUTED_MESSAGE_WIRE_OVERHEAD_BYTES
        - route_len;

    // Act
    let accepted = actor.handle_send(Bytes::from(vec![0x5a; max_deliverable_body]), None);
    let rejected = actor.handle_send(Bytes::from(vec![0x5a; max_deliverable_body + 1]), None);
    let rejected_batch =
        actor.handle_send_batch(&[(Bytes::from(vec![0x5a; max_deliverable_body + 1]), None)]);

    // Assert
    assert!(
        matches!(accepted, QueueResponse::Sent { .. }),
        "a body at the deliverable limit must still be accepted, got {accepted:?}"
    );
    let QueueResponse::Error { message } = rejected else {
        panic!("expected an oversized SEND to be refused, got {rejected:?}");
    };
    assert!(
        message.contains("ERR_MESSAGE_TOO_LARGE"),
        "unexpected error: {message}"
    );
    assert!(
        matches!(rejected_batch, QueueResponse::Error { .. }),
        "a batch carrying an oversized body must be refused too, got {rejected_batch:?}"
    );
    assert_eq!(
        actor.ready_len(),
        1,
        "the rejected bodies must not be stored"
    );
    assert!(actor.admin_dead_letters().is_empty());
}

#[test]
fn should_skip_hydration_when_wire_budget_is_already_exhausted() {
    // Arrange
    // Once the response's wire budget is exhausted by an earlier message,
    // the next ready candidate's fixed per-response overhead alone already
    // guarantees it cannot fit. Hydrating it anyway would pay real storage
    // I/O only to immediately discard the result. Prove this doesn't happen
    // by deleting the second message's header out from under the actor: if
    // hydration were attempted, it would hit "disappeared from storage" and
    // divert the message instead of simply leaving it ready for next time.
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-skip-hydration-on-exhausted-budget");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let message_overhead = crate::domains::queue::protocol::RESERVED_MESSAGE_WIRE_OVERHEAD_BYTES;
    // A tiny explicit budget stands in for "an earlier message this call
    // already consumed most of the response": just enough room for the
    // first (1-byte) message, leaving less than `message_overhead` behind -
    // too little for the second message to fit no matter its body size.
    let initial_budget = message_overhead + 3;
    actor.handle_send(Bytes::from_static(b"x"), None);
    let second_id = match actor.handle_send(Bytes::from_static(b"never hydrated"), None) {
        QueueResponse::Sent { id } => id,
        other => panic!("Expected Sent response, found {other:?}"),
    };

    // Force the second message out of every in-memory cache and delete its
    // header from storage, so any hydration attempt on it fails loudly.
    actor.evict_cached_record(second_id);
    actor.evict_cached_body(second_id);
    let mut txn = store
        .begin_tx(
            queue_key.family.id(),
            cntryl_midge::TransactionMode::ReadWrite,
        )
        .expect("begin write tx");
    txn.delete(QueueActor::header_key(&queue_key, second_id))
        .expect("delete second message's header");
    txn.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit second message header delete");

    // Act
    let mut response_bytes_remaining = initial_budget;
    let (response, wire_budget_exhausted) = actor.handle_receive_for_session_with_wire_budget(
        TEST_SESSION_ID,
        30,
        Some(2),
        &mut response_bytes_remaining,
        message_overhead,
    );

    // Assert
    let QueueResponse::Received { messages } = response else {
        panic!("Expected Received response");
    };
    assert_eq!(
        messages.len(),
        1,
        "only the first message fits the exhausted budget"
    );
    assert!(wire_budget_exhausted);
    assert_eq!(
        actor.ready_len(),
        1,
        "the second message must remain ready, untouched by a failed hydration"
    );
    assert!(
        actor.admin_dead_letters().is_empty(),
        "the second message must not be diverted - it was never hydrated to find out its header is gone"
    );
}
