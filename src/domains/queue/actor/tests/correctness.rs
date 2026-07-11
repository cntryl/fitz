use super::*;

#[test]
fn should_reject_queue_actor_family_mismatch() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(cntryl_midge::OpenOptions::in_memory().build())
            .expect("open test store"),
    );
    let queue_key = QueueKey {
        family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "queue".to_string(),
        resource: "family-mismatch".to_string(),
    };

    // Act
    let result = QueueActor::try_with_clock_and_write_options(
        RouteFamily::new(2),
        queue_key,
        store,
        Box::new(crate::runtime::clock::SystemClock),
        None,
        crate::utils::idempotency::default_dedup_store(),
        cntryl_midge::WriteOptions::buffered(),
    );

    // Assert
    let Err(error) = result else {
        panic!("family mismatch must fail construction");
    };
    assert!(error.contains("queue actor family mismatch"));
}

#[test]
fn should_reject_enqueue_when_message_id_space_is_exhausted() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(cntryl_midge::OpenOptions::in_memory().build())
            .expect("open test store"),
    );
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        unique_queue_key("id-exhaustion"),
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    actor.next_id = u64::MAX;
    actor.next_id_limit = u64::MAX;

    // Act
    let response = actor.handle_send(Bytes::from_static(b"blocked"), None);

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert_eq!(actor.ready_len(), 0);
}

#[test]
fn should_reject_receive_when_delivery_attempt_counter_is_exhausted() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(cntryl_midge::OpenOptions::in_memory().build())
            .expect("open test store"),
    );
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        unique_queue_key("attempt-exhaustion"),
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let message_id = match actor.handle_send(Bytes::from_static(b"blocked"), None) {
        QueueResponse::Sent { id } => id,
        other => panic!("expected send success, found {other:?}"),
    };
    actor
        .records
        .get_mut(&message_id)
        .expect("cached queue record")
        .attempts = u32::MAX;

    // Act
    let response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));

    // Assert
    assert!(matches!(response, QueueResponse::Error { .. }));
    assert_eq!(actor.ready_len(), 1);
    assert!(actor.inflight.is_empty());
}

#[test]
fn should_recover_ready_message_id_at_numeric_maximum_without_looping() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(cntryl_midge::OpenOptions::in_memory().build())
            .expect("open test store"),
    );
    let queue_key = unique_queue_key("max-ready-id");
    let mut txn = store
        .begin_tx(
            queue_key.family.id(),
            cntryl_midge::TransactionMode::ReadWrite,
        )
        .expect("begin index transaction");
    let ready_prefix = QueueActor::ready_index_prefix(&queue_key);
    let ready_key = QueueActor::ready_range_key_with_prefix(
        &ready_prefix,
        QueueActor::READY_SHARDS - 1,
        u64::MAX,
    );
    txn.put(
        QueueActor::index_meta_key(&queue_key),
        QueueActor::encode_index_meta(u64::MAX, 1, 0, None),
        None,
    )
    .expect("write index metadata");
    txn.put(ready_key, u64::MAX.to_le_bytes().to_vec(), None)
        .expect("write max ready range");
    txn.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit index transaction");

    // Act
    let actor = QueueActor::new(
        queue_key.family,
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Assert
    assert_eq!(actor.ready_len(), 1);
    assert!(actor.ready_contains(MessageId::new(u64::MAX)));
}
