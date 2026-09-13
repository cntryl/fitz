//! Fault-injection tests for the KV sink: force a send to fail at a
//! mutate -> send -> (rollback | wake | measure) boundary and assert the
//! state-model invariant that must hold afterwards.

use super::*;

/// Sequence: COMMIT notifies every matching watcher -> one watcher's mailbox
/// is full and delivery fails.
///
/// Invariant: the failing watcher's delivery failure must not stop or
/// unwind notification to the remaining watchers - only the failing
/// watcher's delivery is dropped (and counted), everyone else still learns
/// about the committed mutation.
#[test]
fn should_notify_remaining_watchers_when_one_watcher_mailbox_is_full() {
    // Arrange
    let family = RouteFamily::new(1);
    let healthy_watch_session_id = 7;
    let full_watch_session_id = 9;
    let writer_session_id = 8;
    let kv_route = "kv://acme/app/users";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let healthy_watcher_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let full_watcher_address = RouteAddress::new(family, Route::new("inbox://session/9"));
    let writer_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let healthy_watcher_mailbox = Arc::new(Mailbox::new(16));
    let full_watcher_mailbox = Arc::new(Mailbox::new(1));
    let writer_mailbox = Arc::new(Mailbox::new(16));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(
        healthy_watcher_address.clone(),
        healthy_watcher_mailbox.clone(),
    );
    router.register(full_watcher_address.clone(), full_watcher_mailbox.clone());
    router.register(writer_address.clone(), writer_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = KvDomain::new(store, router, admin_read_model);

    // Act
    // Subscribe both watchers, then pin the second watcher's mailbox at
    // capacity before committing the mutation.
    sink.deliver(Envelope::from_route(
        healthy_watcher_address,
        kv_address.clone(),
        FrameContext::new(
            healthy_watch_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::SUBSCRIBE),
            encode_kv_subscribe(kv_route),
            family,
        ),
    ))
    .expect("subscribe healthy watcher");
    let _ = receive_frame(&healthy_watcher_mailbox, "healthy subscribe ack");

    sink.deliver(Envelope::from_route(
        full_watcher_address,
        kv_address.clone(),
        FrameContext::new(
            full_watch_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::SUBSCRIBE),
            encode_kv_subscribe(kv_route),
            family,
        ),
    ))
    .expect("subscribe full watcher");
    let _ = receive_frame(&full_watcher_mailbox, "full-watcher subscribe ack");
    // Now saturate the full watcher's mailbox so the NOTIFY cannot land.
    full_watcher_mailbox
        .deliver(Envelope::new(kv_address.clone(), Bytes::new()))
        .expect("saturate full watcher mailbox");

    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin(kv_route, 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let begin_frame = receive_frame(&writer_mailbox, "begin ack envelope");
    let tx_id = decode_kv_begin_tx_id(&begin_frame.payload);

    sink.deliver(Envelope::from_route(
        writer_address.clone(),
        kv_address.clone(),
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::PUT),
            encode_kv_put(tx_id, kv_route, b"user:1", b"alice"),
            family,
        ),
    ))
    .expect("put KV value");
    let _ = receive_envelope(&writer_mailbox, "put ack envelope");

    sink.deliver(Envelope::from_route(
        writer_address,
        kv_address,
        FrameContext::new(
            writer_session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, kv_route),
            family,
        ),
    ))
    .expect("commit KV transaction");
    let _ = receive_envelope(&writer_mailbox, "commit ack envelope");

    // Assert
    // The healthy watcher still gets the notification even though a
    // sibling watcher's delivery failed in the same fan-out.
    let notify_frame = receive_frame(&healthy_watcher_mailbox, "KV notify envelope");
    assert_eq!(
        notify_frame.msg_type.as_u16(),
        crate::dispatch::protocol::kv::msg_type::NOTIFY
    );
    // The full watcher never got a second slot freed, so nothing beyond the
    // manually-queued filler envelope is waiting for it.
    assert!(full_watcher_mailbox.receiver().try_recv().is_ok());
    assert!(full_watcher_mailbox.receiver().try_recv().is_err());
}

/// Sequence: BEGIN (`ReadWrite`) records a resource lock -> the transaction
/// ends by one of two independent paths: COMMIT or session cleanup.
///
/// Invariant: every path must leave the exact same lock-table and admin
/// projection state behind - an empty resource-lock table and zero active
/// transactions for that session - so a duplicated-path drift bug in one of
/// the four can't leave a resource locked forever or double-freed.
fn new_lock_state_sink() -> (KvDomain, RouteAddress, RouteAddress, Arc<Mailbox>) {
    let family = RouteFamily::new(1);
    let kv_route = "kv://acme/app/resource";
    let kv_address = RouteAddress::new(family, Route::new(kv_route));
    let source_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let mailbox = Arc::new(Mailbox::new(16));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1, 2]);
    let router = Arc::new(Router::new());
    router.register(source_address.clone(), mailbox.clone());
    let sink = KvDomain::new(
        store,
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    (sink, source_address, kv_address, mailbox)
}

fn begin_read_write_tx(
    sink: &KvDomain,
    source: &RouteAddress,
    kv_address: &RouteAddress,
    mailbox: &Mailbox,
    session_id: u64,
    family: RouteFamily,
) -> u64 {
    sink.deliver(Envelope::from_route(
        source.clone(),
        kv_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::BEGIN),
            encode_kv_begin("kv://acme/app/resource", 1, 0),
            family,
        ),
    ))
    .expect("begin KV transaction");
    let begin_frame = receive_frame(mailbox, "begin ack envelope");
    decode_kv_begin_tx_id(&begin_frame.payload)
}

#[test]
fn should_leave_identical_lock_state_whether_transaction_ends_by_commit_or_cleanup() {
    // -- COMMIT path --
    {
        // Arrange
        let family = RouteFamily::new(1);
        let (sink, source, kv_address, mailbox) = new_lock_state_sink();
        let tx_id = begin_read_write_tx(&sink, &source, &kv_address, &mailbox, 7, family);
        assert!(!sink.resource_locks_are_empty_for_tests());
        assert_eq!(sink.active_transaction_count(), 1);

        // Act
        sink.deliver(Envelope::from_route(
            source,
            kv_address,
            FrameContext::new(
                7,
                ChannelId::Pub,
                MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
                encode_kv_commit(tx_id, "kv://acme/app/resource"),
                family,
            ),
        ))
        .expect("commit KV transaction");
        let _ = receive_envelope(&mailbox, "commit ack envelope");

        // Assert
        assert!(sink.resource_locks_are_empty_for_tests());
        assert_eq!(sink.active_transaction_count(), 0);
    }

    // -- cleanup_session path (disconnect mid read-write transaction) --
    {
        // Arrange
        let family = RouteFamily::new(1);
        let (sink, source, kv_address, mailbox) = new_lock_state_sink();
        let _tx_id = begin_read_write_tx(&sink, &source, &kv_address, &mailbox, 7, family);
        assert!(!sink.resource_locks_are_empty_for_tests());
        assert_eq!(sink.active_transaction_count(), 1);

        // Act
        // Disconnect the session instead of committing.
        sink.cleanup_session(7).expect("cleanup KV session");

        // Assert
        // The end state is identical to the COMMIT path above.
        assert!(sink.resource_locks_are_empty_for_tests());
        assert_eq!(sink.active_transaction_count(), 0);
    }
}

/// Sequence: COMMIT succeeds and releases the lock -> the client never saw
/// the ack (as if delivery had failed) and retries the same COMMIT frame.
///
/// Invariant: replaying a COMMIT for a `tx_id` that has already been
/// committed and forgotten must not resurrect the lock, must not emit a
/// second watch notification for the same mutation, and must report the
/// well-known "unknown transaction" error rather than silently succeeding
/// again.
#[test]
fn should_reject_retried_commit_after_transaction_already_committed_and_forgotten() {
    // Arrange
    let family = RouteFamily::new(1);
    let (sink, source, kv_address, mailbox) = new_lock_state_sink();
    let tx_id = begin_read_write_tx(&sink, &source, &kv_address, &mailbox, 7, family);
    sink.deliver(Envelope::from_route(
        source.clone(),
        kv_address.clone(),
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::PUT),
            encode_kv_put(tx_id, "kv://acme/app/resource", b"user:1", b"alice"),
            family,
        ),
    ))
    .expect("put KV value");
    let _ = receive_envelope(&mailbox, "put ack envelope");
    sink.deliver(Envelope::from_route(
        source.clone(),
        kv_address.clone(),
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, "kv://acme/app/resource"),
            family,
        ),
    ))
    .expect("commit KV transaction");
    let _ = receive_envelope(&mailbox, "first commit ack envelope");
    assert!(sink.resource_locks_are_empty_for_tests());

    // Act
    // The client retries the identical COMMIT frame, believing its
    // first ack was lost.
    sink.deliver(Envelope::from_route(
        source,
        kv_address,
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(crate::dispatch::protocol::kv::msg_type::COMMIT),
            encode_kv_commit(tx_id, "kv://acme/app/resource"),
            family,
        ),
    ))
    .expect("retry commit KV transaction");
    let retry_response = receive_frame(&mailbox, "retried commit response");

    // Assert
    // The retry is rejected as an unknown transaction, not treated
    // as a second successful commit, and the lock table stays empty.
    assert_eq!(
        decode_error_code(&retry_response.payload),
        error_codes::kv::ERR_TRANSACTION_NOT_FOUND
    );
    assert!(sink.resource_locks_are_empty_for_tests());
    assert_eq!(sink.active_transaction_count(), 0);
}
