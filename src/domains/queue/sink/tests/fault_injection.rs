//! Fault-injection tests for mutate -> send -> (rollback | wake | measure)
//! sequences that cross the sink's router/mailbox boundary: a parked
//! long-poll RESERVE whose session is cleaned up before wake, a stale
//! ACK arriving after `cleanup_session_inflight` already returned the
//! message to ready, a NOTIFY delivery failure against the subscription
//! table, and an admin purge on an unrelated queue racing a parked
//! RESERVE.

use super::routing_watch_and_admin::{
    encode_queue_ack, encode_queue_reserve, encode_queue_reserve_wait, encode_queue_send,
    encode_queue_watch, new_queue_domain_sink, receive_queue_frame, receive_response_first_message,
    watch_response_subscription_id,
};
use super::*;

fn queue_key(
    family: RouteFamily,
    realm: &str,
    area: &str,
    resource: &str,
) -> crate::domains::queue::QueueKey {
    crate::domains::queue::QueueKey {
        family,
        realm: realm.to_string(),
        area: area.to_string(),
        resource: resource.to_string(),
    }
}

#[test]
fn should_not_deliver_woken_reserve_to_a_session_cleaned_up_before_enqueue() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let dead_worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let live_worker_address = RouteAddress::new(family, Route::new("inbox://session/9"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let dead_worker_mailbox = Arc::new(Mailbox::new(8));
    let live_worker_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(dead_worker_address.clone(), dead_worker_mailbox.clone());
    router.register(live_worker_address.clone(), live_worker_mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::Buffered,
    );

    // Act
    // Session 8 long-polls an empty queue and parks.
    sink.deliver(Envelope::from_route(
        dead_worker_address,
        queue_address.clone(),
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve_wait(queue_route, 30, 1, 30),
            family,
        ),
    ))
    .expect("parked reserve");
    assert!(
        dead_worker_mailbox.receiver().try_recv().is_err(),
        "reserve parks while the queue is empty"
    );

    // Session 8 disconnects before any message becomes available.
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("queue://cleanup")),
        crate::runtime::SessionCleanup { session_id: 8 },
    ))
    .expect("cleanup dead session");

    // A message is enqueued on the same route the parked reserve was waiting on.
    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address.clone(),
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send(queue_route, b"email"),
            family,
        ),
    ))
    .expect("enqueue after cleanup");
    let _send_ack = receive_queue_frame(&sender_mailbox, "send response");

    // Assert
    // The cleaned-up session's mailbox never receives a wake, and the
    // message is still available for a live consumer instead of having been
    // silently consumed for a session nobody will ever read from again.
    assert!(
        dead_worker_mailbox.receiver().try_recv().is_err(),
        "a cleaned-up session must never receive a woken reserve reply"
    );

    sink.deliver(Envelope::from_route(
        live_worker_address,
        queue_address,
        FrameContext::new(
            9,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(queue_route, 30, 1),
            family,
        ),
    ))
    .expect("live reserve");
    let reserve_frame = receive_queue_frame(&live_worker_mailbox, "live reserve response");
    let (_id, _token) = receive_response_first_message(&reserve_frame);
}

#[test]
fn should_reject_stale_ack_after_visibility_expiry_already_released_inflight_to_ready() {
    // Arrange
    // A still-connected session's own reservation expires (visibility
    // timeout) and is redelivered to ready before its ACK for that original
    // token arrives - a race identical in shape to `cleanup_session_inflight`
    // releasing the reservation out from under a late ACK, but without the
    // separate "session already closed" ingress guard short-circuiting it.
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let fresh_worker_address = RouteAddress::new(family, Route::new("inbox://session/9"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let fresh_worker_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    router.register(fresh_worker_address.clone(), fresh_worker_mailbox.clone());
    let sink = new_queue_domain_sink(
        store,
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::Buffered,
    );

    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address.clone(),
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send(queue_route, b"email"),
            family,
        ),
    ))
    .expect("seed message");
    let _send_ack = receive_queue_frame(&sender_mailbox, "send response");

    sink.deliver(Envelope::from_route(
        worker_address.clone(),
        queue_address.clone(),
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(queue_route, 1, 1),
            family,
        ),
    ))
    .expect("reserve message");
    let reserve_frame = receive_queue_frame(&worker_mailbox, "reserve response");
    let (id, token) = receive_response_first_message(&reserve_frame);

    // Act
    // Let the 1-second inflight visibility window expire, then send the
    // now-stale ACK. The ACK operation itself drives the actor's due-work
    // processing (redelivery) before authorization runs.
    std::thread::sleep(Duration::from_millis(1_100));

    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address.clone(),
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(204),
            encode_queue_ack(queue_route, id, token),
            family,
        ),
    ))
    .expect("stale ack after visibility expiry");
    let ack_frame = receive_queue_frame(&worker_mailbox, "stale ack response");

    // Assert
    // The stale ACK is rejected rather than silently succeeding or
    // deleting a message a live consumer now owns.
    let mut decoder =
        crate::dispatch::protocol::payload_codec::PayloadDecoder::new(ack_frame.payload.as_ref());
    assert_eq!(
        decoder.get_u8().expect("ack status"),
        1,
        "stale ack must fail"
    );
    assert_eq!(decoder.get_string().expect("ack error"), "NotFound");

    // The message must still exist exactly once: a fresh reserve can pick it
    // up with a new token.
    sink.deliver(Envelope::from_route(
        fresh_worker_address,
        queue_address,
        FrameContext::new(
            9,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(queue_route, 30, 1),
            family,
        ),
    ))
    .expect("fresh reserve after stale ack");
    let fresh_reserve_frame = receive_queue_frame(&fresh_worker_mailbox, "fresh reserve response");
    let (fresh_id, fresh_token) = receive_response_first_message(&fresh_reserve_frame);
    assert_eq!(
        fresh_id, id,
        "the single seeded message must be reserved exactly once"
    );
    assert_ne!(fresh_token, token, "redelivery must mint a fresh token");
}

#[test]
fn should_retain_subscription_when_notify_delivery_fails() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let watcher_address = RouteAddress::new(family, Route::new("inbox://session/1"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/2"));
    let watcher_mailbox = Arc::new(Mailbox::new(1));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(watcher_address.clone(), watcher_mailbox.clone());
    router.register(sender_address.clone(), sender_mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::BestEffort,
    );

    sink.deliver(Envelope::from_route(
        watcher_address.clone(),
        queue_address.clone(),
        FrameContext::new(
            1,
            ChannelId::Pub,
            MessageType::new(207),
            encode_queue_watch("queue://acme/jobs/*"),
            family,
        ),
    ))
    .expect("watch readiness");
    let watch_frame = receive_queue_frame(&watcher_mailbox, "watch response");
    let _subscription_id = watch_response_subscription_id(&watch_frame);

    // Fill the watcher's mailbox so the upcoming NOTIFY cannot be delivered.
    watcher_mailbox
        .sender()
        .try_send(Envelope::new(watcher_address.clone(), 1_u8))
        .expect("fill watcher mailbox");

    // Act
    // Enqueue makes the queue ready, which should attempt (and fail) a
    // NOTIFY to the watcher.
    sink.deliver(Envelope::from_route(
        sender_address.clone(),
        queue_address.clone(),
        FrameContext::new(
            2,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send(queue_route, b"first"),
            family,
        ),
    ))
    .expect("first enqueue");
    let _first_send_ack = receive_queue_frame(&sender_mailbox, "first send response");

    // Drain the filler so the mailbox has room again, then confirm the
    // subscription table still holds this watcher rather than having
    // dropped it after the failed NOTIFY.
    let _filler = watcher_mailbox.receiver().try_recv().expect("drain filler");

    // A second queue in the same watched pattern becoming ready must still
    // reach the watcher: this only happens if the earlier failed NOTIFY did
    // not remove the subscription.
    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address,
        FrameContext::new(
            2,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send("queue://acme/jobs/other", b"second"),
            family,
        ),
    ))
    .expect("second enqueue");
    let _second_send_ack = receive_queue_frame(&sender_mailbox, "second send response");

    // Assert
    let notify_frame = receive_queue_frame(&watcher_mailbox, "notify after subscription retained");
    assert_eq!(notify_frame.msg_type.as_u16(), 209);
}

#[test]
fn should_not_wake_pending_reserve_when_an_unrelated_dead_letter_is_purged() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let dlq_key = queue_key(family, "acme", "jobs", "dlq-source");
    let waiting_route = "queue://acme/jobs/waiting";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let waiter_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let waiter_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(waiter_address.clone(), waiter_mailbox.clone());

    // Seed a dead letter on a different queue by exhausting attempts on a
    // short inflight window with a real clock.
    let msg_id = {
        let mut actor = crate::domains::queue::QueueActor::new(
            family,
            dlq_key.clone(),
            store.clone(),
            Some(1),
            crate::utils::idempotency::default_dedup_store(),
        );
        let id = match actor.handle_send(bytes::Bytes::from_static(b"doomed"), None) {
            crate::domains::queue::QueueResponse::Sent { id } => id,
            other => panic!("expected send to succeed, found {other:?}"),
        };
        match actor.handle_receive_for_session(1, 1, Some(1)) {
            crate::domains::queue::QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
            }
            other => panic!("expected receive to succeed, found {other:?}"),
        }
        std::thread::sleep(Duration::from_millis(1_100));
        actor.process_expired_timers();
        assert_eq!(
            actor.admin_dead_letters().len(),
            1,
            "message must be dead-lettered"
        );
        id
    };

    let sink = new_queue_domain_sink(
        store,
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::Buffered,
    );

    sink.deliver(Envelope::from_route(
        waiter_address,
        queue_address,
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve_wait(waiting_route, 30, 1, 30),
            family,
        ),
    ))
    .expect("parked reserve on unrelated queue");
    assert!(
        waiter_mailbox.receiver().try_recv().is_err(),
        "reserve parks while its own queue is empty"
    );

    // Act
    let purged = sink.purge_dead_letter(&dlq_key, msg_id);

    // Assert
    // Purging an unrelated dead letter must not wake a parked
    // reserve waiting on a different route.
    assert_eq!(purged, Ok(true));
    assert!(
        waiter_mailbox.receiver().try_recv().is_err(),
        "purge of an unrelated dead letter must not wake this parked reserve"
    );
}
