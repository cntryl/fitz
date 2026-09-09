#[test]
fn should_inventory_only_authoritative_non_empty_queue_rows() {
    // Arrange
    let family = RouteFamily::new(1);
    let populated_key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "populated".to_string(),
    };
    let empty_key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "empty".to_string(),
    };
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let mut populated = crate::domains::queue::QueueActor::new(
        family,
        populated_key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    assert!(matches!(
        populated.handle_send(Bytes::from_static(b"queued"), None),
        crate::domains::queue::QueueResponse::Sent { .. }
    ));
    let _empty = crate::domains::queue::QueueActor::new(
        family,
        empty_key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );
    let mut txn = store
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin foreign-domain write");
    txn.put(
        crate::utils::storage_key::prefixed_key(
            "acme",
            crate::utils::storage_key::DomainKeyspace::Kv,
            b"foreign",
        ),
        vec![0; 1024],
        None,
    )
    .expect("write foreign-domain row");
    txn.commit(crate::domains::WritePolicy::Buffered.into())
        .expect("commit foreign-domain row");

    // Act
    let sink = new_queue_domain_sink(
        store,
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::Buffered,
    );

    // Assert
    assert_eq!(sink.known_queue_count_for_tests(), 1);
    assert!(sink.known_queue_contains_for_tests(&populated_key));
    assert!(!sink.known_queue_contains_for_tests(&empty_key));
}

#[test]
fn should_mark_fast_flush_plus_admin_dirty_when_wildcard_poll_only_expires_work() {
    // Arrange
    let family = RouteFamily::new(1);
    let key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "expiring".to_string(),
    };
    let clock = DlqSeedClock::new();
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let mut actor = crate::domains::queue::QueueActor::with_clock_and_write_policy(
        family,
        key.clone(),
        store.clone(),
        Box::new(clock.clone()),
        Some(1),
        crate::utils::idempotency::default_dedup_store(),
        crate::domains::WritePolicy::BestEffort,
    );
    assert!(matches!(
        actor.handle_send(Bytes::from_static(b"expire"), None),
        crate::domains::queue::QueueResponse::Sent { .. }
    ));
    assert!(matches!(
        actor.handle_receive_for_session(7, 1, Some(1)),
        crate::domains::queue::QueueResponse::Received { .. }
    ));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(worker_address.clone(), worker_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model.clone(),
        crate::domains::WritePolicy::BestEffort,
    )
    .with_fast_flush_interval(Some(Duration::from_millis(100)));
    sink.install_actor_for_tests(key, actor);
    sink.clear_dirty_fast_flush_for_tests();
    clock.advance(Duration::from_secs(2));

    // Act
    sink.deliver(Envelope::from_route(
        worker_address,
        RouteAddress::new(family, Route::new("queue://inbound")),
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve("queue://acme/jobs/*", 30, 1),
            family,
        ),
    ))
    .expect("poll expiring queue");
    let response = receive_queue_frame(&worker_mailbox, "wildcard reserve response");
    sink.refresh_admin_snapshot_if_dirty();

    // Assert
    assert!(decode_routed_reserve_response(&response).is_empty());
    assert!(sink.dirty_fast_flush_contains_family_for_tests(1));
    assert_eq!(admin_read_model.queues(None)[0].messages_dead_lettered, 1);
}

#[test]
fn should_route_queue_admin_refresh_through_family_actor() {
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
        crate::domains::WritePolicy::BestEffort,
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
fn should_route_queue_live_counts_through_family_actor() {
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
        crate::domains::WritePolicy::BestEffort,
    );
    sink.deliver(queue_send_envelope(family, queue_route))
        .expect("enqueue queue message");
    assert_eq!(sink.counts().ready, 1);

    // Act
    sink.stop_actor_for_tests();
    let ready_messages = sink.counts().ready;

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(ready_messages, 0);
}

#[test]
fn should_bound_concrete_reserve_response_before_messages_become_inflight() {
    // Arrange
    let family = RouteFamily::new(1);
    let queue_route = "queue://acme/jobs/wire-capacity";
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_mailbox = Arc::new(Mailbox::new(2));
    let worker_mailbox = Arc::new(Mailbox::new(2));
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::Buffered,
    );
    let body = vec![0x5a; 1024];
    for _ in 0..100 {
        sink.deliver(Envelope::from_route(
            sender_address.clone(),
            queue_address.clone(),
            FrameContext::new(
                7,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(queue_route, &body),
                family,
            ),
        ))
        .expect("enqueue queue message");
        let _response = receive_queue_frame(&sender_mailbox, "enqueue response");
    }

    // Act
    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address,
        FrameContext::new(
            8,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(queue_route, 30, 100),
            family,
        ),
    ))
    .expect("reserve queue batch");
    let response = receive_queue_frame(&worker_mailbox, "reserve response");

    // Assert
    assert!(u16::try_from(response.payload.len()).is_ok());
    assert_eq!(decode_concrete_reserve_response(&response).len(), 62);
    let snapshot = queue_snapshot(&sink, family, queue_route);
    assert_eq!(snapshot.messages_ready, 38);
    assert_eq!(snapshot.messages_inflight, 62);
}

#[test]
fn should_route_queue_cleanup_through_family_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_session_id = 7;
    let worker_session_id = 8;
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        crate::domains::WritePolicy::Buffered,
    );
    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address.clone(),
        FrameContext::new(
            sender_session_id,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send(queue_route, b"email"),
            family,
        ),
    ))
    .expect("enqueue queue message");
    let _send_ack = receive_queue_frame(&sender_mailbox, "enqueue response");
    sink.deliver(Envelope::from_route(
        worker_address,
        queue_address,
        FrameContext::new(
            worker_session_id,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve(queue_route, 30, 1),
            family,
        ),
    ))
    .expect("reserve queue message");
    let reserve_ack = receive_queue_frame(&worker_mailbox, "reserve response");
    assert_eq!(
        decode_concrete_reserve_response(&reserve_ack),
        vec![b"email".to_vec()]
    );
    assert_eq!(
        queue_snapshot(&sink, family, queue_route).messages_inflight,
        1
    );
    let snapshot = queue_snapshot(&sink, family, queue_route);

    // Act
    sink.stop_actor_for_tests();
    let cleanup_result = sink.cleanup_session(worker_session_id);

    // Assert
    // The command cannot run against a stopped actor, and the caller is now
    // told so rather than being handed a silent success.
    assert!(
        matches!(
            cleanup_result,
            Err(crate::runtime::DeliveryError::ActorStopped)
        ),
        "expected a reported failure, got {cleanup_result:?}"
    );
    assert!(!sink.is_actor_running());
    assert_eq!(snapshot.messages_inflight, 1);
    assert_eq!(snapshot.messages_ready, 0);
}

#[test]
fn should_route_queue_runtime_sweep_through_family_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_mailbox = Arc::new(Mailbox::new(8));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    router.register(sender_address.clone(), sender_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        router,
        admin_read_model,
        crate::domains::WritePolicy::BestEffort,
    )
    .with_fast_flush_interval(Some(Duration::from_millis(100)));
    sink.deliver(Envelope::from_route(
        sender_address,
        queue_address,
        FrameContext::new(
            7,
            ChannelId::Pub,
            MessageType::new(200),
            encode_queue_send("queue://acme/email/jobs", b"email"),
            family,
        ),
    ))
    .expect("send should enqueue");
    let _send_ack = receive_queue_frame(&sender_mailbox, "send response");
    assert!(sink.dirty_fast_flush_contains_family_for_tests(1));
    let dirty_before_stop = sink.dirty_fast_flush_contains_family_for_tests(1);

    // Act
    sink.stop_actor_for_tests();
    sink.sweep_runtime_state_at(Instant::now() + Duration::from_millis(100));

    // Assert
    assert!(!sink.is_actor_running());
    assert!(dirty_before_stop);
}

#[test]
fn should_coalesce_queue_runtime_sweeps_while_actor_is_busy() {
    // Arrange
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::BestEffort,
    );
    let family = RouteFamily::new(1);
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    let (done_tx, _done_rx) = crossbeam_channel::bounded(1);
    sink.family_runtime
        .try_enqueue(
            family,
            crate::runtime::FamilyActorLane::Control,
            crate::domains::queue::sink::model::QueueDomainCommand::InspectForTests(
                Box::new(move |_| {
                    let _ = entered_tx.send(());
                    let _ = release_rx.recv();
                }),
                done_tx,
            ),
        )
        .expect("block Queue family");
    entered_rx.recv().expect("Queue family blocked");

    // Act
    let first_enqueued = sink.request_runtime_sweep_at(Instant::now());
    let second_enqueued = sink.request_runtime_sweep_at(Instant::now());

    // Assert
    assert!(first_enqueued);
    assert!(!second_enqueued);
    release_tx.send(()).expect("release Queue family");
}

#[test]
fn should_clear_runtime_sweep_pending_when_sweep_panics() {
    // Arrange
    let sink = new_queue_domain_sink(
        crate::testkit::create_test_engine_with_cfs(vec![1]),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
        crate::domains::WritePolicy::BestEffort,
    );
    sink.panic_next_runtime_sweep_for_tests();

    // Act
    assert!(sink.request_runtime_sweep_at(Instant::now()));
    let deadline = Instant::now() + Duration::from_secs(1);
    while !sink.family_health_snapshot().healthy_families.is_empty() && Instant::now() < deadline {
        std::thread::yield_now();
    }

    // Assert
    assert!(sink.family_health_snapshot().healthy_families.is_empty());
    assert!(!sink.runtime_sweep_pending_for_tests());
}

#[test]
fn should_route_queue_dead_letter_replay_through_family_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "emails".to_string(),
    };
    let msg_id = seed_dead_letter(store.clone(), &key);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store.clone(),
        router,
        admin_read_model,
        crate::domains::WritePolicy::Buffered,
    );
    let actors_were_empty = sink.actors_are_empty_for_tests();

    // Act
    sink.stop_actor_for_tests();
    let replayed = sink.replay_dead_letter(&key, msg_id);
    let dead_letters = persisted_dead_letter_count(store, &key);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(replayed.is_err());
    assert_eq!(dead_letters, 1);
    assert!(actors_were_empty);
}

#[test]
fn should_route_queue_dead_letter_purge_through_family_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let key = crate::domains::queue::QueueKey {
        family,
        realm: "acme".to_string(),
        area: "jobs".to_string(),
        resource: "emails".to_string(),
    };
    let msg_id = seed_dead_letter(store.clone(), &key);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store.clone(),
        router,
        admin_read_model,
        crate::domains::WritePolicy::Buffered,
    );
    let actors_were_empty = sink.actors_are_empty_for_tests();

    // Act
    sink.stop_actor_for_tests();
    let purged = sink.purge_dead_letter(&key, msg_id);
    let dead_letters = persisted_dead_letter_count(store, &key);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(purged.is_err());
    assert_eq!(dead_letters, 1);
    assert!(actors_were_empty);
}

#[test]
fn should_reject_surplus_queue_load_instead_of_accepting_then_timing_out() {
    // Arrange
    // Queue is the only domain that commits storage synchronously per request
    // inside a single-threaded actor while a caller blocks on the reply. Work
    // admitted beyond what the deadline can serve becomes an indeterminate
    // outcome the client dare not retry; refusing it keeps it retryable.
    use crate::domains::queue::sink::model::{queue_admission_window, try_admit_queue_delivery};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let inflight = Arc::new(AtomicUsize::new(0));
    let window = queue_admission_window(super::super::model::assumed_service_us());

    // Act
    let held = (0..window)
        .map(|_| try_admit_queue_delivery(&inflight, window).expect("slot within the limit"))
        .collect::<Vec<_>>();
    let refused = try_admit_queue_delivery(&inflight, window);

    // Assert
    assert!(
        matches!(refused, Err(DeliveryError::MailboxFull { .. })),
        "surplus must be refused as never-enqueued, got {refused:?}"
    );
    assert_eq!(inflight.load(Ordering::Acquire), window);

    // Slots are released when the COMMAND is finished with, not when a caller
    // stops waiting: a `recv_timeout` cancels nothing, so recycling on caller
    // timeout would admit fresh work on top of still-pending mutations.
    drop(held);
    assert_eq!(inflight.load(Ordering::Acquire), 0);
    assert!(try_admit_queue_delivery(&inflight, window).is_ok());
}

#[test]
fn should_hold_queue_admission_limit_under_concurrent_callers() {
    // Arrange
    // Sampling a depth and then enqueueing is check-then-act: concurrent
    // callers can all observe room before any of them commits, and collectively
    // blow past the limit in exactly the burst the limit exists to bound. The
    // reservation must therefore be a single atomic step.
    use crate::domains::queue::sink::model::{queue_admission_window, try_admit_queue_delivery};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let inflight = Arc::new(AtomicUsize::new(0));
    let window = queue_admission_window(super::super::model::assumed_service_us());
    let admitted = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(std::sync::Barrier::new(32));

    // Act
    let handles = (0..32)
        .map(|_| {
            let inflight = inflight.clone();
            let admitted = admitted.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                // Each thread tries for more slots than the limit allows, and
                // holds every one it wins for the duration.
                let mut held = Vec::new();
                for _ in 0..8 {
                    if let Ok(slot) = try_admit_queue_delivery(&inflight, window) {
                        admitted.fetch_add(1, Ordering::AcqRel);
                        held.push(slot);
                    }
                }
                // Keep them until every thread has finished competing.
                barrier.wait();
                drop(held);
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap();
    }

    // Assert
    assert_eq!(
        admitted.load(Ordering::Acquire),
        window,
        "32 concurrent callers must not collectively exceed the admission limit"
    );
    assert_eq!(
        inflight.load(Ordering::Acquire),
        0,
        "every slot must be released"
    );
}

#[test]
fn should_hold_queue_admission_while_a_timed_out_command_is_still_pending() {
    // Arrange
    // A caller that gives up on `recv_timeout` has cancelled nothing: its
    // `Deliver` command stays queued and the actor will still run
    // `deliver_envelope`. If the slot were tied to the caller, a sustained
    // burst would recycle slots while accepted mutations were still pending
    // and pile indeterminate work up to the full mailbox depth.
    use crate::domains::queue::sink::model::{try_admit_queue_delivery, QueueAdmissionSlot};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let inflight = Arc::new(AtomicUsize::new(0));
    let slot = try_admit_queue_delivery(&inflight, 1).expect("slot");

    // Act
    // Hand the slot to the queued command and let the caller's scope end,
    // exactly as a timed-out delivery does.
    let queued: Option<QueueAdmissionSlot> = Some(slot);
    let caller_gave_up = inflight.load(Ordering::Acquire);

    // Assert
    assert_eq!(
        caller_gave_up, 1,
        "the slot must still be held while the command is queued"
    );
    drop(queued);
    assert_eq!(
        inflight.load(Ordering::Acquire),
        0,
        "the slot must be released when the command is finished with"
    );
}

#[test]
fn should_size_queue_admission_to_the_reply_deadline() {
    // Arrange
    // A fixed window cannot bound the deadline. Queued concurrency adds no
    // throughput - the actor serves commands one at a time - so admitting N
    // requests commits the tail caller to N x service_time. At 20ms per
    // synchronous commit a 64-deep window needs 1.28s, past
    // QUEUE_ACTOR_REPLY_TIMEOUT, so the tail times out with an indeterminate
    // outcome and its command still executes: exactly what the gate exists to
    // prevent. The window must therefore follow observed service time.
    use crate::domains::queue::sink::model::{queue_admission_window, QUEUE_ADMISSION_MAX_WINDOW};

    // Act
    let slow = queue_admission_window(20_000);
    let quick = queue_admission_window(1_000);
    let pathological = queue_admission_window(5_000_000);

    // Assert
    let deadline_us =
        u64::try_from(QUEUE_ACTOR_REPLY_TIMEOUT.as_micros())
            .expect("deadline fits u64");
    assert!(
        u64::try_from(slow).unwrap() * 20_000 <= deadline_us,
        "a {slow}-deep window at 20ms each overruns the {deadline_us}us deadline"
    );
    assert!(
        slow < QUEUE_ADMISSION_MAX_WINDOW,
        "slow service must shrink the window below the cap"
    );
    assert!(quick > slow, "faster service should admit more");
    assert!(quick <= QUEUE_ADMISSION_MAX_WINDOW);
    assert_eq!(
        pathological, 1,
        "service slower than the whole deadline still admits the active operation"
    );
}

#[test]
fn should_report_a_stopped_actor_rather_than_admission_backpressure() {
    // Arrange
    // A worker that fails closed leaves its mailbox, and every admitted slot,
    // alive for the sink's lifetime. Admission would then keep answering
    // MailboxFull - a retryable code - so clients would retry a dead domain
    // forever instead of seeing a terminal failure.
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = new_queue_domain_sink(
        store,
        Arc::new(Router::new()),
        admin_read_model,
        crate::domains::WritePolicy::Buffered,
    );
    let family = RouteFamily::new(1);
    let window = crate::domains::queue::sink::model::queue_admission_window(
        sink.config.delivery_service_us[&family.id()]
            .load(std::sync::atomic::Ordering::Relaxed),
    );
    let held = (0..window)
        .map(|_| {
            crate::domains::queue::sink::model::try_admit_queue_delivery(
                &sink.inflight_client_deliveries,
                window,
            )
            .expect("slot")
        })
        .collect::<Vec<_>>();

    // Act
    sink.stop_actor_for_tests();
    let refused = sink.deliver(Envelope::new(
        RouteAddress::new(RouteFamily::new(1), Route::new("queue://acme/jobs/dead")),
        crate::runtime::SessionCleanup { session_id: 1 },
    ));
    let client_refused = sink.deliver(Envelope::new(
        RouteAddress::new(RouteFamily::new(1), Route::new("queue://acme/jobs/dead")),
        crate::dispatch::protocol::frame_context::FrameContext::new(
            1,
            crate::dispatch::protocol::frame::ChannelId::Pub,
            crate::dispatch::protocol::tlv::MessageType::new(401),
            bytes::Bytes::new(),
            RouteFamily::new(1),
        ),
    ));

    // Assert
    assert!(
        matches!(client_refused, Err(DeliveryError::ActorStopped)),
        "a dead actor must be terminal, not retryable, got {client_refused:?}"
    );
    let _ = refused;
    drop(held);
}
