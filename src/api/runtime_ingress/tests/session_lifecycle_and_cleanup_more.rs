#[test]
pub(super) fn should_keep_session_cleanup_order_aligned_with_domain_manifest() {
    // Arrange
    let mut cleanup_domains = DispatchDomain::SESSION_CLEANUP_ORDER
        .iter()
        .map(|domain| domain.as_str())
        .collect::<Vec<_>>();
    let mut registered_domains = DispatchDomain::ALL
        .iter()
        .map(|domain| domain.as_str())
        .collect::<Vec<_>>();

    // Act
    cleanup_domains.sort_unstable();
    registered_domains.sort_unstable();

    // Assert
    assert_eq!(cleanup_domains, registered_domains);
}

#[tokio::test]
async fn should_cleanup_registered_domains_and_remove_session_state_on_close() {
    // Arrange
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let ingress = make_cleanup_ingress(router.clone(), admin_read_model.clone());
    let session_id = 77;
    let mut session = make_session_info(session_id, TransportKind::WebSocket);
    session.route_family = RouteFamily::new(77);

    let sinks = DispatchDomain::SESSION_CLEANUP_ORDER
        .iter()
        .copied()
        .map(|domain| {
            let sink = Arc::new(CleanupTrackingSink::default());
            router.register_domain_pattern(domain.as_str(), sink.clone());
            sink
        })
        .collect::<Vec<_>>();

    ingress.on_open(session).await.unwrap();
    assert_eq!(admin_read_model.sessions().len(), 1);

    // Act
    ingress.on_close(session_id, CloseReason::ClientClose).await;

    // Assert
    assert_eq!(ingress.session_count(), 0);
    assert!(ingress.get_session(session_id).is_none());
    assert!(ingress.get_session_actor(session_id).is_none());
    assert!(admin_read_model.sessions().is_empty());
    for sink in sinks {
        assert_eq!(sink.recorded_sessions(), vec![session_id]);
    }
}

#[tokio::test]
async fn should_close_active_sessions_concurrently_and_reject_late_opens() {
    // Arrange
    let router = Arc::new(crate::runtime::Router::new());
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(2);
    let (release_tx, release_rx) = crossbeam_channel::bounded(2);
    let sink = Arc::new(BlockingSessionCleanupSink {
        entered: entered_tx,
        release: release_rx,
        blocked_sessions: Mutex::new(std::collections::HashSet::new()),
    });
    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        router.register_domain_pattern(domain.as_str(), sink.clone());
    }
    let ingress = Arc::new(make_cleanup_ingress(router, AdminReadModel::new()));
    for session_id in [101, 102] {
        ingress
            .on_open(make_session_info(session_id, TransportKind::WebSocket))
            .await
            .expect("open session");
    }

    // Act
    let closing_ingress = ingress.clone();
    let close = tokio::spawn(async move {
        closing_ingress
            .close_all_sessions(CloseReason::ServerClose("test shutdown".to_string()))
            .await;
    });
    let first = tokio::task::spawn_blocking(move || {
        let first = entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first cleanup should start");
        let second = entered_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("second cleanup should start before the first completes");
        [first, second]
    })
    .await;
    let late_open = ingress
        .on_open(make_session_info(103, TransportKind::WebSocket))
        .await;
    release_tx.send(()).expect("release first cleanup");
    release_tx.send(()).expect("release second cleanup");
    let mut entered = first.expect("join cleanup entry wait");
    close.await.expect("join session close");

    // Assert
    entered.sort_unstable();
    assert_eq!(entered, [101, 102]);
    assert_eq!(late_open, Err("broker is shutting down".to_string()));
    assert_eq!(ingress.session_count(), 0);
}

#[tokio::test]
async fn should_bound_concurrent_session_closes_during_shutdown() {
    // Arrange
    const SESSION_COUNT: usize = 40;
    const CLOSE_CONCURRENCY: usize = 32;
    let router = Arc::new(crate::runtime::Router::new());
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(SESSION_COUNT);
    let (release_tx, release_rx) = crossbeam_channel::bounded(SESSION_COUNT);
    let sink = Arc::new(BlockingSessionCleanupSink {
        entered: entered_tx,
        release: release_rx,
        blocked_sessions: Mutex::new(std::collections::HashSet::new()),
    });
    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        router.register_domain_pattern(domain.as_str(), sink.clone());
    }
    let ingress = Arc::new(make_cleanup_ingress(router, AdminReadModel::new()));
    for session_id in 1..=SESSION_COUNT {
        ingress
            .on_open(make_session_info(
                u64::try_from(session_id).expect("test session id fits u64"),
                TransportKind::Tcp,
            ))
            .await
            .expect("open session");
    }

    // Act
    let closing_ingress = ingress.clone();
    let close = tokio::spawn(async move {
        closing_ingress
            .close_all_sessions(CloseReason::ServerClose("test shutdown".to_string()))
            .await;
    });
    let observation = tokio::task::spawn_blocking(move || {
        let mut entered = 0usize;
        while entered < CLOSE_CONCURRENCY {
            entered_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("bounded cleanup should start");
            entered += 1;
        }
        let overflow = entered_rx.recv_timeout(Duration::from_millis(100));
        if overflow.is_ok() {
            entered += 1;
        }
        for _ in 0..entered {
            release_tx.send(()).expect("release initial cleanup");
        }
        while entered < SESSION_COUNT {
            entered_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("remaining cleanup should start");
            entered += 1;
            release_tx.send(()).expect("release remaining cleanup");
        }
        overflow
    })
    .await
    .expect("join cleanup concurrency observation");
    close.await.expect("join session close");

    // Assert
    assert!(observation.is_err(), "more than 32 cleanups ran at once");
    assert_eq!(ingress.session_count(), 0);
}

#[tokio::test]
async fn should_not_retain_closed_session_ids_after_connection_churn() {
    // Arrange
    let ingress = RuntimeIngress::new(false);

    // Act
    for session_id in 1..=100 {
        ingress
            .on_open(make_session_info(session_id, TransportKind::WebSocket))
            .await
            .expect("open session");
        ingress.on_close(session_id, CloseReason::ClientClose).await;
    }

    // Assert
    assert_eq!(ingress.session_count(), 0);
    assert!(
        ingress.registry.closing_sessions.is_empty(),
        "closed session ids must not accumulate after cleanup"
    );
}

#[tokio::test]
async fn should_record_cleanup_failures_when_on_close_cannot_reach_all_domains() {
    // Arrange
    let collector = crate::observability::metrics();

    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let ingress = make_cleanup_ingress(router.clone(), admin_read_model.clone());
    let session_id = 88;
    let mut session = make_session_info(session_id, TransportKind::Tcp);
    session.route_family = RouteFamily::new(88);

    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        if domain == DispatchDomain::Queue {
            continue;
        }

        let sink = Arc::new(CleanupTrackingSink::default());
        router.register_domain_pattern(domain.as_str(), sink);
    }

    ingress.on_open(session).await.unwrap();
    let failures_before = collector.counter_get(obs::METRIC_SESSION_CLEANUP_FAILURES);

    // Act
    ingress.on_close(session_id, CloseReason::ClientClose).await;

    // Assert
    assert!(
        collector.counter_get(obs::METRIC_SESSION_CLEANUP_FAILURES) > failures_before,
        "expected cleanup failure metric to increase"
    );
    assert_eq!(ingress.session_count(), 0);
    assert!(ingress.get_session(session_id).is_none());
    assert!(ingress.get_session_actor(session_id).is_none());
    assert!(admin_read_model.sessions().is_empty());
    assert!(ingress.cleanup.pending_session_cleanups.contains_key(&session_id));
}

#[tokio::test]
async fn should_retry_pending_session_cleanup_without_later_traffic() {
    // Arrange
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let ingress = make_cleanup_ingress(router.clone(), admin_read_model);
    let session_id = 89;
    let mut session = make_session_info(session_id, TransportKind::Tcp);
    session.route_family = RouteFamily::new(89);

    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        if domain == DispatchDomain::Queue {
            continue;
        }

        let sink = Arc::new(CleanupTrackingSink::default());
        router.register_domain_pattern(domain.as_str(), sink);
    }

    ingress.on_open(session).await.unwrap();
    ingress.on_close(session_id, CloseReason::ClientClose).await;
    assert!(ingress.cleanup.pending_session_cleanups.contains_key(&session_id));

    let queue_sink = Arc::new(CleanupTrackingSink::default());
    router.register_domain_pattern(DispatchDomain::Queue.as_str(), queue_sink.clone());

    // Act
    tokio::time::timeout(Duration::from_secs(1), async {
        while ingress.cleanup.pending_session_cleanups.contains_key(&session_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cleanup worker should retry without later traffic");

    // Assert
    assert!(!ingress.cleanup.pending_session_cleanups.contains_key(&session_id));
    assert_eq!(queue_sink.recorded_sessions(), vec![session_id]);
}

#[tokio::test]
async fn should_give_up_and_stop_retrying_session_cleanup_that_can_never_succeed() {
    // Arrange: never register Queue's sink, so its cleanup can never
    // succeed. Without a give-up threshold, the retry worker would keep
    // this ticket pending forever (capped-but-endless exponential backoff),
    // so the pending gauge would never return to zero for a genuinely dead
    // domain actor.
    let collector = crate::observability::metrics();
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let ingress = make_cleanup_ingress(router.clone(), admin_read_model);
    let session_id = 90;
    let mut session = make_session_info(session_id, TransportKind::Tcp);
    session.route_family = RouteFamily::new(90);

    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        if domain == DispatchDomain::Queue {
            continue;
        }
        let sink = Arc::new(CleanupTrackingSink::default());
        router.register_domain_pattern(domain.as_str(), sink);
    }

    ingress.on_open(session).await.unwrap();
    ingress.on_close(session_id, CloseReason::ClientClose).await;
    assert!(ingress.cleanup.pending_session_cleanups.contains_key(&session_id));
    let permanent_failures_before =
        collector.counter_get(obs::METRIC_SESSION_CLEANUP_PERMANENT_FAILURES);

    // Act: Queue's sink is intentionally never registered, so this ticket
    // can never succeed - the worker must eventually give up.
    tokio::time::timeout(Duration::from_secs(10), async {
        while ingress.cleanup.pending_session_cleanups.contains_key(&session_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cleanup worker should give up instead of retrying forever");

    // Assert
    assert!(!ingress.cleanup.pending_session_cleanups.contains_key(&session_id));
    assert!(
        collector.counter_get(obs::METRIC_SESSION_CLEANUP_PERMANENT_FAILURES)
            > permanent_failures_before,
        "expected a permanent-failure metric increment"
    );
    assert_eq!(
        collector.gauge_get(obs::METRIC_SESSION_CLEANUP_PENDING),
        0,
        "pending gauge should return to zero after giving up"
    );
}

#[tokio::test]
async fn should_cleanup_real_notice_domain_subscription_on_close() {
    // Arrange
    let family = RouteFamily::new(91);
    let session_id = 91;
    let notice_route = "notice://acme/app/events";
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/91"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let notice_address = RouteAddress::new(family, Route::new(notice_route));

    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let notice_sink = Arc::new(NoticeDomain::new_with_families(
        router.clone(),
        admin_read_model.clone(),
        &[family],
    ));
    let subscriber_mailbox = Arc::new(Mailbox::new(8));

    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    router.register_domain_pattern("notice", notice_sink.clone());
    register_fallback_cleanup_domains(&router, DispatchDomain::Notice);

    let ingress = make_cleanup_ingress(router, admin_read_model.clone());
    let mut session = make_session_info(session_id, TransportKind::WebSocket);
    session.route_family = family;
    ingress.on_open(session).await.unwrap();

    notice_sink
        .deliver(Envelope::from_route(
            subscriber_address,
            notice_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(501),
                encode_notice_subscribe(notice_route),
                family,
            ),
        ))
        .expect("subscribe notice route");

    let _subscribe_response = subscriber_mailbox
        .receiver()
        .try_recv()
        .expect("notice subscribe response");
    notice_sink.refresh_admin_snapshot_if_dirty();
    assert_eq!(notice_sink.subscription_count(), Ok(1));
    assert_eq!(admin_read_model.notice_subscriptions(None, None).len(), 1);

    // Act
    ingress.on_close(session_id, CloseReason::ClientClose).await;
    notice_sink.refresh_admin_snapshot_if_dirty();
    notice_sink
        .deliver(Envelope::from_route(
            publisher_address,
            notice_address,
            FrameContext::new(
                11,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(notice_route, b"hello"),
                family,
            ),
        ))
        .expect("publish after cleanup");

    // Assert
    assert_eq!(notice_sink.subscription_count(), Ok(0));
    assert!(admin_read_model.notice_subscriptions(None, None).is_empty());
    assert!(subscriber_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_cleanup_real_queue_inflight_on_close() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_session_id = 7;
    let worker_session_id = 92;
    let next_worker_session_id = 12;
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/92"));
    let next_worker_address = RouteAddress::new(family, Route::new("inbox://session/12"));

    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let queue_sink = Arc::new(QueueDomain::new(
        store,
        router.clone(),
        admin_read_model.clone(),
        crate::domains::WritePolicy::Buffered,
        crate::utils::idempotency::default_dedup_store(),
    ));

    let sender_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let next_worker_mailbox = Arc::new(Mailbox::new(8));

    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    router.register(next_worker_address.clone(), next_worker_mailbox.clone());
    router.register_domain_pattern("queue", queue_sink.clone());
    register_fallback_cleanup_domains(&router, DispatchDomain::Queue);

    let ingress = make_cleanup_ingress(router, admin_read_model.clone());
    let mut worker_session = make_session_info(worker_session_id, TransportKind::WebSocket);
    worker_session.route_family = family;
    ingress.on_open(worker_session).await.unwrap();

    queue_sink
        .deliver(Envelope::from_route(
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
    let _send_ack = sender_mailbox
        .receiver()
        .try_recv()
        .expect("enqueue response");

    queue_sink
        .deliver(Envelope::from_route(
            worker_address,
            queue_address.clone(),
            FrameContext::new(
                worker_session_id,
                ChannelId::Pub,
                MessageType::new(202),
                encode_queue_reserve(queue_route, 30, 1),
                family,
            ),
        ))
        .expect("reserve queue message");
    let _reserve_ack = worker_mailbox
        .receiver()
        .try_recv()
        .expect("reserve response");

    queue_sink.refresh_admin_snapshot_if_dirty();
    assert_eq!(admin_read_model.queue_inflight(None).len(), 1);

    // Act
    ingress
        .on_close(worker_session_id, CloseReason::ClientClose)
        .await;
    queue_sink.refresh_admin_snapshot_if_dirty();
    queue_sink
        .deliver(Envelope::from_route(
            next_worker_address,
            queue_address,
            FrameContext::new(
                next_worker_session_id,
                ChannelId::Pub,
                MessageType::new(202),
                encode_queue_reserve(queue_route, 30, 1),
                family,
            ),
        ))
        .expect("reserve queue message after cleanup");
    queue_sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let reserve_after_cleanup = next_worker_mailbox
        .receiver()
        .try_recv()
        .expect("reserve response after cleanup")
        .into_payload::<FrameContext>()
        .expect("reserve response frame after cleanup");
    assert_eq!(
        queue_receive_response_message_count(&reserve_after_cleanup),
        1
    );

    assert_queue_cleanup_admin_state(&admin_read_model, next_worker_session_id);
}

/// Fails cleanup forever for one session, and fails every other session
/// exactly once before succeeding - so the retry worker keeps observing
/// progress on other tickets while the stuck one never advances.
struct StickySessionFailureSink {
    stuck_session_id: u64,
    seen_once: Mutex<std::collections::HashSet<u64>>,
}

impl StickySessionFailureSink {
    fn new(stuck_session_id: u64) -> Self {
        Self {
            stuck_session_id,
            seen_once: Mutex::new(std::collections::HashSet::new()),
        }
    }
}

impl MailboxSink for StickySessionFailureSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let cleanup = envelope
            .payload::<crate::runtime::SessionCleanup>()
            .expect("cleanup payload");
        if cleanup.session_id == self.stuck_session_id {
            // A merely busy actor, not a dead one.
            return Err(DeliveryError::Timeout);
        }
        if self.seen_once.lock().unwrap().insert(cleanup.session_id) {
            return Err(DeliveryError::Timeout);
        }
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[tokio::test]
async fn should_retry_stuck_cleanup_for_full_window_while_other_tickets_progress() {
    // Arrange
    // The give-up threshold is documented as ~2.3s of retrying,
    // derived from an exponential backoff. That backoff is worker-global and
    // is reset to its 10ms floor whenever *any other* ticket succeeds, so
    // under normal session churn a stuck ticket must not be abandoned in a
    // small fraction of the intended window.
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let ingress = Arc::new(make_cleanup_ingress(router.clone(), admin_read_model));
    let stuck_session_id = 9_100;

    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        if domain == DispatchDomain::Queue {
            continue;
        }
        router.register_domain_pattern(domain.as_str(), Arc::new(CleanupTrackingSink::default()));
    }
    router.register_domain_pattern(
        DispatchDomain::Queue.as_str(),
        Arc::new(StickySessionFailureSink::new(stuck_session_id)),
    );

    let mut stuck = make_session_info(stuck_session_id, TransportKind::Tcp);
    stuck.route_family = RouteFamily::new(91);
    ingress.on_open(stuck).await.unwrap();
    ingress
        .on_close(stuck_session_id, CloseReason::ClientClose)
        .await;
    assert!(ingress
        .cleanup
        .pending_session_cleanups
        .contains_key(&stuck_session_id));

    // Act
    // Keep other tickets flowing through the worker so `made_progress`
    // resets the shared backoff on essentially every pass.
    let churn_ingress = ingress.clone();
    let churn = tokio::spawn(async move {
        for index in 0..300_u64 {
            let session_id = 9_200 + index;
            let mut session = make_session_info(session_id, TransportKind::Tcp);
            session.route_family = RouteFamily::new(91);
            churn_ingress.on_open(session).await.unwrap();
            churn_ingress
                .on_close(session_id, CloseReason::ClientClose)
                .await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });

    let started = std::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(15), async {
        while ingress
            .cleanup
            .pending_session_cleanups
            .contains_key(&stuck_session_id)
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("stuck cleanup ticket should eventually be given up on");
    let elapsed = started.elapsed();
    churn.abort();

    // Assert
    assert!(
        elapsed >= Duration::from_secs(1),
        "stuck ticket abandoned after only {elapsed:?}; concurrent progress on other \
         tickets must not collapse its retry window"
    );
}
