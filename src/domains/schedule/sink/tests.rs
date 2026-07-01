use super::*;
use crate::domains::schedule::metrics::METRIC_PENDING_CLAIMS_EXPIRED_TOTAL;
use crate::observability::metrics::MetricsCollector;
use crate::protocol::frame::ChannelId;
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::protocol::tlv::MessageType;
use crate::runtime::clock::Clock;
use crate::runtime::mailbox::Mailbox;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[derive(Clone)]
struct MockClock {
    state: Arc<std::sync::Mutex<MockClockState>>,
}

#[derive(Clone, Copy)]
struct MockClockState {
    instant: Instant,
    epoch_ms: u64,
}

impl MockClock {
    fn new(epoch_ms: u64) -> Self {
        Self {
            state: Arc::new(std::sync::Mutex::new(MockClockState {
                instant: Instant::now(),
                epoch_ms,
            })),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut state = self.state.lock().expect("lock mock clock");
        state.instant += duration;
        state.epoch_ms = state
            .epoch_ms
            .saturating_add(u128_to_u64_saturating(duration.as_millis()));
    }
}

impl Clock for MockClock {
    fn now_instant(&self) -> Instant {
        self.state.lock().expect("lock mock clock").instant
    }

    fn now_epoch_ms(&self) -> u64 {
        self.state.lock().expect("lock mock clock").epoch_ms
    }
}

fn encode_schedule_create(route: &str, cron: &str, payload: &[u8]) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_string(cron);
    encoder.put_bytes(payload);
    Bytes::from(encoder.finish())
}

fn encode_schedule_subscribe(route: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    Bytes::from(encoder.finish())
}

fn drain_mailbox(mailbox: &Mailbox) {
    while mailbox.receiver().try_recv().is_ok() {}
}

fn receive_envelope(mailbox: &Mailbox, label: &str) -> crate::runtime::Envelope {
    mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|_| panic!("{label}"))
}

fn assert_no_envelope(mailbox: &Mailbox) {
    assert!(mailbox
        .receiver()
        .recv_timeout(Duration::from_millis(50))
        .is_err());
}

fn wait_for_schedule_count(sink: &ScheduleDomainSink, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if sink.schedule_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(sink.schedule_count(), expected);
}

fn wait_for_subscription_count(sink: &ScheduleDomainSink, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if sink.subscription_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(sink.subscription_count(), expected);
}

fn wait_for_pending_fire_count(sink: &ScheduleDomainSink, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if sink.pending_fire_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(sink.pending_fire_count(), expected);
}

fn wait_for_pending_ack_retry_count(sink: &ScheduleDomainSink, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if sink.pending_ack_retry_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(sink.pending_ack_retry_count(), expected);
}

fn wait_for_notify_failure_count(sink: &ScheduleDomainSink, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if sink.notify_failure_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(sink.notify_failure_count(), expected);
}

fn wait_for_ack_failure_count(sink: &ScheduleDomainSink, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if sink.ack_failure_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(sink.ack_failure_count(), expected);
}

fn wait_for_metric_counter(metrics: &MetricsCollector, name: &str, expected: u64) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if metrics.counter_get(name) == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(metrics.counter_get(name), expected);
}

#[test]
fn should_create_schedule_domain_sink() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = ScheduleDomainSink::new(store, router, admin_read_model);

    // Assert
    assert!(sink.state.active.load(Ordering::Relaxed));
    assert!(sink.is_actor_running());
}

#[test]
fn should_publish_schedule_notify_to_subscribers_when_due() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let schedule_route = "schedule://acme/jobs/nightly/run";
    let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(ScheduleDomainSink::new(
        store,
        router.clone(),
        admin_read_model,
    ));
    router.register_domain_pattern("schedule", sink.clone());

    let create_ctx = FrameContext::new(
        session_id,
        ChannelId::Sub,
        MessageType::new(700),
        encode_schedule_create(schedule_route, "* * * * *", b"nightly"),
        family,
    );
    let subscribe_ctx = FrameContext::new(
        session_id,
        ChannelId::Sub,
        MessageType::new(703),
        encode_schedule_subscribe(schedule_route),
        family,
    );

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        schedule_address.clone(),
        create_ctx,
    ))
    .expect("create schedule");
    let _create_ack = receive_envelope(&subscriber_mailbox, "create ack envelope");
    wait_for_schedule_count(&sink, 1);

    // Act
    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        schedule_address,
        subscribe_ctx,
    ))
    .expect("subscribe schedule");
    let subscribe_envelope = receive_envelope(&subscriber_mailbox, "subscribe ack envelope");
    let subscribe_frame = subscribe_envelope
        .into_payload::<FrameContext>()
        .expect("subscribe ack frame");
    let mut subscribe_decoder = PayloadDecoder::new(&subscribe_frame.payload);
    let _subscribe_status = subscribe_decoder.get_u8().expect("subscribe status");
    let subscription_id = subscribe_decoder
        .get_optional_u64()
        .expect("subscription id")
        .expect("subscription id present");
    wait_for_subscription_count(&sink, 1);

    {
        let mut actors = sink.state.core.actors.lock();
        let actor = actors.get_mut(&family).expect("schedule actor");
        actor.bench_prepare_scan(1);
    }

    sink.scan_due_schedules();

    // Assert
    let notify_envelope = receive_envelope(&subscriber_mailbox, "schedule notify envelope");
    let notify_frame = notify_envelope
        .into_payload::<FrameContext>()
        .expect("schedule notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 705);

    let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
    let notified_subscription_id = notify_decoder.get_u64().expect("notify subscription id");
    let notified_payload = notify_decoder.get_bytes().expect("notify payload");

    assert_eq!(notified_subscription_id, subscription_id);
    assert_eq!(notified_payload.as_ref(), b"nightly");
    assert!(notify_decoder.is_complete());
}

#[test]
fn should_retry_pending_claim_after_restart_given_initial_live_publish_failure() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let schedule_route = "schedule://acme/jobs/replay/run";
    let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let initial_sink = Arc::new(ScheduleDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model.clone(),
    ));

    initial_sink
        .deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(700),
                encode_schedule_create(schedule_route, "* * * * *", b"replay"),
                family,
            ),
        ))
        .expect("create schedule");
    wait_for_schedule_count(&initial_sink, 1);

    {
        let mut actors = initial_sink.state.core.actors.lock();
        let actor = actors.get_mut(&family).expect("schedule actor");
        actor.bench_prepare_scan(1);
    }

    // Act
    initial_sink.scan_due_schedules();
    wait_for_pending_fire_count(&initial_sink, 1);

    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let restarted_sink = Arc::new(ScheduleDomainSink::new(
        store,
        router.clone(),
        admin_read_model,
    ));
    router.register_domain_pattern("schedule", restarted_sink.clone());
    restarted_sink
        .preload_persisted_families()
        .expect("preload persisted families");

    restarted_sink
        .deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(schedule_route),
                family,
            ),
        ))
        .expect("subscribe schedule");
    let subscribe_envelope = receive_envelope(&subscriber_mailbox, "subscribe ack envelope");
    let subscribe_frame = subscribe_envelope
        .into_payload::<FrameContext>()
        .expect("subscribe ack frame");
    let mut subscribe_decoder = PayloadDecoder::new(&subscribe_frame.payload);
    let _subscribe_status = subscribe_decoder.get_u8().expect("subscribe status");
    let subscription_id = subscribe_decoder
        .get_optional_u64()
        .expect("subscription id")
        .expect("subscription id present");

    restarted_sink.scan_due_schedules();

    // Assert
    let notify_envelope = receive_envelope(&subscriber_mailbox, "schedule notify envelope");
    let notify_frame = notify_envelope
        .into_payload::<FrameContext>()
        .expect("schedule notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 705);

    let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
    let notified_subscription_id = notify_decoder.get_u64().expect("notify subscription id");
    let notified_payload = notify_decoder.get_bytes().expect("notify payload");

    assert_eq!(notified_subscription_id, subscription_id);
    assert_eq!(notified_payload.as_ref(), b"replay");
    assert!(notify_decoder.is_complete());

    restarted_sink.scan_due_schedules();
    wait_for_pending_fire_count(&restarted_sink, 0);
    assert_no_envelope(&subscriber_mailbox);
}

#[test]
fn should_retry_ack_without_republishing_given_same_broker_ack_persist_failure() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let schedule_route = "schedule://acme/jobs/retry/run";
    let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(ScheduleDomainSink::new(
        store,
        router.clone(),
        admin_read_model,
    ));
    router.register_domain_pattern("schedule", sink.clone());

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        schedule_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(700),
            encode_schedule_create(schedule_route, "* * * * *", b"retry"),
            family,
        ),
    ))
    .expect("create schedule");
    let _create_ack = receive_envelope(&subscriber_mailbox, "create ack envelope");
    wait_for_schedule_count(&sink, 1);

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        schedule_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(703),
            encode_schedule_subscribe(schedule_route),
            family,
        ),
    ))
    .expect("subscribe schedule");
    let _subscribe_ack = receive_envelope(&subscriber_mailbox, "subscribe ack envelope");
    drain_mailbox(&subscriber_mailbox);

    {
        let mut actors = sink.state.core.actors.lock();
        let actor = actors.get_mut(&family).expect("schedule actor");
        actor.bench_prepare_scan(1);
        let claimed = actor.bench_claim_due_fires();
        assert_eq!(claimed.len(), 1);
        actor.fail_next_store_commit_for_tests();
    }

    // Act
    sink.scan_due_schedules();
    let first_notify = receive_envelope(&subscriber_mailbox, "first schedule notify envelope");
    wait_for_ack_failure_count(&sink, 1);
    wait_for_pending_fire_count(&sink, 1);
    wait_for_pending_ack_retry_count(&sink, 1);
    let pending_after_failed_ack = sink.pending_fire_count();
    let pending_ack_retries = sink.pending_ack_retry_count();
    sink.scan_due_schedules();
    wait_for_pending_fire_count(&sink, 0);
    wait_for_pending_ack_retry_count(&sink, 0);

    // Assert
    let notify_frame = first_notify
        .into_payload::<FrameContext>()
        .expect("first schedule notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 705);
    let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
    let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
    let notified_payload = notify_decoder.get_bytes().expect("notify payload");
    assert_eq!(notified_payload.as_ref(), b"retry");
    assert!(notify_decoder.is_complete());
    assert_eq!(sink.ack_failure_count(), 1);
    assert_eq!(pending_after_failed_ack, 1);
    assert_eq!(pending_ack_retries, 1);
    assert_eq!(sink.pending_fire_count(), 0);
    assert_no_envelope(&subscriber_mailbox);
}

#[test]
fn should_store_cloud_strict_write_options_given_strict_cloud_policy() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = ScheduleDomainSink::new(store, router, admin_read_model)
        .with_write_options(cntryl_midge::WriteOptions::cloud_strict());

    // Assert
    assert!(sink.state.core.write_options.is_cloud_strict());
}

#[test]
fn should_increment_expired_pending_claim_metric_when_cleanup_removes_orphans() {
    // Arrange
    let family = RouteFamily::new(1);
    let clock = Arc::new(MockClock::new(1_700_000_000_000));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let metrics = MetricsCollector::new();
    let sink = ScheduleDomainSink::new(store.clone(), router, admin_read_model)
        .with_metrics(metrics.clone());
    let mut actor = crate::domains::schedule::ScheduleActor::new_with_clock(
        family,
        store,
        cntryl_midge::WriteOptions::buffered(),
        clock.clone(),
    );
    let create_response = actor.handle(crate::domains::schedule::ScheduleMessage::Create {
        route: "schedule://acme/jobs/cleanup/run".to_string(),
        cron: "* * * * *".to_string(),
        payload: Bytes::from_static(b"cleanup"),
    });
    assert!(matches!(
        create_response,
        crate::domains::schedule::ScheduleResponse::Ok
    ));
    actor.bench_prepare_scan(1);
    let claimed = actor.bench_claim_due_fires();
    assert_eq!(claimed.len(), 1);
    clock.advance(Duration::from_millis(11));
    sink.state
        .core
        .pending_claim_ttl_ms
        .store(10, Ordering::Relaxed);
    let now_elapsed_ms =
        u128_to_u64_saturating(sink.state.core.snapshot_epoch.elapsed().as_millis());
    sink.state.core.last_pending_claim_cleanup_elapsed_ms.store(
        now_elapsed_ms.saturating_sub(SCHEDULE_PENDING_CLAIM_CLEANUP_INTERVAL_MS),
        Ordering::Relaxed,
    );
    sink.state.core.actors.lock().insert(family, actor);

    // Act
    sink.scan_due_schedules();

    // Assert
    wait_for_metric_counter(&metrics, METRIC_PENDING_CLAIMS_EXPIRED_TOTAL, 1);
    let actors = sink.state.core.actors.lock();
    assert_eq!(
        actors
            .get(&family)
            .expect("schedule actor")
            .pending_fire_count(),
        0
    );
}

#[test]
fn should_read_admin_pending_claims_through_actor_command() {
    // Arrange
    let family = RouteFamily::new(1);
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = ScheduleDomainSink::new(store.clone(), router, admin_read_model);
    let mut actor = crate::domains::schedule::ScheduleActor::new(
        family,
        store,
        cntryl_midge::WriteOptions::buffered(),
    );
    actor
        .create_schedule(
            "schedule://acme/jobs/nightly/run".to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"nightly"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);
    assert_eq!(actor.bench_claim_due_fires().len(), 1);
    sink.state.core.actors.lock().insert(family, actor);

    // Act
    sink.stop();
    let claims = sink.admin_pending_claims(family);

    // Assert
    assert!(claims.is_empty());
}

#[test]
fn should_remove_schedule_subscriptions_given_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let schedule_route = "schedule://acme/jobs/nightly/run";
    let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(ScheduleDomainSink::new(
        store,
        router.clone(),
        admin_read_model,
    ));
    router.register_domain_pattern("schedule", sink.clone());

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        schedule_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(700),
            encode_schedule_create(schedule_route, "* * * * *", b"nightly"),
            family,
        ),
    ))
    .expect("create schedule");
    let _create_ack = receive_envelope(&subscriber_mailbox, "create ack envelope");
    wait_for_schedule_count(&sink, 1);
    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        schedule_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(703),
            encode_schedule_subscribe(schedule_route),
            family,
        ),
    ))
    .expect("subscribe schedule");
    let _subscribe_ack = receive_envelope(&subscriber_mailbox, "subscribe ack envelope");
    wait_for_subscription_count(&sink, 1);

    // Act
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("schedule://cleanup")),
        crate::runtime::SessionCleanup { session_id },
    ))
    .expect("cleanup session");
    wait_for_subscription_count(&sink, 0);
    {
        let mut actors = sink.state.core.actors.lock();
        let actor = actors.get_mut(&family).expect("schedule actor");
        actor.bench_prepare_scan(1);
    }
    sink.scan_due_schedules();
    wait_for_pending_fire_count(&sink, 0);

    // Assert
    assert_eq!(sink.subscription_count(), 0);
    assert_no_envelope(&subscriber_mailbox);
    assert!(sink.state.core.sub_families.lock().is_empty());
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_retain_other_schedule_subscription_given_unsubscribe_on_same_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let removed_route = "schedule://acme/jobs/nightly/run";
    let retained_route = "schedule://acme/jobs/weekly/report";
    let removed_address = RouteAddress::new(family, Route::new(removed_route));
    let retained_address = RouteAddress::new(family, Route::new(retained_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(16));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = ScheduleDomainSink::new(store, router, admin_read_model);

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        removed_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(703),
            encode_schedule_subscribe(removed_route),
            family,
        ),
    ))
    .expect("subscribe removed schedule route");
    let _removed_subscribe_ack =
        receive_envelope(&subscriber_mailbox, "removed subscribe ack envelope");

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        retained_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(703),
            encode_schedule_subscribe(retained_route),
            family,
        ),
    ))
    .expect("subscribe retained schedule route");
    let _retained_subscribe_ack =
        receive_envelope(&subscriber_mailbox, "retained subscribe ack envelope");
    wait_for_subscription_count(&sink, 2);
    assert_eq!(sink.subscription_count(), 2);
    drain_mailbox(&subscriber_mailbox);

    // Act
    sink.deliver(Envelope::from_route(
        subscriber_address,
        removed_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(704),
            encode_schedule_subscribe(removed_route),
            family,
        ),
    ))
    .expect("unsubscribe removed schedule route");
    let unsubscribe_envelope = receive_envelope(&subscriber_mailbox, "unsubscribe ack envelope");
    let unsubscribe_frame = unsubscribe_envelope
        .into_payload::<FrameContext>()
        .expect("unsubscribe ack frame");
    let mut unsubscribe_decoder = PayloadDecoder::new(&unsubscribe_frame.payload);
    let unsubscribe_status = unsubscribe_decoder.get_u8().expect("unsubscribe status");
    assert_eq!(unsubscribe_status, 0);
    assert!(unsubscribe_decoder.is_complete());
    wait_for_subscription_count(&sink, 1);
    assert_eq!(sink.subscription_count(), 1);

    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("schedule://events/removed")),
        crate::runtime::DomainPublishEvent::new(
            family,
            Route::new(removed_route),
            Bytes::from_static(b"nightly"),
        ),
    ))
    .expect("deliver removed schedule event");
    assert_no_envelope(&subscriber_mailbox);

    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("schedule://events/retained")),
        crate::runtime::DomainPublishEvent::new(
            family,
            Route::new(retained_route),
            Bytes::from_static(b"weekly"),
        ),
    ))
    .expect("deliver retained schedule event");

    // Assert
    let notify_envelope =
        receive_envelope(&subscriber_mailbox, "retained schedule notify envelope");
    let notify_frame = notify_envelope
        .into_payload::<FrameContext>()
        .expect("retained schedule notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 705);
    let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
    let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
    let notified_payload = notify_decoder.get_bytes().expect("notify payload");
    assert_eq!(notified_payload.as_ref(), b"weekly");
    assert!(notify_decoder.is_complete());
    assert_no_envelope(&subscriber_mailbox);
}

#[test]
fn should_count_live_publish_failure_given_domain_routing_error() {
    // Arrange — create a due schedule but do NOT register the "schedule" domain
    // handler so that router.route() returns an error when the live publish
    // handoff is attempted.
    let family = RouteFamily::new(1);
    let schedule_route = "schedule://acme/jobs/orphan/run";
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(ScheduleDomainSink::new(
        store.clone(),
        router.clone(),
        admin_read_model,
    ));
    // Intentionally do NOT register the "schedule" domain handler so routing fails.

    {
        let mut actors = sink.state.core.actors.lock();
        let mut actor = crate::domains::schedule::ScheduleActor::new(
            family,
            store,
            cntryl_midge::WriteOptions::buffered(),
        );
        actor
            .create_schedule(
                schedule_route.to_string(),
                "* * * * *".to_string(),
                Bytes::from_static(b"orphan"),
            )
            .expect("create schedule");
        actor.bench_prepare_scan(1);
        actors.insert(family, actor);
    }

    assert_eq!(sink.notify_failure_count(), 0, "no failures before scan");

    // Act — scan claims an occurrence but the live publish handoff cannot be routed
    sink.scan_due_schedules();

    // Assert
    wait_for_notify_failure_count(&sink, 1);
    assert_eq!(
        sink.notify_failure_count(),
        1,
        "live publish handoff failure should be counted when domain routing returns an error"
    );
    assert_eq!(
        sink.ack_failure_count(),
        0,
        "ack failure counter must remain zero when the publish itself failed"
    );
}
