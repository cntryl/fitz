use super::{
    assert_queue_success_frames, black_box, prepare_ack_case, send_queue_request, stress,
    tier2_stress, Arc, Bytes, Duration, Instant, RouteFamily, StressContext, CLIENT_SESSION_ID,
};
use fitz::domains::queue::{retention::QueueRetention, QueueResponse};

const CONTROL_MESSAGES: usize = 256;

#[derive(Clone)]
struct ExpiryClock {
    instant: Instant,
    elapsed_seconds: Arc<std::sync::atomic::AtomicU64>,
}

impl fitz::domains::queue::Clock for ExpiryClock {
    fn now_instant(&self) -> Instant {
        self.instant
            + Duration::from_secs(
                self.elapsed_seconds
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
    }
    fn now_epoch_ms(&self) -> u64 {
        1_700_000_000_000
            + self
                .elapsed_seconds
                .load(std::sync::atomic::Ordering::Relaxed)
                * 1_000
    }
}

fn measure_sparse_expiry(ctx: &mut StressContext, backlog: usize, name: &str) {
    use fitz::domains::queue::QueueKey;
    use fitz::testkit::domain_internals::queue::QueueActor;
    let clock = ExpiryClock {
        instant: Instant::now(),
        elapsed_seconds: Arc::default(),
    };
    let family = RouteFamily::new(1);
    let key = QueueKey {
        family,
        realm: "bench".into(),
        area: "controls".into(),
        resource: "expiry".into(),
    };
    let mut actor = QueueActor::with_clock(
        family,
        key,
        fitz::benchkit::create_bench_store(),
        Box::new(clock.clone()),
        None,
        fitz::utils::idempotency::default_dedup_store(),
    )
    .with_retention(QueueRetention {
        message_seconds: Some(3_600),
        dlq_seconds: None,
    });
    actor.handle_send_with_ttl(Bytes::new(), None, std::num::NonZeroU32::new(2));
    let response = actor.handle_send_batch(&vec![(Bytes::new(), None); backlog]);
    assert!(matches!(response, QueueResponse::SentBatch { ids } if ids.len() == backlog));
    clock
        .elapsed_seconds
        .store(3_601, std::sync::atomic::Ordering::Relaxed);
    let start = Instant::now();
    let changed = actor.process_due_work();
    let elapsed = start.elapsed();
    assert!(changed);
    assert_eq!(actor.admin_snapshot().messages_ready, backlog + 1 - 32);
    ctx.parameter("backlog", backlog);
    ctx.parameter("expired_non_head_messages_per_pass", 32);
    tier2_stress::record_completed(ctx, 32);
    tier2_stress::record_duration(ctx, name, elapsed, 32);
}

#[stress(tier = 2, name = "expiry_sparse_16k_completed")]
fn should_expire_sparse_16k_completed(ctx: &mut StressContext) {
    measure_sparse_expiry(ctx, 16_384, "expiry_sparse_16k_completed");
}

#[stress(tier = 2, name = "expiry_sparse_64k_completed")]
fn should_expire_sparse_64k_completed(ctx: &mut StressContext) {
    measure_sparse_expiry(ctx, 65_536, "expiry_sparse_64k_completed");
}

fn measure_nack(ctx: &mut StressContext, visibility_seconds: u64, name: &str) {
    let case = prepare_ack_case();
    let requests: Vec<_> = case
        .ack_requests
        .iter()
        .map(|(_, payload)| {
            let mut payload = payload.to_vec();
            payload.extend_from_slice(&visibility_seconds.to_be_bytes());
            Bytes::from(payload)
        })
        .collect();
    let start = Instant::now();
    for payload in requests {
        send_queue_request(
            &case.router,
            &case.source,
            case.route,
            CLIENT_SESSION_ID,
            210,
            black_box(payload),
            case.family,
        );
    }
    let responses = case
        .inbox
        .drain_after_count(CONTROL_MESSAGES, Duration::from_secs(2));
    let elapsed = start.elapsed();
    assert_queue_success_frames(responses, CONTROL_MESSAGES);
    ctx.parameter("completion", "all_responses_received");
    tier2_stress::record_completed(ctx, CONTROL_MESSAGES as u64);
    tier2_stress::record_duration(ctx, name, elapsed, CONTROL_MESSAGES as u64);
    case.router.clear();
}

#[stress(tier = 2, name = "nack_ready_256_completed")]
fn should_nack_ready_256_completed(ctx: &mut StressContext) {
    measure_nack(ctx, 0, "nack_ready_256_completed");
}

#[stress(tier = 2, name = "nack_delayed_256_completed")]
fn should_nack_delayed_256_completed(ctx: &mut StressContext) {
    measure_nack(ctx, 60, "nack_delayed_256_completed");
}

fn measure_reserve_counters(ctx: &mut StressContext, retained: bool, name: &str) {
    let mut actor = fitz::benchkit::create_bench_queue_actor("bench", "controls", "reserve", None)
        .with_retention(QueueRetention {
            message_seconds: retained.then_some(86_400),
            dlq_seconds: None,
        });
    let response =
        actor.handle_send_batch(&vec![(Bytes::from_static(b"work"), None); CONTROL_MESSAGES]);
    assert!(matches!(response, QueueResponse::SentBatch { ids } if ids.len() == CONTROL_MESSAGES));
    let start = Instant::now();
    let response = actor.handle_receive_for_session(CLIENT_SESSION_ID, 30, Some(CONTROL_MESSAGES));
    let elapsed = start.elapsed();
    let QueueResponse::Received { messages } = response else {
        panic!("reservation batch");
    };
    assert_eq!(messages.len(), CONTROL_MESSAGES);
    assert!(messages.iter().all(|message| message.attempts == 1));
    ctx.parameter("completion", "counter_transaction_and_live_tokens");
    tier2_stress::record_completed(ctx, CONTROL_MESSAGES as u64);
    tier2_stress::record_duration(ctx, name, elapsed, CONTROL_MESSAGES as u64);
}

#[stress(tier = 2, name = "reserve_counters_256_completed")]
fn should_reserve_counters_256_completed(ctx: &mut StressContext) {
    measure_reserve_counters(ctx, false, "reserve_counters_256_completed");
}

#[stress(tier = 2, name = "reserve_ttl_counters_256_completed")]
fn should_reserve_ttl_counters_256_completed(ctx: &mut StressContext) {
    measure_reserve_counters(ctx, true, "reserve_ttl_counters_256_completed");
}

#[stress(tier = 2, name = "nack_dlq_32_4k_completed")]
fn should_nack_dlq_32_4k_completed(ctx: &mut StressContext) {
    let mut actor = fitz::benchkit::create_bench_queue_actor("bench", "controls", "dlq", Some(1))
        .with_retention(QueueRetention {
            message_seconds: Some(86_400),
            dlq_seconds: Some(3_600),
        });
    let response = actor.handle_send_batch(&vec![(Bytes::from(vec![7; 4_096]), None); 32]);
    assert!(matches!(response, QueueResponse::SentBatch { ids } if ids.len() == 32));
    let QueueResponse::Received { messages } =
        actor.handle_receive_for_session(CLIENT_SESSION_ID, 30, Some(32))
    else {
        panic!("reservation batch");
    };
    let start = Instant::now();
    for message in messages {
        assert_eq!(
            actor.handle_nack_for_session(CLIENT_SESSION_ID, message.id, message.token, 0),
            QueueResponse::Nacked
        );
    }
    let elapsed = start.elapsed();
    assert_eq!(actor.admin_dead_letters().len(), 32);
    ctx.parameter("completion", "dlq_header_body_and_index_ttl");
    tier2_stress::record_completed(ctx, 32);
    tier2_stress::record_duration(ctx, "nack_dlq_32_4k_completed", elapsed, 32);
}
