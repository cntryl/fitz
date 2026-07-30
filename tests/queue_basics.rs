//! Executable Queue protocol contract tests.

use bytes::Bytes;
use fitz::domains::queue::{
    Clock, QueueActor, QueueKey, QueueMessage, QueueResponse, ReservedMessage,
};
use fitz::protocol::{error_queue, queue_codec};
use fitz::runtime::routing::RouteFamily;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TEST_SESSION_ID: u64 = 1;

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[derive(Clone)]
struct TestClock {
    state: Arc<Mutex<(Instant, u64)>>,
}

impl TestClock {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new((Instant::now(), 1_700_000_000_000))),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut state = self.state.lock().expect("clock lock");
        state.0 += duration;
        state.1 = state
            .1
            .saturating_add(u128_to_u64_saturating(duration.as_millis()));
    }
}

impl Clock for TestClock {
    fn now_instant(&self) -> Instant {
        self.state.lock().expect("clock lock").0
    }

    fn now_epoch_ms(&self) -> u64 {
        self.state.lock().expect("clock lock").1
    }
}

struct QueueProtocolHarness {
    actor: QueueActor,
    clock: TestClock,
    store: Arc<cntryl_midge::Engine>,
    key: QueueKey,
    route: String,
}

impl QueueProtocolHarness {
    fn new(resource: &str) -> Self {
        let store = Arc::new(
            cntryl_midge::Engine::open(
                cntryl_midge::OpenOptions::in_memory()
                    .build()
                    .expect("build in-memory test options"),
            )
            .expect("open queue store"),
        );
        Self::with_store(resource, store)
    }

    fn with_store(resource: &str, store: Arc<cntryl_midge::Engine>) -> Self {
        let clock = TestClock::new();
        let key = QueueKey {
            family: RouteFamily::new(0),
            realm: "test".to_string(),
            area: "jobs".to_string(),
            resource: resource.to_string(),
        };
        let actor = QueueActor::with_clock(
            RouteFamily::new(0),
            key.clone(),
            store.clone(),
            Box::new(clock.clone()),
            None,
            fitz::utils::idempotency::default_dedup_store(),
        );
        Self {
            actor,
            clock,
            store,
            route: format!("queue://test/jobs/{resource}"),
            key,
        }
    }

    fn restart(&mut self) {
        self.actor = QueueActor::with_clock(
            RouteFamily::new(0),
            self.key.clone(),
            self.store.clone(),
            Box::new(self.clock.clone()),
            None,
            fitz::utils::idempotency::default_dedup_store(),
        );
    }

    fn execute(&mut self, msg_type: u16, payload: &[u8]) -> QueueResponse {
        match queue_codec::parse_request(msg_type, RouteFamily::new(0), payload)
            .expect("parse queue request")
        {
            QueueMessage::Send {
                body,
                delay_seconds,
                ..
            } => self.actor.handle_send(body, delay_seconds),
            QueueMessage::Receive {
                inflight_seconds,
                batch_size,
                ..
            } => {
                self.actor
                    .handle_receive_for_session(TEST_SESSION_ID, inflight_seconds, batch_size)
            }
            QueueMessage::Extend {
                id,
                token,
                inflight_seconds,
                ..
            } => self
                .actor
                .handle_extend_for_session(TEST_SESSION_ID, id, token, inflight_seconds),
            QueueMessage::Ack { id, token, .. } => {
                self.actor
                    .handle_ack_for_session(TEST_SESSION_ID, id, token)
            }
            QueueMessage::InflightExpired { .. } => unreachable!("internal queue message"),
        }
    }

    fn send(&mut self, body: &[u8]) -> QueueResponse {
        self.execute(
            queue_codec::msg_type::ENQUEUE,
            &enqueue_payload(&self.route, body, None),
        )
    }

    fn reserve(&mut self, inflight_seconds: u64, batch_size: u32) -> Vec<ReservedMessage> {
        match self.execute(
            queue_codec::msg_type::RESERVE,
            &reserve_payload(&self.route, inflight_seconds, batch_size),
        ) {
            QueueResponse::Received { messages } => messages,
            response => panic!("expected received response, got {response:?}"),
        }
    }
}

fn route_payload(route: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&usize_to_u32_saturating(route.len()).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload
}

fn enqueue_payload(route: &str, body: &[u8], delay_seconds: Option<u64>) -> Vec<u8> {
    let mut payload = route_payload(route);
    payload.extend_from_slice(&usize_to_u32_saturating(body.len()).to_be_bytes());
    payload.extend_from_slice(body);
    payload.push(u8::from(delay_seconds.is_some()));
    if let Some(delay_seconds) = delay_seconds {
        payload.extend_from_slice(&delay_seconds.to_be_bytes());
    }
    payload
}

fn reserve_payload(route: &str, inflight_seconds: u64, batch_size: u32) -> Vec<u8> {
    let mut payload = route_payload(route);
    payload.extend_from_slice(&inflight_seconds.to_be_bytes());
    payload.push(1);
    payload.extend_from_slice(&batch_size.to_be_bytes());
    payload
}

fn extend_payload(route: &str, id: u64, token: u64, inflight_seconds: u64) -> Vec<u8> {
    let mut payload = route_payload(route);
    payload.extend_from_slice(&id.to_be_bytes());
    payload.extend_from_slice(&token.to_be_bytes());
    payload.extend_from_slice(&inflight_seconds.to_be_bytes());
    payload
}

fn complete_payload(route: &str, id: u64, token: u64) -> Vec<u8> {
    let mut payload = route_payload(route);
    payload.extend_from_slice(&id.to_be_bytes());
    payload.extend_from_slice(&token.to_be_bytes());
    payload
}

#[test]
fn should_reject_queue_complete_given_token_from_previous_reservation() {
    // Arrange
    let mut harness = QueueProtocolHarness::new("cycle");

    // Act
    let sent = harness.send(b"payload");
    let reserved = harness.reserve(30, 1);
    harness.clock.advance(Duration::from_secs(31));
    harness.actor.process_expired_timers();
    let replacement = harness.reserve(30, 1);
    let completed = harness.execute(
        queue_codec::msg_type::COMPLETE,
        &complete_payload(
            &harness.route,
            replacement[0].id.as_u64(),
            reserved[0].token,
        ),
    );
    let completed_with_current_token = harness.execute(
        queue_codec::msg_type::COMPLETE,
        &complete_payload(
            &harness.route,
            replacement[0].id.as_u64(),
            replacement[0].token,
        ),
    );

    // Assert
    assert!(matches!(sent, QueueResponse::Sent { .. }));
    assert_eq!(reserved.len(), 1);
    assert_eq!(reserved[0].body, Bytes::from_static(b"payload"));
    assert_ne!(reserved[0].token, 0);
    assert_eq!(completed, QueueResponse::InvalidToken);
    assert_eq!(completed_with_current_token, QueueResponse::Acked);
    assert!(harness.reserve(30, 1).is_empty());
}

#[test]
fn should_batch_messages_in_fifo_order_with_unique_tokens() {
    // Arrange
    let mut harness = QueueProtocolHarness::new("batch");
    for body in [b"first".as_slice(), b"", b"third"] {
        assert!(matches!(harness.send(body), QueueResponse::Sent { .. }));
    }

    // Act
    let reserved = harness.reserve(30, 3);

    // Assert
    assert_eq!(reserved.len(), 3);
    assert_eq!(reserved[0].body, Bytes::from_static(b"first"));
    assert_eq!(reserved[1].body, Bytes::new());
    assert_eq!(reserved[2].body, Bytes::from_static(b"third"));
    assert_ne!(reserved[0].token, reserved[1].token);
    assert_ne!(reserved[1].token, reserved[2].token);
}

#[test]
fn should_not_complete_queue_message_given_expired_token() {
    // Arrange
    let mut harness = QueueProtocolHarness::new("extend");
    harness.send(b"payload");
    let reserved = harness.reserve(30, 1);
    let id = reserved[0].id.as_u64();
    let token = reserved[0].token;

    // Act
    let response = harness.execute(
        queue_codec::msg_type::EXTEND,
        &extend_payload(&harness.route, id, token.wrapping_add(1), 60),
    );

    // Assert
    assert_eq!(response, QueueResponse::InvalidToken);
}

#[test]
fn should_extend_valid_inflight() {
    // Arrange
    let mut harness = QueueProtocolHarness::new("extend-valid");
    harness.send(b"payload");
    let reserved = harness.reserve(30, 1);

    // Act
    let response = harness.execute(
        queue_codec::msg_type::EXTEND,
        &extend_payload(
            &harness.route,
            reserved[0].id.as_u64(),
            reserved[0].token,
            60,
        ),
    );

    // Assert
    assert_eq!(response, QueueResponse::Extended);
}

#[test]
fn should_recover_queue_delayed_message_given_restart_under_strict_policy() {
    // Arrange
    let mut harness = QueueProtocolHarness::new("redelivery");
    harness.send(b"payload");
    let first = harness.reserve(30, 1);

    // Act
    harness.clock.advance(Duration::from_secs(31));
    harness.actor.process_expired_timers();
    let second = harness.reserve(30, 1);

    // Assert
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].id, first[0].id);
    assert_eq!(second[0].body, first[0].body);
    assert_ne!(second[0].token, first[0].token);
    assert_eq!(second[0].attempts, 2);
}

#[test]
fn should_allow_fast_policy_loss_given_unflushed_recent_enqueue() {
    // Arrange
    let mut harness = QueueProtocolHarness::new("restart");
    harness.send(b"durable");

    // Act
    harness.restart();
    let reserved = harness.reserve(30, 1);
    let completed = harness.execute(
        queue_codec::msg_type::COMPLETE,
        &complete_payload(&harness.route, reserved[0].id.as_u64(), reserved[0].token),
    );
    harness.restart();

    // Assert
    assert_eq!(reserved[0].body, Bytes::from_static(b"durable"));
    assert_eq!(completed, QueueResponse::Acked);
    assert!(harness.reserve(30, 1).is_empty());
}

#[test]
fn should_bound_wire_batch_preallocation_by_available_work() {
    // Arrange
    let mut harness = QueueProtocolHarness::new("large-batch");
    harness.send(b"one");

    // Act
    let reserved = harness.reserve(30, u32::MAX);

    // Assert
    assert_eq!(reserved.len(), 1);
    assert_eq!(reserved[0].body, Bytes::from_static(b"one"));
}

#[test]
fn should_reject_overflowing_queue_durations_without_panicking() {
    // Arrange
    let mut harness = QueueProtocolHarness::new("duration");
    harness.send(b"one");
    let reserved = harness.reserve(30, 1);

    // Act
    let delayed = harness.execute(
        queue_codec::msg_type::ENQUEUE,
        &enqueue_payload(&harness.route, b"later", Some(u64::MAX)),
    );
    let receive = harness.execute(
        queue_codec::msg_type::RESERVE,
        &reserve_payload(&harness.route, u64::MAX, 1),
    );
    let extend = harness.execute(
        queue_codec::msg_type::EXTEND,
        &extend_payload(
            &harness.route,
            reserved[0].id.as_u64(),
            reserved[0].token,
            u64::MAX,
        ),
    );

    // Assert
    assert!(matches!(delayed, QueueResponse::BadRequest { .. }));
    assert!(matches!(receive, QueueResponse::BadRequest { .. }));
    assert!(matches!(extend, QueueResponse::BadRequest { .. }));
}

#[test]
fn should_reject_trailing_queue_request_bytes() {
    // Arrange
    let route = "queue://test/jobs/trailing";
    let cases = [
        (
            queue_codec::msg_type::ENQUEUE,
            enqueue_payload(route, b"payload", None),
        ),
        (
            queue_codec::msg_type::EXTEND,
            extend_payload(route, 1, 2, 30),
        ),
        (
            queue_codec::msg_type::COMPLETE,
            complete_payload(route, 1, 2),
        ),
    ];

    // Act
    let results = cases.map(|(msg_type, mut payload)| {
        payload.push(0xff);
        queue_codec::parse_request(msg_type, RouteFamily::new(0), &payload)
    });

    // Assert
    assert!(results.into_iter().all(|result| result.is_err()));
}

#[test]
fn should_deduplicate_complete_for_same_inflight_token() {
    // Arrange
    let mut harness = QueueProtocolHarness::new("dedup");
    harness.send(b"one");
    let reserved = harness.reserve(30, 1);
    let complete = complete_payload(&harness.route, reserved[0].id.as_u64(), reserved[0].token);

    // Act
    let first = harness.execute(queue_codec::msg_type::COMPLETE, &complete);
    let second = harness.execute(queue_codec::msg_type::COMPLETE, &complete);

    // Assert
    assert_eq!(first, QueueResponse::Acked);
    assert_eq!(second, QueueResponse::Acked);
}

#[test]
fn should_allocate_queue_error_codes_from_documented_range() {
    // Arrange
    let codes = [
        error_queue::ERR_INVALID_TOKEN,
        error_queue::ERR_INFLIGHT_EXPIRED,
        error_queue::ERR_MESSAGE_NOT_FOUND,
        error_queue::ERR_QUEUE_NOT_FOUND,
        error_queue::ERR_QUEUE_FULL,
        error_queue::ERR_UNAUTHORIZED,
    ];

    // Act
    let all_allocated_to_queue = codes.into_iter().all(|code| (4000..4100).contains(&code));

    // Assert
    assert!(all_allocated_to_queue);
    assert_eq!(error_queue::ERR_INVALID_TOKEN, 4001);
    assert_eq!(error_queue::ERR_INFLIGHT_EXPIRED, 4002);
    assert_eq!(error_queue::ERR_MESSAGE_NOT_FOUND, 4003);
    assert_eq!(error_queue::ERR_QUEUE_NOT_FOUND, 4004);
    assert_eq!(error_queue::ERR_UNAUTHORIZED, 4009);
}
