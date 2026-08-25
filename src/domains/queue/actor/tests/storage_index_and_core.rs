use super::*;
pub(super) use crate::runtime::routing::RouteFamily;
pub(super) use crate::testkit::create_test_engine_with_cfs;
pub(super) use uuid::Uuid;

pub(super) const TEST_SESSION_ID: u64 = 1;

/// Mock clock for deterministic testing
#[derive(Clone)]
pub struct MockClock {
    state: Arc<std::sync::Mutex<MockClockState>>,
}

#[derive(Clone, Copy)]
pub(super) struct MockClockState {
    instant: Instant,
    epoch_ms: u64,
}

impl Default for MockClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MockClock {
    pub fn new() -> Self {
        Self {
            state: Arc::new(std::sync::Mutex::new(MockClockState {
                instant: Instant::now(),
                epoch_ms: 1_700_000_000_000, // deterministic-ish base
            })),
        }
    }

    pub fn advance(&self, duration: Duration) {
        let mut state = self.state.lock().unwrap();
        state.instant += duration;
        state.epoch_ms = state
            .epoch_ms
            .saturating_add(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
    }
}

impl Clock for MockClock {
    fn now_instant(&self) -> Instant {
        self.state.lock().unwrap().instant
    }

    fn now_epoch_ms(&self) -> u64 {
        self.state.lock().unwrap().epoch_ms
    }
}

pub(super) fn unique_queue_key(resource_prefix: &str) -> QueueKey {
    QueueKey {
        family: RouteFamily::new(0), /* CF=0 for Midge test limitation */
        realm: "test".to_string(),
        area: "queue".to_string(),
        resource: format!("{}-{}", resource_prefix, Uuid::new_v4()),
    }
}

pub(super) fn send_and_reserve_single_message(
    actor: &mut QueueActor,
    body: &str,
) -> (MessageId, u64) {
    let send_response = actor.handle_send(Bytes::from(body.to_string()), None);
    let QueueResponse::Sent { id } = send_response else {
        panic!("Expected Sent response");
    };

    let receive_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
    match receive_response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            (id, messages[0].token)
        }
        _ => panic!("Expected Received response"),
    }
}

pub(super) fn send_and_dead_letter_single_message(
    actor: &mut QueueActor,
    clock: &MockClock,
    body: &str,
    attempts: u32,
) -> MessageId {
    let msg_id = match actor.handle_send(Bytes::from(body.to_string()), None) {
        QueueResponse::Sent { id } => id,
        other => panic!("Expected Sent response, found {other:?}"),
    };

    for expected_attempt in 1..=attempts {
        match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
            QueueResponse::Received { messages } => {
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].attempts, expected_attempt);
            }
            other => panic!("Expected Received response, found {other:?}"),
        }
        clock.advance(Duration::from_secs(31));
        actor.process_expired_timers();
    }

    msg_id
}

pub(super) fn read_index_meta(
    store: &Arc<cntryl_midge::MidgeEngine>,
    queue_key: &QueueKey,
) -> Option<Bytes> {
    let txn = store
        .begin_tx(
            queue_key.family.id(),
            cntryl_midge::TransactionMode::ReadOnly,
        )
        .expect("begin read tx");
    txn.get(&QueueActor::index_meta_key(queue_key))
        .expect("read index meta")
}

pub(super) fn read_ready_index_ranges(
    store: &Arc<cntryl_midge::MidgeEngine>,
    queue_key: &QueueKey,
) -> Vec<(usize, ReadyRange)> {
    let txn = store
        .begin_tx(
            queue_key.family.id(),
            cntryl_midge::TransactionMode::ReadOnly,
        )
        .expect("begin read tx");
    let prefix = QueueActor::ready_index_prefix(queue_key);
    let query = cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&prefix));
    let iter = txn.scan(&query).expect("scan ready index");
    let mut ranges = Vec::new();

    for entry in iter {
        let (key, value) = entry.expect("read ready index row");
        let (shard, start) =
            QueueActor::parse_ready_range_key(&key, &prefix).expect("parse ready key");
        let range = QueueActor::decode_ready_range(start, &value).expect("decode ready range");
        ranges.push((shard, range));
    }

    ranges.sort_unstable_by_key(|(shard, range)| (*shard, range.next));
    ranges
}

pub(super) fn read_delayed_index_entries(
    store: &Arc<cntryl_midge::MidgeEngine>,
    queue_key: &QueueKey,
) -> Vec<(MessageId, u64)> {
    let txn = store
        .begin_tx(
            queue_key.family.id(),
            cntryl_midge::TransactionMode::ReadOnly,
        )
        .expect("begin read tx");
    let prefix = QueueActor::delayed_index_prefix(queue_key);
    let query = cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&prefix));
    let iter = txn.scan(&query).expect("scan delayed index");
    let mut entries = Vec::new();

    for entry in iter {
        let (key, _value) = entry.expect("read delayed index row");
        let (visible_at_ms, id) =
            QueueActor::parse_delayed_index_key(&key, &prefix).expect("parse delayed key");
        entries.push((id, visible_at_ms));
    }

    entries.sort_unstable_by_key(|(id, visible_at_ms)| (*visible_at_ms, id.as_u64()));
    entries
}

pub(super) fn read_dlq_index_entries(
    store: &Arc<cntryl_midge::MidgeEngine>,
    queue_key: &QueueKey,
) -> Vec<(MessageId, u64)> {
    let txn = store
        .begin_tx(
            queue_key.family.id(),
            cntryl_midge::TransactionMode::ReadOnly,
        )
        .expect("begin read tx");
    let prefix = QueueActor::dlq_index_prefix(queue_key);
    let query = cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&prefix));
    let iter = txn.scan(&query).expect("scan dlq index");
    let mut entries = Vec::new();

    for entry in iter {
        let (key, _value) = entry.expect("read DLQ index row");
        let (dead_lettered_at_ms, id) =
            QueueActor::parse_dlq_index_key(&key, &prefix).expect("parse dlq key");
        entries.push((id, dead_lettered_at_ms));
    }

    entries.sort_unstable_by_key(|(id, dead_lettered_at_ms)| (*dead_lettered_at_ms, id.as_u64()));
    entries
}

pub(super) fn clear_queue_index(
    store: &Arc<cntryl_midge::MidgeEngine>,
    queue_key: &QueueKey,
    meta_override: Option<Vec<u8>>,
) {
    let cf_id = queue_key.family.id();
    let mut txn = store
        .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin write tx");
    let ready_prefix = QueueActor::ready_index_prefix(queue_key);
    let delayed_prefix = QueueActor::delayed_index_prefix(queue_key);
    let ready_query = cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&ready_prefix));
    let delayed_query = cntryl_midge::Query::new().prefix(Bytes::copy_from_slice(&delayed_prefix));
    let ready_iter = txn.scan(&ready_query).expect("scan ready index");
    let delayed_iter = txn.scan(&delayed_query).expect("scan delayed index");
    let mut keys = Vec::new();

    for entry in ready_iter {
        let (key, _) = entry.expect("read ready index row");
        keys.push(key.to_vec());
    }
    for entry in delayed_iter {
        let (key, _) = entry.expect("read delayed index row");
        keys.push(key.to_vec());
    }
    keys.push(QueueActor::index_meta_key(queue_key));

    for key in keys {
        txn.delete(key).expect("delete queue index key");
    }

    if let Some(meta) = meta_override {
        txn.put(QueueActor::index_meta_key(queue_key), meta, None)
            .expect("override index meta");
    }

    txn.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit index mutation");
}

pub(super) fn put_queue_validation_row(
    store: &cntryl_midge::Engine,
    suffix: &[u8],
    value: Vec<u8>,
) {
    let mut txn = store
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .expect("begin write tx");
    txn.put(
        storage_key::prefixed_key("test", DomainKeyspace::Queue, suffix),
        value,
        None,
    )
    .expect("write queue validation row");
    txn.commit(cntryl_midge::WriteOptions::buffered())
        .expect("commit queue validation row");
}

pub(super) fn authoritative_queue_validation_suffix(family_marker: u8, id: Option<u64>) -> Vec<u8> {
    let mut suffix = Vec::new();
    storage_key::push_segment(&mut suffix, "jobs");
    storage_key::push_segment(&mut suffix, "email");
    suffix.push(family_marker);
    if let Some(id) = id {
        suffix.extend_from_slice(&id.to_be_bytes());
    }
    suffix
}

#[test]
pub(super) fn should_reject_malformed_authoritative_queue_rows_during_preflight() {
    // Arrange
    let cases = [
        (
            authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_META, None),
            b"broken".to_vec(),
            "meta",
        ),
        (
            authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_HEADER, Some(1)),
            b"broken".to_vec(),
            "header",
        ),
    ];

    // Act
    let errors = cases
        .into_iter()
        .map(|(suffix, value, category)| {
            let store = create_test_engine_with_cfs(vec![1]);
            put_queue_validation_row(store.as_ref(), &suffix, value);
            (
                QueueActor::validate_persisted_state_for_existing_families(store.as_ref())
                    .expect_err("malformed queue row should fail preflight"),
                category,
            )
        })
        .collect::<Vec<_>>();

    // Assert
    assert!(errors
        .into_iter()
        .all(|(error, category)| error.contains(&format!("key_category={category}"))));
}

#[test]
pub(super) fn should_fail_closed_given_incomplete_queue_record_under_buffered_policy() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    put_queue_validation_row(
        store.as_ref(),
        &authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_HEADER, Some(1)),
        QueueActor::encode_record_header(&QueueRecord::ready(
            Bytes::from_static(b"payload"),
            1,
            1,
            1_700_000_000_000,
        )),
    );

    // Act
    let result = QueueActor::validate_persisted_state_for_existing_families(store.as_ref());

    // Assert
    assert!(result
        .expect_err("missing queue body should fail preflight")
        .contains("missing body for split header"));
}

#[test]
pub(super) fn should_reject_orphan_queue_body_during_preflight() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    put_queue_validation_row(
        store.as_ref(),
        &authoritative_queue_validation_suffix(QUEUE_KEY_FAMILY_BODY, Some(1)),
        b"payload".to_vec(),
    );

    // Act
    let result = QueueActor::validate_persisted_state_for_existing_families(store.as_ref());

    // Assert
    assert!(result
        .expect_err("orphan queue body should fail preflight")
        .contains("orphan body row"));
}

#[test]
pub(super) fn should_order_queue_ready_range_keys_by_typed_numeric_suffix() {
    // Arrange
    let prefix = vec![QUEUE_KEY_FAMILY_READY_INDEX];

    // Act
    let key1 = QueueActor::ready_range_key_with_prefix(&prefix, 1, 2);
    let key2 = QueueActor::ready_range_key_with_prefix(&prefix, 1, 10);

    // Assert
    assert!(key1 < key2);
}

#[test]
pub(super) fn should_reserve_enqueued_message() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs");
    // Use CF=0 here because the in-memory Midge test engine exposes only the default CF.
    // Production queues still use the normal RouteFamily -> CF mapping.
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    let body = Bytes::from("test message");
    let enqueue_response = actor.handle_send(body.clone(), None);
    let QueueResponse::Sent { id: msg_id } = enqueue_response else {
        panic!("Expected Enqueued response");
    };

    // Assert
    assert_eq!(actor.ready_len(), 1);
    let reserve_response = actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1));
    match reserve_response {
        QueueResponse::Received { messages } => {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].id, msg_id);
            assert_eq!(messages[0].body, body);
            assert_eq!(messages[0].attempts, 1);
            assert_eq!(messages[0].inflight_seconds, 30);
        }
        _ => panic!("Expected Received response"),
    }
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(actor.inflight.len(), 1);
}

#[test]
pub(super) fn should_track_success_rates_given_enqueue_then_complete() {
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
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(0),
        unique_queue_key("rates"),
        store,
        Box::new(clock.clone()),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    assert!(matches!(
        actor.handle_send(Bytes::from_static(b"one"), None),
        QueueResponse::Sent { .. }
    ));
    clock.advance(Duration::from_secs(10));
    assert!(matches!(
        actor.handle_send(Bytes::from_static(b"two"), None),
        QueueResponse::Sent { .. }
    ));
    let (id, token) = match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
        QueueResponse::Received { messages } => (messages[0].id, messages[0].token),
        other => panic!("Expected Received response, found {other:?}"),
    };
    assert_eq!(
        actor.handle_ack_for_session(TEST_SESSION_ID, id, token),
        QueueResponse::Acked
    );

    // Assert
    let active_snapshot = actor.admin_snapshot();
    assert_eq!(active_snapshot.enqueue_success_total, 2);
    assert_eq!(active_snapshot.complete_success_total, 1);
    assert!((active_snapshot.in_rate_per_second - (2.0 / 60.0)).abs() < f64::EPSILON);
    assert!((active_snapshot.out_rate_per_second - (1.0 / 60.0)).abs() < f64::EPSILON);

    clock.advance(Duration::from_secs(61));
    let expired_window_snapshot = actor.admin_snapshot();
    assert_eq!(expired_window_snapshot.enqueue_success_total, 2);
    assert_eq!(expired_window_snapshot.complete_success_total, 1);
    assert!(expired_window_snapshot.in_rate_per_second.abs() < f64::EPSILON);
    assert!(expired_window_snapshot.out_rate_per_second.abs() < f64::EPSILON);
}

#[test]
pub(super) fn should_bound_hot_body_cache_size() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-body-cache");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    for i in 0..(QueueActor::BODY_CACHE_LIMIT + 32) {
        let body = Bytes::from(format!("message-{i}"));
        let response = actor.handle_send(body, None);
        assert!(matches!(response, QueueResponse::Sent { .. }));
    }

    // Assert
    assert!(actor.records.len() <= QueueActor::RECORD_CACHE_LIMIT);
    assert_eq!(actor.body_cache.len(), QueueActor::BODY_CACHE_LIMIT);
    assert!(actor.body_cache_bytes <= QueueActor::BODY_CACHE_LIMIT_BYTES);
}

#[test]
pub(super) fn should_wake_waiters_given_batch_send_transitions_empty_queue_to_ready() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-batch-wake");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    let response = actor.handle_send_batch(&[(Bytes::from_static(b"ready"), None)]);

    // Assert
    assert!(matches!(response, QueueResponse::SentBatch { .. }));
    assert!(actor.take_needs_wake_waiters());
    assert!(!actor.take_needs_wake_waiters());
}

#[test]
pub(super) fn should_not_wake_waiters_given_batch_send_only_delayed_messages() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-batch-delayed-no-wake");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    let response = actor.handle_send_batch(&[(Bytes::from_static(b"delayed"), Some(60))]);

    // Assert
    assert!(matches!(response, QueueResponse::SentBatch { .. }));
    assert!(!actor.take_needs_wake_waiters());
    assert_eq!(actor.ready_len(), 0);
    assert_eq!(actor.delayed.len(), 1);
}

#[test]
pub(super) fn should_bound_metadata_cache_size() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-record-cache");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    for i in 0..(QueueActor::RECORD_CACHE_LIMIT + 32) {
        let body = Bytes::from(format!("message-{i}"));
        let response = actor.handle_send(body, None);
        assert!(matches!(response, QueueResponse::Sent { .. }));
    }

    // Assert
    assert_eq!(actor.records.len(), QueueActor::RECORD_CACHE_LIMIT);
    let max_fifo_len = QueueActor::RECORD_CACHE_LIMIT
        * QueueActor::RECORD_CACHE_FIFO_SLACK_MULTIPLIER
        + actor.records.len();
    assert!(actor.record_cache_fifo.len() <= max_fifo_len);
}

#[test]
pub(super) fn should_bound_hot_body_cache_total_bytes() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-body-cache-bytes");
    let mut actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key,
        store,
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Act
    // Bodies this large predate the SEND deliverable-body limit, so seed them
    // unvalidated: the cache byte accounting still has to bound them.
    let body_size = QueueActor::BODY_CACHE_LIMIT_BYTES / 4 + 1;
    for i in 0..5 {
        let byte = u8::try_from(i).expect("body byte should fit in u8");
        let body = Bytes::from(vec![byte; body_size]);
        let response = actor.handle_send_unvalidated_for_tests(body, None);
        assert!(matches!(response, QueueResponse::Sent { .. }));
    }

    // Assert
    assert_eq!(actor.records.len(), 5);
    assert!(actor.body_cache.len() < 5);
    assert!(actor.body_cache_bytes <= QueueActor::BODY_CACHE_LIMIT_BYTES);
}

#[test]
pub(super) fn should_not_hydrate_metadata_cache_during_recovery() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-recovery-meta");

    {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        for i in 0..64 {
            let body = Bytes::from(format!("recovered-{i}"));
            let response = actor.handle_send(body, None);
            assert!(matches!(response, QueueResponse::Sent { .. }));
        }
    }

    // Act
    let actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Assert
    assert_eq!(actor.ready_len(), 64);
    assert!(actor.records.is_empty());
    assert!(actor.record_cache_fifo.is_empty());
    assert!(actor.body_cache.is_empty());
    assert_eq!(actor.recovery_path, RecoveryPath::IndexHit);
    assert!(QueueActor::index_meta_is_valid(
        &read_index_meta(&store, &queue_key).expect("index meta should exist")
    ));
    assert!(!read_ready_index_ranges(&store, &queue_key).is_empty());
}

#[test]
pub(super) fn should_rewrite_missing_queue_index_via_fallback() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-missing-index");

    {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        for i in 0..12 {
            let response = actor.handle_send(Bytes::from(format!("visible-{i}")), None);
            assert!(matches!(response, QueueResponse::Sent { .. }));
        }
    }

    clear_queue_index(&store, &queue_key, None);

    // Act
    let actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Assert
    assert_eq!(actor.ready_len(), 12);
    assert_eq!(actor.recovery_path, RecoveryPath::IndexMissingFallback);
    assert!(QueueActor::index_meta_is_valid(
        &read_index_meta(&store, &queue_key).expect("rewritten index meta should exist")
    ));
    assert!(!read_ready_index_ranges(&store, &queue_key).is_empty());
}

#[test]
pub(super) fn should_rewrite_corrupted_queue_index_meta_via_fallback() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-corrupt-index");

    {
        let mut actor = QueueActor::new(
            RouteFamily::new(0),
            queue_key.clone(),
            store.clone(),
            None,
            crate::utils::idempotency::default_dedup_store(),
        );

        for i in 0..8 {
            let response = actor.handle_send(Bytes::from(format!("task-{i}")), None);
            assert!(matches!(response, QueueResponse::Sent { .. }));
        }
    }

    clear_queue_index(&store, &queue_key, Some(vec![0, 0]));

    // Act
    let actor = QueueActor::new(
        RouteFamily::new(0),
        queue_key.clone(),
        store.clone(),
        None,
        crate::utils::idempotency::default_dedup_store(),
    );

    // Assert
    assert_eq!(actor.ready_len(), 8);
    assert_eq!(actor.recovery_path, RecoveryPath::IndexInvalidFallback);
    assert!(QueueActor::index_meta_is_valid(
        &read_index_meta(&store, &queue_key).expect("rewritten index meta should exist")
    ));
}

#[test]
pub(super) fn should_plan_ready_index_mutations_for_persisted_ready_ranges() {
    // Arrange
    let mut shards: Vec<VecDeque<ReadyRange>> = (0..QueueActor::READY_SHARDS)
        .map(|_| VecDeque::new())
        .collect();
    for id in [1_u64, 9, 17, 25] {
        QueueActor::stage_persisted_ready_append(&mut shards, MessageId::new(id));
    }

    // Act
    let (shard, mutation) =
        QueueActor::plan_ready_index_mutation(&shards, MessageId::new(1)).expect("head mutation");
    // Assert
    assert_eq!(shard, 1);
    assert_eq!(
        mutation,
        PersistedReadyMutation::Replace {
            removed: ReadyRange { next: 1, end: 25 },
            inserted: ReadyRange { next: 9, end: 25 },
        }
    );

    let (shard, mutation) =
        QueueActor::plan_ready_index_mutation(&shards, MessageId::new(25)).expect("tail mutation");
    assert_eq!(shard, 1);
    assert_eq!(
        mutation,
        PersistedReadyMutation::Replace {
            removed: ReadyRange { next: 1, end: 25 },
            inserted: ReadyRange { next: 1, end: 17 },
        }
    );

    let mut split_shards = shards.clone();
    let (shard, mutation) =
        QueueActor::plan_ready_index_mutation(&split_shards, MessageId::new(17))
            .expect("middle mutation");
    assert_eq!(shard, 1);
    assert_eq!(
        mutation,
        PersistedReadyMutation::Split {
            removed: ReadyRange { next: 1, end: 25 },
            left: ReadyRange { next: 1, end: 9 },
            right: ReadyRange { next: 25, end: 25 },
        }
    );
    QueueActor::apply_ready_index_mutation_to_shards(&mut split_shards, shard, mutation);
    assert_eq!(split_shards[1].len(), 2);
}

#[test]
pub(super) fn should_remove_delayed_index_entry_after_ack_even_when_visibility_passed() {
    // Arrange
    let store = Arc::new(
        cntryl_midge::Engine::open(
            cntryl_midge::OpenOptions::in_memory()
                .build()
                .expect("build in-memory test options"),
        )
        .expect("Failed to open Midge"),
    );
    let queue_key = unique_queue_key("jobs-delayed-index");
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

    assert_eq!(actor.ready_len(), 0);
    assert_eq!(read_delayed_index_entries(&store, &queue_key).len(), 1);

    // Act
    clock.advance(Duration::from_secs(2));
    actor.process_delayed_messages();

    let reserved = match actor.handle_receive_for_session(TEST_SESSION_ID, 30, Some(1)) {
        QueueResponse::Received { messages } => messages,
        other => panic!("Expected Received response, got {other:?}"),
    };
    assert_eq!(reserved.len(), 1);

    let message = &reserved[0];
    assert!(matches!(
        actor.handle_ack_for_session(TEST_SESSION_ID, message.id, message.token),
        QueueResponse::Acked
    ));
    // Assert
    assert!(read_delayed_index_entries(&store, &queue_key).is_empty());
}
