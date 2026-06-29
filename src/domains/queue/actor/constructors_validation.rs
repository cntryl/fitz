use super::*;

impl QueueActor {
    pub(in crate::domains::queue::actor) const READY_SHARDS: usize = 8;
    pub(in crate::domains::queue::actor) const ID_RESERVATION_BLOCK: u64 = 256;
    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) const META_VERSION_V2: u8 = 2;
    pub(in crate::domains::queue::actor) const HEADER_VERSION_V2: u8 = 2;
    #[allow(dead_code)]
    pub(in crate::domains::queue::actor) const ACK_DEDUP_TTL_MS: u64 = 5 * 60 * 1_000;
    pub(in crate::domains::queue::actor) const INDEX_VERSION_V1: u8 = 1;
    pub(in crate::domains::queue::actor) const INDEX_VERSION_V2: u8 = 2;
    pub(in crate::domains::queue::actor) const INDEX_META_VALID_MARKER: u8 = 1;
    pub(in crate::domains::queue::actor) const INDEX_META_NEXT_DELAY_NONE: u64 = u64::MAX;
    pub(in crate::domains::queue::actor) const RECORD_CACHE_LIMIT: usize = 16 * 1024;
    pub(in crate::domains::queue::actor) const RECORD_CACHE_FIFO_SLACK_MULTIPLIER: usize = 2;
    pub(in crate::domains::queue::actor) const BODY_CACHE_LIMIT: usize = 1024;
    pub(in crate::domains::queue::actor) const BODY_CACHE_LIMIT_BYTES: usize = 16 * 1024 * 1024;
    pub(in crate::domains::queue::actor) const BODY_CACHE_FIFO_SLACK_MULTIPLIER: usize = 2;

    /// Create a new queue actor using buffered commits for durable stores.
    pub fn new(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        Self::new_with_write_options(
            family,
            queue_key,
            store,
            max_attempts,
            dedup_store,
            cntryl_midge::WriteOptions::buffered(),
        )
    }

    /// Create a new queue actor with explicit commit policy selection.
    pub fn new_with_write_options(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        commit_write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        Self::try_new_with_write_options(
            family,
            queue_key,
            store,
            max_attempts,
            dedup_store,
            commit_write_options,
        )
        .expect("recover queue actor from store")
    }

    pub fn try_new_with_write_options(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        commit_write_options: cntryl_midge::WriteOptions,
    ) -> Result<Self, String> {
        Self::try_with_clock_and_write_options(
            family,
            queue_key,
            store,
            Box::new(SystemClock),
            max_attempts,
            dedup_store,
            commit_write_options,
        )
    }

    /// Create a new queue actor with a custom clock (for testing) using buffered commits.
    pub fn with_clock(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        clock: Box<dyn Clock>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        Self::with_clock_and_write_options(
            family,
            queue_key,
            store,
            clock,
            max_attempts,
            dedup_store,
            cntryl_midge::WriteOptions::buffered(),
        )
    }

    /// Create a new queue actor with a custom clock and explicit commit policy.
    pub fn with_clock_and_write_options(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        clock: Box<dyn Clock>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        commit_write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        Self::try_with_clock_and_write_options(
            family,
            queue_key,
            store,
            clock,
            max_attempts,
            dedup_store,
            commit_write_options,
        )
        .expect("recover queue actor from store")
    }

    pub fn try_with_clock_and_write_options(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        clock: Box<dyn Clock>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        commit_write_options: cntryl_midge::WriteOptions,
    ) -> Result<Self, String> {
        let now = Instant::now();

        let mut actor = Self {
            family,
            meta_key: Self::meta_key(&queue_key),
            index_meta_key: Self::index_meta_key(&queue_key),
            header_key_prefix: Self::header_key_prefix(&queue_key),
            body_key_prefix: Self::body_key_prefix(&queue_key),
            legacy_message_key_prefix: Self::legacy_message_key_prefix(&queue_key),
            ready_index_prefix: Self::ready_index_prefix(&queue_key),
            delayed_index_prefix: Self::delayed_index_prefix(&queue_key),
            inflight_index_prefix: Self::inflight_index_prefix(&queue_key),
            dlq_index_prefix: Self::dlq_index_prefix(&queue_key),
            ack_dedup_prefix: Self::ack_dedup_prefix(&queue_key),
            queue_key,
            store,
            commit_write_options,
            next_id: 1,
            next_ready_seq: 1,
            next_id_limit: 1,
            ready: VecDeque::new(),
            ready_shards: (0..Self::READY_SHARDS).map(|_| VecDeque::new()).collect(),
            persisted_ready_shards: (0..Self::READY_SHARDS).map(|_| VecDeque::new()).collect(),
            persisted_ready_count: 0,
            ready_count: 0,
            dlq_count: 0,
            oldest_ready_enqueued_at_ms: None,
            next_ready_shard: 0,
            records: HashMap::with_capacity_and_hasher(128, FxBuildHasher::default()),
            record_layouts: HashMap::with_capacity_and_hasher(128, FxBuildHasher::default()),
            record_cache_fifo: VecDeque::with_capacity(128),
            body_cache: HashMap::with_capacity_and_hasher(128, FxBuildHasher::default()),
            body_cache_fifo: VecDeque::with_capacity(Self::BODY_CACHE_LIMIT),
            body_cache_bytes: 0,
            inflight: HashMap::with_capacity_and_hasher(64, FxBuildHasher::default()),
            timers: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
            persisted_delayed: HashMap::with_capacity_and_hasher(64, FxBuildHasher::default()),
            persisted_dlq: HashMap::with_capacity_and_hasher(32, FxBuildHasher::default()),
            persisted_next_delayed_visibility_ms: None,
            index_meta_written: false,
            recovery_path: RecoveryPath::Empty,
            needs_wake_waiters: false,
            clock,
            max_attempts,
            dedup_store,
            enqueue_success_window: RollingRateWindow::new(),
            complete_success_window: RollingRateWindow::new(),
            // Initialize deadlines to now (will process on first receive if queues are not empty)
            next_expiration_deadline: now,
            next_delayed_deadline: now,
        };

        actor.recover_from_store()?;
        Ok(actor)
    }

    pub fn validate_persisted_state_for_existing_families(
        store: &cntryl_midge::Engine,
    ) -> Result<(), String> {
        let families = store
            .list_column_families()
            .map_err(|error| format!("list queue column families failed: {error}"))?;

        for family in families {
            if family.id() == 0 {
                continue;
            }
            Self::validate_persisted_state_for_family(store, family.id())?;
        }

        Ok(())
    }

    pub(in crate::domains::queue::actor) fn validate_persisted_state_for_family(
        store: &cntryl_midge::Engine,
        family: u32,
    ) -> Result<(), String> {
        let txn = store
            .begin_tx(family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| {
                format!(
                    "queue validation failed: family={} key_category=transaction error={:?}",
                    family, error
                )
            })?;
        let mut iter = txn.scan(&cntryl_midge::Query::new()).map_err(|error| {
            format!(
                "queue validation failed: family={} key_category=scan error={:?}",
                family, error
            )
        })?;
        let mut required_bodies = HashSet::<Vec<u8>>::new();
        let mut body_rows = HashSet::<Vec<u8>>::new();

        for (key, value) in iter.collect_all() {
            let Some(suffix) = storage_key::strip_domain_prefix(&key, DomainKeyspace::Queue) else {
                continue;
            };

            let Some((queue_prefix, family_marker, suffix_tail)) =
                Self::split_authoritative_key(suffix)
            else {
                continue;
            };

            match family_marker {
                QUEUE_KEY_FAMILY_META => {
                    let Some(meta) = Self::decode_meta(&value) else {
                        return Err(format!(
                            "queue validation failed: family={} key_category=meta error=invalid encoding",
                            family
                        ));
                    };
                    if meta.next_id == 0 {
                        return Err(format!(
                            "queue validation failed: family={} key_category=meta error=next_id is zero",
                            family
                        ));
                    }
                }
                QUEUE_KEY_FAMILY_HEADER => {
                    Self::validate_authoritative_message_id(family, "header", suffix_tail)?;
                    let is_split = Self::is_versioned_header(&value) || value.len() == 12;
                    if Self::decode_record_header(&value).is_err()
                        && Self::decode_legacy_record(value.clone()).is_err()
                    {
                        return Err(format!(
                            "queue validation failed: family={} key_category=header error=invalid encoding",
                            family
                        ));
                    }
                    if is_split {
                        required_bodies.insert(Self::body_suffix(queue_prefix, suffix_tail));
                    }
                }
                QUEUE_KEY_FAMILY_LEGACY_MESSAGE => {
                    Self::validate_authoritative_message_id(family, "legacy_message", suffix_tail)?;
                    Self::decode_legacy_record(value).map_err(|error| {
                        format!(
                            "queue validation failed: family={} key_category=legacy_message error={}",
                            family, error
                        )
                    })?;
                }
                QUEUE_KEY_FAMILY_BODY => {
                    Self::validate_authoritative_message_id(family, "body", suffix_tail)?;
                    body_rows.insert(Self::body_suffix(queue_prefix, suffix_tail));
                }
                _ => {}
            }
        }

        for required_body in required_bodies {
            if !body_rows.remove(&required_body) {
                return Err(format!(
                    "queue validation failed: family={} key_category=body error=missing body for split header",
                    family
                ));
            }
        }
        if !body_rows.is_empty() {
            return Err(format!(
                "queue validation failed: family={} key_category=body error=orphan body row",
                family
            ));
        }

        Ok(())
    }
}
