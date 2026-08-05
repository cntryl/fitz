use super::{
    storage_key, Arc, BinaryHeap, Clock, DomainKeyspace, FxBuildHasher, HashMap, HashSet, Instant,
    QueueActor, QueueKey, RecoveryPath, RollingRateWindow, RouteFamily, SystemClock, VecDeque,
    QUEUE_KEY_FAMILY_BODY, QUEUE_KEY_FAMILY_DELAYED_INDEX, QUEUE_KEY_FAMILY_DLQ_INDEX,
    QUEUE_KEY_FAMILY_HEADER, QUEUE_KEY_FAMILY_INDEX_META, QUEUE_KEY_FAMILY_INFLIGHT_INDEX,
    QUEUE_KEY_FAMILY_META, QUEUE_KEY_FAMILY_READY_INDEX,
};

struct QueueValidationRow {
    key: Vec<u8>,
    queue_storage_prefix: Vec<u8>,
    message_id: u64,
    category: &'static str,
}

#[derive(Default)]
struct QueueValidationScan {
    required_bodies: HashMap<Vec<u8>, QueueValidationRow>,
    body_rows: HashMap<Vec<u8>, QueueValidationRow>,
    index_keys: HashMap<Vec<u8>, Vec<Vec<u8>>>,
}

impl QueueActor {
    pub(in crate::domains::queue::actor) const READY_SHARDS: usize = 8;
    pub(in crate::domains::queue::actor) const ID_RESERVATION_BLOCK: u64 = 256;
    pub(in crate::domains::queue::actor) const HEADER_VERSION_V2: u8 = 2;
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
    ///
    /// # Panics
    ///
    /// Panics when persisted queue state cannot be recovered.
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

    /// # Errors
    ///
    /// Returns an error when persisted queue state cannot be recovered.
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
    ///
    /// # Panics
    ///
    /// Panics when persisted queue state cannot be recovered.
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

    /// # Errors
    ///
    /// Returns an error when persisted queue state cannot be recovered.
    pub fn try_with_clock_and_write_options(
        family: RouteFamily,
        queue_key: QueueKey,
        store: Arc<cntryl_midge::MidgeEngine>,
        clock: Box<dyn Clock>,
        max_attempts: Option<u32>,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        commit_write_options: cntryl_midge::WriteOptions,
    ) -> Result<Self, String> {
        if family != queue_key.family {
            return Err(format!(
                "queue actor family mismatch: actor={}, queue={}",
                family.as_u64(),
                queue_key.family.as_u64()
            ));
        }

        let now = Instant::now();

        let mut actor = Self {
            meta_key: Self::meta_key(&queue_key),
            index_meta_key: Self::index_meta_key(&queue_key),
            header_key_prefix: Self::header_key_prefix(&queue_key),
            body_key_prefix: Self::body_key_prefix(&queue_key),
            ready_index_prefix: Self::ready_index_prefix(&queue_key),
            delayed_index_prefix: Self::delayed_index_prefix(&queue_key),
            dlq_index_prefix: Self::dlq_index_prefix(&queue_key),
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
            records: HashMap::with_capacity_and_hasher(128, FxBuildHasher),
            record_cache_fifo: VecDeque::with_capacity(128),
            body_cache: HashMap::with_capacity_and_hasher(128, FxBuildHasher),
            body_cache_fifo: VecDeque::with_capacity(Self::BODY_CACHE_LIMIT),
            body_cache_bytes: 0,
            inflight: HashMap::with_capacity_and_hasher(64, FxBuildHasher),
            timers: BinaryHeap::new(),
            delayed: BinaryHeap::new(),
            persisted_delayed: HashMap::with_capacity_and_hasher(64, FxBuildHasher),
            persisted_dlq: HashMap::with_capacity_and_hasher(32, FxBuildHasher),
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

    /// Validate persisted queue state and reconcile policy-permitted partial rows.
    ///
    /// # Errors
    ///
    /// Returns an error when persisted queue state cannot be validated or reconciled.
    pub(crate) fn prepare_persisted_state_for_existing_families(
        store: &cntryl_midge::Engine,
        queue_write_options: cntryl_midge::WriteOptions,
        recovery_write_options: cntryl_midge::WriteOptions,
    ) -> Result<(), String> {
        if !queue_write_options.is_best_effort() {
            return Self::validate_persisted_state_for_existing_families(store);
        }

        let families = store
            .list_column_families()
            .map_err(|error| format!("list queue column families failed: {error}"))?;

        for family in families {
            if family.id() == 0 {
                continue;
            }
            Self::reconcile_fast_persisted_state_for_family(
                store,
                family.id(),
                recovery_write_options,
            )?;
        }

        Ok(())
    }

    /// # Errors
    ///
    /// Returns an error when any existing queue family contains invalid persisted state.
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
                    "queue reconciliation failed: family={family} key_category=transaction error={error:?}"
                )
            })?;
        let scan = Self::scan_persisted_state_for_family(&txn, family)?;

        if scan
            .required_bodies
            .keys()
            .any(|body_key| !scan.body_rows.contains_key(body_key))
        {
            return Err(format!(
                "queue validation failed: family={family} key_category=body error=missing body for split header"
            ));
        }
        if scan
            .body_rows
            .keys()
            .any(|body_key| !scan.required_bodies.contains_key(body_key))
        {
            return Err(format!(
                "queue validation failed: family={family} key_category=body error=orphan body row"
            ));
        }

        Ok(())
    }

    fn scan_persisted_state_for_family(
        txn: &cntryl_midge::Transaction,
        family: u32,
    ) -> Result<QueueValidationScan, String> {
        let iter = txn.scan(&cntryl_midge::Query::new()).map_err(|error| {
            format!("queue validation failed: family={family} key_category=scan error={error:?}")
        })?;
        let mut scan = QueueValidationScan::default();

        for (key, value) in iter.try_collect().map_err(|error| {
            format!("queue validation failed: family={family} key_category=scan error={error:?}")
        })? {
            let Some(suffix) = storage_key::strip_domain_prefix(&key, DomainKeyspace::Queue) else {
                continue;
            };

            let Some((queue_prefix, family_marker, suffix_tail)) =
                Self::split_authoritative_key(suffix)
            else {
                continue;
            };
            let mut queue_storage_prefix = key[..key.len() - suffix.len()].to_vec();
            queue_storage_prefix.extend_from_slice(queue_prefix);

            match family_marker {
                QUEUE_KEY_FAMILY_META => {
                    let Some(next_id) = Self::decode_meta(&value) else {
                        return Err(format!(
                            "queue validation failed: family={family} key_category=meta error=invalid encoding"
                        ));
                    };
                    if next_id == 0 {
                        return Err(format!(
                            "queue validation failed: family={family} key_category=meta error=next_id is zero"
                        ));
                    }
                }
                QUEUE_KEY_FAMILY_HEADER => {
                    Self::validate_authoritative_message_id(family, "header", suffix_tail)?;
                    if Self::decode_record_header(&value).is_err() {
                        return Err(format!(
                            "queue validation failed: family={family} key_category=header error=invalid encoding"
                        ));
                    }
                    let body_key = Self::queue_storage_key(
                        &queue_storage_prefix,
                        QUEUE_KEY_FAMILY_BODY,
                        suffix_tail,
                    );
                    scan.required_bodies.insert(
                        body_key,
                        QueueValidationRow {
                            key: key.to_vec(),
                            queue_storage_prefix,
                            message_id: Self::message_id_from_validated_tail(suffix_tail),
                            category: "header",
                        },
                    );
                }
                QUEUE_KEY_FAMILY_BODY => {
                    Self::validate_authoritative_message_id(family, "body", suffix_tail)?;
                    scan.body_rows.insert(
                        key.to_vec(),
                        QueueValidationRow {
                            key: key.to_vec(),
                            queue_storage_prefix,
                            message_id: Self::message_id_from_validated_tail(suffix_tail),
                            category: "body",
                        },
                    );
                }
                QUEUE_KEY_FAMILY_INDEX_META
                | QUEUE_KEY_FAMILY_READY_INDEX
                | QUEUE_KEY_FAMILY_DELAYED_INDEX
                | QUEUE_KEY_FAMILY_INFLIGHT_INDEX
                | QUEUE_KEY_FAMILY_DLQ_INDEX => {
                    scan.index_keys
                        .entry(queue_storage_prefix)
                        .or_default()
                        .push(key.to_vec());
                }
                _ => {}
            }
        }

        Ok(scan)
    }

    fn reconcile_fast_persisted_state_for_family(
        store: &cntryl_midge::Engine,
        family: u32,
        recovery_write_options: cntryl_midge::WriteOptions,
    ) -> Result<(), String> {
        let mut txn = store
            .begin_tx(family, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|error| {
                format!(
                    "queue validation failed: family={family} key_category=transaction error={error:?}"
                )
            })?;
        let QueueValidationScan {
            required_bodies,
            mut body_rows,
            index_keys,
        } = Self::scan_persisted_state_for_family(&txn, family)?;
        let mut incomplete_rows = Vec::new();

        for (body_key, header_row) in required_bodies {
            if body_rows.remove(&body_key).is_none() {
                incomplete_rows.push(header_row);
            }
        }
        incomplete_rows.extend(body_rows.into_values());

        if incomplete_rows.is_empty() {
            return Ok(());
        }
        if !recovery_write_options.is_sync()
            && !recovery_write_options.is_cloud_async()
            && !recovery_write_options.is_cloud_strict()
        {
            return Err(format!(
                "queue reconciliation failed: family={family} key_category=reconciliation error=durable recovery write policy required"
            ));
        }

        let mut affected_queues = HashSet::new();
        for row in &incomplete_rows {
            txn.delete(row.key.clone()).map_err(|error| {
                format!(
                    "queue reconciliation failed: family={family} key_category={} error={error:?}",
                    row.category
                )
            })?;
            affected_queues.insert(row.queue_storage_prefix.clone());
        }

        for queue_storage_prefix in &affected_queues {
            for key in index_keys.get(queue_storage_prefix).into_iter().flatten() {
                txn.delete(key.clone()).map_err(|error| {
                    format!(
                        "queue reconciliation failed: family={family} key_category=index error={error:?}"
                    )
                })?;
            }
        }

        txn.commit(recovery_write_options).map_err(|error| {
            format!(
                "queue reconciliation failed: family={family} key_category=commit error={error:?}"
            )
        })?;
        for row in &incomplete_rows {
            tracing::warn!(
                domain = "queue",
                route_family = family,
                key_category = row.category,
                message_id = row.message_id,
                "Discarded incomplete fast-policy queue record during startup"
            );
        }
        tracing::warn!(
            domain = "queue",
            route_family = family,
            discarded_rows = incomplete_rows.len(),
            affected_queues = affected_queues.len(),
            "Reconciled incomplete fast-policy queue records and invalidated indexes"
        );
        Ok(())
    }

    fn queue_storage_key(
        queue_storage_prefix: &[u8],
        family_marker: u8,
        suffix_tail: &[u8],
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(queue_storage_prefix.len() + 1 + suffix_tail.len());
        key.extend_from_slice(queue_storage_prefix);
        key.push(family_marker);
        key.extend_from_slice(suffix_tail);
        key
    }

    fn message_id_from_validated_tail(suffix_tail: &[u8]) -> u64 {
        u64::from_be_bytes(
            suffix_tail
                .try_into()
                .expect("validated queue message id must contain eight bytes"),
        )
    }
}
