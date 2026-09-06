//! Queue actor construction and dependency initialization.

use super::{
    Arc, BinaryHeap, Clock, FxBuildHasher, HashMap, Instant, QueueActor, QueueKey, RecoveryPath,
    RollingRateWindow, RouteFamily, SystemClock, VecDeque,
};

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
            recovery_store: Arc::new(super::recovery_store::QueueRecoveryStore::new(
                store.clone(),
                queue_key.clone(),
            )),
            body_key_prefix: Self::body_key_prefix(&queue_key),
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
}
