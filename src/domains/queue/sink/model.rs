pub(super) use crate::domains::queue::{
    projection::{QueueAdminProjection, QueueProjectionEntry, QueueProjectionState},
    QueueAdminSnapshot, QueueClientFrame, QueueClientRequest, QueueMetrics, QueueNotification,
    QueueSubscriptionMessage,
};
pub(super) use crate::domains::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
pub(super) use crate::observability as obs;
#[cfg(test)]
pub(super) use crate::protocol::frame_context::FrameContext;
pub(super) use crate::runtime::{DeliveryError, Envelope, MailboxSink, ManagedActor, Router};
pub(super) use parking_lot::Mutex;
pub(super) use std::collections::{HashMap, HashSet};
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
pub(super) use std::sync::Arc;
pub(super) use std::time::{Duration, Instant};

pub(super) struct WarmQueueActor {
    pub(super) actor: Arc<Mutex<crate::domains::queue::QueueActor>>,
    pub(super) last_used: Instant,
}

pub(super) struct QueueSubscription {
    pub(super) pattern: crate::runtime::matcher::Pattern,
    pub(super) session_id: u64,
    pub(super) subscription_id: u64,
    pub(super) subscriber: crate::runtime::routing::RouteAddress,
}

impl RoutedSubscription for QueueSubscription {
    fn pattern(&self) -> &crate::runtime::matcher::Pattern {
        &self.pattern
    }

    fn session_id(&self) -> u64 {
        self.session_id
    }

    fn subscription_id(&self) -> u64 {
        self.subscription_id
    }
}

#[derive(Clone, Copy)]
pub(super) struct QueueReadyNotification {
    pub(super) family_id: crate::runtime::routing::RouteFamily,
    pub(super) snapshot: QueueAdminSnapshot,
}

pub(super) const QUEUE_ACTOR_IDLE_TTL: Duration = Duration::from_mins(5);
pub(super) const QUEUE_IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
pub(super) const QUEUE_DEDUP_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Queue domain runtime core with per-queue `QueueActor` instances.
///
/// This core:
/// - Maintains per-queue `QueueActor` instances keyed by `QueueKey`
/// - Parses TLV frames to `QueueMessage`
/// - Dispatches to the correct actor based on route
/// - Returns responses
/// - Tracks queue-local watch subscriptions for the current broker process
/// - Exposes only warm in-memory queue/admin state for the current broker process
pub struct QueueDomainCore {
    /// Fitz storage facade over the current Midge engine.
    pub(super) store: crate::storage::FitzStorageEngine,
    /// Commit policy for queue persistence on this runtime.
    pub(super) queue_write_options: cntryl_midge::WriteOptions,
    /// Deduplication store shared by warm actors created through this sink.
    pub(super) dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    /// Per-queue actors keyed by `QueueKey`
    pub(super) actors: Mutex<HashMap<crate::domains::queue::QueueKey, WarmQueueActor>>,
    /// Queue-local watch subscriptions scoped to this broker process.
    pub(super) families: Mutex<HashMap<u64, RoutedSubscriptionSet<QueueSubscription>>>,
    pub(super) next_sub_id: AtomicU64,
    pub(super) ready_states: Mutex<HashMap<crate::domains::queue::QueueKey, bool>>,
    /// Router for routing response envelopes back
    pub(super) router: Arc<Router>,
    pub(super) projection: QueueAdminProjection,
    pub(super) metrics: Option<QueueMetrics>,
    pub(super) active: AtomicBool,
    pub(super) next_idle_sweep_at: Mutex<Instant>,
    pub(super) next_dedup_sweep_at: Mutex<Instant>,
    pub(super) dirty_fast_flush_families: Mutex<HashSet<u32>>,
    pub(super) fast_flush_interval: Option<Duration>,
    pub(super) next_fast_flush_at: Mutex<Instant>,
}

pub(super) enum QueueDomainCommand {
    Deliver(
        Envelope,
        crossbeam_channel::Sender<Result<(), DeliveryError>>,
    ),
}

pub(super) struct QueueDomainActor {
    pub(super) core: Arc<QueueDomainCore>,
}

pub(super) struct QueueDomainRuntime<'a> {
    pub(super) core: &'a QueueDomainCore,
}

/// Queue domain sink with a managed actor mailbox in front of queue runtime state.
pub struct QueueDomainSink {
    pub(super) core: Arc<QueueDomainCore>,
    pub(super) actor: ManagedActor<QueueDomainCommand>,
}

impl std::ops::Deref for QueueDomainSink {
    type Target = QueueDomainCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl std::ops::DerefMut for QueueDomainSink {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::get_mut(&mut self.core).expect("Queue sink builders must run before sharing the sink")
    }
}

impl std::ops::Deref for QueueDomainRuntime<'_> {
    type Target = QueueDomainCore;

    fn deref(&self) -> &Self::Target {
        self.core
    }
}
