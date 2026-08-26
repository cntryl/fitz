#[cfg(test)]
pub(super) use crate::dispatch::protocol::frame_context::FrameContext;
pub(super) use crate::domains::queue::{
    projection::{QueueAdminProjection, QueueProjectionEntry, QueueProjectionState},
    MessageId, QueueActorLiveCounts, QueueClientFrame, QueueClientRequest, QueueKey, QueueMetrics,
    QueueNotification, QueueSubscriptionMessage,
};
pub(super) use crate::domains::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
pub(super) use crate::observability as obs;
pub(super) use crate::runtime::{DeliveryError, Envelope, MailboxSink, ManagedActor, Router};
pub(super) use parking_lot::Mutex;
pub(super) use std::collections::{HashMap, HashSet, VecDeque};
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
    pub(super) counts: QueueActorLiveCounts,
}

pub(super) struct PendingQueueReserve {
    pub(super) envelope: Envelope,
    pub(super) meta: crate::runtime::ClientFrameMeta,
    pub(super) request_started: Option<Instant>,
    pub(super) message: crate::domains::queue::protocol::QueueMessage,
    pub(super) deadline: Instant,
}

pub(super) const QUEUE_ACTOR_IDLE_TTL: Duration = Duration::from_mins(5);
pub(super) const QUEUE_IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
pub(super) const QUEUE_IDLE_SWEEP_BATCH_SIZE: usize = 64;
pub(super) const QUEUE_DEDUP_SWEEP_INTERVAL: Duration = Duration::from_secs(30);
pub(super) use crate::domains::queue::actor::QUEUE_ACTOR_REPLY_TIMEOUT;

/// Queue domain runtime core with per-queue `QueueActor` instances.
///
/// This core:
/// - Maintains per-queue `QueueActor` instances keyed by `QueueKey`
/// - Parses TLV frames to `QueueMessage`
/// - Dispatches to the correct actor based on route
/// - Returns responses
/// - Tracks queue-local watch subscriptions for the current broker process
/// - Exposes only warm in-memory queue/admin state for the current broker process
pub(super) struct QueueDomainCore {
    /// Measured delivery service time in microseconds, written by the actor
    /// and read by admission to size its window.
    pub(super) delivery_service_us: ServiceEstimateUs,
    /// Fitz storage facade over the current Midge engine.
    pub(super) store: crate::storage::FitzStorageEngine,
    /// Commit policy for queue persistence on this runtime.
    pub(super) queue_write_options: cntryl_midge::WriteOptions,
    /// Deduplication store shared by warm actors created through this sink.
    pub(super) dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    /// Per-queue actors keyed by `QueueKey`
    pub(super) actors: Mutex<HashMap<crate::domains::queue::QueueKey, WarmQueueActor>>,
    /// Round-robin actor keys used to bound idle-sweep work per tick.
    pub(super) idle_sweep_keys: Mutex<VecDeque<crate::domains::queue::QueueKey>>,
    /// Durable and live queue identities available to wildcard reserve selectors.
    pub(super) known_queue_keys: Mutex<HashSet<crate::domains::queue::QueueKey>>,
    /// Startup inventory failure surfaced by wildcard reserve on infallible constructors.
    pub(super) inventory_error: Mutex<Option<String>>,
    /// Bounded, allocation-free rotation seed for fair wildcard reserve starts.
    pub(super) wildcard_reserve_sequence: AtomicU64,
    /// Queue-local watch subscriptions scoped to this broker process.
    pub(super) families: Mutex<HashMap<u64, RoutedSubscriptionSet<QueueSubscription>>>,
    /// Sessions disconnect cleanup has already run for; guards against a
    /// stale queued request recreating a subscription or pending reserve.
    /// See `cleanup.rs`.
    pub(super) cleaned_up_sessions: Mutex<super::cleanup::CleanedUpSessions>,
    pub(super) next_sub_id: AtomicU64,
    pub(super) ready_states: Mutex<HashMap<crate::domains::queue::QueueKey, bool>>,
    /// FIFO long-poll RESERVE requests waiting for a matching ready message.
    pub(super) pending_reserves: Mutex<VecDeque<PendingQueueReserve>>,
    /// Router for routing response envelopes back
    pub(super) router: Arc<Router>,
    pub(super) projection: QueueAdminProjection,
    pub(super) metrics: Option<QueueMetrics>,
    pub(super) active: AtomicBool,
    pub(super) runtime_sweep_pending: AtomicBool,
    #[cfg(test)]
    pub(super) panic_next_runtime_sweep: AtomicBool,
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
        // Released when this command is finished with, not when the caller
        // stops waiting for it.
        Option<QueueAdmissionSlot>,
    ),
    RefreshAdminSnapshotIfDirty(crossbeam_channel::Sender<()>),
    ReadLiveCounts(crossbeam_channel::Sender<QueueLiveCounts>),
    CleanupSession(u64, crossbeam_channel::Sender<()>),
    SweepRuntimeStateAt(Instant, Option<crossbeam_channel::Sender<()>>),
    ReplayDeadLetter(
        QueueKey,
        MessageId,
        crossbeam_channel::Sender<Result<bool, String>>,
    ),
    PurgeDeadLetter(
        QueueKey,
        MessageId,
        crossbeam_channel::Sender<Result<bool, String>>,
    ),
    #[cfg(test)]
    PanicForTests,
}

#[derive(Default)]
pub(super) struct QueueLiveCounts {
    pub(super) pending: usize,
    pub(super) ready: usize,
    pub(super) delayed: usize,
    pub(super) inflight: usize,
    pub(super) dead_letters: usize,
}

/// Running estimate of how long one delivery takes the actor to serve,
/// in microseconds. Written by the actor, read by admission.
pub(super) type ServiceEstimateUs = Arc<std::sync::atomic::AtomicU64>;

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
    /// Client requests currently blocked on the actor's reply.
    pub(super) inflight_client_deliveries: Arc<std::sync::atomic::AtomicUsize>,
}

impl std::ops::Deref for QueueDomainRuntime<'_> {
    type Target = QueueDomainCore;

    fn deref(&self) -> &Self::Target {
        self.core
    }
}

/// Hard ceiling on concurrent client requests, whatever the measured service
/// time suggests.
pub(super) const QUEUE_ADMISSION_MAX_WINDOW: usize = 64;

/// Fraction of the reply deadline the admitted backlog may consume, leaving
/// headroom for enqueue, scheduling, and a slower-than-average commit.
const QUEUE_ADMISSION_BUDGET_NUMERATOR: u32 = 4;
const QUEUE_ADMISSION_BUDGET_DENOMINATOR: u32 = 5;

/// Assumed per-delivery service time until the actor has measured one.
const QUEUE_ADMISSION_ASSUMED_SERVICE_US: u64 = 5_000;

/// How many client requests may be in flight against the queue actor.
///
/// Queued concurrency adds no throughput: the actor serves deliveries one at a
/// time, so admitting `n` requests commits the tail caller to `n x
/// service_time`. A fixed window therefore cannot bound the deadline - at 20ms
/// per synchronous commit, a 64-deep window needs 1.28s and the tail caller
/// times out with an indeterminate outcome while its command still executes,
/// which is precisely what admission exists to prevent.
///
/// The window is derived from observed service time instead, so the admitted
/// backlog stays inside the reply deadline as the backend gets slower. It never
/// drops below 1: the active operation is always admitted, or nothing would
/// ever run to produce a new measurement.
pub(super) fn queue_admission_window(service_us: u64) -> usize {
    let deadline_us = u64::try_from(QUEUE_ACTOR_REPLY_TIMEOUT.as_micros()).unwrap_or(u64::MAX);
    let budget_us = deadline_us.saturating_mul(u64::from(QUEUE_ADMISSION_BUDGET_NUMERATOR))
        / u64::from(QUEUE_ADMISSION_BUDGET_DENOMINATOR);
    let service_us = service_us.max(1);
    let window = usize::try_from(budget_us / service_us).unwrap_or(QUEUE_ADMISSION_MAX_WINDOW);
    window.clamp(1, QUEUE_ADMISSION_MAX_WINDOW)
}

/// Blend a fresh delivery duration into the running service estimate.
///
/// A simple exponential average: fast enough to react to a backend slowdown
/// within a few deliveries, damped enough that one outlier does not slam the
/// window shut.
pub(super) fn blend_service_estimate(previous_us: u64, observed_us: u64) -> u64 {
    if previous_us == 0 {
        return observed_us.max(1);
    }
    ((previous_us * 3) + observed_us.max(1)) / 4
}

/// Admit one client delivery, sizing the window from measured service time and
/// preferring terminal actor failure over a retryable rejection.
///
/// # Errors
///
/// `MailboxFull` when the live actor already has as much work as its deadline
/// can serve, or `ActorStopped` when the actor has terminated.
pub(super) fn admit_client_delivery(
    inflight: &Arc<std::sync::atomic::AtomicUsize>,
    service: &ServiceEstimateUs,
    actor_running: bool,
) -> Result<QueueAdmissionSlot, crate::runtime::DeliveryError> {
    let window = queue_admission_window(service.load(std::sync::atomic::Ordering::Relaxed));
    try_admit_queue_delivery(inflight, window)
        .map_err(|error| classify_admission_failure(error, actor_running))
}

/// Fold one observed delivery duration into the shared estimate.
pub(super) fn record_service_sample(estimate: &ServiceEstimateUs, started_at: Instant) {
    use std::sync::atomic::Ordering;

    let observed_us = u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX);
    let previous = estimate.load(Ordering::Relaxed);
    estimate.store(
        blend_service_estimate(previous, observed_us),
        Ordering::Relaxed,
    );
}

/// A full window means "retry later" only while the actor is alive.
///
/// A worker that fails closed leaves its mailbox - and every admitted slot -
/// alive for the sink's lifetime, so admission would keep answering
/// `MailboxFull` and clients would retry a dead domain forever.
pub(super) fn classify_admission_failure(
    error: crate::runtime::DeliveryError,
    actor_running: bool,
) -> crate::runtime::DeliveryError {
    if actor_running {
        error
    } else {
        crate::runtime::DeliveryError::ActorStopped
    }
}

/// Starting value for the service estimate before anything is measured.
pub(super) const fn assumed_service_us() -> u64 {
    QUEUE_ADMISSION_ASSUMED_SERVICE_US
}

/// Holds an admission slot until the queued command is finished with.
///
/// The slot travels with the command rather than with the blocked caller.
/// A caller that gives up on `recv_timeout` has NOT cancelled anything: the
/// `Deliver` command is still queued and the actor will still run
/// `deliver_envelope`, only to find the reply channel gone. Releasing on
/// caller timeout would therefore recycle slots while the work they admitted
/// is still pending, letting a sustained burst pile accepted - and
/// indeterminate - mutations up to the full mailbox depth.
///
/// Dropping with the command covers completion, actor death, and mailbox
/// teardown alike, so a slot can never leak.
#[derive(Debug)]
pub(super) struct QueueAdmissionSlot {
    inflight: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for QueueAdmissionSlot {
    fn drop(&mut self) {
        self.inflight
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

/// Reserve an in-flight slot, or refuse the request.
///
/// The reservation is a single atomic compare-and-update, so the limit holds
/// under concurrency: callers cannot collectively exceed it by all observing
/// room before any of them commits.
///
/// # Errors
///
/// Returns `MailboxFull` when the actor already has as many blocked callers as
/// its deadline can serve. That is deliberately the same error an actually-full
/// mailbox produces: nothing was enqueued, so ingress answers with a retryable
/// code and the client may safely re-send.
pub(super) fn try_admit_queue_delivery(
    inflight: &Arc<std::sync::atomic::AtomicUsize>,
    window: usize,
) -> Result<QueueAdmissionSlot, crate::runtime::DeliveryError> {
    use std::sync::atomic::Ordering;

    inflight
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < window).then_some(current + 1)
        })
        .map(|_| QueueAdmissionSlot {
            inflight: Arc::clone(inflight),
        })
        .map_err(|current| crate::runtime::DeliveryError::MailboxFull {
            capacity: window,
            current_len: current,
        })
}
