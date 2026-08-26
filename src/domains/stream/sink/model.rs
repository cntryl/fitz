pub(super) use crate::dispatch::protocol::payload_codec::PayloadEncoder;
pub(super) use crate::domains::stream::store::StreamAdminRecord;
pub(super) use crate::domains::stream::StreamMetrics;
pub(super) use crate::domains::stream::{
    ReadResponse, StreamActor, StreamClientFrame, StreamClientRequest, StreamClientResponseBody,
    StreamFilteredReason, StreamMetadata, StreamReadItem, StreamRecord, StreamStorageLayout,
    StreamStore,
};
pub(super) use crate::domains::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
pub(super) use crate::runtime::routing::{route_triplet, Route, RouteAddress, RouteFamily};
pub(super) use crate::runtime::{
    DeliveryError, Envelope, KeyedActorPool, MailboxSink, ManagedActor, Router,
};
pub(super) use parking_lot::Mutex;
pub(super) use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
pub(super) use std::sync::{Arc, Weak};
pub(super) use std::time::{Duration, Instant};

pub(super) struct StreamSubscription {
    pub(super) pattern: crate::runtime::matcher::Pattern,
    pub(super) session_id: u64,
    pub(super) subscription_id: u64,
    pub(super) subscriber: RouteAddress,
}

#[derive(Clone)]
pub(super) struct StreamNotificationTarget {
    pub(super) session_id: u64,
    pub(super) subscription_id: u64,
    pub(super) subscriber: RouteAddress,
}

#[derive(Clone)]
pub(super) enum StreamVisibilityFrontier {
    Resource,
    Area {
        realm: String,
        area: String,
        last_offset: u64,
    },
    Realm {
        realm: String,
        last_offset: u64,
    },
    Global {
        last_offset: u64,
    },
}

pub(super) struct PendingStreamNotification {
    pub(super) target: StreamNotificationTarget,
    pub(super) pattern: String,
    pub(super) event: crate::runtime::DomainPublishEvent,
    pub(super) frontier: StreamVisibilityFrontier,
}

pub(super) struct ReadyStreamNotification {
    pub(super) target: StreamNotificationTarget,
    pub(super) event: crate::runtime::DomainPublishEvent,
}

impl RoutedSubscription for StreamSubscription {
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

pub struct AdminStreamReadRequest<'a> {
    pub family: RouteFamily,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub from_offset: u64,
    pub limit: u64,
    pub discriminator: Option<String>,
}

#[derive(Clone, Copy)]
pub(super) struct StreamReadExecution<'a> {
    pub(super) family_id: RouteFamily,
    pub(super) route: &'a Route,
    pub(super) from_offset: u64,
    pub(super) limit: u64,
    pub(super) max_bytes: Option<usize>,
    pub(super) filter: Option<&'a crate::domains::stream::protocol::StreamFilterSet>,
    pub(super) cursor_fingerprint: Option<u64>,
    pub(super) captured_watermark: Option<u64>,
}

/// Storage-mode-compatible write options selected before Stream initialization.
#[derive(Clone, Copy)]
pub struct StreamStorageWriteOptions {
    sync_intent: cntryl_midge::WriteOptions,
    buffered_intent: cntryl_midge::WriteOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSinkInitError(String);

impl StreamSinkInitError {
    pub(super) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for StreamSinkInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for StreamSinkInitError {}

impl StreamStorageWriteOptions {
    #[must_use]
    pub fn new(
        sync_intent: cntryl_midge::WriteOptions,
        buffered_intent: cntryl_midge::WriteOptions,
    ) -> Self {
        Self {
            sync_intent,
            buffered_intent,
        }
    }

    #[must_use]
    pub fn local() -> Self {
        Self::new(
            cntryl_midge::WriteOptions::sync(),
            cntryl_midge::WriteOptions::buffered(),
        )
    }

    #[must_use]
    pub fn cloud_background() -> Self {
        Self::new(
            cntryl_midge::WriteOptions::cloud_async(),
            cntryl_midge::WriteOptions::cloud_async(),
        )
    }

    #[must_use]
    pub fn cloud_strict() -> Self {
        Self::new(
            cntryl_midge::WriteOptions::cloud_strict(),
            cntryl_midge::WriteOptions::cloud_async(),
        )
    }

    pub(super) fn sync_intent(self) -> cntryl_midge::WriteOptions {
        self.sync_intent
    }

    pub(super) fn buffered_intent(self) -> cntryl_midge::WriteOptions {
        self.buffered_intent
    }
}

pub(super) struct StreamAdminReadCommand {
    pub(super) request: AdminStreamReadRequestOwned,
    pub(super) reply: crossbeam_channel::Sender<
        Result<
            (
                Vec<crate::domains::stream::protocol::StreamReadItem>,
                crate::domains::stream::protocol::ReadCursor,
            ),
            String,
        >,
    >,
}

pub(super) struct AdminStreamReadRequestOwned {
    pub(super) family: RouteFamily,
    pub(super) realm: String,
    pub(super) area: String,
    pub(super) resource: String,
    pub(super) from_offset: u64,
    pub(super) limit: u64,
    pub(super) discriminator: Option<String>,
}

impl AdminStreamReadRequestOwned {
    pub(super) fn as_borrowed(&self) -> AdminStreamReadRequest<'_> {
        AdminStreamReadRequest {
            family: self.family,
            realm: &self.realm,
            area: &self.area,
            resource: &self.resource,
            from_offset: self.from_offset,
            limit: self.limit,
            discriminator: self.discriminator.clone(),
        }
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub(super) struct StreamResourceScope {
    pub(super) family: RouteFamily,
    pub(super) realm: String,
    pub(super) area: String,
    pub(super) resource: String,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub(super) struct StreamAreaScope {
    pub(super) family: RouteFamily,
    pub(super) realm: String,
    pub(super) area: String,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub(super) struct StreamRealmScope {
    pub(super) family: RouteFamily,
    pub(super) realm: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) enum StreamWorkKey {
    Resource(StreamResourceScope),
    Selector(String),
    SubscriptionSession(u64),
    Notification(String),
    UnresolvedSession(u64),
}

pub(super) struct CommitNotification {
    pub(super) family: RouteFamily,
    pub(super) route: Route,
    pub(super) payload: bytes::Bytes,
}

pub(super) struct OperationOutcome {
    pub(super) response: StreamClientResponseBody,
    pub(super) notification: Option<CommitNotification>,
    pub(super) admin_dirty: bool,
}

impl
    From<(
        StreamClientResponseBody,
        Option<(RouteFamily, Route, bytes::Bytes)>,
        bool,
    )> for OperationOutcome
{
    fn from(
        (response, notification, admin_dirty): (
            StreamClientResponseBody,
            Option<(RouteFamily, Route, bytes::Bytes)>,
            bool,
        ),
    ) -> Self {
        Self {
            response,
            notification: notification.map(|(family, route, payload)| CommitNotification {
                family,
                route,
                payload,
            }),
            admin_dirty,
        }
    }
}

#[derive(Default)]
pub(super) struct StreamRealmSnapshot {
    pub(super) areas: BTreeSet<String>,
    pub(super) resource_count: usize,
    pub(super) families: BTreeSet<u64>,
}

#[derive(Default)]
pub(super) struct StreamAreaSnapshot {
    pub(super) resource_count: usize,
    pub(super) families: BTreeSet<u64>,
}

pub(super) const STREAM_OPERATIONS_TOTAL: &str = "fitz_stream_operations_total";

impl StreamResourceScope {
    pub(super) fn resource_route(&self) -> Route {
        Route::new(format!(
            "stream://{}/{}/{}",
            self.realm, self.area, self.resource
        ))
    }
}

#[derive(Clone)]
pub(super) struct StreamSessionOwner {
    pub(super) key: StreamResourceScope,
    pub(super) owner_session_id: u64,
    pub(super) actor: Arc<Mutex<StreamActor>>,
}

pub(super) struct SubscriptionRegistry {
    pub(super) families: Mutex<HashMap<u64, RoutedSubscriptionSet<StreamSubscription>>>,
    pub(super) next_id: Arc<AtomicU64>,
    pub(super) pending: Mutex<Vec<PendingStreamNotification>>,
}

impl SubscriptionRegistry {
    pub(super) fn new(next_id: Arc<AtomicU64>) -> Self {
        Self {
            families: Mutex::new(HashMap::new()),
            next_id,
            pending: Mutex::new(Vec::new()),
        }
    }
}

pub(super) struct AdminSnapshotState {
    pub(super) read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(super) dirty: Arc<AtomicBool>,
}

pub(super) struct CleanedUpSessions {
    ids: HashSet<u64>,
    order: std::collections::VecDeque<u64>,
}

impl CleanedUpSessions {
    pub(super) fn new() -> Self {
        Self {
            ids: HashSet::new(),
            order: std::collections::VecDeque::new(),
        }
    }

    pub(super) fn contains(&self, session_id: u64) -> bool {
        self.ids.contains(&session_id)
    }

    pub(super) fn insert(&mut self, session_id: u64) {
        if !self.ids.insert(session_id) {
            return;
        }
        self.order.push_back(session_id);
        while self.order.len() > crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY {
            if let Some(expired) = self.order.pop_front() {
                self.ids.remove(&expired);
            }
        }
    }
}

impl AdminSnapshotState {
    pub(super) fn new(
        read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        dirty: Arc<AtomicBool>,
    ) -> Self {
        Self { read_model, dirty }
    }

    pub(super) fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub(super) fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::AcqRel)
    }
}

pub(super) struct WatermarkCoordinators {
    pub(super) area: Arc<
        KeyedActorPool<
            StreamAreaScope,
            crate::domains::stream::protocol::StreamCoordinationMessage,
        >,
    >,
    pub(super) realm: Arc<
        KeyedActorPool<
            StreamRealmScope,
            crate::domains::stream::protocol::StreamCoordinationMessage,
        >,
    >,
}

pub(super) struct StreamDomainCore {
    pub(super) store: crate::storage::FitzStorageEngine,
    pub(super) stream_store: Arc<StreamStore>,
    pub(super) actors: Mutex<HashMap<StreamResourceScope, Arc<Mutex<StreamActor>>>>,
    pub(super) session_owners: Mutex<HashMap<u64, StreamSessionOwner>>,
    pub(super) cleaned_up_sessions: Mutex<CleanedUpSessions>,
    pub(super) subscriptions: SubscriptionRegistry,
    pub(super) next_session_id: Arc<AtomicU64>,
    pub(super) cursor_integrity_key: Arc<[u8; 32]>,
    pub(super) router: Arc<Router>,
    pub(super) admin_snapshot: AdminSnapshotState,
    pub(super) sync_write_mode: crate::domains::stream::protocol::StreamWriteMode,
    pub(super) metrics: Option<StreamMetrics>,
    pub(super) active: Arc<AtomicBool>,
    /// Weak family-core registry used only to aggregate live/admin views.
    /// Mutable delivery state itself remains owned by each family core.
    pub(super) family_cores: Arc<Mutex<BTreeMap<u64, Weak<StreamDomainCore>>>>,
    pub(super) watermark_coordinators: WatermarkCoordinators,
    /// Measured non-family-actor delivery service time, written by the actor
    /// and read by admission to size its window. Unused by family cores -
    /// `deliver_to_family` never blocks its caller and so never admits.
    pub(super) delivery_service_us: StreamServiceEstimateUs,
}

pub(super) enum StreamDomainCommand {
    Deliver(
        Envelope,
        crossbeam_channel::Sender<Result<(), DeliveryError>>,
        Option<StreamAdmissionSlot>,
    ),
    ReadLiveCounts(crossbeam_channel::Sender<StreamLiveCounts>),
    ReadResourceRecords(StreamAdminReadCommand),
    RefreshAdminSnapshotIfDirty(crossbeam_channel::Sender<()>),
    RunMaintenance {
        family: u64,
        reply: Option<crossbeam_channel::Sender<()>>,
    },
    #[cfg(test)]
    SyncAdminSnapshot(crossbeam_channel::Sender<()>),
    #[cfg(test)]
    PanicForTests,
}

#[derive(Default)]
pub(super) struct StreamLiveCounts {
    pub(super) streams: usize,
    pub(super) append_sessions: usize,
    pub(super) subscriptions: usize,
}

pub(super) struct StreamDomainActor {
    pub(super) core: Arc<StreamDomainCore>,
}

pub struct StreamDomainSink {
    pub(super) core: Arc<StreamDomainCore>,
    pub(super) actor: Option<ManagedActor<StreamDomainCommand>>,
    pub(super) family_runtime: Option<
        crate::runtime::keyed_family_executor::KeyedFamilyExecutor<
            StreamWorkKey,
            StreamDomainCommand,
            Arc<StreamDomainCore>,
        >,
    >,
    pub(super) family_families: Option<Vec<RouteFamily>>,
    /// Client requests currently blocked on the (non-family) actor's reply.
    /// Only `deliver_to_actor` admits against this - `deliver_to_family`
    /// never blocks its caller, so it needs no admission window.
    pub(super) inflight_client_deliveries: Arc<AtomicUsize>,
}

pub(super) type StreamServiceEstimateUs = Arc<AtomicU64>;

/// How long `deliver_to_actor` waits for the (non-family) actor's reply.
pub(super) const STREAM_ACTOR_REPLY_TIMEOUT: Duration = Duration::from_secs(1);

/// Hard ceiling on concurrent client requests blocked on the actor, whatever
/// the measured service time suggests.
pub(super) const STREAM_ADMISSION_MAX_WINDOW: usize = 64;

/// Fraction of the reply deadline the admitted backlog may consume, leaving
/// headroom for the delivery itself and a slower-than-average request.
const STREAM_ADMISSION_BUDGET_NUMERATOR: u32 = 4;
const STREAM_ADMISSION_BUDGET_DENOMINATOR: u32 = 5;

/// Assumed per-delivery service time until the actor has measured one.
const STREAM_ADMISSION_ASSUMED_SERVICE_US: u64 = 5_000;

/// How many client requests may be blocked on the (non-family) Stream actor.
///
/// See `queue_admission_window` (`domains::queue::sink::model`) for the full
/// rationale: a fixed window cannot bound the caller's reply deadline, since
/// the actor serves deliveries one at a time and admitting `n` requests
/// commits the tail caller to `n x service_time`. Sizing the window from
/// observed service time keeps the admitted backlog inside the deadline as
/// the actor gets slower, and never drops below 1 so the active operation is
/// always admitted.
pub(super) fn stream_admission_window(service_us: u64) -> usize {
    let deadline_us = u64::try_from(STREAM_ACTOR_REPLY_TIMEOUT.as_micros()).unwrap_or(u64::MAX);
    let budget_us = deadline_us.saturating_mul(u64::from(STREAM_ADMISSION_BUDGET_NUMERATOR))
        / u64::from(STREAM_ADMISSION_BUDGET_DENOMINATOR);
    let service_us = service_us.max(1);
    let window = usize::try_from(budget_us / service_us).unwrap_or(STREAM_ADMISSION_MAX_WINDOW);
    window.clamp(1, STREAM_ADMISSION_MAX_WINDOW)
}

/// Blend a fresh delivery duration into the running service estimate.
pub(super) fn blend_stream_service_estimate(previous_us: u64, observed_us: u64) -> u64 {
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
/// `MailboxFull` when the live actor already has as much blocked-caller work
/// as its deadline can serve, or `ActorStopped` when the actor has terminated.
pub(super) fn admit_stream_client_delivery(
    inflight: &Arc<AtomicUsize>,
    service: &StreamServiceEstimateUs,
    actor_running: bool,
) -> Result<StreamAdmissionSlot, DeliveryError> {
    let window = stream_admission_window(service.load(Ordering::Relaxed));
    try_admit_stream_delivery(inflight, window)
        .map_err(|error| classify_stream_admission_failure(error, actor_running))
}

/// Fold one observed delivery duration into the shared estimate.
pub(super) fn record_stream_service_sample(
    estimate: &StreamServiceEstimateUs,
    started_at: Instant,
) {
    let observed_us = u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX);
    let previous = estimate.load(Ordering::Relaxed);
    estimate.store(
        blend_stream_service_estimate(previous, observed_us),
        Ordering::Relaxed,
    );
}

/// A full window means "retry later" only while the actor is alive - a worker
/// that fails closed leaves every admitted slot alive for the sink's
/// lifetime, so admission would keep answering `MailboxFull` forever.
pub(super) fn classify_stream_admission_failure(
    error: DeliveryError,
    actor_running: bool,
) -> DeliveryError {
    if actor_running {
        error
    } else {
        DeliveryError::ActorStopped
    }
}

/// Starting value for the service estimate before anything is measured.
pub(super) const fn stream_assumed_service_us() -> u64 {
    STREAM_ADMISSION_ASSUMED_SERVICE_US
}

/// Holds an admission slot until the queued command is finished with.
///
/// The slot travels with the command rather than with the blocked caller: a
/// caller that gives up on `recv_timeout` has not cancelled anything, so
/// releasing on caller timeout would recycle slots while the work they
/// admitted is still pending. Dropping with the command covers completion,
/// actor death, and mailbox teardown alike.
#[derive(Debug)]
pub(super) struct StreamAdmissionSlot {
    inflight: Arc<AtomicUsize>,
}

impl Drop for StreamAdmissionSlot {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Reserve an in-flight slot, or refuse the request.
///
/// # Errors
///
/// Returns `MailboxFull` when the actor already has as many blocked callers as
/// its deadline can serve - deliberately the same error an actually-full
/// mailbox produces, so nothing was enqueued and ingress answers with a
/// retryable code.
pub(super) fn try_admit_stream_delivery(
    inflight: &Arc<AtomicUsize>,
    window: usize,
) -> Result<StreamAdmissionSlot, DeliveryError> {
    inflight
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < window).then_some(current + 1)
        })
        .map(|_| StreamAdmissionSlot {
            inflight: Arc::clone(inflight),
        })
        .map_err(|current| DeliveryError::MailboxFull {
            capacity: window,
            current_len: current,
        })
}
