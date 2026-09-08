pub(super) use crate::dispatch::protocol::payload_codec::PayloadEncoder;
pub(super) use crate::domains::stream::metrics::StreamDurableMetrics;
pub(super) use crate::domains::stream::StreamMetrics;
pub(super) use crate::domains::stream::{
    StreamActor, StreamClientFrame, StreamClientRequest, StreamClientResponseBody,
    StreamFilteredReason, StreamMetadata, StreamReadItem, StreamRecord, StreamStorageLayout,
    StreamStore,
};
pub(super) use crate::domains::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
pub(super) use crate::runtime::routing::{route_triplet, Route, RouteAddress, RouteFamily};
pub(super) use crate::runtime::{CleanedUpSessions, DeliveryError, Envelope, MailboxSink, Router};
pub(super) use parking_lot::Mutex;
pub(super) use std::collections::{BTreeMap, BTreeSet, HashMap};
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
pub(super) use std::sync::{Arc, Weak};
pub(super) use std::time::Duration;

pub(super) fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

pub(super) fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

pub(super) fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

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
    sync_intent: crate::domains::WritePolicy,
    buffered_intent: crate::domains::WritePolicy,
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
        sync_intent: crate::domains::WritePolicy,
        buffered_intent: crate::domains::WritePolicy,
    ) -> Self {
        Self {
            sync_intent,
            buffered_intent,
        }
    }

    #[must_use]
    pub fn local() -> Self {
        Self::new(
            crate::domains::WritePolicy::Sync,
            crate::domains::WritePolicy::Buffered,
        )
    }

    #[must_use]
    pub fn cloud_background() -> Self {
        Self::new(
            crate::domains::WritePolicy::CloudAsync,
            crate::domains::WritePolicy::CloudAsync,
        )
    }

    #[must_use]
    pub fn cloud_strict() -> Self {
        Self::new(
            crate::domains::WritePolicy::CloudStrict,
            crate::domains::WritePolicy::CloudAsync,
        )
    }

    pub(super) fn sync_intent(self) -> crate::domains::WritePolicy {
        self.sync_intent
    }

    pub(super) fn buffered_intent(self) -> crate::domains::WritePolicy {
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

pub(super) struct CommitNotification {
    pub(super) family: RouteFamily,
    pub(super) route: Route,
    pub(super) payload: bytes::Bytes,
}

pub(super) struct WatermarkCommit {
    pub(super) family: RouteFamily,
    pub(super) realm: String,
    pub(super) area: String,
    pub(super) batch: crate::domains::stream::protocol::BatchCommitted,
}

pub(super) struct StreamCommitOutcome {
    pub(super) response: StreamClientResponseBody,
    pub(super) notification: Option<(RouteFamily, Route, bytes::Bytes)>,
    pub(super) admin_dirty: bool,
    pub(super) watermark: Option<WatermarkCommit>,
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
    pub(super) area: HashMap<
        StreamAreaScope,
        (
            crate::domains::stream::area_actor::AreaActor,
            crate::runtime::actor::Context<crate::domains::stream::area_actor::AreaActor>,
        ),
    >,
    pub(super) realm: HashMap<
        StreamRealmScope,
        (
            crate::domains::stream::realm_actor::RealmActor,
            crate::runtime::actor::Context<crate::domains::stream::realm_actor::RealmActor>,
        ),
    >,
}

impl WatermarkCoordinators {
    pub(super) fn new() -> Self {
        Self {
            area: HashMap::new(),
            realm: HashMap::new(),
        }
    }
}

pub(super) struct StreamFamilyState {
    pub(super) core: Arc<StreamDomainCore>,
    pub(super) watermark_coordinators: WatermarkCoordinators,
    pub(super) watermark_router: Arc<Router>,
    pub(super) watermark_events: Arc<Mutex<Vec<crate::runtime::DomainPublishEvent>>>,
    pub(super) pending_watermark_commits: Vec<WatermarkCommit>,
}

struct WatermarkEventSink {
    events: Arc<Mutex<Vec<crate::runtime::DomainPublishEvent>>>,
}

impl MailboxSink for WatermarkEventSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let event = envelope
            .payload::<crate::runtime::DomainPublishEvent>()
            .ok_or(DeliveryError::ActorStopped)?;
        self.events.lock().push(event.clone());
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl StreamFamilyState {
    pub(super) fn new(core: Arc<StreamDomainCore>) -> Self {
        let watermark_router = Arc::new(Router::new());
        let watermark_events = Arc::new(Mutex::new(Vec::new()));
        watermark_router.register_domain_pattern(
            "stream",
            Arc::new(WatermarkEventSink {
                events: watermark_events.clone(),
            }),
        );
        Self {
            core,
            watermark_coordinators: WatermarkCoordinators::new(),
            watermark_router,
            watermark_events,
            pending_watermark_commits: Vec::new(),
        }
    }
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
    pub(super) durable_metrics: Arc<StreamDurableMetrics>,
    pub(super) active: Arc<AtomicBool>,
    /// Weak family-core registry used only to aggregate live/admin views.
    /// Mutable delivery state itself remains owned by each family core.
    pub(super) family_cores: Arc<Mutex<BTreeMap<u64, Weak<StreamDomainCore>>>>,
}

pub(super) enum StreamDomainCommand {
    Deliver(
        Envelope,
        crossbeam_channel::Sender<Result<(), DeliveryError>>,
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
    PanicForFailpoint,
    #[cfg(test)]
    BlockForTests(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
}

#[derive(Default)]
pub(super) struct StreamLiveCounts {
    pub(super) streams: usize,
    pub(super) append_sessions: usize,
    pub(super) subscriptions: usize,
}

pub struct StreamDomainSink {
    pub(super) core: Arc<StreamDomainCore>,
    pub(super) family_runtime: crate::runtime::FamilyActorPoolRuntime<StreamDomainCommand>,
    pub(super) family_families: Vec<RouteFamily>,
}

/// How long synchronous callers wait for a family actor's delivery outcome.
pub(super) const STREAM_ACTOR_REPLY_TIMEOUT: Duration = Duration::from_secs(1);
