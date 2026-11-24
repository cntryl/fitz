use super::budget::Budget;
use super::delivery::{DeliveryQueue, SharedDeliveryBudgets};
use super::subscriptions::{StreamSubscriptionLimits, SubscriptionRegistry};
use crate::domain::metrics::StreamDurableMetrics;
use crate::domain::StreamMetrics;
use crate::domain::{StreamActor, StreamClientResponseBody, StreamStore};
use fitz_core::runtime::routing::{Route, RouteFamily};
use fitz_core::runtime::{CleanedUpSessions, DeliveryError, Envelope, MailboxSink, Router};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;

pub(super) fn u64_to_usize_saturating(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

pub(super) fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

pub(super) fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
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
    pub(super) filter: Option<&'a crate::domain::protocol::StreamFilterSet>,
    pub(super) cursor_fingerprint: Option<u64>,
    pub(super) captured_watermark: Option<u64>,
}

/// Storage-mode-compatible write options selected before Stream initialization.
#[derive(Clone, Copy)]
pub struct StreamStorageWriteOptions {
    sync_intent: fitz_storage::domains::WritePolicy,
    buffered_intent: fitz_storage::domains::WritePolicy,
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
        sync_intent: fitz_storage::domains::WritePolicy,
        buffered_intent: fitz_storage::domains::WritePolicy,
    ) -> Self {
        Self {
            sync_intent,
            buffered_intent,
        }
    }

    #[must_use]
    pub fn local() -> Self {
        Self::new(
            fitz_storage::domains::WritePolicy::Sync,
            fitz_storage::domains::WritePolicy::Buffered,
        )
    }

    #[must_use]
    #[cfg(test)]
    pub fn cloud_background() -> Self {
        Self::new(
            fitz_storage::domains::WritePolicy::CloudAsync,
            fitz_storage::domains::WritePolicy::CloudAsync,
        )
    }

    #[must_use]
    #[cfg(test)]
    pub fn cloud_strict() -> Self {
        Self::new(
            fitz_storage::domains::WritePolicy::CloudStrict,
            fitz_storage::domains::WritePolicy::CloudAsync,
        )
    }

    pub(super) fn sync_intent(self) -> fitz_storage::domains::WritePolicy {
        self.sync_intent
    }

    pub(super) fn buffered_intent(self) -> fitz_storage::domains::WritePolicy {
        self.buffered_intent
    }
}

pub(super) struct StreamAdminReadCommand {
    pub(super) request: AdminStreamReadRequestOwned,
    pub(super) reply: crossbeam_channel::Sender<
        Result<
            (
                Vec<crate::domain::protocol::StreamReadItem>,
                crate::domain::protocol::ReadCursor,
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

pub(super) struct WatermarkCommit {
    pub(super) family: RouteFamily,
    pub(super) realm: String,
    pub(super) area: String,
    pub(super) batch: crate::domain::protocol::BatchCommitted,
    /// Which coordinators still need this commit. A commit that could not
    /// reach a coordinator at the cap is re-queued with only that target.
    pub(super) targets: WatermarkTargets,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WatermarkTargets {
    pub(super) area: bool,
    pub(super) realm: bool,
}

impl WatermarkTargets {
    pub(super) const fn both() -> Self {
        Self {
            area: true,
            realm: true,
        }
    }

    pub(super) const fn is_empty(self) -> bool {
        !self.area && !self.realm
    }
}

pub(super) struct StreamCommitOutcome {
    pub(super) response: StreamClientResponseBody,
    pub(super) committed: Option<(fitz_core::runtime::DomainPublishEvent, WatermarkCommit)>,
    pub(super) admin_dirty: bool,
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

pub(super) struct WatermarkCoordinators {
    pub(super) area: HashMap<
        StreamAreaScope,
        (
            crate::domain::area_actor::AreaActor,
            fitz_core::runtime::actor::Context<crate::domain::area_actor::AreaActor>,
        ),
    >,
    pub(super) realm: HashMap<
        StreamRealmScope,
        (
            crate::domain::realm_actor::RealmActor,
            fitz_core::runtime::actor::Context<crate::domain::realm_actor::RealmActor>,
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

pub(super) struct StreamFamilyRuntime {
    pub(super) core: StreamFamilyState,
    pub(super) continuation_queued: bool,
    pub(super) last_watermark_generation: u64,
    pub(super) watermark_coordinators: WatermarkCoordinators,
    pub(super) watermark_router: Arc<Router>,
    pub(super) watermark_events:
        crossbeam_channel::Receiver<fitz_core::runtime::DomainPublishEvent>,
    pub(super) pending_watermark_commits: Vec<WatermarkCommit>,
}

struct WatermarkEventSink {
    events: crossbeam_channel::Sender<fitz_core::runtime::DomainPublishEvent>,
}

impl MailboxSink for WatermarkEventSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let event = envelope
            .payload::<fitz_core::runtime::DomainPublishEvent>()
            .ok_or(DeliveryError::ActorStopped)?;
        self.events
            .send(event.clone())
            .map_err(|_| DeliveryError::ActorStopped)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl StreamFamilyRuntime {
    pub(super) fn new(core: StreamFamilyState) -> Self {
        let last_watermark_generation = core.observability.watermark_generation();
        let watermark_router = Arc::new(Router::new());
        let (watermark_event_sender, watermark_events) = crossbeam_channel::unbounded();
        watermark_router.register_domain_pattern(
            "stream",
            Arc::new(WatermarkEventSink {
                events: watermark_event_sender,
            }),
        );
        Self {
            core,
            continuation_queued: false,
            last_watermark_generation,
            watermark_coordinators: WatermarkCoordinators::new(),
            watermark_router,
            watermark_events,
            pending_watermark_commits: Vec::new(),
        }
    }
}

pub(super) struct StreamFamilyState {
    pub(super) family: RouteFamily,
    pub(super) admin_provisioned_families: Vec<u64>,
    pub(super) admin_unprovisioned_owner: RouteFamily,
    pub(super) stream_store: Arc<StreamStore>,
    pub(super) actors: HashMap<StreamResourceScope, StreamActor>,
    pub(super) session_owners: super::session_owners::StreamSessionOwners,
    pub(super) cleaned_up_sessions: CleanedUpSessions,
    pub(super) subscriptions: SubscriptionRegistry,
    pub(super) delivery: DeliveryQueue,
    pub(super) next_session_id: Arc<AtomicU64>,
    pub(super) cursor_integrity_key: Arc<[u8; 32]>,
    pub(super) router: Arc<Router>,
    pub(super) observability: super::observability_state::StreamObservability,
    pub(super) sync_write_mode: crate::domain::protocol::StreamWriteMode,
    pub(super) metrics: Option<StreamMetrics>,
    pub(super) active: Arc<AtomicBool>,
}

pub(super) enum StreamDomainCommand {
    ContinueDelivery,
    Deliver(
        Envelope,
        crossbeam_channel::Sender<Result<(), DeliveryError>>,
    ),
    ReadLiveCounts(crossbeam_channel::Sender<StreamLiveCounts>),
    ReadResourceRecords(StreamAdminReadCommand),
    CaptureStreamSnapshot(
        crate::snapshot::SnapshotSelector,
        crossbeam_channel::Sender<Result<crate::snapshot::SnapshotArtifact, String>>,
    ),
    RestoreStreamSnapshot(
        crate::snapshot::SnapshotArtifact,
        crossbeam_channel::Sender<Result<Vec<String>, String>>,
    ),
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
    #[cfg(test)]
    InspectForTests(
        Box<dyn FnOnce(&mut StreamFamilyRuntime) + Send>,
        crossbeam_channel::Sender<()>,
    ),
}

#[derive(Default)]
pub(super) struct StreamLiveCounts {
    pub(super) streams: usize,
    pub(super) append_sessions: usize,
    pub(super) subscriptions: usize,
}

pub struct StreamDomain {
    pub(super) config: StreamDomainConfig,
    pub(super) active: Arc<AtomicBool>,
    pub(super) family_runtime: fitz_core::runtime::FamilyActorPoolRuntime<StreamDomainCommand>,
    pub(super) family_families: Vec<RouteFamily>,
}

#[derive(Clone, Copy)]
pub struct StreamRuntimeLimits {
    pub max_message_bytes: usize,
    pub subscriptions: StreamSubscriptionLimits,
}

impl From<usize> for StreamRuntimeLimits {
    fn from(max_message_bytes: usize) -> Self {
        Self {
            max_message_bytes,
            subscriptions: StreamSubscriptionLimits::default(),
        }
    }
}

#[derive(Clone)]
pub(super) struct StreamDomainConfig {
    pub(super) stream_store: Arc<StreamStore>,
    pub(super) next_session_id: Arc<AtomicU64>,
    pub(super) next_subscription_id: Arc<AtomicU64>,
    pub(super) subscription_bytes: Budget,
    pub(super) subscriptions_per_session: usize,
    pub(super) delivery_budgets: SharedDeliveryBudgets,
    pub(super) cursor_integrity_key: Arc<[u8; 32]>,
    pub(super) router: Arc<Router>,
    pub(super) admin_projection: Arc<super::projection::StreamAdminProjection>,
    pub(super) admin_dirty_flags: Arc<HashMap<RouteFamily, Arc<AtomicBool>>>,
    pub(super) admin_provisioned_families: Vec<u64>,
    pub(super) admin_unprovisioned_owner: RouteFamily,
    pub(super) sync_write_mode: crate::domain::protocol::StreamWriteMode,
    pub(super) metrics: Option<StreamMetrics>,
    pub(super) live_gauges: Arc<super::live_gauges::StreamLiveGaugeCoordinator>,
    pub(super) durable_metrics: Arc<StreamDurableMetrics>,
    pub(super) active: Arc<AtomicBool>,
}

/// Control and admin callers retain a short family-actor reply deadline.
pub(super) const STREAM_ACTOR_REPLY_TIMEOUT: Duration = Duration::from_secs(1);

/// Normal client commands may wait for a synchronous storage commit. Match the
/// explicit storage runtime response setting instead of timing out a healthy
/// commit at four seconds. Queueing still consumes this bounded wait; expiry
/// after dispatch remains an unknown outcome and is never safe to blindly retry.
pub(super) const STREAM_CLIENT_ACTOR_REPLY_TIMEOUT: Duration = Duration::from_secs(60);
