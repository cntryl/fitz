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
    DeliveryError, Envelope, FamilyActorPoolRuntime, KeyedActorPool, MailboxSink, ManagedActor,
    Router,
};
pub(super) use parking_lot::Mutex;
pub(super) use std::collections::{BTreeMap, BTreeSet, HashMap};
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
pub(super) use std::sync::{Arc, Weak};

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
pub(super) struct StreamActorKey {
    pub(super) family_id: u64,
    pub(super) realm: String,
    pub(super) area: String,
    pub(super) resource: String,
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

impl StreamActorKey {
    pub(super) fn resource_route(&self) -> Route {
        Route::new(format!(
            "stream://{}/{}/{}",
            self.realm, self.area, self.resource
        ))
    }
}

#[derive(Clone)]
pub(super) struct StreamSessionOwner {
    pub(super) key: StreamActorKey,
    pub(super) owner_session_id: u64,
    pub(super) actor: Arc<Mutex<StreamActor>>,
}

pub(super) struct StreamDomainCore {
    pub(super) store: crate::storage::FitzStorageEngine,
    pub(super) stream_store: Arc<StreamStore>,
    pub(super) actors: Mutex<HashMap<StreamActorKey, Arc<Mutex<StreamActor>>>>,
    pub(super) session_owners: Mutex<HashMap<u64, StreamSessionOwner>>,
    pub(super) families: Mutex<HashMap<u64, RoutedSubscriptionSet<StreamSubscription>>>,
    pub(super) next_sub_id: Arc<AtomicU64>,
    pub(super) next_session_id: Arc<AtomicU64>,
    pub(super) cursor_integrity_key: Arc<[u8; 32]>,
    pub(super) pending_notifications: Mutex<Vec<PendingStreamNotification>>,
    pub(super) router: Arc<Router>,
    pub(super) admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(super) admin_snapshot_dirty: Arc<AtomicBool>,
    pub(super) sync_write_mode: crate::domains::stream::protocol::StreamWriteMode,
    pub(super) metrics: Option<StreamMetrics>,
    pub(super) active: Arc<AtomicBool>,
    /// Weak family-core registry used only to aggregate live/admin views.
    /// Mutable delivery state itself remains owned by each family core.
    pub(super) family_cores: Arc<Mutex<BTreeMap<u64, Weak<StreamDomainCore>>>>,
    /// Lazily spawned `AreaActor` mailboxes, keyed by (family, realm, area).
    pub(super) area_watermark_actors: Arc<
        KeyedActorPool<
            (u64, String, String),
            crate::domains::stream::protocol::StreamCoordinationMessage,
        >,
    >,
    /// Lazily spawned `RealmActor` mailboxes, keyed by (family, realm).
    pub(super) realm_watermark_actors: Arc<
        KeyedActorPool<(u64, String), crate::domains::stream::protocol::StreamCoordinationMessage>,
    >,
}

pub(super) enum StreamDomainCommand {
    Deliver(
        Envelope,
        crossbeam_channel::Sender<Result<(), DeliveryError>>,
    ),
    ReadLiveCounts(crossbeam_channel::Sender<StreamLiveCounts>),
    ReadResourceRecords(StreamAdminReadCommand),
    RefreshAdminSnapshotIfDirty(crossbeam_channel::Sender<()>),
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

pub(super) struct StreamDomainRuntime<'a> {
    pub(super) core: &'a StreamDomainCore,
}

pub struct StreamDomainSink {
    pub(super) core: Arc<StreamDomainCore>,
    pub(super) actor: ManagedActor<StreamDomainCommand>,
    pub(super) family_runtime: Option<FamilyActorPoolRuntime<StreamDomainCommand>>,
    pub(super) family_families: Option<Vec<RouteFamily>>,
}

impl std::ops::Deref for StreamDomainRuntime<'_> {
    type Target = StreamDomainCore;

    fn deref(&self) -> &Self::Target {
        self.core
    }
}
