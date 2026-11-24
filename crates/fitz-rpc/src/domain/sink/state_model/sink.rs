use super::{
    Arc, AtomicBool, AtomicU64, AtomicUsize, DeliveryError, Duration, Envelope,
    FamilyActorPoolRuntime, Instant, Router, RpcState,
};
#[cfg(test)]
use super::{RouteAddress, RpcSessionCleanupResult, RpcWorkerCleanupResult};
use fitz_core::runtime::CleanedUpSessions;

#[derive(Clone)]
pub(in crate::domain::sink) struct RpcDomainConfig {
    pub(in crate::domain::sink) router: Arc<Router>,
    pub(in crate::domain::sink) admin_read_model: Arc<crate::control::admin::AdminReadModel>,
    pub(in crate::domain::sink) request_timeout: Duration,
    pub(in crate::domain::sink) cancellation_grace_period: Duration,
    pub(in crate::domain::sink) route_pending_capacity: usize,
    pub(in crate::domain::sink) global_pending_capacity: usize,
    pub(in crate::domain::sink) global_pending_count: Arc<AtomicUsize>,
    pub(in crate::domain::sink) snapshot_epoch: Instant,
    pub(in crate::domain::sink) metrics: Option<crate::domain::RpcMetrics>,
}

/// Mutable RPC state owned exclusively by one route-family worker.
pub(in crate::domain::sink) struct RpcFamilyState {
    pub(in crate::domain::sink) family: fitz_core::runtime::routing::RouteFamily,
    pub(in crate::domain::sink) state: RpcState,
    pub(in crate::domain::sink) cleaned_up_sessions: CleanedUpSessions,
    pub(in crate::domain::sink) router: Arc<Router>,
    pub(in crate::domain::sink) admin_read_model: Arc<crate::control::admin::AdminReadModel>,
    pub(in crate::domain::sink) request_timeout: Duration,
    pub(in crate::domain::sink) cancellation_grace_period: Duration,
    pub(in crate::domain::sink) route_pending_capacity: usize,
    pub(in crate::domain::sink) global_pending_capacity: usize,
    pub(in crate::domain::sink) global_pending_count: Arc<AtomicUsize>,
    pub(in crate::domain::sink) snapshot_dirty: AtomicBool,
    pub(in crate::domain::sink) snapshot_syncing: AtomicBool,
    pub(in crate::domain::sink) last_snapshot_elapsed_us: AtomicU64,
    pub(in crate::domain::sink) last_inline_timeout_elapsed_us: AtomicU64,
    pub(in crate::domain::sink) snapshot_epoch: Instant,
    pub(in crate::domain::sink) metrics: Option<crate::domain::RpcMetrics>,
}

pub(in crate::domain::sink) enum RpcDomainCommand {
    Deliver(
        Envelope,
        crossbeam_channel::Sender<Result<(), DeliveryError>>,
    ),
    ExpireTimedOutRequestsAt(Instant, Option<crossbeam_channel::Sender<()>>),
    ReadLiveCounts(crossbeam_channel::Sender<RpcLiveCounts>),
    #[cfg(test)]
    SyncAdminSnapshot(Option<crossbeam_channel::Sender<()>>),
    RefreshAdminSnapshotIfDirty(Option<crossbeam_channel::Sender<()>>),
    #[cfg(test)]
    ApplySessionCleanupForTests(u64, crossbeam_channel::Sender<RpcSessionCleanupResult>),
    #[cfg(test)]
    ApplyWorkerUnsubscribeForTests(
        RouteAddress,
        u64,
        crossbeam_channel::Sender<RpcWorkerCleanupResult>,
    ),
    PanicForFailpoint,
    #[cfg(test)]
    BlockForTests(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
    #[cfg(test)]
    InspectForTests(
        Box<dyn FnOnce(&mut RpcFamilyState) + Send>,
        crossbeam_channel::Sender<()>,
    ),
    #[cfg(test)]
    ForwardQueuedDispatchForTests(super::RpcQueuedDispatch, crossbeam_channel::Sender<()>),
}

#[derive(Clone, Default)]
pub(in crate::domain::sink) struct RpcLiveCounts {
    pub(in crate::domain::sink) workers: usize,
    pub(in crate::domain::sink) pending_requests: usize,
}

pub(in crate::domain::sink) struct RpcFamilyRuntime<'a> {
    pub(in crate::domain::sink) core: &'a mut RpcFamilyState,
    pub(in crate::domain::sink) active: &'a AtomicBool,
}

pub struct RpcDomain {
    pub(in crate::domain::sink) config: RpcDomainConfig,
    pub(in crate::domain::sink) active: Arc<AtomicBool>,
    pub(in crate::domain::sink) family_runtime: FamilyActorPoolRuntime<RpcDomainCommand>,
    pub(in crate::domain::sink) family_families: Vec<fitz_core::runtime::routing::RouteFamily>,
}
