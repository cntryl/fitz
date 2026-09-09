use super::{
    Arc, AtomicBool, AtomicU64, AtomicUsize, DeliveryError, Duration, Envelope,
    FamilyActorPoolRuntime, Instant, Router, RpcState,
};
#[cfg(test)]
use super::{RouteAddress, RpcSessionCleanupResult, RpcWorkerCleanupResult};
use crate::runtime::CleanedUpSessions;

#[derive(Clone)]
pub(in crate::domains::rpc::sink) struct RpcDomainConfig {
    pub(in crate::domains::rpc::sink) router: Arc<Router>,
    pub(in crate::domains::rpc::sink) admin_read_model:
        Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(in crate::domains::rpc::sink) request_timeout: Duration,
    pub(in crate::domains::rpc::sink) route_pending_capacity: usize,
    pub(in crate::domains::rpc::sink) global_pending_count: Arc<AtomicUsize>,
    pub(in crate::domains::rpc::sink) snapshot_epoch: Instant,
    pub(in crate::domains::rpc::sink) metrics: Option<crate::domains::rpc::RpcMetrics>,
}

/// Mutable RPC state owned exclusively by one route-family worker.
pub(in crate::domains::rpc::sink) struct RpcFamilyState {
    pub(in crate::domains::rpc::sink) family: crate::runtime::routing::RouteFamily,
    pub(in crate::domains::rpc::sink) state: RpcState,
    pub(in crate::domains::rpc::sink) cleaned_up_sessions: CleanedUpSessions,
    pub(in crate::domains::rpc::sink) router: Arc<Router>,
    pub(in crate::domains::rpc::sink) admin_read_model:
        Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(in crate::domains::rpc::sink) request_timeout: Duration,
    pub(in crate::domains::rpc::sink) route_pending_capacity: usize,
    pub(in crate::domains::rpc::sink) global_pending_count: Arc<AtomicUsize>,
    pub(in crate::domains::rpc::sink) snapshot_dirty: AtomicBool,
    pub(in crate::domains::rpc::sink) snapshot_syncing: AtomicBool,
    pub(in crate::domains::rpc::sink) last_snapshot_elapsed_us: AtomicU64,
    pub(in crate::domains::rpc::sink) last_inline_timeout_elapsed_us: AtomicU64,
    pub(in crate::domains::rpc::sink) snapshot_epoch: Instant,
    pub(in crate::domains::rpc::sink) metrics: Option<crate::domains::rpc::RpcMetrics>,
}

pub(in crate::domains::rpc::sink) enum RpcDomainCommand {
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
pub(in crate::domains::rpc::sink) struct RpcLiveCounts {
    pub(in crate::domains::rpc::sink) workers: usize,
    pub(in crate::domains::rpc::sink) pending_requests: usize,
}

pub(in crate::domains::rpc::sink) struct RpcFamilyRuntime<'a> {
    pub(in crate::domains::rpc::sink) core: &'a mut RpcFamilyState,
    pub(in crate::domains::rpc::sink) active: &'a AtomicBool,
}

pub(crate) struct RpcDomain {
    pub(in crate::domains::rpc::sink) config: RpcDomainConfig,
    pub(in crate::domains::rpc::sink) active: Arc<AtomicBool>,
    pub(in crate::domains::rpc::sink) family_runtime: FamilyActorPoolRuntime<RpcDomainCommand>,
    pub(in crate::domains::rpc::sink) family_families: Vec<crate::runtime::routing::RouteFamily>,
}
