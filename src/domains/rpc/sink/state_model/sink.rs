use super::{
    Arc, AtomicBool, AtomicU64, DeliveryError, Duration, Envelope, Instant, ManagedActor, Mutex,
    Router, RpcState,
};
#[cfg(test)]
use super::{RouteAddress, RpcSessionCleanupResult, RpcWorkerCleanupResult};

pub(in crate::domains::rpc::sink) struct RpcDomainCore {
    pub(in crate::domains::rpc::sink) state: Mutex<RpcState>,
    pub(in crate::domains::rpc::sink) router: Arc<Router>,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) admin_read_model:
        Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(in crate::domains::rpc::sink) request_timeout: Duration,
    pub(in crate::domains::rpc::sink) route_pending_capacity: usize,
    pub(in crate::domains::rpc::sink) snapshot_dirty: AtomicBool,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) snapshot_syncing: AtomicBool,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) last_snapshot_elapsed_us: AtomicU64,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
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
}

#[derive(Default)]
pub(in crate::domains::rpc::sink) struct RpcLiveCounts {
    pub(in crate::domains::rpc::sink) workers: usize,
    pub(in crate::domains::rpc::sink) pending_requests: usize,
}

pub(in crate::domains::rpc::sink) struct RpcDomainActor {
    pub(in crate::domains::rpc::sink) core: Arc<RpcDomainCore>,
    pub(in crate::domains::rpc::sink) active: Arc<AtomicBool>,
}

pub(in crate::domains::rpc::sink) struct RpcDomainRuntime<'a> {
    pub(in crate::domains::rpc::sink) core: &'a RpcDomainCore,
    pub(in crate::domains::rpc::sink) active: &'a AtomicBool,
}

pub struct RpcDomainSink {
    pub(in crate::domains::rpc::sink) core: Arc<RpcDomainCore>,
    pub(in crate::domains::rpc::sink) active: Arc<AtomicBool>,
    pub(in crate::domains::rpc::sink) actor: ManagedActor<RpcDomainCommand>,
}

impl std::ops::Deref for RpcDomainRuntime<'_> {
    type Target = RpcDomainCore;

    fn deref(&self) -> &Self::Target {
        self.core
    }
}
