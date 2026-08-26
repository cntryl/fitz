use super::{
    Arc, AtomicBool, AtomicU64, AtomicUsize, BTreeMap, DeliveryError, Duration, Envelope,
    FamilyActorPoolRuntime, Instant, ManagedActor, Mutex, Router, RpcState, Weak,
};
#[cfg(test)]
use super::{RouteAddress, RpcSessionCleanupResult, RpcWorkerCleanupResult};
use crate::runtime::CleanedUpSessions;

pub(in crate::domains::rpc::sink) struct RpcDomainCore {
    pub(in crate::domains::rpc::sink) state: Mutex<RpcState>,
    /// Sessions disconnect cleanup has already run for in this family core;
    /// guards against a stale queued request recreating state. See
    /// `sink/cleanup.rs`. Local per family core, like `state` - a session
    /// cleaned up in one family does not need to be rejected in another.
    pub(in crate::domains::rpc::sink) cleaned_up_sessions: Mutex<CleanedUpSessions>,
    pub(in crate::domains::rpc::sink) router: Arc<Router>,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) admin_read_model:
        Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(in crate::domains::rpc::sink) request_timeout: Duration,
    pub(in crate::domains::rpc::sink) route_pending_capacity: usize,
    pub(in crate::domains::rpc::sink) global_pending_count: Arc<AtomicUsize>,
    pub(in crate::domains::rpc::sink) enforce_global_pending_count: bool,
    pub(in crate::domains::rpc::sink) snapshot_dirty: Arc<AtomicBool>,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) snapshot_syncing: Arc<AtomicBool>,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) last_snapshot_elapsed_us: Arc<AtomicU64>,
    pub(in crate::domains::rpc::sink) last_inline_timeout_elapsed_us: Arc<AtomicU64>,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(in crate::domains::rpc::sink) snapshot_epoch: Instant,
    pub(in crate::domains::rpc::sink) metrics: Option<crate::domains::rpc::RpcMetrics>,
    /// Weak family-core registry used to aggregate admin and metric views.
    /// Mutable RPC state itself remains local to each family core.
    pub(in crate::domains::rpc::sink) family_cores: Arc<Mutex<BTreeMap<u32, Weak<RpcDomainCore>>>>,
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
    #[cfg(test)]
    PanicForTests,
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
    pub(in crate::domains::rpc::sink) family_runtime:
        Option<FamilyActorPoolRuntime<RpcDomainCommand>>,
    pub(in crate::domains::rpc::sink) family_families:
        Option<Vec<crate::runtime::routing::RouteFamily>>,
}

impl std::ops::Deref for RpcDomainRuntime<'_> {
    type Target = RpcDomainCore;

    fn deref(&self) -> &Self::Target {
        self.core
    }
}
