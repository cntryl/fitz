use super::{Arc, AtomicBool, AtomicU64, Duration, Instant, Mutex, Router, RpcState};

pub struct RpcDomainCore {
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

pub struct RpcDomainSink {
    pub(in crate::domains::rpc::sink) core: RpcDomainCore,
    pub(in crate::domains::rpc::sink) active: AtomicBool,
}

impl std::ops::Deref for RpcDomainSink {
    type Target = RpcDomainCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl std::ops::DerefMut for RpcDomainSink {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}
