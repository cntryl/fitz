use crate::core::domain::{Domain, DomainContext, DomainResponse};
use std::sync::Arc;

/// Static, branch-predicted domain registry
/// Zero HashMap lookups and zero string→domain indirection.
/// Engine calls straight into domain handlers.
///
/// This is the highest-performance arrangement for Fitz:
/// - fully synchronous domain calls
/// - deterministic dispatch path
/// - predictable branch behavior
#[derive(Debug)]
pub struct DomainRegistry {
    notice: Arc<dyn Domain>,
    rpc: Arc<dyn Domain>,
    queue: Arc<dyn Domain>,
    lease: Arc<dyn Domain>,
    control: Arc<dyn Domain>,
    stream: Arc<dyn Domain>,
    kv: Arc<dyn Domain>,
}

impl Default for DomainRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl DomainRegistry {
    /// Dispatch synchronously to the correct domain.
    ///
    /// Returns:
    /// - `Ok(DomainResponse)` on success
    /// - `Err(String)` if scheme is unsupported or handler errors
    #[inline(always)]
    pub fn dispatch(&self, scheme: &str, request: DomainContext) -> Result<DomainResponse, String> {
        let out = match scheme {
            "notice" => self.notice.handle(request),
            "rpc" => self.rpc.handle(request),
            "queue" => self.queue.handle(request),
            "lease" => self.lease.handle(request),
            "control" => self.control.handle(request),
            "stream" => self.stream.handle(request),
            "kv" => self.kv.handle(request),
            _ => return Err(format!("unsupported scheme: {}", scheme)),
        };

        Ok(out)
    }

    /// Dispatch cleanup to each domain.
    /// Called by engine when connections drop or channels orphan.
    pub fn cleanup_channel(&self, rf: crate::routing::RouteFamilyId, channel_id: u32) {
        self.notice.cleanup_channel(rf, channel_id);
        self.rpc.cleanup_channel(rf, channel_id);
        self.queue.cleanup_channel(rf, channel_id);
        self.lease.cleanup_channel(rf, channel_id);
        self.control.cleanup_channel(rf, channel_id);
        self.stream.cleanup_channel(rf, channel_id);
        self.kv.cleanup_channel(rf, channel_id);
    }

    /// Initialize registry with domain instances.
    ///
    /// ***CRITICAL NOTE:***
    /// Stream/Queue/KV must share the same backing store.
    pub fn new() -> Self {
        use crate::core::{
            control::ControlDomain, kv::KvDomain, lease::LeaseDomain, notice::NoticeDomain,
            queue::QueueDomain, rpc::RpcDomain, stream::StreamDomain,
        };
        // use crate::routing::GlobalInternTable;
        use crate::storage::midge_adapter;

        // Shared string interner for lease keys
        //let interner = Arc::new(GlobalInternTable::new());

        // Shared storage backend
        let kv_store = midge_adapter::create_memory_store().expect("memory store init failed");

        // Domains (Notice/RPC/Lease have no storage deps)
        let notice = Arc::new(NoticeDomain::new());
        let rpc = Arc::new(RpcDomain::new());
        let lease = Arc::new(LeaseDomain::new());
        let control = Arc::new(ControlDomain::new());

        // Storage-backed domains
        let stream = Arc::new(StreamDomain::new(Arc::clone(&kv_store)));
        let queue = Arc::new(QueueDomain::new(Arc::clone(&kv_store)));
        let kv = Arc::new(KvDomain::new(kv_store));

        Self {
            notice,
            rpc,
            queue,
            lease,
            control,
            stream,
            kv,
        }
    }

    /// Expose notice domain for engine fanout (control → notice)
    pub fn get_notice_domain(&self) -> Arc<dyn Domain> {
        Arc::clone(&self.notice)
    }
}
