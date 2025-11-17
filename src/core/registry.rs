// New DomainRegistry pattern - cleaner orchestration

use crate::core::domain::{Domain, DomainContext, DomainResponse, SubSender};
use std::sync::Arc;

/// A registry that knows how to route to domains without HashMap lookups
/// This is compiled/optimized at startup, not interpreted at runtime
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
    /// Route a request to the appropriate domain handler
    ///
    /// Returns Ok(response) if domain found and handled successfully
    /// Returns Err if scheme not found or domain returns error
    pub fn dispatch(&self, scheme: &str, request: DomainContext) -> Result<DomainResponse, String> {
        match scheme {
            "notice" => Ok(self.notice.handle(request)),
            "rpc" => Ok(self.rpc.handle(request)),
            "queue" => Ok(self.queue.handle(request)),
            "lease" => Ok(self.lease.handle(request)),
            "control" => Ok(self.control.handle(request)),
            "stream" => Ok(self.stream.handle(request)),
            "kv" => Ok(self.kv.handle(request)),
            _ => Err(format!("unsupported scheme: {}", scheme)),
        }
    }

    /// Cleanup a channel across all domains
    /// Domains use this to cleanup subscriptions, inboxes, resources, etc.
    pub fn cleanup_channel(&self, rf: crate::storage::RouteFamilyId, channel_id: u32) {
        self.notice.cleanup_channel(rf, channel_id);
        self.rpc.cleanup_channel(rf, channel_id);
        self.queue.cleanup_channel(rf, channel_id);
        self.lease.cleanup_channel(rf, channel_id);
        self.control.cleanup_channel(rf, channel_id);
        self.stream.cleanup_channel(rf, channel_id);
        self.kv.cleanup_channel(rf, channel_id);
    }

    /// Subscribe to notifications for a route pattern
    /// Returns subscription ID for later unsubscribe
    pub fn subscribe(
        &self,
        rf: crate::routing::RouteFamilyId,
        route_pattern: String,
        channel_id: u32,
        sender: SubSender,
    ) -> Result<u64, String> {
        self.stream.subscribe(rf, route_pattern, channel_id, sender)
    }

    /// Unsubscribe from notifications
    /// Returns true if subscription was found and removed
    pub fn unsubscribe(&self, subscription_id: u64) -> Result<bool, String> {
        self.stream.unsubscribe(subscription_id)
    }

    /// Create a new registry with all domains initialized
    pub fn new() -> Self {
        use crate::core::{
            control::ControlDomain, kv::KvDomain, lease::LeaseDomain, notice::NoticeDomain,
            queue::QueueDomain, rpc::RpcDomain, stream::StreamDomain,
        };
        use crate::storage::midge_adapter;

        // Initialize domains
        let notice = Arc::new(NoticeDomain::new());
        let rpc = Arc::new(RpcDomain::new());
        let lease = Arc::new(LeaseDomain::new());

        // Create storage backend for stream, queue, and kv domains
        let kv_store = midge_adapter::create_memory_store()
            .expect("Failed to create memory store for domains");
        let stream = Arc::new(StreamDomain::new(Arc::clone(&kv_store)));
        let queue = Arc::new(QueueDomain::new(Arc::clone(&kv_store)));
        let kv = Arc::new(KvDomain::new(kv_store));

        // Control shares notice service
        let control = Arc::new(ControlDomain::with_notice_service(notice.get_service()));

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
}

// Usage in engine.rs:
//
// Instead of:
//   let mut domains: HashMap<&'static str, Arc<dyn Domain>> = HashMap::new();
//   domains.insert("notice", Arc::clone(&notice_domain) as Arc<dyn Domain>);
//   // ... etc
//   let domain = domains.get(scheme_str)?;
//
// Now:
//   let registry = DomainRegistry::new();
//   let response = registry.dispatch(scheme, request).await?;
