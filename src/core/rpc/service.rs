//! RPC domain service - handles request/reply coordination and inbox management
//!
//! v3: numeric-backed interned inbox ids + string-backed correlation tracking,
//! with first-match routing to avoid per-call Vec allocations.

use crate::routing::{
    GlobalInternTable, InternId, RouteFamilyId, RouteTable, RtSubscription, DEFAULT_RF,
};
use fxhash::FxHashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// RPC message to be delivered to a channel
#[derive(Debug, Clone)]
pub struct RpcMessage {
    pub route: String,
    pub correlation_id: Option<String>,
    pub body: Vec<u8>,
    pub reply_route: Option<String>,
    pub seq: Option<u32>,
    pub is_stream_end: bool,
}

/// Result of routing an RPC request/reply - pure routing data, no I/O
#[derive(Debug)]
pub struct RpcDeliveryResult {
    /// Matched target with channel_id and message payload
    pub target: Option<(u32, RpcMessage)>,
    /// Error message if routing failed
    pub error: Option<String>,
}

impl RpcDeliveryResult {
    pub fn success(channel_id: u32, message: RpcMessage) -> Self {
        Self {
            target: Some((channel_id, message)),
            error: None,
        }
    }

    pub fn error(msg: String) -> Self {
        Self {
            target: None,
            error: Some(msg),
        }
    }
}

/// Inbox ownership and security context
#[derive(Debug, Clone)]
struct InboxContext {
    /// Channel ID that owns this inbox
    owner_channel_id: u32,
    /// Subscription ID for the inbox
    subscription_id: Option<u64>,
}

/// RPC service handles request/reply coordination with inbox management
///
/// Key features:
/// - Cryptographically secure inbox routes
/// - Inbox ownership enforcement (only owner can subscribe)
/// - Handler authorization (only handlers of active requests can publish to client inboxes)
/// - Automatic cleanup on session close
/// - Lock-free subscription ID generation using atomics
/// - String interning for route pattern + inbox routes
pub struct RpcService {
    /// Next subscription ID (atomic for lock-free generation)
    sub_id_counter: AtomicU64,

    /// Route table for RPC handler subscriptions (rpc://realm/area/resource/operation)
    handler_routes: RouteTable,

    /// Route table for inbox subscriptions (inbox://*)
    inbox_routes: RouteTable,

    /// Inbox ownership tracking (interned inbox_id -> context)
    inboxes: FxHashMap<InternId, InboxContext>,

    /// Active RPC requests for correlation tracking
    /// Maps correlation_id -> (handler_route, reply_route)
    ///
    /// We keep these as Strings to avoid extra interner lookups on the hot reply path.
    active_requests: FxHashMap<String, (String, String)>,

    /// Shared string interner for route pattern + inbox routes
    interner: Arc<GlobalInternTable>,
}

impl RpcService {
    pub fn new(interner: Arc<GlobalInternTable>) -> Self {
        Self {
            sub_id_counter: AtomicU64::new(1),
            handler_routes: RouteTable::new(),
            inbox_routes: RouteTable::new(),
            // Pre-allocate capacity for typical workloads to reduce rehashing
            inboxes: FxHashMap::with_capacity_and_hasher(16, Default::default()),
            active_requests: FxHashMap::with_capacity_and_hasher(32, Default::default()),
            interner,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test() -> Self {
        Self::new(Arc::new(GlobalInternTable::new()))
    }

    /// Allocate a cryptographically secure inbox route for a channel
    /// Returns the inbox route (e.g., "inbox://<uuid>")
    pub fn allocate_inbox(&mut self, channel_id: u32) -> String {
        // Generate cryptographically secure random route using UUID v4
        // "inbox://" (8) + UUID (36) = 44 bytes
        let mut inbox_route = String::with_capacity(44);
        inbox_route.push_str("inbox://");

        use std::fmt::Write;
        let _ = write!(&mut inbox_route, "{}", uuid::Uuid::new_v4());

        // Intern once and store by ID
        let inbox_id = self.interner.intern(&inbox_route);

        self.inboxes.insert(
            inbox_id,
            InboxContext {
                owner_channel_id: channel_id,
                subscription_id: None,
            },
        );

        inbox_route
    }

    /// Subscribe to an RPC handler route (e.g., "rpc://acme/auth/user/create")
    /// Returns subscription ID
    pub fn subscribe_handler(
        &mut self,
        rf: RouteFamilyId,
        route_pattern: String,
        channel_id: u32,
    ) -> u64 {
        let id = self.sub_id_counter.fetch_add(1, Ordering::Relaxed);

        // Intern the route pattern for global deduplication (helps other tables / metrics)
        let _pattern_id = self.interner.intern(&route_pattern);

        let sub = RtSubscription {
            id,
            route_pattern,
            channel_id,
        };

        self.handler_routes.insert(rf, sub);
        id
    }

    /// Subscribe to an inbox route (with ownership enforcement)
    /// Returns Ok(sub_id) if authorized, Err if not owner
    pub fn subscribe_inbox(
        &mut self,
        rf: RouteFamilyId,
        inbox_route: String,
        channel_id: u32,
    ) -> Result<u64, String> {
        // Resolve intern ID for this inbox route
        let inbox_id = self.interner.intern(&inbox_route);

        match self.inboxes.get_mut(&inbox_id) {
            Some(ctx) if ctx.owner_channel_id == channel_id => {
                let id = self.sub_id_counter.fetch_add(1, Ordering::Relaxed);

                let sub = RtSubscription {
                    id,
                    route_pattern: inbox_route, // moved in
                    channel_id,
                };

                self.inbox_routes.insert(rf, sub);
                ctx.subscription_id = Some(id);

                Ok(id)
            }
            Some(_) => Err("Permission denied: not inbox owner".to_string()),
            None => Err(format!("Inbox not found: {}", inbox_id)),
        }
    }

    /// Unsubscribe from handler or inbox
    pub fn unsubscribe(&mut self, rf: RouteFamilyId, sub_id: u64) -> bool {
        // Try handler routes first
        if self.handler_routes.remove(rf, sub_id).is_some() {
            return true;
        }

        // Try inbox routes
        if let Some(sub) = self.inbox_routes.remove(rf, sub_id) {
            // Clear subscription ID in inbox context via interned ID
            let inbox_id = self.interner.intern(&sub.route_pattern);
            if let Some(ctx) = self.inboxes.get_mut(&inbox_id) {
                ctx.subscription_id = None;
            }
            return true;
        }

        false
    }

    /// Register an active RPC request for correlation tracking
    pub fn register_request(
        &mut self,
        correlation_id: String,
        handler_route: String,
        reply_route: String,
    ) {
        self.active_requests
            .insert(correlation_id, (handler_route, reply_route));
    }

    /// Deregister an RPC request (on completion or timeout)
    /// Returns the original (handler_route, reply_route)
    pub fn deregister_request(&mut self, correlation_id: &str) -> Option<(String, String)> {
        self.active_requests.remove(correlation_id)
    }

    /// Check if a reply is allowed to publish into an inbox
    /// Hot path: called for every RPC reply
    #[inline]
    pub fn can_publish_to_inbox(&self, inbox_route: &str, correlation_id: &str) -> bool {
        // Just compare the stored reply route to this inbox_route.
        // This avoids interner lookups on the hot reply path.
        self.active_requests
            .get(correlation_id)
            .map(|(_, reply_route)| reply_route.as_str() == inbox_route)
            .unwrap_or(false)
    }

    /// Route an RPC request to a handler (pure routing, no I/O)
    /// Returns channel_id and message to deliver, or error
    pub fn route_request(
        &self,
        rf: RouteFamilyId,
        route: &str,
        correlation_id: Option<&str>,
        reply_route: Option<&str>,
        body: &[u8],
    ) -> RpcDeliveryResult {
        // Find first matching handler without allocating a Vec
        let mut it = self.handler_routes.matching_subscribers(rf, route);
        let Some(handler) = it.next() else {
            return RpcDeliveryResult::error("No handlers available".to_string());
        };

        let message = RpcMessage {
            route: route.to_string(),
            correlation_id: correlation_id.map(|s| s.to_string()),
            body: body.to_vec(),
            reply_route: reply_route.map(|s| s.to_string()),
            seq: None,
            is_stream_end: false,
        };

        RpcDeliveryResult::success(handler.channel_id, message)
    }

    /// Route an RPC reply to an inbox (pure routing, no I/O)
    /// Returns channel_id and message to deliver, or error
    pub fn route_reply(
        &self,
        rf: RouteFamilyId,
        inbox_route: &str,
        correlation_id: Option<&str>,
        body: &[u8],
        seq: Option<u64>,
        is_stream_end: bool,
    ) -> RpcDeliveryResult {
        // Authorization: require correlation id
        let Some(corr_id) = correlation_id else {
            return RpcDeliveryResult::error("Missing correlation ID for reply".to_string());
        };

        if !self.can_publish_to_inbox(inbox_route, corr_id) {
            return RpcDeliveryResult::error("Unauthorized inbox access".to_string());
        }

        // Find first inbox subscriber without allocating a Vec
        let mut it = self.inbox_routes.matching_subscribers(rf, inbox_route);
        let Some(subscriber) = it.next() else {
            return RpcDeliveryResult::error("Inbox not found".to_string());
        };

        let message = RpcMessage {
            route: inbox_route.to_string(),
            correlation_id: Some(corr_id.to_string()),
            body: body.to_vec(),
            reply_route: None,
            seq: seq.map(|s| s as u32),
            is_stream_end,
        };

        RpcDeliveryResult::success(subscriber.channel_id, message)
    }

    /// Cleanup all resources for a channel (on disconnect)
    pub fn cleanup_channel(&mut self, rf: RouteFamilyId, channel_id: u32) {
        // Remove handler subscriptions for the route family
        self.handler_routes.cleanup_channel(rf, channel_id);

        // Remove inbox subscriptions and deallocate inboxes for the route family
        self.inbox_routes.cleanup_channel(rf, channel_id);

        // Remove inboxes owned by this channel
        self.inboxes
            .retain(|_, ctx| ctx.owner_channel_id != channel_id);

        // Note: active_requests is not keyed by channel today; we keep behavior consistent.
    }

    /// Get handler route table size (for metrics)
    pub fn handler_count(&self) -> usize {
        self.handler_routes.len()
    }

    /// Get inbox count (for metrics)
    pub fn inbox_count(&self) -> usize {
        self.inboxes.len()
    }
}

impl Default for RpcService {
    fn default() -> Self {
        Self::new(Arc::new(GlobalInternTable::new()))
    }
}

// --- Sync Benchmark Helpers ---------------------------------------------------

impl RpcService {
    /// Synchronous version of inbox route generation for benchmarking
    pub fn bench_inbox_allocation(&self) -> String {
        let mut inbox_route = String::with_capacity(44);
        inbox_route.push_str("inbox://");

        use std::fmt::Write;
        let _ = write!(&mut inbox_route, "{}", uuid::Uuid::new_v4());

        inbox_route
    }

    /// Synchronous version of correlation tracking for benchmarking
    /// Demonstrates core domain logic: request registration/deregistration
    pub fn bench_request_tracking(&self) -> usize {
        use fxhash::FxHashMap;

        let mut active_requests = FxHashMap::with_capacity_and_hasher(32, Default::default());

        for i in 0..10 {
            let corr_id = format!("req-{}", i);
            let handler_route = format!("rpc://test/svc/op{}", i);
            let reply_route = format!("inbox://inbox-{}", i);
            active_requests.insert(corr_id, (handler_route, reply_route));
        }

        for i in 0..5 {
            let corr_id = format!("req-{}", i);
            active_requests.remove(&corr_id);
        }

        active_requests.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_allocate_unique_inboxes_for_same_channel() {
        let mut service = RpcService::new_for_test();

        let inbox1 = service.allocate_inbox(1);
        let inbox2 = service.allocate_inbox(1);

        assert_ne!(inbox1, inbox2);
        assert!(inbox1.starts_with("inbox://"));
        assert!(inbox2.starts_with("inbox://"));
    }

    #[test]
    fn should_enforce_inbox_ownership_across_channels() {
        let mut service = RpcService::new_for_test();
        let inbox = service.allocate_inbox(1);

        let result1 = service.subscribe_inbox(DEFAULT_RF, inbox.clone(), 1);
        let result2 = service.subscribe_inbox(DEFAULT_RF, inbox.clone(), 2);

        assert!(result1.is_ok());
        assert!(result2.is_err());
    }

    #[test]
    fn should_cleanup_channel_resources_when_channel_disconnects() {
        let mut service = RpcService::new_for_test();
        let inbox = service.allocate_inbox(1);
        let _ = service.subscribe_inbox(DEFAULT_RF, inbox.clone(), 1);
        let _handler_sub =
            service.subscribe_handler(DEFAULT_RF, "rpc://test/svc/op".to_string(), 1);

        service.cleanup_channel(DEFAULT_RF, 1);

        assert_eq!(service.inbox_count(), 0);
        assert_eq!(service.handler_count(), 0);
    }

    #[test]
    fn should_generate_inbox_route() {
        let service = RpcService::new_for_test();

        let inbox = service.bench_inbox_allocation();

        assert!(inbox.starts_with("inbox://"));
        assert_eq!(inbox.len(), 44);
    }

    #[test]
    fn should_track_requests_synchronously() {
        let service = RpcService::new_for_test();

        let remaining = service.bench_request_tracking();

        assert_eq!(remaining, 5);
    }
}
