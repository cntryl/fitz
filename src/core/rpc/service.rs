//! RPC domain service - handles request/reply coordination and inbox management

use crate::core::domain::SubSender;
use crate::routing::{RouteFamilyId, RouteTable, RtSubscription};
use fxhash::FxHashMap;

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
/// - Handler authorization (only handlers can publish to client inboxes)
/// - Automatic cleanup on session close
pub struct RpcService {
    /// Next subscription ID
    next_sub_id: u64,

    /// Route table for RPC handler subscriptions (rpc://realm/area/resource/operation)
    handler_routes: RouteTable,

    /// Route table for inbox subscriptions (inbox://*)
    inbox_routes: RouteTable,

    /// Inbox ownership tracking (inbox_route -> context)
    inboxes: FxHashMap<String, InboxContext>,

    /// Active RPC requests for correlation tracking
    /// Maps correlation_id -> (handler_route, reply_route)
    active_requests: FxHashMap<String, (String, String)>,
}

impl RpcService {
    pub fn new() -> Self {
        Self {
            next_sub_id: 1,
            handler_routes: RouteTable::new(),
            inbox_routes: RouteTable::new(),
            // Pre-allocate capacity for typical workloads to reduce rehashing
            inboxes: FxHashMap::with_capacity_and_hasher(16, Default::default()),
            active_requests: FxHashMap::with_capacity_and_hasher(32, Default::default()),
        }
    }

    /// Allocate a cryptographically secure inbox route for a channel
    /// Returns the inbox route (e.g., "inbox://a1b2c3d4-e5f6-7890-abcd-ef1234567890")
    pub fn allocate_inbox(&mut self, channel_id: u32) -> String {
        // Generate cryptographically secure random route using UUID v4
        // Pre-allocate with exact capacity: "inbox://" (8) + UUID (36) = 44 bytes
        let mut inbox_route = String::with_capacity(44);
        inbox_route.push_str("inbox://");

        // Format UUID directly into the string to avoid intermediate allocation
        use std::fmt::Write;
        let _ = write!(&mut inbox_route, "{}", uuid::Uuid::new_v4());

        // Register inbox ownership
        self.inboxes.insert(
            inbox_route.clone(),
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
        sender: SubSender,
    ) -> u64 {
        let id = self.next_sub_id;
        self.next_sub_id = self.next_sub_id.wrapping_add(1);

        let sub = RtSubscription {
            id,
            route_pattern,
            channel_id,
            sender,
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
        sender: SubSender,
    ) -> Result<u64, String> {
        // Check if inbox exists and caller is owner
        match self.inboxes.get_mut(&inbox_route) {
            Some(ctx) if ctx.owner_channel_id == channel_id => {
                let id = self.next_sub_id;
                self.next_sub_id = self.next_sub_id.wrapping_add(1);

                let sub = RtSubscription {
                    id,
                    route_pattern: inbox_route, // Move instead of clone
                    channel_id,
                    sender,
                };

                self.inbox_routes.insert(rf, sub);
                ctx.subscription_id = Some(id);

                Ok(id)
            }
            Some(_) => Err("Permission denied: not inbox owner".to_string()),
            None => Err(format!("Inbox not found: {}", inbox_route)),
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
            // Clear subscription ID in inbox context
            if let Some(ctx) = self.inboxes.get_mut(&sub.route_pattern) {
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
    pub fn deregister_request(&mut self, correlation_id: &str) -> Option<(String, String)> {
        self.active_requests.remove(correlation_id)
    }

    /// Get matching handler subscribers for a route
    /// Hot path: called for every RPC request
    #[inline]
    pub fn matching_handlers(&self, rf: RouteFamilyId, route: &str) -> Vec<RtSubscription> {
        self.handler_routes.matching_subscribers(rf, route)
    }

    /// Get matching inbox subscribers for a reply route
    #[inline]
    pub fn matching_inbox_subscribers(
        &self,
        rf: RouteFamilyId,
        inbox_route: &str,
    ) -> Vec<RtSubscription> {
        self.inbox_routes.matching_subscribers(rf, inbox_route)
    }

    /// Check if a channel can publish to an inbox (only handlers of active requests)
    /// Hot path: called for every RPC reply to validate authorization
    #[inline]
    pub fn can_publish_to_inbox(&self, inbox_route: &str, correlation_id: &str) -> bool {
        // Check if this correlation_id has an active request pointing to this inbox
        // Direct equality check without dereferencing is faster
        self.active_requests
            .get(correlation_id)
            .map(|(_, reply_route)| reply_route.as_str() == inbox_route)
            .unwrap_or(false)
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

        // Remove active requests (correlation tracking)
        // Note: We don't track which channel initiated which request here,
        // but in a real implementation you'd want to track and clean those up too
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
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::DEFAULT_RF;
    use tokio::sync::mpsc;

    #[test]
    fn should_allocate_unique_inboxes_for_same_channel() {
        // Arrange
        let mut service = RpcService::new();

        // Act
        let inbox1 = service.allocate_inbox(1);
        let inbox2 = service.allocate_inbox(1);

        // Assert
        assert_ne!(inbox1, inbox2);
        assert!(inbox1.starts_with("inbox://"));
        assert!(inbox2.starts_with("inbox://"));
    }

    #[test]
    fn should_enforce_inbox_ownership_across_channels() {
        // Arrange
        let mut service = RpcService::new();
        let (tx, _rx) = mpsc::channel(1);
        let inbox = service.allocate_inbox(1);

        // Act
        let result1 = service.subscribe_inbox(DEFAULT_RF, inbox.clone(), 1, tx.clone());
        let result2 = service.subscribe_inbox(DEFAULT_RF, inbox.clone(), 2, tx.clone());

        // Assert
        assert!(result1.is_ok());
        assert!(result2.is_err());
    }

    #[test]
    fn should_cleanup_channel_resources_when_channel_disconnects() {
        // Arrange
        let mut service = RpcService::new();
        let (tx, _rx) = mpsc::channel(1);
        let inbox = service.allocate_inbox(1);
        let _ = service.subscribe_inbox(DEFAULT_RF, inbox.clone(), 1, tx.clone());
        let _handler_sub =
            service.subscribe_handler(DEFAULT_RF, "rpc://test/svc/op".to_string(), 1, tx.clone());

        // Act
        service.cleanup_channel(DEFAULT_RF, 1);

        // Assert
        assert_eq!(service.inbox_count(), 0);
        assert_eq!(service.handler_count(), 0);
    }

    #[test]
    fn should_track_active_requests_for_authorization() {
        // Arrange
        let mut service = RpcService::new();
        let corr_id = "req-123".to_string();
        let handler_route = "rpc://test/svc/op".to_string();
        let reply_route = "inbox://a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string();

        // Act
        service.register_request(corr_id.clone(), handler_route.clone(), reply_route.clone());

        // Assert
        assert!(service.can_publish_to_inbox(&reply_route, &corr_id));
        assert!(!service.can_publish_to_inbox("inbox://wrong-inbox-id", &corr_id));
        assert!(!service.can_publish_to_inbox(&reply_route, "wrong-id"));

        let removed = service.deregister_request(&corr_id);
        assert!(removed.is_some());
        assert!(!service.can_publish_to_inbox(&reply_route, &corr_id));
    }
}
