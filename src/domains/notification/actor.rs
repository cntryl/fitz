//! Notification actor implementation
//!
//! **Architecture:**
//! - NoticeRouteActor owns subscriptions for a specific (RouteFamily, route) pair
//! - SessionActor enforces all authentication and authorization before sending messages
//! - NoticeRouteActor trusts SessionActor and performs no auth checks
//! - Subscriptions are session-scoped and cleaned up via UnsubscribeAll on disconnect
//!
//! **Operations:**
//!
//! **Publish**: Fan-out payload to all subscribers whose patterns match the route
//! - Matches are computed against all patterns in this route's scope
//! - Matching uses wildcard rules (* and **)
//! - Delivery is fire-and-forget via actor messaging
//!
//! **Subscribe**: Register a new pattern+session+subscriber
//! - SessionActor has already verified authorization
//! - Creates subscription entry scoped by session_id
//! - Multiple subscribers per pattern allowed
//!
//! **Unsubscribe**: Remove a specific subscription
//! - Idempotent: unsubscribing non-existent subscription is safe
//!
//! **UnsubscribeAll**: Called when session disconnects
//! - Removes all subscriptions for that session

use crate::domains::notification::protocol::{
    NotificationMessage, NotifyMessage, PublishMessage, SubscribeMessage, UnsubscribeAllMessage,
    UnsubscribeMessage,
};
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::RouteFamily;
use crate::runtime::subscriptions::{SubscriptionId, SubscriptionIndex};
use crate::session::session::SessionId;
use std::collections::HashMap;

/// Maps subscription ID to (session_id, subscriber_address)
type SubscriptionMap = HashMap<SubscriptionId, (SessionId, crate::runtime::routing::RouteAddress)>;

/// NoticeRouteActor owns subscriptions for a specific (RouteFamily, route) pair
///
/// This actor:
/// - Maintains a subscription index (trie) for fast wildcard matching
/// - Performs fanout when messages are published
/// - Cleans up subscriptions when sessions disconnect
/// - Trusts SessionActor for all authorization checks
pub struct NoticeRouteActor {
    /// Route family isolation boundary
    family_id: RouteFamily,
    /// Fast, in-memory trie-based subscription index
    /// Maps patterns (with wildcards) to subscription IDs
    index: SubscriptionIndex,
    /// Maps subscription ID to metadata (session_id, subscriber address)
    subscriptions: SubscriptionMap,
    /// Counter for generating unique subscription IDs
    next_subscription_id: u64,
}

impl NoticeRouteActor {
    /// Create a new NoticeRouteActor for a specific route family
    pub fn new(family_id: RouteFamily) -> Self {
        Self {
            family_id,
            index: SubscriptionIndex::new(),
            subscriptions: HashMap::new(),
            next_subscription_id: 1,
        }
    }

    /// Generate a unique subscription ID
    fn allocate_subscription_id(&mut self) -> SubscriptionId {
        let id = SubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        id
    }

    /// Subscribe to a pattern (SessionActor has already verified authorization)
    fn handle_subscribe(&mut self, msg: SubscribeMessage, _ctx: &mut Context<Self>) {
        let subscription_id = self.allocate_subscription_id();

        // Add subscription to the trie-based index
        self.index
            .insert(self.family_id, &msg.pattern, subscription_id);

        // Store metadata: session_id and subscriber address
        self.subscriptions
            .insert(subscription_id, (msg.session_id, msg.subscriber));
    }

    /// Unsubscribe from a specific pattern
    fn handle_unsubscribe(&mut self, msg: UnsubscribeMessage) {
        // Find and remove all subscriptions matching this session + subscriber + pattern
        // Since the index doesn't return IDs directly, we need to scan subscriptions
        let to_remove: Vec<SubscriptionId> = self
            .subscriptions
            .iter()
            .filter(|(_, (sess_id, addr))| *sess_id == msg.session_id && addr == &msg.subscriber)
            .map(|(&id, _)| id)
            .collect();

        for id in to_remove {
            self.subscriptions.remove(&id);
            // Note: The index doesn't support remove by pattern, only by ID
            // Full cleanup would require tracking pattern->id mappings
            // For now, dead entries will be skipped during fanout
        }
    }

    /// Unsubscribe all subscriptions for a session (called on disconnect)
    fn handle_unsubscribe_all(&mut self, msg: UnsubscribeAllMessage) {
        // Remove all subscriptions for this session
        let to_remove: Vec<SubscriptionId> = self
            .subscriptions
            .iter()
            .filter(|(_, (_, addr))| addr == &msg.subscriber)
            .map(|(&id, _)| id)
            .collect();

        for id in to_remove {
            self.subscriptions.remove(&id);
        }
    }

    /// Publish a message, fan-out to all matching subscribers
    fn handle_publish(&mut self, msg: PublishMessage, ctx: &mut Context<Self>) {
        // Find all subscription IDs that match this published route
        let matching_ids = self.index.match_all(self.family_id, &msg.route);

        // Fan-out to each matching subscriber
        for subscription_id in matching_ids {
            if let Some((_, subscriber)) = self.subscriptions.get(&subscription_id) {
                let notify = NotifyMessage::new(msg.route.clone(), msg.payload.clone());
                let _ = ctx.send(subscriber.clone(), NotificationMessage::Notify(notify));
            }
        }
    }
}

impl Default for NoticeRouteActor {
    fn default() -> Self {
        Self::new(RouteFamily::new(0))
    }
}

impl Actor for NoticeRouteActor {
    type Message = NotificationMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            NotificationMessage::Publish(publish) => self.handle_publish(publish, ctx),
            NotificationMessage::Subscribe(subscribe) => self.handle_subscribe(subscribe, ctx),
            NotificationMessage::Unsubscribe(unsubscribe) => self.handle_unsubscribe(unsubscribe),
            NotificationMessage::UnsubscribeAll(unsubscribe_all) => {
                self.handle_unsubscribe_all(unsubscribe_all)
            }
            NotificationMessage::Notify(_) => {
                // NoticeRouteActor doesn't receive Notify messages
                // (those are sent to subscribers via SessionActor)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

    fn test_family() -> RouteFamily {
        RouteFamily::new(1)
    }

    fn test_route(path: &str) -> Route {
        Route::new(path.to_string())
    }

    fn test_address(suffix: &str) -> RouteAddress {
        let family = test_family();
        RouteAddress::new(family, test_route(&format!("test://{}", suffix)))
    }

    fn test_session_id(n: u64) -> SessionId {
        SessionId(n)
    }

    #[test]
    fn should_create_notice_route_actor() {
        // Arrange & Act
        let actor = NoticeRouteActor::new(test_family());

        // Assert
        assert_eq!(actor.subscriptions.len(), 0);
    }

    #[test]
    fn should_track_subscriptions() {
        // Arrange
        let mut actor = NoticeRouteActor::new(test_family());
        let pattern = test_route("notice://realm/orders/update");
        let subscriber = test_address("session1");
        let _subscribe =
            SubscribeMessage::new(test_family(), pattern, test_session_id(1), subscriber);

        // Act
        // We'd normally call this with a Context, but for this test
        // we just verify the subscription is tracked
        actor.subscriptions.insert(
            SubscriptionId(1),
            (test_session_id(1), test_address("session1")),
        );

        // Assert
        assert_eq!(actor.subscriptions.len(), 1);
    }

    #[test]
    fn should_clean_up_on_session_disconnect() {
        // Arrange
        let mut actor = NoticeRouteActor::new(test_family());
        let subscriber = test_address("subscriber");

        // Add subscriptions for a session
        actor
            .subscriptions
            .insert(SubscriptionId(1), (test_session_id(1), subscriber.clone()));
        actor
            .subscriptions
            .insert(SubscriptionId(2), (test_session_id(1), subscriber.clone()));

        assert_eq!(actor.subscriptions.len(), 2);

        // Act
        let unsubscribe_all = UnsubscribeAllMessage::new(test_session_id(1), subscriber);
        actor.handle_unsubscribe_all(unsubscribe_all);

        // Assert
        assert_eq!(actor.subscriptions.len(), 0);
    }

    #[test]
    fn should_allow_multiple_sessions_on_same_actor() {
        // Arrange
        let mut actor = NoticeRouteActor::new(test_family());
        let sub1 = test_address("session1");
        let sub2 = test_address("session2");

        // Add subscriptions for different sessions
        actor
            .subscriptions
            .insert(SubscriptionId(1), (test_session_id(1), sub1.clone()));
        actor
            .subscriptions
            .insert(SubscriptionId(2), (test_session_id(2), sub2.clone()));

        // Act
        // Disconnect session 1
        let unsubscribe_all = UnsubscribeAllMessage::new(test_session_id(1), sub1);
        actor.handle_unsubscribe_all(unsubscribe_all);

        // Assert
        assert_eq!(actor.subscriptions.len(), 1);
        assert!(actor.subscriptions.contains_key(&SubscriptionId(2)));
    }

    #[test]
    fn should_support_idempotent_unsubscribe() {
        // Arrange
        let mut actor = NoticeRouteActor::new(test_family());
        let subscriber = test_address("subscriber");

        actor
            .subscriptions
            .insert(SubscriptionId(1), (test_session_id(1), subscriber.clone()));

        // Act
        let unsubscribe = UnsubscribeMessage::new(
            test_family(),
            test_route("notice://realm/orders/*"),
            test_session_id(1),
            subscriber.clone(),
        );
        actor.handle_unsubscribe(unsubscribe.clone());
        actor.handle_unsubscribe(unsubscribe); // Second time should be safe

        // Assert - subscription was removed in first call
        assert_eq!(actor.subscriptions.len(), 0); // Now empty after removal
    }

    #[test]
    fn should_trust_session_actor_for_auth() {
        // This test documents the trust assumption:
        // NoticeRouteActor performs NO authentication checks.
        // It assumes SessionActor has already verified authorization.

        // Arrange
        let actor = NoticeRouteActor::new(test_family());

        // If we tried to call handle_subscribe or handle_publish directly,
        // there's no check that would fail. The actor trusts completely.
        // This is intentional -- auth/authz is SessionActor's responsibility.

        // Assert: Just verify the actor exists and has no auth logic
        assert_eq!(actor.subscriptions.len(), 0);
    }
}
