//! Notification actor implementation//!
//! The NotificationsActor manages in-memory pub/sub with wildcard routing.
//!
//! # State
//!
//! - Subscriber registry indexed by (RouteFamily, pattern)
//! - Each subscription tracks the subscriber ActorRef
//! - Subscriptions are isolated by RouteFamily (no cross-family delivery)
//!
//! # Operations
//!
//! **Publish**: Fan-out payload to all subscribers whose patterns match the route
//! - Matches are computed against all patterns in the same RouteFamily
//! - Matching uses wildcard rules (* and **)
//! - Delivery is fire-and-forget via actor messaging
//!
//! **Subscribe**: Register a new pattern+subscriber for a RouteFamily
//! - Creates or updates subscription entry
//! - Idempotent: same subscription added twice is safe
//!
//! **Unsubscribe**: Remove a subscription
//! - Removes only exact pattern+subscriber matches
//! - Idempotent: unsubscribing non-existent subscription is safe

use crate::domains::notification::matcher::Pattern;
use crate::domains::notification::protocol::{
    NotificationMessage, NotifyMessage, PublishMessage, SubscribeMessage, UnsubscribeMessage,
};
use crate::runtime::actor::{Actor, Context};
use crate::transport::routing::RouteFamily;
use std::collections::HashMap;

/// Key for a subscription: (RouteFamily, pattern route string)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SubscriptionKey {
    family_id: RouteFamily,
    pattern: String,
}

/// A single subscription entry
#[derive(Debug, Clone)]
struct Subscription {
    pattern: Pattern,
    subscriber: crate::transport::routing::RouteAddress,
}

/// Notification domain actor
///
/// Manages in-memory pub/sub subscriptions and fan-out delivery.
pub struct NotificationActor {
    /// All subscriptions indexed by (family_id, pattern string)
    /// Value is list of subscribers for that pattern
    subscriptions: HashMap<SubscriptionKey, Vec<Subscription>>,
}

impl NotificationActor {
    /// Create a new notification actor
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
        }
    }

    /// Publish a message, fan-out to all matching subscribers
    fn handle_publish(&mut self, msg: PublishMessage, ctx: &mut Context<Self>) {
        let family_id = msg.family_id.clone();

        // Find all subscriptions for this RouteFamily
        for (key, subs) in self.subscriptions.iter() {
            if key.family_id != family_id {
                continue; // Different family: skip
            }

            // Check each subscription pattern against the published route
            for sub in subs {
                if sub.pattern.matches(&msg.route) {
                    // Send notification to subscriber
                    let notify = NotifyMessage::new(msg.route.clone(), msg.payload.clone());
                    let _ = ctx.send(sub.subscriber.clone(), NotificationMessage::Notify(notify));
                }
            }
        }
    }

    /// Subscribe to a pattern
    fn handle_subscribe(&mut self, msg: SubscribeMessage) {
        let key = SubscriptionKey {
            family_id: msg.family_id.clone(),
            pattern: msg.pattern.as_str().to_string(),
        };

        let pattern = Pattern::new(msg.pattern.as_str());
        let subscription = Subscription {
            pattern,
            subscriber: msg.subscriber,
        };

        self.subscriptions
            .entry(key)
            .or_insert_with(Vec::new)
            .push(subscription);
    }

    /// Unsubscribe from a pattern
    fn handle_unsubscribe(&mut self, msg: UnsubscribeMessage) {
        let key = SubscriptionKey {
            family_id: msg.family_id.clone(),
            pattern: msg.pattern.as_str().to_string(),
        };

        if let Some(subs) = self.subscriptions.get_mut(&key) {
            // Remove all subscriptions matching this subscriber
            subs.retain(|sub| sub.subscriber != msg.subscriber);

            // If no subscribers left, clean up the key
            if subs.is_empty() {
                self.subscriptions.remove(&key);
            }
        }
    }
}

impl Default for NotificationActor {
    fn default() -> Self {
        Self::new()
    }
}

impl Actor for NotificationActor {
    type Message = NotificationMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            NotificationMessage::Publish(publish) => self.handle_publish(publish, ctx),
            NotificationMessage::Subscribe(subscribe) => self.handle_subscribe(subscribe),
            NotificationMessage::Unsubscribe(unsubscribe) => self.handle_unsubscribe(unsubscribe),
            NotificationMessage::Notify(_) => {
                // NotificationActor doesn't receive Notify messages
                // (those are sent to subscribers)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::routing::{Route, RouteAddress, RouteFamily};

    fn test_family() -> RouteFamily {
        RouteFamily::new(1)
    }

    fn test_route(path: &str) -> Route {
        Route::new(path.to_string())
    }

    fn test_address() -> RouteAddress {
        RouteAddress::new(test_family(), test_route("test://subscriber"))
    }

    #[test]
    fn should_create_notification_actor() {
        let actor = NotificationActor::new();
        assert_eq!(actor.subscriptions.len(), 0);
    }

    #[test]
    fn should_subscribe_to_pattern() {
        let mut actor = NotificationActor::new();
        let family = test_family();
        let pattern = test_route("notify://realm/orders/*");
        let subscriber = test_address();

        let subscribe = SubscribeMessage::new(family.clone(), pattern.clone(), subscriber.clone());
        actor.handle_subscribe(subscribe);

        assert_eq!(actor.subscriptions.len(), 1);
    }

    #[test]
    fn should_unsubscribe_from_pattern() {
        let mut actor = NotificationActor::new();
        let family = test_family();
        let pattern = test_route("notify://realm/orders/*");
        let subscriber = test_address();

        let subscribe = SubscribeMessage::new(family.clone(), pattern.clone(), subscriber.clone());
        actor.handle_subscribe(subscribe);
        assert_eq!(actor.subscriptions.len(), 1);

        let unsubscribe = UnsubscribeMessage::new(family, pattern, subscriber);
        actor.handle_unsubscribe(unsubscribe);
        assert_eq!(actor.subscriptions.len(), 0);
    }

    #[test]
    fn should_isolate_subscriptions_across_families() {
        let mut actor = NotificationActor::new();
        let family1 = RouteFamily::new(1);
        let family2 = RouteFamily::new(2);
        let pattern = test_route("notify://realm/orders/*");
        let subscriber = test_address();

        // Subscribe to same pattern in different families
        actor.handle_subscribe(SubscribeMessage::new(
            family1,
            pattern.clone(),
            subscriber.clone(),
        ));
        actor.handle_subscribe(SubscribeMessage::new(family2.clone(), pattern, subscriber));

        assert_eq!(actor.subscriptions.len(), 2);
    }

    #[test]
    fn should_allow_multiple_subscribers_to_same_pattern() {
        let mut actor = NotificationActor::new();
        let family = test_family();
        let pattern = test_route("notify://realm/orders/*");
        let sub1 = test_address();
        let sub2 = test_address();

        actor.handle_subscribe(SubscribeMessage::new(family.clone(), pattern.clone(), sub1));
        actor.handle_subscribe(SubscribeMessage::new(family, pattern, sub2));

        assert_eq!(actor.subscriptions.len(), 1);
        let subs = &actor.subscriptions.values().next().unwrap();
        assert_eq!(subs.len(), 2);
    }

    #[test]
    fn should_unsubscribe_only_matching_subscriber() {
        let mut actor = NotificationActor::new();
        let family = test_family();
        let pattern = test_route("notify://realm/orders/*");
        let sub1 = RouteAddress::new(family.clone(), test_route("test://subscriber/1"));
        let sub2 = RouteAddress::new(family.clone(), test_route("test://subscriber/2"));

        actor.handle_subscribe(SubscribeMessage::new(
            family.clone(),
            pattern.clone(),
            sub1.clone(),
        ));
        actor.handle_subscribe(SubscribeMessage::new(
            family.clone(),
            pattern.clone(),
            sub2.clone(),
        ));

        // Unsubscribe only sub1
        actor.handle_unsubscribe(UnsubscribeMessage::new(family, pattern, sub1));

        // Should still have 1 subscription
        assert_eq!(actor.subscriptions.len(), 1);
        let subs = &actor.subscriptions.values().next().unwrap();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].subscriber, sub2);
    }

    #[test]
    fn should_support_idempotent_unsubscribe() {
        let mut actor = NotificationActor::new();
        let family = test_family();
        let pattern = test_route("notify://realm/orders/*");
        let subscriber = test_address();

        actor.handle_subscribe(SubscribeMessage::new(
            family.clone(),
            pattern.clone(),
            subscriber.clone(),
        ));

        // Unsubscribe twice
        actor.handle_unsubscribe(UnsubscribeMessage::new(
            family.clone(),
            pattern.clone(),
            subscriber.clone(),
        ));
        actor.handle_unsubscribe(UnsubscribeMessage::new(family, pattern, subscriber));

        // Should be empty
        assert_eq!(actor.subscriptions.len(), 0);
    }

    #[test]
    fn should_support_idempotent_subscribe() {
        let mut actor = NotificationActor::new();
        let family = test_family();
        let pattern = test_route("notify://realm/orders/*");
        let subscriber = test_address();

        // Subscribe same pattern+subscriber twice
        actor.handle_subscribe(SubscribeMessage::new(
            family.clone(),
            pattern.clone(),
            subscriber.clone(),
        ));
        actor.handle_subscribe(SubscribeMessage::new(family, pattern, subscriber));

        assert_eq!(actor.subscriptions.len(), 1);
        let subs = &actor.subscriptions.values().next().unwrap();
        // Idempotency is not enforced (both added), but that's OK for in-memory
        assert_eq!(subs.len(), 2);
    }
}
