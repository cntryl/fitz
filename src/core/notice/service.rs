//! Notice domain service - owns all notice business logic and subscription routing
//!
//! The notice service maintains its own internal subscription registry for
//! fine-tuned control over pub/sub semantics, backpressure, and routing.

use std::collections::HashMap;
use tokio::sync::mpsc;

/// Type alias for subscriber channels
pub type SubSender = mpsc::Sender<(String, Option<String>, Vec<u8>, Option<String>, Option<u32>, bool)>;

/// Subscription entry maintained by the notice service
#[derive(Debug)]
struct Subscription {
    id: u64,
    route_pattern: String,
    channel_id: u32,
    sender: SubSender,
}

/// Notice service handles ephemeral pub/sub operations
/// - Subscribe/Unsubscribe: manage in-memory subscriptions
/// - Publish: dispatch notifications to matching subscribers
/// - Best-effort delivery with backpressure handling
pub struct NoticeService {
    next_sub_id: u64,
    subscriptions: HashMap<u64, Subscription>,
}

impl NoticeService {
    /// Create a new notice service
    pub fn new() -> Self {
        Self {
            next_sub_id: 1,
            subscriptions: HashMap::new(),
        }
    }

    /// Subscribe to a route pattern
    /// Returns subscription ID
    pub fn subscribe(&mut self, route_pattern: String, channel_id: u32, sender: SubSender) -> u64 {
        let id = self.next_sub_id;
        self.next_sub_id = self.next_sub_id.wrapping_add(1);
        
        self.subscriptions.insert(
            id,
            Subscription {
                id,
                route_pattern,
                channel_id,
                sender,
            },
        );
        
        id
    }

    /// Unsubscribe by subscription ID
    /// Returns true if subscription was found and removed
    pub fn unsubscribe(&mut self, sub_id: u64) -> bool {
        self.subscriptions.remove(&sub_id).is_some()
    }

    /// Cleanup all subscriptions for a channel (e.g., on disconnect)
    pub fn cleanup_channel(&mut self, channel_id: u32) {
        self.subscriptions.retain(|_, sub| sub.channel_id != channel_id);
    }

    /// Publish a notification to all matching subscribers
    /// Returns (delivered_count, failed_count)
    pub fn publish(
        &mut self,
        route: &str,
        msg_id: Option<&str>,
        body: &[u8],
        reply_to: Option<&str>,
        seq: Option<u32>,
        end: bool,
    ) -> (usize, usize) {
        let mut delivered = 0;
        let mut failed = 0;
        let mut dead_subs = Vec::new();

        for (sub_id, sub) in &self.subscriptions {
            if route_matches(&sub.route_pattern, route) {
                // Try to send notification (best-effort, non-blocking)
                match sub.sender.try_send((
                    route.to_string(),
                    msg_id.map(|s| s.to_string()),
                    body.to_vec(),
                    reply_to.map(|s| s.to_string()),
                    seq,
                    end,
                )) {
                    Ok(_) => delivered += 1,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Backpressure: drop notification for this subscriber
                        failed += 1;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        // Subscriber disconnected, mark for removal
                        dead_subs.push(*sub_id);
                        failed += 1;
                    }
                }
            }
        }

        // Cleanup dead subscriptions
        for sub_id in dead_subs {
            self.subscriptions.remove(&sub_id);
        }

        (delivered, failed)
    }

    /// Get count of active subscriptions
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }
}

impl Default for NoticeService {
    fn default() -> Self {
        Self::new()
    }
}

/// Route pattern matching for notices
/// - Exact match
/// - Trailing '*' wildcard as prefix match (e.g., "a/b/*")
/// - Hierarchical prefix match (e.g., "a/b" matches "a/b/c")
/// - Global wildcard '*' matches all routes
fn route_matches(pattern: &str, route: &str) -> bool {
    // Global wildcard
    if pattern == "*" {
        return true;
    }

    // Exact match
    if pattern == route {
        return true;
    }

    // Trailing wildcard: "a/b/*" matches "a/b/c" and "a/b/c/d"
    if let Some(prefix) = pattern.strip_suffix("/*") {
        if route == prefix || route.starts_with(&format!("{}/", prefix)) {
            return true;
        }
    }

    // Hierarchical prefix: "a/b" matches "a/b/c"
    if route.starts_with(&format!("{}/", pattern)) {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_match_exact_route() {
        assert!(route_matches("a/b/c", "a/b/c"));
        assert!(!route_matches("a/b/c", "a/b/d"));
    }

    #[test]
    fn should_match_global_wildcard() {
        assert!(route_matches("*", "a/b/c"));
        assert!(route_matches("*", "anything"));
    }

    #[test]
    fn should_match_trailing_wildcard() {
        assert!(route_matches("a/b/*", "a/b/c"));
        assert!(route_matches("a/b/*", "a/b/c/d"));
        assert!(!route_matches("a/b/*", "a/c/d"));
    }

    #[test]
    fn should_match_hierarchical_prefix() {
        assert!(route_matches("a/b", "a/b/c"));
        assert!(route_matches("a/b", "a/b/c/d"));
        assert!(!route_matches("a/b", "a/c/d"));
    }

    #[tokio::test]
    async fn should_subscribe_and_publish() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);

        // Act
        let sub_id = service.subscribe("test/route".to_string(), 1, tx);
        let (delivered, failed) = service.publish("test/route", Some("msg-1"), b"hello", None, None, false);

        // Assert
        assert_eq!(delivered, 1);
        assert_eq!(failed, 0);
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.0, "test/route");
        assert_eq!(msg.2, b"hello");
    }

    #[tokio::test]
    async fn should_unsubscribe() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);
        let sub_id = service.subscribe("test/route".to_string(), 1, tx);

        // Act
        let removed = service.unsubscribe(sub_id);
        let (delivered, _) = service.publish("test/route", Some("msg-1"), b"hello", None, None, false);

        // Assert
        assert!(removed);
        assert_eq!(delivered, 0);
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn should_cleanup_channel() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx1, _rx1) = mpsc::channel(10);
        let (tx2, _rx2) = mpsc::channel(10);
        service.subscribe("route1".to_string(), 1, tx1);
        service.subscribe("route2".to_string(), 2, tx2);

        // Act
        service.cleanup_channel(1);

        // Assert
        assert_eq!(service.subscription_count(), 1);
    }

    #[tokio::test]
    async fn should_handle_backpressure() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, _rx) = mpsc::channel(1);
        service.subscribe("test/route".to_string(), 1, tx);

        // Act - fill the channel and overflow
        service.publish("test/route", Some("msg-1"), b"1", None, None, false);
        let (delivered, failed) = service.publish("test/route", Some("msg-2"), b"2", None, None, false);

        // Assert - second publish should fail due to backpressure
        assert_eq!(failed, 1);
    }
}
