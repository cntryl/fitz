//! Notice domain service - owns all notice business logic and subscription routing
//!
//! This implementation delegates subscription bookkeeping to an internal
//! in-memory RouteTable (route_table.rs) and uses the shared SubSender alias
//! from crate::core::domain.

use crate::core::domain::SubSender;
use crate::routing::{RouteFamilyId, RouteTable, RtSubscription};

#[cfg(test)]
use crate::routing::DEFAULT_RF;
use smallvec::SmallVec;
use tokio::sync::mpsc;

/// Notice service handles ephemeral pub/sub operations
/// - Subscribe/Unsubscribe: manage in-memory subscriptions
/// - Publish: dispatch notifications to matching subscribers
/// - Best-effort delivery with backpressure handling
pub struct NoticeService {
    next_sub_id: u64,
    route_table: RouteTable,
}

/// Result of a publish operation: delivered and failed counts
#[derive(Debug, Clone)]
pub struct PublishResult {
    pub delivered: usize,
    pub failed: usize,
}

impl NoticeService {
    /// Create a new notice service
    pub fn new() -> Self {
        Self {
            next_sub_id: 1,
            route_table: RouteTable::new(),
        }
    }

    /// Subscribe to a route pattern for a specific route family (tenant)
    /// Returns subscription ID
    pub fn subscribe(
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

        self.route_table.insert(rf, sub);

        id
    }

    /// Unsubscribe by subscription ID for a specific route family (tenant)
    /// Returns true if subscription was found and removed
    pub fn unsubscribe(&mut self, rf: RouteFamilyId, sub_id: u64) -> bool {
        self.route_table.remove(rf, sub_id).is_some()
    }

    /// Cleanup all subscriptions for a channel in a specific route family (tenant)
    pub fn cleanup_channel(&mut self, rf: RouteFamilyId, channel_id: u32) {
        self.route_table.cleanup_channel(rf, channel_id);
    }

    /// Publish a notification to all matching subscribers in a specific route family (tenant)
    /// Returns `PublishResult` containing delivered and failed counts
    ///
    /// Optimizations:
    /// - Uses SmallVec for dead_subs (typically 0-2 dead subs per publish)
    /// - Early return for no subscribers
    /// - Optimizes single subscriber case (no pre-allocation needed)
    pub fn publish(
        &mut self,
        rf: RouteFamilyId,
        route: &str,
        msg_id: Option<&str>,
        body: &[u8],
    ) -> PublishResult {
        let matches = self.route_table.matching_subscribers(rf, route);

        // Fast path: no subscribers
        if matches.is_empty() {
            return PublishResult { delivered: 0, failed: 0 };
        }

        let mut delivered = 0usize;
        let mut failed = 0usize;
        let mut dead_subs = SmallVec::<[u64; 4]>::new();

        // Optimized path for single subscriber (most common case)
        if matches.len() == 1 {
            let sub = &matches[0];
                match sub.sender.try_send((
                route.to_string(),
                msg_id.map(|s| s.to_string()),
                body.to_vec(),
                None,  // Notices never have reply_to
                None,  // Notices never have seq
                false, // Notices never have end flag
            )) {
                            Ok(_) => return PublishResult { delivered: 1, failed: 0 },
                Err(mpsc::error::TrySendError::Full(_)) => {
                    return PublishResult { delivered: 0, failed: 1 }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Subscriber disconnected, remove it
                    let _ = self.route_table.remove(rf, sub.id);
                    return PublishResult { delivered: 0, failed: 1 };
                }
            }
        }

        // Multi-subscriber path: pre-allocate to avoid repeated conversions
        let route_owned = route.to_string();
        let msg_id_owned = msg_id.map(|s| s.to_string());
        let body_owned = body.to_vec();

        for sub in matches {
            match sub.sender.try_send((
                route_owned.clone(),
                msg_id_owned.clone(),
                body_owned.clone(),
                None,  // Notices never have reply_to
                None,  // Notices never have seq
                false, // Notices never have end flag
            )) {
                Ok(_) => delivered += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Backpressure: drop notification for this subscriber
                    failed += 1;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Subscriber disconnected, mark for removal
                    dead_subs.push(sub.id);
                    failed += 1;
                }
            }
        }

        // Cleanup dead subscriptions
        for sub_id in dead_subs {
            let _ = self.route_table.remove(rf, sub_id);
        }

        PublishResult { delivered, failed }
    }

    /// Get count of active subscriptions
    pub fn subscription_count(&self) -> usize {
        self.route_table.len()
    }
}

impl Default for NoticeService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_subscribe_and_publish() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);

        // Act
        let _sub_id = service.subscribe(DEFAULT_RF, "test/route".to_string(), 1, tx);
        let r = service.publish(DEFAULT_RF, "test/route", Some("msg-1"), b"hello");
        let delivered = r.delivered;
        let failed = r.failed;

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
        let sub_id = service.subscribe(DEFAULT_RF, "test/route".to_string(), 1, tx);

        // Act
        let removed = service.unsubscribe(DEFAULT_RF, sub_id);
        let r = service.publish(DEFAULT_RF, "test/route", Some("msg-1"), b"hello");
        let delivered = r.delivered;

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
        service.subscribe(DEFAULT_RF, "route1".to_string(), 1, tx1);
        service.subscribe(DEFAULT_RF, "route2".to_string(), 2, tx2);

        // Act
        service.cleanup_channel(DEFAULT_RF, 1);

        // Assert
        assert_eq!(service.subscription_count(), 1);
    }

    #[tokio::test]
    async fn should_handle_backpressure() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, _rx) = mpsc::channel(1);
        service.subscribe(DEFAULT_RF, "test/route".to_string(), 1, tx);

        // Act - fill the channel and overflow
        let _ = service.publish(DEFAULT_RF, "test/route", Some("msg-1"), b"1");
        let r = service.publish(DEFAULT_RF, "test/route", Some("msg-2"), b"2");
        let failed = r.failed;

        // Assert - second publish should fail due to backpressure
        assert_eq!(failed, 1);
    }

    // ========================================================================
    // COMPREHENSIVE WILDCARD PATTERN MATCHING TESTS
    // ========================================================================

    #[tokio::test]
    async fn should_match_global_wildcard_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);
        service.subscribe(DEFAULT_RF, "*".to_string(), 1, tx);

        // Act & Assert - global wildcard matches everything
        let test_routes = vec![
            "notice://realm/area/resource/op",
            "a/b/c",
            "single",
            "anything/goes/here",
        ];

        for route in test_routes {
            let r = service.publish(DEFAULT_RF, route, None, b"test");
            let delivered = r.delivered;
            let failed = r.failed;
            assert_eq!(
                delivered, 1,
                "Route '{}' should match global wildcard",
                route
            );
            assert_eq!(failed, 0);
            let _ = rx.recv().await.unwrap();
        }
    }

    #[tokio::test]
    async fn should_match_realm_wildcard_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);
        service.subscribe(DEFAULT_RF, "notice://acme/*".to_string(), 1, tx);

        // Act & Assert - should match all under realm
        let matching_routes = vec![
            ("notice://acme/prod/syslog/error", true),
            ("notice://acme/dev/app/warning", true),
            ("notice://acme/staging/db/critical", true),
            ("notice://other/prod/syslog/error", false),
            ("notice://acme", true), // Exact match to prefix
        ];

        for (route, should_match) in matching_routes {
            let r = service.publish(DEFAULT_RF, route, None, b"test");
            let delivered = r.delivered;
            if should_match {
                assert_eq!(
                    delivered, 1,
                    "Route '{}' should match realm wildcard",
                    route
                );
                let _ = rx.recv().await.unwrap();
            } else {
                assert_eq!(
                    delivered, 0,
                    "Route '{}' should not match realm wildcard",
                    route
                );
            }
        }
    }

    #[tokio::test]
    async fn should_match_area_wildcard_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);
        service.subscribe(DEFAULT_RF, "notice://acme/prod/*".to_string(), 1, tx);

        // Act & Assert
        let matching_routes = vec![
            ("notice://acme/prod/syslog/error", true),
            ("notice://acme/prod/app/info", true),
            ("notice://acme/prod/db/query", true),
            ("notice://acme/dev/syslog/error", false),
            ("notice://acme/staging/app/info", false),
            ("notice://other/prod/syslog/error", false),
        ];

        for (route, should_match) in matching_routes {
            let r = service.publish(DEFAULT_RF, route, None, b"test");
            let delivered = r.delivered;
            if should_match {
                assert_eq!(delivered, 1, "Route '{}' should match area wildcard", route);
                let _ = rx.recv().await.unwrap();
            } else {
                assert_eq!(
                    delivered, 0,
                    "Route '{}' should not match area wildcard",
                    route
                );
            }
        }
    }

    #[tokio::test]
    async fn should_match_resource_wildcard_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);
        service.subscribe(DEFAULT_RF, "notice://acme/prod/syslog/*".to_string(), 1, tx);

        // Act & Assert
        let matching_routes = vec![
            ("notice://acme/prod/syslog/error", true),
            ("notice://acme/prod/syslog/warning", true),
            ("notice://acme/prod/syslog/info", true),
            ("notice://acme/prod/app/error", false),
            ("notice://acme/dev/syslog/error", false),
        ];

        for (route, should_match) in matching_routes {
            let r = service.publish(DEFAULT_RF, route, None, b"test");
            let delivered = r.delivered;
            if should_match {
                assert_eq!(
                    delivered, 1,
                    "Route '{}' should match resource wildcard",
                    route
                );
                let _ = rx.recv().await.unwrap();
            } else {
                assert_eq!(
                    delivered, 0,
                    "Route '{}' should not match resource wildcard",
                    route
                );
            }
        }
    }

    #[tokio::test]
    async fn should_match_exact_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);
        service.subscribe(
            DEFAULT_RF,
            "notice://acme/prod/syslog/critical".to_string(),
            1,
            tx,
        );

        // Act & Assert
        let matching_routes = vec![
            ("notice://acme/prod/syslog/critical", true),
            ("notice://acme/prod/syslog/error", false),
            ("notice://acme/prod/syslog/warning", false),
            ("notice://acme/prod/app/critical", false),
        ];

        for (route, should_match) in matching_routes {
            let r = service.publish(DEFAULT_RF, route, None, b"test");
            let delivered = r.delivered;
            if should_match {
                assert_eq!(delivered, 1, "Route '{}' should match exact pattern", route);
                let _ = rx.recv().await.unwrap();
            } else {
                assert_eq!(
                    delivered, 0,
                    "Route '{}' should not match exact pattern",
                    route
                );
            }
        }
    }

    #[tokio::test]
    async fn should_match_hierarchical_prefix_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);
        service.subscribe(DEFAULT_RF, "notice://acme/prod/syslog".to_string(), 1, tx);

        // Act & Assert - without trailing /*, still matches child paths
        let matching_routes = vec![
            ("notice://acme/prod/syslog", true),              // Exact
            ("notice://acme/prod/syslog/error", true),        // Child
            ("notice://acme/prod/syslog/warning", true),      // Child
            ("notice://acme/prod/syslog/info/verbose", true), // Deep child
            ("notice://acme/prod/app", false),                // Different resource
            ("notice://acme/prod", false),                    // Parent
        ];

        for (route, should_match) in matching_routes {
            let r = service.publish(DEFAULT_RF, route, None, b"test");
            let delivered = r.delivered;
            if should_match {
                assert_eq!(
                    delivered, 1,
                    "Route '{}' should match hierarchical prefix",
                    route
                );
                let _ = rx.recv().await.unwrap();
            } else {
                assert_eq!(
                    delivered, 0,
                    "Route '{}' should not match hierarchical prefix",
                    route
                );
            }
        }
    }

    #[tokio::test]
    async fn should_deliver_to_multiple_matching_subscriptions() {
        // Arrange - multiple overlapping subscriptions
        let mut service = NoticeService::new();
        let (tx1, mut rx1) = mpsc::channel(10);
        let (tx2, mut rx2) = mpsc::channel(10);
        let (tx3, mut rx3) = mpsc::channel(10);
        let (tx4, mut rx4) = mpsc::channel(10);

        service.subscribe(DEFAULT_RF, "*".to_string(), 1, tx1); // Global
        service.subscribe(DEFAULT_RF, "notice://acme/*".to_string(), 2, tx2); // Realm
        service.subscribe(DEFAULT_RF, "notice://acme/prod/*".to_string(), 3, tx3); // Area
        service.subscribe(
            DEFAULT_RF,
            "notice://acme/prod/syslog/error".to_string(),
            4,
            tx4,
        ); // Exact

        // Act
        let route = "notice://acme/prod/syslog/error";
        let r = service.publish(DEFAULT_RF, route, Some("msg-1"), b"alert");
        let delivered = r.delivered;
        let failed = r.failed;

        // Assert - all 4 should receive the message
        assert_eq!(delivered, 4);
        assert_eq!(failed, 0);

        let msg1 = rx1.recv().await.unwrap();
        assert_eq!(msg1.0, route);
        assert_eq!(msg1.2, b"alert");

        let msg2 = rx2.recv().await.unwrap();
        assert_eq!(msg2.0, route);

        let msg3 = rx3.recv().await.unwrap();
        assert_eq!(msg3.0, route);

        let msg4 = rx4.recv().await.unwrap();
        assert_eq!(msg4.0, route);
    }

    #[tokio::test]
    async fn should_not_match_partial_segment_names() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, _rx) = mpsc::channel(10);
        service.subscribe(DEFAULT_RF, "notice://realm".to_string(), 1, tx);

        // Act & Assert - should not match partial names
        let non_matching_routes = vec![
            "notice://realm123",
            "notice://realm-prod",
            "notice://rea",
            "notice://realms",
        ];

        for route in non_matching_routes {
            let r = service.publish(DEFAULT_RF, route, None, b"test");
            let delivered = r.delivered;
            assert_eq!(
                delivered, 0,
                "Route '{}' should not match partial segment",
                route
            );
        }
    }

    #[tokio::test]
    async fn should_preserve_message_metadata_in_delivery() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);
        service.subscribe(DEFAULT_RF, "test/*".to_string(), 1, tx);

        // Act
            let r = service.publish(DEFAULT_RF, "test/alerts", Some("msg-123"), b"payload data");
        let delivered = r.delivered;
        let failed = r.failed;

        // Assert
        assert_eq!(delivered, 1);
        assert_eq!(failed, 0);

        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.0, "test/alerts"); // route
        assert_eq!(msg.1, Some("msg-123".to_string())); // msg_id
        assert_eq!(msg.2, b"payload data"); // body
        assert_eq!(msg.3, None); // reply_to (always None for notices)
        assert_eq!(msg.4, None); // seq (always None for notices)
        assert!(!msg.5); // end (always false for notices)
    }

    #[tokio::test]
    async fn should_handle_no_matching_subscriptions() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, _rx) = mpsc::channel(10);
        service.subscribe(DEFAULT_RF, "notice://acme/prod/*".to_string(), 1, tx);

        // Act - publish to non-matching route
        let res = service.publish(
            DEFAULT_RF,
            "notice://other/staging/app/info",
            None,
            b"orphan message",
        );

        // Assert - no deliveries
        assert_eq!(res.delivered, 0);
        assert_eq!(res.failed, 0);
    }

    #[tokio::test]
    async fn should_handle_empty_route_segments() {
        // Arrange
        let mut service = NoticeService::new();
        let (tx, mut rx) = mpsc::channel(10);
        service.subscribe(DEFAULT_RF, "".to_string(), 1, tx);

        // Act
        let r = service.publish(DEFAULT_RF, "", None, b"empty");
        let delivered = r.delivered;

        // Assert - exact match on empty string
        assert_eq!(delivered, 1);
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.0, "");
    }
}
