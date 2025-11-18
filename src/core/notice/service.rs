//! Notice domain service - owns all notice business logic and subscription routing
//!
//! This implementation delegates subscription bookkeeping to an internal
//! in-memory RouteTable (route_table.rs).
//! Pure sync - no channels, no I/O. Returns routing decisions as data.

use crate::routing::{RouteFamilyId, RouteTable, RtSubscription};
use smallvec::SmallVec;

#[cfg(test)]
use crate::routing::DEFAULT_RF;

/// Notice service handles ephemeral pub/sub operations
/// - Subscribe/Unsubscribe: manage in-memory subscriptions
/// - Publish: dispatch notifications to matching subscribers
/// - Best-effort delivery with backpressure handling
#[derive(Debug)]
pub struct NoticeService {
    next_sub_id: u64,
    route_table: RouteTable,
}

/// Matched subscription for a publish operation
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MatchedSubscription {
    pub id: u64,
    pub channel_id: u32,
    pub route_pattern: String,
}

/// Result of a publish operation: list of matched subscribers
/// Transport layer is responsible for actual delivery
/// Optimized: SmallVec avoids heap allocation for <8 subscribers
#[derive(Debug, Clone)]
pub struct PublishResult {
    pub subscribers: SmallVec<[(u32, u64); 8]>, // (channel_id, sub_id)
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
    ///
    /// Pure sync operation - just updates routing table
    /// Transport layer maintains channel_id -> channel mapping
    pub fn subscribe(&mut self, rf: RouteFamilyId, route_pattern: String, channel_id: u32) -> u64 {
        let id = self.next_sub_id;
        self.next_sub_id = self.next_sub_id.wrapping_add(1);

        let sub = RtSubscription {
            id,
            route_pattern,
            channel_id,
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
    /// Returns list of matched subscriptions - Transport layer handles actual delivery
    ///
    /// Pure sync operation - no I/O, no channels, no waiting
    /// Domain responsibility: route matching only
    /// Transport responsibility: message delivery, backpressure, dead connection cleanup
    pub fn publish(
        &self,
        rf: RouteFamilyId,
        route: &str,
        _msg_id: Option<&str>,
        _body: &[u8],
    ) -> PublishResult {
        let subscribers: SmallVec<[(u32, u64); 8]> = self
            .route_table
            .matching_subscribers(rf, route)
            .map(|sub| (sub.channel_id, sub.id))
            .collect();

        PublishResult { subscribers }
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

    #[test]
    fn should_subscribe_and_publish() {
        // Arrange
        let mut service = NoticeService::new();

        // Act
        let _sub_id = service.subscribe(DEFAULT_RF, "test/route".to_string(), 1);
        let r = service.publish(DEFAULT_RF, "test/route", Some("msg-1"), b"hello");

        // Assert
        assert_eq!(r.subscribers.len(), 1);
        assert_eq!(r.subscribers[0].0, 1); // channel_id
        assert_eq!(r.subscribers[0].1, 1); // sub_id
    }

    #[test]
    fn should_unsubscribe() {
        // Arrange
        let mut service = NoticeService::new();
        let sub_id = service.subscribe(DEFAULT_RF, "test/route".to_string(), 1);

        // Act
        let removed = service.unsubscribe(DEFAULT_RF, sub_id);
        let r = service.publish(DEFAULT_RF, "test/route", Some("msg-1"), b"hello");

        // Assert
        assert!(removed);
        assert_eq!(r.subscribers.len(), 0);
    }

    #[test]
    fn should_cleanup_channel() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "route1".to_string(), 1);
        service.subscribe(DEFAULT_RF, "route2".to_string(), 2);

        // Act
        service.cleanup_channel(DEFAULT_RF, 1);

        // Assert
        assert_eq!(service.subscription_count(), 1);
    }

    #[test]
    fn should_return_no_subscribers_when_none_match() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "test/route".to_string(), 1);

        // Act
        let r = service.publish(DEFAULT_RF, "other/route", None, b"data");

        // Assert
        assert_eq!(r.subscribers.len(), 0);
    }

    // ========================================================================
    // COMPREHENSIVE WILDCARD PATTERN MATCHING TESTS
    // Pure routing tests - no channels, domains return matched subscribers
    // ========================================================================

    #[test]
    fn should_match_global_wildcard_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "*".to_string(), 1);
        let test_routes = vec![
            "notice://realm/area/resource/op",
            "a/b/c",
            "single",
            "anything/goes/here",
        ];

        for route in test_routes {
            // Act
            let r = service.publish(DEFAULT_RF, route, None, b"test");

            // Assert
            assert_eq!(
                r.subscribers.len(),
                1,
                "Route '{}' should match global wildcard",
                route
            );
        }
    }

    #[test]
    fn should_match_realm_wildcard_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "notice://acme/*".to_string(), 1);
        let matching_routes = vec![
            ("notice://acme/prod/syslog/error", true),
            ("notice://acme/dev/app/warning", true),
            ("notice://acme/staging/db/critical", true),
            ("notice://other/prod/syslog/error", false),
            ("notice://acme", true), // Exact match to prefix
        ];

        for (route, should_match) in matching_routes {
            // Act
            let r = service.publish(DEFAULT_RF, route, None, b"test");

            // Assert
            if should_match {
                assert_eq!(
                    r.subscribers.len(),
                    1,
                    "Route '{}' should match realm wildcard",
                    route
                );
            } else {
                assert_eq!(
                    r.subscribers.len(),
                    0,
                    "Route '{}' should not match realm wildcard",
                    route
                );
            }
        }
    }

    #[test]
    fn should_match_area_wildcard_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "notice://acme/prod/*".to_string(), 1);
        let matching_routes = vec![
            ("notice://acme/prod/syslog/error", true),
            ("notice://acme/prod/app/info", true),
            ("notice://acme/prod/db/query", true),
            ("notice://acme/dev/syslog/error", false),
            ("notice://acme/staging/app/info", false),
            ("notice://other/prod/syslog/error", false),
        ];

        for (route, should_match) in matching_routes {
            // Act
            let r = service.publish(DEFAULT_RF, route, None, b"test");

            // Assert
            if should_match {
                assert_eq!(
                    r.subscribers.len(),
                    1,
                    "Route '{}' should match area wildcard",
                    route
                );
            } else {
                assert_eq!(
                    r.subscribers.len(),
                    0,
                    "Route '{}' should not match area wildcard",
                    route
                );
            }
        }
    }

    #[test]
    fn should_match_resource_wildcard_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "notice://acme/prod/syslog/*".to_string(), 1);
        let matching_routes = vec![
            ("notice://acme/prod/syslog/error", true),
            ("notice://acme/prod/syslog/warning", true),
            ("notice://acme/prod/syslog/info", true),
            ("notice://acme/prod/app/error", false),
            ("notice://acme/dev/syslog/error", false),
        ];

        for (route, should_match) in matching_routes {
            // Act
            let r = service.publish(DEFAULT_RF, route, None, b"test");

            // Assert
            if should_match {
                assert_eq!(
                    r.subscribers.len(),
                    1,
                    "Route '{}' should match resource wildcard",
                    route
                );
            } else {
                assert_eq!(
                    r.subscribers.len(),
                    0,
                    "Route '{}' should not match resource wildcard",
                    route
                );
            }
        }
    }

    #[test]
    fn should_match_exact_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(
            DEFAULT_RF,
            "notice://acme/prod/syslog/critical".to_string(),
            1,
        );
        let matching_routes = vec![
            ("notice://acme/prod/syslog/critical", true),
            ("notice://acme/prod/syslog/error", false),
            ("notice://acme/prod/syslog/warning", false),
            ("notice://acme/prod/app/critical", false),
        ];

        for (route, should_match) in matching_routes {
            // Act
            let r = service.publish(DEFAULT_RF, route, None, b"test");

            // Assert
            if should_match {
                assert_eq!(
                    r.subscribers.len(),
                    1,
                    "Route '{}' should match exact pattern",
                    route
                );
            } else {
                assert_eq!(
                    r.subscribers.len(),
                    0,
                    "Route '{}' should not match exact pattern",
                    route
                );
            }
        }
    }

    #[test]
    fn should_match_hierarchical_prefix_subscription() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "notice://acme/prod/syslog".to_string(), 1);
        let matching_routes = vec![
            ("notice://acme/prod/syslog", true),              // Exact
            ("notice://acme/prod/syslog/error", true),        // Child
            ("notice://acme/prod/syslog/warning", true),      // Child
            ("notice://acme/prod/syslog/info/verbose", true), // Deep child
            ("notice://acme/prod/app", false),                // Different resource
            ("notice://acme/prod", false),                    // Parent
        ];

        for (route, should_match) in matching_routes {
            // Act
            let r = service.publish(DEFAULT_RF, route, None, b"test");

            // Assert
            if should_match {
                assert_eq!(
                    r.subscribers.len(),
                    1,
                    "Route '{}' should match hierarchical prefix",
                    route
                );
            } else {
                assert_eq!(
                    r.subscribers.len(),
                    0,
                    "Route '{}' should not match hierarchical prefix",
                    route
                );
            }
        }
    }

    #[test]
    fn should_deliver_to_multiple_matching_subscriptions() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "*".to_string(), 1); // Global
        service.subscribe(DEFAULT_RF, "notice://acme/*".to_string(), 2); // Realm
        service.subscribe(DEFAULT_RF, "notice://acme/prod/*".to_string(), 3); // Area
        service.subscribe(DEFAULT_RF, "notice://acme/prod/syslog/error".to_string(), 4); // Exact

        // Act
        let route = "notice://acme/prod/syslog/error";
        let r = service.publish(DEFAULT_RF, route, Some("msg-1"), b"alert");

        // Assert
        assert_eq!(r.subscribers.len(), 4);
        let mut channel_ids: Vec<u32> = r.subscribers.iter().map(|s| s.0).collect();
        channel_ids.sort();
        assert_eq!(channel_ids, vec![1, 2, 3, 4]);
    }

    #[test]
    fn should_not_match_partial_segment_names() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "notice://realm".to_string(), 1);
        let non_matching_routes = vec![
            "notice://realm123",
            "notice://realm-prod",
            "notice://rea",
            "notice://realms",
        ];

        for route in non_matching_routes {
            // Act
            let r = service.publish(DEFAULT_RF, route, None, b"test");

            // Assert
            assert_eq!(
                r.subscribers.len(),
                0,
                "Route '{}' should not match partial segment",
                route
            );
        }
    }

    #[test]
    fn should_return_matched_subscriber_for_publish() {
        // Arrange
        let mut service = NoticeService::new();
        let sub_id = service.subscribe(DEFAULT_RF, "test/*".to_string(), 1);

        // Act
        let r = service.publish(DEFAULT_RF, "test/alerts", Some("msg-123"), b"payload data");

        // Assert
        assert_eq!(r.subscribers.len(), 1);
        assert_eq!(r.subscribers[0].1, sub_id); // sub_id
        assert_eq!(r.subscribers[0].0, 1); // channel_id
    }

    #[test]
    fn should_handle_no_matching_subscriptions() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "notice://acme/prod/*".to_string(), 1);

        // Act
        let res = service.publish(
            DEFAULT_RF,
            "notice://other/staging/app/info",
            None,
            b"orphan message",
        );

        // Assert
        assert_eq!(res.subscribers.len(), 0);
    }

    #[test]
    fn should_handle_empty_route_segments() {
        // Arrange
        let mut service = NoticeService::new();
        service.subscribe(DEFAULT_RF, "".to_string(), 1);

        // Act
        let r = service.publish(DEFAULT_RF, "", None, b"empty");

        // Assert
        assert_eq!(r.subscribers.len(), 1);
    }
}
