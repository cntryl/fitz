//! Lease protocol message types and responses
//!
//! Defines the message types for lease operations:
//! - **Acquire**: Request exclusive ownership
//! - **Extend**: Extend lease expiration
//! - **Release**: Relinquish ownership
//! - **Query**: Inspect lease status (debugging)
//! - **Tick**: Runtime-driven expiration (scheduler)

use crate::runtime::routing::{Route, RouteFamily};

/// Parsed lease identity
///
/// Leases are uniquely identified by (RouteFamily, realm, area, resource):
/// - RouteFamily: Routing isolation boundary (opaque u64)
/// - realm/area/resource: Logical identity within the family
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LeaseKey {
    pub family: RouteFamily,
    pub realm: String,
    pub area: String,
    pub resource: String,
}

impl LeaseKey {
    /// Parse a route into lease key
    ///
    /// Expected route format: `{scheme}://{realm}/{area}/{resource}/{operation}`
    /// or `/{realm}/{area}/{resource}/{operation}`
    ///
    /// Returns None if the route doesn't match the expected format.
    pub fn from_route(family: RouteFamily, route: &Route) -> Option<Self> {
        let path = route.as_str();

        // Strip scheme if present (e.g., "lease://..." → "...")
        let path_without_scheme = if let Some(pos) = path.find("://") {
            &path[pos + 3..]
        } else {
            path
        };

        let parts: Vec<&str> = path_without_scheme
            .trim_start_matches('/')
            .split('/')
            .collect();

        if parts.len() >= 3 {
            Some(LeaseKey {
                family,
                realm: parts[0].to_string(),
                area: parts[1].to_string(),
                resource: parts[2].to_string(),
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::Route;

    #[test]
    fn should_parse_lease_route_with_scheme() {
        // Arrange
        let route = Route::new("lease://acme/locks/db-migration");
        let family = RouteFamily::new(1);

        // Act
        let key = LeaseKey::from_route(family, &route);

        // Assert
        assert!(key.is_some());
        let key = key.unwrap();
        assert_eq!(key.realm, "acme");
        assert_eq!(key.area, "locks");
        assert_eq!(key.resource, "db-migration");
    }

    #[test]
    fn should_parse_lease_route_without_scheme() {
        // Arrange
        let route = Route::new("acme/locks/db-migration");
        let family = RouteFamily::new(2);

        // Act
        let key = LeaseKey::from_route(family, &route);

        // Assert
        assert!(key.is_some());
        let key = key.unwrap();
        assert_eq!(key.realm, "acme");
        assert_eq!(key.area, "locks");
        assert_eq!(key.resource, "db-migration");
    }

    #[test]
    fn should_reject_lease_route_with_too_few_segments() {
        // Arrange
        let route = Route::new("lease://acme/locks");
        let family = RouteFamily::new(1);

        // Act
        let key = LeaseKey::from_route(family, &route);

        // Assert
        assert!(key.is_none());
    }
}

/// Lease errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseError {
    /// Invalid realm format (3020)
    InvalidRealm,

    /// Realm mismatch - operation targets different realm than existing lease (3021)
    RealmMismatch,
}

impl LeaseError {
    pub fn code(&self) -> u16 {
        match self {
            LeaseError::InvalidRealm => 3020,
            LeaseError::RealmMismatch => 3021,
        }
    }
}

/// Lease domain messages
///
/// All lease operations are asynchronous and return responses via
/// the actor messaging system.
#[derive(Debug, Clone)]
pub enum LeaseMessage {
    /// Acquire a lease
    ///
    /// Route format: `/{realm}/{area}/{resource}/acquire`
    /// Lease identity: (family_id, realm, area, resource)
    /// If the lease is unowned or expired, grants it to the owner.
    /// If already owned by this owner, returns the existing token (idempotent).
    /// If owned by another owner and wait_seconds > 0, queues the request.
    /// If owned by another owner and wait_seconds = 0, fails immediately.
    ///
    /// wait_seconds: Maximum time to wait for lease to become available.
    /// If 0, returns immediately with HeldByOther if unavailable.
    /// If > 0, client waits up to this duration for the lease.
    /// Must not exceed max_wait_seconds (default 30).
    Acquire {
        family_id: RouteFamily,
        route: Route,
        owner_id: String,
        ttl_secs: u64,
        wait_seconds: u32,
    },

    /// Extend a lease
    ///
    /// Route format: `/{realm}/{area}/{resource}/extend`
    /// Lease identity: (family_id, realm, area, resource)
    /// Extends the lease expiration if the fencing token matches.
    /// Fails if the token is outdated or the lease is no longer held.
    Extend {
        family_id: RouteFamily,
        route: Route,
        owner_id: String,
        fencing_token: u64,
        ttl_secs: u64,
    },

    /// Release a lease
    ///
    /// Route format: `/{realm}/{area}/{resource}/release`
    /// Lease identity: (family_id, realm, area, resource)
    /// Releases the lease if the fencing token matches.
    /// Fails if the token is outdated or the lease is not held.
    Release {
        family_id: RouteFamily,
        route: Route,
        owner_id: String,
        fencing_token: u64,
    },

    /// Query lease status (for testing/debugging)
    ///
    /// Route format: `/{realm}/{area}/{resource}/query`
    /// Lease identity: (family_id, realm, area, resource)
    Query {
        family_id: RouteFamily,
        route: Route,
    },

    /// Periodic tick for runtime-driven expiration
    ///
    /// The lease actor responds by proactively expiring old leases
    /// and checking if any waiting acquisitions can now be granted.
    /// This is sent periodically by the scheduler to ensure leases
    /// are expired even when not being actively accessed.
    Tick,

    /// Subscribe to availability notifications
    ///
    /// Route format: `/{realm}/{area}/{resource}/subscribe`
    /// Lease identity: (family_id, realm, area, resource)
    /// Subscribes to change notifications on a lease. Notifications are published
    /// when the lease is released or expires. Client provides a pattern that will be
    /// used to match against notification routes.
    ///
    /// pattern: Wildcard pattern to subscribe to (e.g., "lease://acme/locks/db-migration/changed")
    Subscribe {
        family_id: RouteFamily,
        pattern: String,
    },

    /// Unsubscribe from availability notifications
    ///
    /// Route format: `/{realm}/{area}/{resource}/unsubscribe`
    /// Lease identity: (family_id, realm, area, resource)
    /// Unsubscribes from change notifications for a specific pattern.
    /// If pattern is not currently subscribed, returns UnsubscribeOk anyway (idempotent).
    ///
    /// pattern: Wildcard pattern to unsubscribe from
    Unsubscribe {
        family_id: RouteFamily,
        pattern: String,
    },

    /// Unsubscribe from all availability notifications
    ///
    /// Removes all active subscriptions for this session.
    /// Called automatically on session disconnect.
    UnsubscribeAll,
}

/// Lease operation responses
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseResponse {
    /// Lease successfully acquired
    Acquired { fencing_token: u64 },

    /// Lease already held by this owner (idempotent acquire)
    AlreadyHeld { fencing_token: u64 },

    /// Acquire request queued and waiting for lease to become available
    ///
    /// The client should wait for a response with either Acquired, Timeout, or error.
    /// The fencing_token is provisional and will be confirmed when the lease is granted.
    Queued { fencing_token: u64 },

    /// Already waiting for this lease with the same owner_id
    ///
    /// A second Acquire(wait) from the same owner while one is already pending.
    /// Returns the existing queued state rather than queuing twice.
    AlreadyQueued { fencing_token: u64 },

    /// Acquire request timed out before lease became available
    ///
    /// The client waited the full wait_seconds duration but the lease
    /// remained unavailable (held by another owner).
    Timeout,

    /// Too many waiters queued for this lease
    ///
    /// The pending queue depth has reached max_queue_depth.
    /// Reject this acquire to prevent unbounded memory growth.
    /// The client should back off and retry later.
    QueueFull { pending_count: usize },

    /// Lease successfully extended
    Extended { fencing_token: u64 },

    /// Lease successfully released
    Released,

    /// Lease is held by another owner
    HeldByOther { current_owner: String },

    /// Lease not held by this owner
    NotHeld,

    /// Fencing token is outdated
    Fenced { current_token: u64 },

    /// Lease has expired
    Expired,

    /// Lease does not exist (query only)
    NotFound,

    /// Lease status (query only)
    Status {
        owner_id: String,
        fencing_token: u64,
        expires_in_secs: u64,
        pending_waiters: usize,
    },

    /// Successfully subscribed to lease notifications
    SubscribeOk { subscription_id: u64 },

    /// Successfully unsubscribed from lease notifications
    UnsubscribeOk,
}
