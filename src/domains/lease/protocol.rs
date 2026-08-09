//! Lease protocol message types and responses
//!
//! Defines the message types for ephemeral lease operations inside one broker
//! process:
//! - **`Acquire`**: Request exclusive ownership
//! - **`Extend`**: Extend lease expiration
//! - **`Release`**: Relinquish ownership
//! - **`Query`**: Inspect lease status (debugging)
//! - **`Subscribe / Unsubscribe`**: Watch concrete lease routes for change notifications
//! - **`Tick`**: Runtime-driven expiration (scheduler)

use crate::runtime::routing::{route_triplet, Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;
use bytes::Bytes;

/// Parsed lease identity
///
/// Leases are uniquely identified by (`RouteFamily`, realm, area, resource):
/// - `RouteFamily`: Routing isolation boundary (opaque u64)
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
    /// Expected route format: `lease://{realm}/{area}/{resource}`
    /// or `{realm}/{area}/{resource}`
    ///
    /// Returns None if the route doesn't match the expected format.
    #[must_use]
    pub fn from_route(family: RouteFamily, route: &Route) -> Option<Self> {
        Self::from_route_str(family, route.as_str())
    }

    /// Parse a route string into a lease key without constructing a `Route`.
    ///
    /// Expected route format: `lease://{realm}/{area}/{resource}`
    /// or `{realm}/{area}/{resource}`.
    ///
    /// Returns None if the route doesn't match the expected format.
    #[must_use]
    pub fn from_route_str(family: RouteFamily, route: &str) -> Option<Self> {
        let parts = route_triplet(route)?;

        if !parts.realm.is_empty()
            && !parts.area.is_empty()
            && !parts.resource.is_empty()
            && !parts.realm.contains('*')
            && !parts.area.contains('*')
            && !parts.resource.contains('*')
        {
            Some(LeaseKey {
                family,
                realm: parts.realm.to_string(),
                area: parts.area.to_string(),
                resource: parts.resource.to_string(),
            })
        } else {
            None
        }
    }

    /// Convert key back into a canonical lease route string (no operation suffix).
    #[must_use]
    pub fn to_route(&self) -> Route {
        let mut s =
            String::with_capacity(8 + self.realm.len() + self.area.len() + self.resource.len());
        s.push_str("lease://");
        s.push_str(&self.realm);
        s.push('/');
        s.push_str(&self.area);
        s.push('/');
        s.push_str(&self.resource);
        Route::new(&s)
    }
}

#[must_use]
pub(crate) fn session_scoped_owner_id(session_id: u64, owner_id: &str) -> String {
    let session_prefix = session_id.to_string();
    if owner_id.is_empty() {
        let mut scoped = String::with_capacity("session:".len() + session_prefix.len());
        scoped.push_str("session:");
        scoped.push_str(&session_prefix);
        scoped
    } else {
        let mut scoped =
            String::with_capacity("session::".len() + session_prefix.len() + owner_id.len());
        scoped.push_str("session:");
        scoped.push_str(&session_prefix);
        scoped.push(':');
        scoped.push_str(owner_id);
        scoped
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
    #[must_use]
    pub fn code(&self) -> u16 {
        match self {
            LeaseError::InvalidRealm => 3020,
            LeaseError::RealmMismatch => 3021,
        }
    }
}

/// Lease domain messages
///
/// All lease operations are handled synchronously by the actor and return
/// responses through the actor messaging system.
#[derive(Debug, Clone)]
pub enum LeaseMessage {
    /// Acquire a lease
    ///
    /// Route format: `lease://{realm}/{area}/{resource}`
    /// Lease identity: (`family_id`, realm, area, resource)
    /// If the lease is unowned or expired, grants it to the owner.
    /// If already owned by this owner, returns the existing token (idempotent).
    /// If owned by another owner and `wait_seconds` > 0, queues the request.
    /// If owned by another owner and `wait_seconds` = 0, fails immediately.
    ///
    /// `wait_seconds`: Maximum time to wait for lease to become available.
    /// If 0, returns immediately with `HeldByOther` if unavailable.
    /// If > 0, client waits up to this duration for the lease.
    /// Must not exceed `max_wait_seconds` (default 30).
    Acquire {
        family_id: RouteFamily,
        route: Route,
        owner_id: String,
        ttl_secs: u64,
        wait_seconds: u32,
    },

    /// Extend a lease
    ///
    /// Route format: `lease://{realm}/{area}/{resource}`
    /// Lease identity: (`family_id`, realm, area, resource)
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
    /// Route format: `lease://{realm}/{area}/{resource}`
    /// Lease identity: (`family_id`, realm, area, resource)
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
    /// Route format: `lease://{realm}/{area}/{resource}`
    /// Lease identity: (`family_id`, realm, area, resource)
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
}

/// Lease watch messages handled by `LeaseDomainSink` before actor dispatch.
#[derive(Debug, Clone)]
pub enum LeaseSubscriptionMessage {
    /// Subscribe to lease change notifications for one exact route.
    Subscribe {
        family_id: RouteFamily,
        route: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },

    /// Remove an active lease watch for this session.
    Unsubscribe {
        family_id: RouteFamily,
        route: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
}

/// Parsed client request delivered to the Lease domain sink.
#[derive(Debug, Clone)]
pub struct LeaseClientRequest {
    pub meta: ClientFrameMeta,
    pub frame: Result<LeaseClientFrame, String>,
}

impl LeaseClientRequest {
    #[must_use]
    pub fn new(meta: ClientFrameMeta, frame: Result<LeaseClientFrame, String>) -> Self {
        Self { meta, frame }
    }
}

/// Crate-private lease request with hot-path fields resolved before actor dispatch.
#[derive(Debug, Clone)]
pub(crate) struct PreparedLeaseClientRequest {
    pub(crate) meta: ClientFrameMeta,
    pub(crate) frame: Result<PreparedLeaseOperation, String>,
}

impl PreparedLeaseClientRequest {
    #[must_use]
    pub(crate) fn new(
        meta: ClientFrameMeta,
        frame: Result<PreparedLeaseOperation, String>,
    ) -> Self {
        Self { meta, frame }
    }
}

/// Lease operation classified after wire parsing and session owner scoping.
#[derive(Debug, Clone)]
pub(crate) enum PreparedLeaseOperation {
    Acquire {
        key: LeaseKey,
        owner_id: String,
        ttl_secs: u64,
        wait_seconds: u32,
    },
    Extend {
        key: LeaseKey,
        owner_id: String,
        fencing_token: u64,
        ttl_secs: u64,
    },
    Release {
        key: LeaseKey,
        owner_id: String,
        fencing_token: u64,
    },
    Query {
        key: LeaseKey,
    },
    NotFound,
}

/// Lease request classified after wire parsing.
#[derive(Debug, Clone)]
pub enum LeaseClientFrame {
    Op(LeaseMessage),
    Sub(LeaseSubscriptionMessage),
}

/// Typed Lease response to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct LeaseClientResponse {
    pub meta: ClientFrameMeta,
    pub response: LeaseResponse,
}

impl LeaseClientResponse {
    #[must_use]
    pub fn new(meta: ClientFrameMeta, response: LeaseResponse) -> Self {
        Self { meta, response }
    }
}

/// Typed Lease watch notification to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct LeaseClientNotification {
    pub session_id: u64,
    pub route_family: RouteFamily,
    pub subscription_id: u64,
    pub route: Route,
    pub payload: Bytes,
}

impl LeaseClientNotification {
    pub fn new(
        session_id: u64,
        route_family: RouteFamily,
        subscription_id: u64,
        route: Route,
        payload: Bytes,
    ) -> Self {
        Self {
            session_id,
            route_family,
            subscription_id,
            route,
            payload,
        }
    }
}

/// Lease operation responses
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseResponse {
    /// Lease successfully acquired.
    ///
    /// Fencing tokens are monotonic across this broker process, not scoped to
    /// one lease key. Tokens from different lease identities are opaque and
    /// must never be ordered or compared with each other.
    Acquired { fencing_token: u64 },

    /// Lease already held by this owner (idempotent acquire)
    AlreadyHeld { fencing_token: u64 },

    /// Acquire request queued and waiting for lease to become available
    ///
    /// The client should wait for a response with either Acquired, Timeout, or error.
    /// The `fencing_token` is provisional and will be confirmed when the lease is granted.
    Queued { fencing_token: u64 },

    /// Already waiting for this lease with the same `owner_id`
    ///
    /// A second Acquire(wait) from the same owner while one is already pending.
    /// Returns the existing queued state rather than queuing twice.
    AlreadyQueued { fencing_token: u64 },

    /// Acquire request timed out before lease became available
    ///
    /// The client waited the full `wait_seconds` duration but the lease
    /// remained unavailable (held by another owner).
    Timeout,

    /// Too many waiters queued for this lease
    ///
    /// The pending queue depth has reached `max_queue_depth`.
    /// Reject this acquire to prevent unbounded memory growth.
    /// The client should back off and retry later.
    QueueFull { pending_count: usize },

    /// Lease successfully extended. The returned token has the same
    /// per-process, cross-key non-comparability contract as `Acquired`.
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

    /// Request rejected before lease state was touched.
    Error(String),

    /// Lease subscription route is not an exact three-segment `lease://` route.
    InvalidSubscriptionRoute(String),

    /// Successfully subscribed to lease notifications
    SubscribeOk { subscription_id: u64 },

    /// Successfully unsubscribed from lease notifications
    UnsubscribeOk,
}
