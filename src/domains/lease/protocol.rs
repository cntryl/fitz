//! Lease protocol message types and responses
//!
//! Defines the message types for lease operations:
//! - **Acquire**: Request exclusive ownership
//! - **Renew**: Extend lease expiration
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
    /// Expected route format: `/{realm}/{area}/{resource}/{operation}`
    ///
    /// Returns None if the route doesn't match the expected format.
    pub fn from_route(family: RouteFamily, route: &Route) -> Option<Self> {
        let parts: Vec<&str> = route.as_str().trim_start_matches('/').split('/').collect();

        if parts.len() >= 4 {
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
    /// If owned by another owner, fails.
    Acquire {
        family_id: RouteFamily,
        route: Route,
        owner_id: String,
        ttl_secs: u64,
    },

    /// Renew a lease
    ///
    /// Route format: `/{realm}/{area}/{resource}/renew`
    /// Lease identity: (family_id, realm, area, resource)
    /// Extends the lease expiration if the fencing token matches.
    /// Fails if the token is outdated or the lease is no longer held.
    Renew {
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
    /// The lease actor responds by proactively expiring old leases.
    /// This is sent periodically by the scheduler to ensure leases
    /// are expired even when not being actively accessed.
    Tick,
}

/// Lease operation responses
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseResponse {
    /// Lease successfully acquired
    Acquired { fencing_token: u64 },

    /// Lease already held by this owner (idempotent acquire)
    AlreadyHeld { fencing_token: u64 },

    /// Lease successfully renewed
    Renewed { fencing_token: u64 },

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
    },
}
