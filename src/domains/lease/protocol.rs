//! Lease domain protocol and message types
//!
//! The lease protocol provides distributed locking with fencing tokens
//! for ordering guarantees. Leases have time-to-live (TTL) and can be
//! acquired, renewed, and released.
//!
//! # Route Family Isolation
//!
//! Leases are always scoped to (RouteFamily, Route) tuples. RouteFamily
//! is the unit of isolation - leases in different families never conflict,
//! even if they use identical route strings. This enables multi-realm
//! deployments where each realm operates independently.
//!
//! # Fencing Tokens
//!
//! Each lease acquisition returns a monotonically increasing fencing token.
//! Clients must include this token in subsequent operations. This provides
//! ordering guarantees and prevents "split brain" scenarios where an old
//! lease holder believes it still owns the lease.
//!
//! # Expiration
//!
//! Leases expire after their TTL. An expired lease can be acquired by a new
//! owner. The new owner receives a higher fencing token.
//!
//! # Idempotency
//!
//! All operations are idempotent:
//! - Acquiring an already-held lease returns the existing token
//! - Renewing with the current token succeeds
//! - Releasing with an outdated token fails safely

use crate::transport::routing::{Route, RouteFamily};

/// Lease domain messages
///
/// All lease operations are asynchronous and return responses via
/// the actor messaging system.
#[derive(Debug, Clone)]
pub enum LeaseMessage {
    /// Acquire a lease
    ///
    /// Lease identity is (family_id, route) - route families provide hard isolation.
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
    /// Lease identity is (family_id, route) - route families provide hard isolation.
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
    /// Lease identity is (family_id, route) - route families provide hard isolation.
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
    /// Lease identity is (family_id, route) - route families provide hard isolation.
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
