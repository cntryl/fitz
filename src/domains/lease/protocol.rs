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

use crate::runtime::routing::{route_exact_triplet, Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;
use bytes::Bytes;

/// Largest `owner_id` `ACQUIRE` accepts, in bytes.
///
/// `owner_id` was otherwise unbounded (only implicitly limited by the
/// ACQUIRE request's own ~64 KiB wire frame). A single oversized `owner_id`
/// combined with a near-maximum route can make one `LIST` item alone exceed
/// the response's TLV payload limit, permanently breaking every matching
/// `LIST` scan until that lease disappears — this bound closes that off at
/// its source rather than trying to special-case an oversized item in the
/// `LIST` response.
pub const LEASE_MAX_OWNER_ID_BYTES: usize = 512;
/// Default `LIST` page size when the caller does not request one.
pub const LEASE_LIST_DEFAULT_PAGE_SIZE: u32 = 100;
/// Largest page size a `LIST` caller may request.
pub const LEASE_LIST_MAX_PAGE_SIZE: u32 = 500;
/// Largest number of outstanding `LIST` snapshots retained across all
/// sessions and families at once; the least-recently-touched one is evicted
/// past this bound (issue #219 §8 bounded work).
pub(crate) const LEASE_LIST_MAX_SNAPSHOTS: usize = 256;
/// Largest number of raw candidate leases a wildcard `LIST` scan examines
/// (matching or not) while materializing its snapshot. A scan is a true
/// point-in-time snapshot only if it is built from one atomic pass over the
/// requesting family's current state, so this cannot be spread across
/// multiple actor messages without letting concurrent
/// acquire/release/expiry/renew activity add, remove, or change items the
/// snapshot has already promised are fixed (issue #219 §2). A family whose
/// candidate count exceeds this bound therefore fails the scan outright with
/// a typed error asking the caller to narrow the selector, rather than
/// silently doing unbounded work or silently weakening the snapshot
/// guarantee. This is generous enough that realistic Lease usage (explicit
/// coordination locks, not bulk storage) never hits it in one family.
///
/// Shrunk under `cfg(test)` so the ceiling itself is exercised by a
/// realistically small, fast-running fixture instead of requiring tens of
/// thousands of real leases.
#[cfg(not(test))]
pub(crate) const LEASE_LIST_MAX_CANDIDATES_PER_SCAN: usize = 50_000;
#[cfg(test)]
pub(crate) const LEASE_LIST_MAX_CANDIDATES_PER_SCAN: usize = 50;
/// Soft ceiling on one `LIST` page's encoded byte size, kept well under the
/// 65_535-byte TLV frame payload limit (`encode_single_tlv_frame`) so a page
/// full of near-maximum-length routes and owner IDs still fits on the wire.
/// Items are appended to a page only while under this budget; anything left
/// over is served on a later page instead. `LEASE_MAX_OWNER_ID_BYTES` keeps
/// any single item comfortably under this budget on its own (issue #219
/// §2/§8).
pub(crate) const LEASE_LIST_PAGE_BYTE_BUDGET: usize = 60_000;
/// Largest total number of not-yet-served items retained across every
/// outstanding `LIST` snapshot at once (every session, every family). A
/// single broad scan, or many concurrent ones, cannot retain more than this
/// many duplicated inventory copies; the least-recently-touched snapshot is
/// evicted before this bound is exceeded (issue #219 §8).
#[cfg(not(test))]
pub(crate) const LEASE_LIST_MAX_RETAINED_ITEMS_TOTAL: usize = 500_000;
#[cfg(test)]
pub(crate) const LEASE_LIST_MAX_RETAINED_ITEMS_TOTAL: usize = 500;
/// Largest encoded item bytes one session may retain across unfinished
/// `LIST` snapshots. This prevents one authorized reader from consuming the
/// entire broker-wide snapshot budget with abandoned broad scans.
#[cfg(not(test))]
pub(crate) const LEASE_LIST_MAX_RETAINED_BYTES_PER_SESSION: usize = 16 * 1024 * 1024;
#[cfg(test)]
pub(crate) const LEASE_LIST_MAX_RETAINED_BYTES_PER_SESSION: usize = 32 * 1024;
/// Largest encoded item bytes retained by all unfinished `LIST` snapshots.
/// The cap is independent of item count because valid route and owner lengths
/// vary by orders of magnitude.
#[cfg(not(test))]
pub(crate) const LEASE_LIST_MAX_RETAINED_BYTES_TOTAL: usize = 64 * 1024 * 1024;
#[cfg(test)]
pub(crate) const LEASE_LIST_MAX_RETAINED_BYTES_TOTAL: usize = 128 * 1024;
/// A `LIST` snapshot a session never returns to page through — abandoned
/// mid-scan, or after receiving one page — is reclaimed after this long
/// idle, even without a disconnect (issue #219 §8).
pub(crate) const LEASE_LIST_SNAPSHOT_IDLE_TTL_SECS: u64 = 300;

/// Opaque continuation for one paginated `LIST` scan.
///
/// Bound to the broker-lifetime snapshot it was issued from: a cursor from a
/// different selector, `RouteFamily`, or broker lifetime fails explicitly
/// (`LeaseResponse::InvalidListCursor`) rather than silently restarting or
/// narrowing the read (issue #219 §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseListCursor {
    pub snapshot_id: u64,
    pub offset: u32,
}

/// One current held lease reported by `LIST`.
///
/// Every field is read-only observation: an item can never be turned into an
/// owned Lease handle, and `holder_incarnation` is derived from the raw
/// broker session ID rather than being that ID (issue #219 §2, §6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseListItem {
    pub route: Route,
    /// The client-supplied logical `owner_id`, never the internal
    /// session-scoped disambiguation string.
    pub owner_id: String,
    /// Opaque per-live-session identifier; stable for one session, distinct
    /// across sessions, and never the raw session ID.
    pub holder_incarnation: u64,
    pub acquired_at: String,
    pub expires_in_secs: u64,
    pub renewals: u32,
}

/// Parsed lease identity
///
/// Leases are uniquely identified by (`RouteFamily`, realm, area, resource):
/// - `RouteFamily`: Routing isolation boundary (opaque u64)
/// - realm/area/resource: Logical identity within the family
///
/// `Ord` orders primarily by `family`, then realm/area/resource, so a
/// `BTreeMap<LeaseKey, _>` keeps every family's leases in one contiguous,
/// route-ordered range. `LIST` uses that range to scan only the requesting
/// family's inventory (never another family's) and to resume a bounded scan
/// deterministically across calls, without a separate sort step.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
        let parts = route_exact_triplet(route)?;

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

/// Recover the client-supplied logical `owner_id` from a session-scoped
/// owner string built by [`session_scoped_owner_id`].
///
/// `SinkLeaseState` retains only the scoped form so `Acquire`/`Extend` can
/// tell two different live sessions using the same logical `owner_id` apart.
/// Lease `LIST` must report the logical `owner_id` a caller actually passed,
/// never the internal `session:{id}[:owner]` disambiguation string, so this
/// is the inverse of that scoping.
#[must_use]
pub(crate) fn logical_owner_id(scoped: &str) -> &str {
    scoped
        .strip_prefix("session:")
        .and_then(|rest| rest.split_once(':'))
        .map_or("", |(_session_digits, owner)| owner)
}

/// Derive an opaque, per-live-session identifier for Lease `LIST` from a raw
/// broker session ID using a process-local keyed hasher.
///
/// `holder_incarnation` groups every lease held by one live session without
/// exposing the raw session ID as the public holder identity (issue #219
/// §2). It is deterministic for one broker process and session, so it stays
/// stable for the life of that session and changes on reconnect (a new
/// session ID is always assigned). The process-local key makes the mapping
/// non-invertible to clients; a public bijection would merely obfuscate and
/// still disclose the raw session ID.
#[must_use]
pub(crate) fn holder_incarnation(
    hasher: &std::collections::hash_map::RandomState,
    session_id: u64,
) -> u64 {
    use std::hash::BuildHasher;

    hasher.hash_one(session_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::Route;

    #[test]
    fn should_round_trip_logical_owner_id_through_session_scoping() {
        // Arrange / Act / Assert
        assert_eq!(
            logical_owner_id(&session_scoped_owner_id(7, "owner1")),
            "owner1"
        );
        assert_eq!(logical_owner_id(&session_scoped_owner_id(7, "")), "");
    }

    #[test]
    fn should_preserve_colons_in_logical_owner_id() {
        // Arrange
        let scoped = session_scoped_owner_id(42, "team:renderers");

        // Act / Assert
        assert_eq!(logical_owner_id(&scoped), "team:renderers");
    }

    #[test]
    fn should_derive_stable_holder_incarnation_per_session() {
        // Arrange
        let first_process = std::collections::hash_map::RandomState::new();
        let second_process = std::collections::hash_map::RandomState::new();

        // Act
        let first = holder_incarnation(&first_process, 7);

        // Assert: stable within one broker process, distinct for another
        // session, and keyed differently after a broker restart.
        assert_eq!(first, holder_incarnation(&first_process, 7));
        assert_ne!(first, holder_incarnation(&first_process, 8));
        assert_ne!(first, holder_incarnation(&second_process, 7));
    }

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

    #[test]
    fn should_reject_lease_route_with_trailing_segment() {
        // Arrange
        let route = Route::new("lease://acme/locks/db-migration/trailing");
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

    /// List the current held-lease inventory matching a selector.
    ///
    /// `pattern` may be exact or use the shared depth-three `*`/`**`
    /// grammar (see `DomainDescriptor::compile_registration_pattern`).
    /// `LIST` is read-only: it never grants, renews, or releases a lease,
    /// and never exposes a raw session ID as the holder identity (issue
    /// #219 §2).
    List {
        family_id: RouteFamily,
        pattern: Route,
        cursor: Option<LeaseListCursor>,
        limit: Option<u32>,
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

    /// `LIST` selector failed the shared depth-three pattern grammar.
    InvalidListPattern(String),

    /// `LIST` cursor did not match the selector, `RouteFamily`, or broker
    /// lifetime it was issued from, or the snapshot it names was evicted or
    /// already exhausted.
    InvalidListCursor,

    /// One page of the current held-lease inventory matching a `LIST`
    /// selector. `next_cursor` is `Some` when more matching items remain.
    ListPage {
        items: Vec<LeaseListItem>,
        next_cursor: Option<LeaseListCursor>,
    },

    /// Successfully subscribed to lease notifications
    SubscribeOk { subscription_id: u64 },

    /// Successfully unsubscribed from lease notifications
    UnsubscribeOk,
}
