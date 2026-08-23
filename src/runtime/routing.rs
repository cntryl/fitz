// LAYER: RUNTIME
//! Route-based addressing with family isolation
//!
//! # Universal Addressing Model
//!
//! Fitz uses **`RouteFamily` + Route** as the universal addressing and isolation
//! model across the entire runtime. All domains (RPC, Notifications, Queue,
//! Stream, KV, Lease) use this same addressing pattern.
//!
//! # Core Concepts
//!
//! ## `RouteFamily`
//!
//! A **`RouteFamily`** is a hard isolation boundary represented as an integer (u32).
//!
//! **Properties:**
//! - Opaque identifier with no semantic meaning to the runtime
//! - No hierarchy, prefix semantics, or inheritance between families
//! - Isolation applies to routing, leasing, coordination, and state
//! - **Alignment:** `RouteFamilyId` aligns 1:1 with Midge `ColumnFamilyId` (same value)
//!
//! ## Route
//!
//! A **Route** is a structured identifier following this pattern:
//!
//! ```text
//! {scheme}://{realm}/{area}/{resource}/{operation}
//! ```
//!
//! **Components:**
//! - **scheme**: Addressing intent or interaction pattern (e.g., `rpc`, `kv`,
//!   `queue`, `stream`, `notice`, `lease`, `schedule`, plus the runtime-internal
//!   `inbox`)
//!   - Each of the seven domains owns exactly one scheme, and registers its sink
//!     under that scheme (`DomainDescriptor::register_sink`). The router's
//!     domain-pattern fallback keys off it directly.
//!   - Domain dispatch for client frames is resolved from the message type via
//!     the protocol manifest, not from the scheme. The scheme is then required
//!     to agree with the resolved domain, so a route cannot be dispatched to one
//!     domain while naming another.
//!
//! - **realm**: Top-level logical namespace
//!   - May represent tenant, organization, department, environment, or any root concept
//!   - Semantics are user-defined, not enforced by the runtime
//!   - **IMPORTANT:** Realm is just a string in the route path, NOT the `RouteFamily`
//!
//! - **area**: Logical subsystem or bounded context
//!
//! - **resource**: Entity, stream, queue, or keyspace identifier
//!
//! - **operation**: Action or verb (optional per scheme)
//!
//! **Examples:**
//! ```text
//! rpc://acme/auth/users/authenticate
//! notice://acme/events/orders/created
//! queue://acme/jobs/worker/process
//! stream://acme/analytics/events/append
//! lease://acme/locks/db/migration/acquire
//! ```
//!
//! ## Full Address
//!
//! A full address is **always** the pair: `(RouteFamilyId, Route)`
//!
//! **Rules:**
//! - Every message send must specify a `RouteFamilyId`
//! - `RouteFamilyId` is never optional and has no default
//! - Replies must preserve the original `RouteFamilyId`
//! - Domains must never observe or influence state outside their `RouteFamily`
//!
//! # Invariants
//!
//! **CRITICAL: These invariants must be maintained:**
//!
//! 1. **No cross-family routing**: A route lookup in family A will never
//!    return results from family B, even if the route strings match.
//!
//! 2. **No cross-family leases**: A lease acquired in family A has no effect
//!    on the same resource name in family B.
//!
//! 3. **No cross-family messages**: Messages sent to (family A, route X) will
//!    never be delivered to (family B, route X).
//!
//! 4. **No cross-family state**: Domains operate within `RouteFamily` boundaries;
//!    state changes in family A never affect family B.
//!
//! 5. **Scheme must agree with the dispatched domain**: A client frame's domain
//!    comes from its message type via the protocol manifest. The route's scheme
//!    is then checked against that domain and rejected on mismatch, so naming
//!    one domain's scheme never reaches another domain's sink.
//!
//! 6. **Realm semantics are opaque**: The runtime does not interpret realm
//!    values; users define their own organizational semantics.
//!
//! # Examples
//!
//! ```
//! # use fitz::runtime::routing::{RouteFamily, Route, RouteAddress};
//! // Two completely independent routing families
//! let family_prod = RouteFamily::new(1);
//! let family_staging = RouteFamily::new(2);
//!
//! // Same route pattern in different families = completely independent
//! let prod_addr = RouteAddress::new(
//!     family_prod,
//!     Route::new("rpc://acme/auth/users/authenticate".to_string())
//! );
//! let staging_addr = RouteAddress::new(
//!     family_staging,
//!     Route::new("rpc://acme/auth/users/authenticate".to_string())
//! );
//!
//! // These addresses are isolated:
//! // - Messages to prod_addr never reach staging_addr
//! // - Leases in prod never conflict with staging
//! // - State in prod is invisible to staging
//! ```
//!
//! # Multi-tenancy Example
//!
//! ```
//! # use fitz::runtime::routing::{RouteFamily, Route, RouteAddress};
//! // Option 1: RouteFamily-per-tenant (strongest isolation)
//! let tenant_a_family = RouteFamily::new(100);
//! let tenant_b_family = RouteFamily::new(200);
//!
//! let tenant_a_addr = RouteAddress::new(
//!     tenant_a_family,
//!     Route::new("rpc://app/orders/create".to_string())
//! );
//! let tenant_b_addr = RouteAddress::new(
//!     tenant_b_family,
//!     Route::new("rpc://app/orders/create".to_string())
//! );
//!
//! // Option 2: Shared RouteFamily, realm-per-tenant (logical isolation)
//! let shared_family = RouteFamily::new(1);
//!
//! let tenant_a_logical = RouteAddress::new(
//!     shared_family,
//!     Route::new("rpc://tenant-a/orders/create".to_string())
//! );
//! let tenant_b_logical = RouteAddress::new(
//!     shared_family,
//!     Route::new("rpc://tenant-b/orders/create".to_string())
//! );
//! ```

use std::fmt;
#[allow(unused_imports)]
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RouteTriplet<'a> {
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RouteQuad<'a> {
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub operation: &'a str,
}

pub(crate) fn route_triplet(route: &str) -> Option<RouteTriplet<'_>> {
    let mut segments = route_segments(route);
    Some(RouteTriplet {
        realm: segments.next()?,
        area: segments.next()?,
        resource: segments.next()?,
    })
}

pub(crate) fn route_exact_triplet(route: &str) -> Option<RouteTriplet<'_>> {
    let mut segments = route_segments(route);
    let triplet = RouteTriplet {
        realm: segments.next()?,
        area: segments.next()?,
        resource: segments.next()?,
    };
    if segments.next().is_some() {
        return None;
    }
    Some(triplet)
}

pub(crate) fn route_quad(route: &str) -> Option<RouteQuad<'_>> {
    let mut segments = route_segments(route);
    Some(RouteQuad {
        realm: segments.next()?,
        area: segments.next()?,
        resource: segments.next()?,
        operation: segments.next()?,
    })
}

pub(crate) fn route_exact_quad(route: &str) -> Option<RouteQuad<'_>> {
    let mut segments = route_segments(route);
    let quad = RouteQuad {
        realm: segments.next()?,
        area: segments.next()?,
        resource: segments.next()?,
        operation: segments.next()?,
    };
    if segments.next().is_some() {
        return None;
    }
    Some(quad)
}

/// Split a route into its scheme and path, or `None` when it carries no scheme.
///
/// This is the single definition of where a route's scheme ends. Route parsing,
/// wildcard matching, and router dispatch all resolve the `://` boundary the
/// same way, so a route can never be read as one scheme by one layer and a
/// different one by another.
#[inline]
pub(crate) fn split_scheme(route: &str) -> Option<(&str, &str)> {
    route.split_once("://")
}

pub(crate) fn route_scheme(route: &str) -> Option<&str> {
    split_scheme(route).map(|(scheme, _)| scheme)
}

fn route_path(route: &str) -> &str {
    split_scheme(route).map_or(route, |(_, path)| path)
}

fn route_segments(route: &str) -> std::str::Split<'_, char> {
    route_path(route).trim_start_matches('/').split('/')
}

/// An opaque route family identifier
///
/// Route families create hard isolation boundaries in Fitz.
/// All addressing, routing, and coordination is scoped to a family.
///
/// # Isolation Guarantee
///
/// `RouteFamily` provides **complete isolation**:
/// - No routing leaks between families
/// - No lease conflicts between families
/// - No message delivery between families
/// - No shared state between families
///
/// # Alignment with Storage
///
/// `RouteFamilyId` aligns 1:1 (by value) with Midge `ColumnFamilyId`:
/// - `RouteFamily` stores u32 (aligned with Midge `ColumnFamilyId`)
/// - `ColumnFamilyId` stores u32 (Midge internal storage type)
/// - Wire format may send u64 for future compatibility; values are validated on parse
/// - Alignment is contractual, not enforced by storage code
///
/// # Design
///
/// `RouteFamily` is intentionally opaque:
/// - Numeric ID for efficiency
/// - No hierarchy or inheritance
/// - Pure identity comparison
///
/// This ensures families are true isolation boundaries.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RouteFamily {
    id: u32,
}

impl RouteFamily {
    /// Create a route family from an already validated storage identifier.
    ///
    /// Wire and admin inputs must use [`TryFrom<u64>`], so an out-of-range
    /// value is rejected rather than silently becoming a different family.
    ///
    /// # Example
    ///
    /// ```
    /// # use fitz::runtime::routing::RouteFamily;
    /// let family = RouteFamily::new(1);
    /// assert_eq!(family.id(), 1);
    /// ```
    #[inline]
    #[must_use]
    pub fn new(id: u32) -> Self {
        Self { id }
    }

    /// Get the family ID as u32
    #[inline]
    #[must_use]
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Get the family ID as u64 (for APIs that expect u64)
    #[inline]
    #[must_use]
    pub fn as_u64(&self) -> u64 {
        u64::from(self.id)
    }
}

impl TryFrom<u64> for RouteFamily {
    type Error = std::num::TryFromIntError;

    fn try_from(id: u64) -> Result<Self, Self::Error> {
        Ok(Self::new(u32::try_from(id)?))
    }
}

impl fmt::Debug for RouteFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RouteFamily({})", self.id)
    }
}

impl fmt::Display for RouteFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

/// A route within a family
///
/// Routes are structured identifiers following the pattern:
///
/// ```text
/// {scheme}://{realm}/{area}/{resource}/{operation}
/// ```
///
/// # Structure
///
/// - **scheme**: Addressing intent (e.g., `rpc`, `inbox`, `notify`, `queue`, `stream`, `lease`)
///   - NOT a domain identifier
///   - Multiple schemes may map to the same domain
///
/// - **realm**: Top-level namespace (user-defined semantics)
///   - NOT the `RouteFamily` (realm is a string in the route)
///   - May represent tenant, org, env, or any organizational concept
///   - Runtime does not enforce semantics
///
/// - **area**: Logical subsystem or bounded context
///
/// - **resource**: Entity, stream, queue, or keyspace identifier
///
/// - **operation**: Action or verb (optional)
///
/// # Examples
///
/// ```text
/// rpc://acme/auth/users/authenticate
/// notice://acme/events/orders/created
/// queue://acme/jobs/worker/process
/// stream://acme/analytics/events/append
/// lease://acme/locks/db/migration/acquire
/// ```
///
/// # Design
///
/// Route is a wrapper around String:
/// - Runtime treats routes as opaque strings
/// - No parsing or validation at the routing layer
/// - Pure string equality for lookups
/// - Domains may parse and interpret the structure
///
/// # Route vs `RouteFamily`
///
/// **IMPORTANT:** Route contains a `{realm}` component as a string,
/// but realm is NOT the `RouteFamily`:
/// - `RouteFamily` is an integer providing hard isolation
/// - realm is a string in the route path providing logical organization
/// - Same realm value can appear in different `RouteFamilies`
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Route {
    path: Arc<str>,
}

impl Route {
    /// Create a new route
    ///
    /// # Arguments
    ///
    /// - `path`: Full route path following `{scheme}://{realm}/{area}/{resource}/{operation}` pattern
    ///
    /// # Example
    ///
    /// ```
    /// # use fitz::runtime::routing::Route;
    /// let route = Route::new("rpc://acme/auth/users/authenticate".to_string());
    /// ```
    #[inline]
    pub fn new(path: impl AsRef<str>) -> Self {
        Self {
            path: Arc::from(path.as_ref()),
        }
    }

    /// Create a route directly from a borrowed string slice.
    #[inline]
    #[must_use]
    pub fn from_ref(path: &str) -> Self {
        Self {
            path: Arc::from(path),
        }
    }

    /// Get the route path as a string slice
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.path
    }
}

impl fmt::Debug for Route {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Route({})", self.path)
    }
}

impl fmt::Display for Route {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.path)
    }
}

impl serde::Serialize for Route {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Route {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::new)
    }
}

/// A complete route address (family + route)
///
/// This is the fundamental addressing unit in Fitz.
/// All messages, leases, and routing decisions are scoped to a `RouteAddress`.
///
/// # Isolation Guarantees
///
/// Two `RouteAddress` instances are equal if and only if both their
/// family AND route match. This ensures complete isolation between families.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RouteAddress {
    family: RouteFamily,
    route: Route,
}

impl RouteAddress {
    /// Create a new route address
    ///
    /// # Example
    ///
    /// ```
    /// # use fitz::runtime::routing::{RouteFamily, Route, RouteAddress};
    /// let family = RouteFamily::new(1);
    /// let route = Route::new("/user/123");
    /// let address = RouteAddress::new(family, route);
    /// ```
    #[inline]
    #[must_use]
    pub fn new(family: RouteFamily, route: Route) -> Self {
        Self { family, route }
    }

    /// Get the route family
    #[inline]
    #[must_use]
    pub fn family(&self) -> &RouteFamily {
        &self.family
    }

    /// Get the route
    #[inline]
    #[must_use]
    pub fn route(&self) -> &Route {
        &self.route
    }

    /// Decompose into (family, route)
    #[inline]
    #[must_use]
    pub fn into_parts(self) -> (RouteFamily, Route) {
        (self.family, self.route)
    }
}

/// Build the canonical inbox address for a session within a route family.
#[inline]
#[must_use]
pub fn session_inbox_address(family: RouteFamily, session_id: u64) -> RouteAddress {
    RouteAddress::new(family, Route::new(format!("inbox://session/{session_id}")))
}

impl fmt::Debug for RouteAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RouteAddress({}/{})", self.family, self.route)
    }
}

impl fmt::Display for RouteAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.family, self.route)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_route_family_values_above_u32_range() {
        // Arrange
        let overflow = u64::from(u32::MAX) + 1;

        // Act
        let result = RouteFamily::try_from(overflow);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_isolate_same_route_in_different_families() {
        // Arrange
        let family_a = RouteFamily::new(1);
        let family_b = RouteFamily::new(2);
        let route = Route::new("/user/123");

        // Act
        let addr1 = RouteAddress::new(family_a, route.clone());
        let addr2 = RouteAddress::new(family_b, route);

        // Assert
        assert_ne!(
            addr1, addr2,
            "Same route in different families must be isolated"
        );
    }

    #[test]
    fn should_allow_same_route_in_different_families_in_hashmap() {
        // Arrange
        use std::collections::HashMap;
        let family_a = RouteFamily::new(1);
        let family_b = RouteFamily::new(2);
        let route_path = "/user/123";

        let addr1 = RouteAddress::new(family_a, Route::new(route_path));
        let addr2 = RouteAddress::new(family_b, Route::new(route_path));

        // Act
        let mut map = HashMap::new();
        map.insert(addr1.clone(), "value-a");
        map.insert(addr2.clone(), "value-b");

        // Assert
        assert_eq!(
            map.len(),
            2,
            "Same route in different families creates distinct keys"
        );
        assert_eq!(map.get(&addr1), Some(&"value-a"));
        assert_eq!(map.get(&addr2), Some(&"value-b"));
    }

    #[test]
    fn should_parse_route_triplet_given_route_prefix() {
        // Arrange
        let route = "notice://acme/app/orders/created";

        // Act
        let parsed = route_triplet(route);

        // Assert
        assert_eq!(
            parsed,
            Some(RouteTriplet {
                realm: "acme",
                area: "app",
                resource: "orders",
            })
        );
    }

    #[test]
    fn should_reject_exact_route_triplet_given_nested_resource() {
        // Arrange
        let route = "kv://acme/app/orders/by/id";

        // Act
        let parsed = route_exact_triplet(route);

        // Assert
        assert!(parsed.is_none());
    }

    #[test]
    fn should_parse_route_quad_without_scheme() {
        // Arrange
        let route = "/acme/app/orders/create";

        // Act
        let parsed = route_quad(route);

        // Assert
        assert_eq!(
            parsed,
            Some(RouteQuad {
                realm: "acme",
                area: "app",
                resource: "orders",
                operation: "create",
            })
        );
    }

    #[test]
    fn should_parse_route_scheme_given_scheme_prefix() {
        // Arrange
        let route = "schedule://acme/app/orders/run";

        // Act
        let scheme = route_scheme(route);

        // Assert
        assert_eq!(scheme, Some("schedule"));
    }

    #[test]
    fn should_reject_exact_route_quad_given_extra_segment() {
        // Arrange
        let route = "stream://acme/app/orders/create/extra";

        // Act
        let parsed = route_exact_quad(route);

        // Assert
        assert!(parsed.is_none());
    }

    #[test]
    fn should_reject_route_prefix_given_missing_segments() {
        // Arrange
        let route = "queue://acme/app";

        // Act
        let triplet = route_triplet(route);
        let quad = route_quad(route);

        // Assert
        assert!(triplet.is_none());
        assert!(quad.is_none());
    }
}
