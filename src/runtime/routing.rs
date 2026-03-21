// LAYER: RUNTIME
//! Route-based addressing with family isolation
//!
//! # Universal Addressing Model
//!
//! Fitz uses **RouteFamily + Route** as the universal addressing and isolation
//! model across the entire runtime. All domains (RPC, Notifications, Queue,
//! Stream, KV, Lease) use this same addressing pattern.
//!
//! # Core Concepts
//!
//! ## RouteFamily
//!
//! A **RouteFamily** is a hard isolation boundary represented as an integer (u32).
//!
//! **Properties:**
//! - Opaque identifier with no semantic meaning to the runtime
//! - No hierarchy, prefix semantics, or inheritance between families
//! - Isolation applies to routing, leasing, coordination, and state
//! - **Alignment:** RouteFamilyId aligns 1:1 with Midge ColumnFamilyId (same value)
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
//! - **scheme**: Addressing intent or interaction pattern (e.g., `rpc`, `inbox`, `notify`, `queue`, `stream`)
//!   - Scheme is NOT a domain; multiple schemes may map to the same domain
//!   - Domain dispatch is resolved from the Route, not assumed from scheme alone
//!
//! - **realm**: Top-level logical namespace
//!   - May represent tenant, organization, department, environment, or any root concept
//!   - Semantics are user-defined, not enforced by the runtime
//!   - **IMPORTANT:** Realm is just a string in the route path, NOT the RouteFamily
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
//! notify://acme/events/orders/created
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
//! - Every message send must specify a RouteFamilyId
//! - RouteFamilyId is never optional and has no default
//! - Replies must preserve the original RouteFamilyId
//! - Domains must never observe or influence state outside their RouteFamily
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
//! 4. **No cross-family state**: Domains operate within RouteFamily boundaries;
//!    state changes in family A never affect family B.
//!
//! 5. **Schemes don't imply domains**: Multiple schemes can map to the same
//!    domain; routing is resolved from the full Route, not just the scheme.
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

/// An opaque route family identifier
///
/// Route families create hard isolation boundaries in Fitz.
/// All addressing, routing, and coordination is scoped to a family.
///
/// # Isolation Guarantee
///
/// RouteFamily provides **complete isolation**:
/// - No routing leaks between families
/// - No lease conflicts between families
/// - No message delivery between families
/// - No shared state between families
///
/// # Alignment with Storage
///
/// RouteFamilyId aligns 1:1 (by value) with Midge ColumnFamilyId:
/// - RouteFamily stores u32 (aligned with Midge ColumnFamilyId)
/// - ColumnFamilyId stores u32 (Midge internal storage type)
/// - Wire format may send u64 for future compatibility; values are validated on parse
/// - Alignment is contractual, not enforced by storage code
///
/// # Design
///
/// RouteFamily is intentionally opaque:
/// - Numeric ID for efficiency
/// - No hierarchy or inheritance
/// - Pure identity comparison
///
/// This ensures families are true isolation boundaries.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RouteFamily {
    id: u32,
}

impl RouteFamily {
    /// Create a new route family from a wire-format u64 value
    ///
    /// Values above `u32::MAX` are clamped to `u32::MAX` with a warning.
    /// Fitz stores route families as u32 to align 1:1 with Midge ColumnFamilyId.
    ///
    /// # Example
    ///
    /// ```
    /// # use fitz::runtime::routing::RouteFamily;
    /// let family = RouteFamily::new(1);
    /// assert_eq!(family.id(), 1);
    /// ```
    #[inline]
    pub fn new(id: u64) -> Self {
        if id <= u32::MAX as u64 {
            Self { id: id as u32 }
        } else {
            tracing::warn!(
                "RouteFamily {} exceeds u32::MAX, clamping to {}",
                id,
                u32::MAX
            );
            Self { id: u32::MAX }
        }
    }

    /// Create a route family directly from a u32 value
    #[inline]
    pub fn from_u32(id: u32) -> Self {
        Self { id }
    }

    /// Get the family ID as u32
    #[inline]
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Get the family ID as u64 (for APIs that expect u64)
    #[inline]
    pub fn as_u64(&self) -> u64 {
        self.id as u64
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
///   - NOT the RouteFamily (realm is a string in the route)
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
/// notify://acme/events/orders/created
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
/// # Route vs RouteFamily
///
/// **IMPORTANT:** Route contains a `{realm}` component as a string,
/// but realm is NOT the RouteFamily:
/// - RouteFamily is an integer providing hard isolation
/// - realm is a string in the route path providing logical organization
/// - Same realm value can appear in different RouteFamilies
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
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: Arc::from(path.into()),
        }
    }

    /// Create a route directly from a borrowed string slice.
    #[inline]
    pub fn from_ref(path: &str) -> Self {
        Self {
            path: Arc::from(path),
        }
    }

    /// Get the route path as a string slice
    #[inline]
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

/// A complete route address (family + route)
///
/// This is the fundamental addressing unit in Fitz.
/// All messages, leases, and routing decisions are scoped to a RouteAddress.
///
/// # Isolation Guarantees
///
/// Two RouteAddress instances are equal if and only if both their
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
    pub fn new(family: RouteFamily, route: Route) -> Self {
        Self { family, route }
    }

    /// Get the route family
    #[inline]
    pub fn family(&self) -> &RouteFamily {
        &self.family
    }

    /// Get the route
    #[inline]
    pub fn route(&self) -> &Route {
        &self.route
    }

    /// Decompose into (family, route)
    #[inline]
    pub fn into_parts(self) -> (RouteFamily, Route) {
        (self.family, self.route)
    }
}

/// Build the canonical inbox address for a session within a route family.
#[inline]
pub fn session_inbox_address(family: RouteFamily, session_id: u64) -> RouteAddress {
    RouteAddress::new(
        family,
        Route::new(format!("inbox://session/{}", session_id)),
    )
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
    fn should_create_route_family() {
        // Arrange
        // (no setup needed)

        // Act
        let family = RouteFamily::new(1);

        // Assert
        assert_eq!(family.id(), 1);
    }

    #[test]
    fn should_compare_route_families_by_identity() {
        // Arrange
        let family1 = RouteFamily::new(1);
        let family2 = RouteFamily::new(1);
        let family3 = RouteFamily::new(2);

        // Act
        let eq_result = family1 == family2;
        let ne_result = family1 != family3;

        // Assert
        assert!(eq_result);
        assert!(ne_result);
    }

    #[test]
    fn should_hash_route_families_consistently() {
        // Arrange
        let family1 = RouteFamily::new(1);
        let family2 = RouteFamily::new(1);

        let mut hasher1 = std::collections::hash_map::DefaultHasher::new();
        let mut hasher2 = std::collections::hash_map::DefaultHasher::new();

        // Act
        family1.hash(&mut hasher1);
        family2.hash(&mut hasher2);

        // Assert
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn should_create_route() {
        // Arrange
        // (no setup needed)

        // Act
        let route = Route::new("/user/123");

        // Assert
        assert_eq!(route.as_str(), "/user/123");
    }

    #[test]
    fn should_compare_routes_by_path() {
        // Arrange
        let route1 = Route::new("/user/123");
        let route2 = Route::new("/user/123");
        let route3 = Route::new("/user/456");

        // Act
        let eq_result = route1 == route2;
        let ne_result = route1 != route3;

        // Assert
        assert!(eq_result);
        assert!(ne_result);
    }

    #[test]
    fn should_create_route_address() {
        // Arrange
        let family = RouteFamily::new(100);
        let route = Route::new("/service/method");

        // Act
        let address = RouteAddress::new(family, route.clone());

        // Assert
        assert_eq!(address.family(), &family);
        assert_eq!(address.route(), &route);
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
}
