//! Route-based addressing with family isolation
//!
//! Fitz uses route families as hard isolation boundaries. All addressing
//! in Fitz is based on (RouteFamily, Route) tuples.
//!
//! # Route Families
//!
//! A RouteFamily is an opaque identifier that creates an isolation boundary:
//! - Routes have meaning only within their family
//! - The same route string may exist in multiple families
//! - Families are fully isolated: no inheritance, no prefix semantics
//! - All coordination, routing, and leasing is scoped to (family, route)
//!
//! # Invariants
//!
//! **CRITICAL: These invariants must be maintained:**
//!
//! 1. **No cross-family resolution**: A route lookup in family A will never
//!    return results from family B, even if the route strings match.
//!
//! 2. **No cross-family leases**: A lease acquired in family A has no effect
//!    on the same resource name in family B.
//!
//! 3. **No cross-family messages**: Messages sent to (family A, route X) will
//!    never be delivered to (family B, route X), even if route X is identical.
//!
//! # Example
//!
//! ```ignore
//! let family_a = RouteFamily::new(1);
//! let family_b = RouteFamily::new(2);
//!
//! // These are completely independent addresses:
//! let addr1 = RouteAddress::new(family_a, Route::new("/user/123"));
//! let addr2 = RouteAddress::new(family_b, Route::new("/user/123"));
//!
//! // Leases are independent:
//! lease_actor.acquire(family_a, "lock-1"); // Family 1
//! lease_actor.acquire(family_b, "lock-1"); // Family 2 (independent)
//! ```

use std::fmt;
use std::hash::{Hash, Hasher};

/// An opaque route family identifier
///
/// Route families create hard isolation boundaries in Fitz.
/// All addressing, routing, and coordination is scoped to a family.
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
    id: u64,
}

impl RouteFamily {
    /// Create a new route family
    ///
    /// # Arguments
    ///
    /// - `id`: Unique numeric identifier for the family
    ///
    /// # Example
    ///
    /// ```
    /// # use fitz::routing::RouteFamily;
    /// let family = RouteFamily::new(1);
    /// ```
    pub fn new(id: u64) -> Self {
        Self { id }
    }

    /// Get the family ID
    pub fn id(&self) -> u64 {
        self.id
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
/// Routes are string-based addresses that have meaning only within
/// their RouteFamily. The same route string in different families
/// represents completely independent addresses.
///
/// # Design
///
/// Route is a simple wrapper around String:
/// - No parsing or validation (opaque to the runtime)
/// - No prefix matching or wildcards (yet)
/// - Pure string equality for lookups
///
/// Higher-level domains may impose structure (e.g., "/user/123"),
/// but the routing layer treats routes as opaque strings.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Route {
    path: String,
}

impl Route {
    /// Create a new route
    ///
    /// # Arguments
    ///
    /// - `path`: Route path (e.g., "/user/123", "db-shard-1")
    ///
    /// # Example
    ///
    /// ```
    /// # use fitz::routing::Route;
    /// let route = Route::new("/user/123");
    /// ```
    pub fn new(path: impl Into<String>) -> Self {
        Self { path: path.into() }
    }

    /// Get the route path as a string slice
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
    /// # use fitz::routing::{RouteFamily, Route, RouteAddress};
    /// let family = RouteFamily::new(1);
    /// let route = Route::new("/user/123");
    /// let address = RouteAddress::new(family, route);
    /// ```
    pub fn new(family: RouteFamily, route: Route) -> Self {
        Self { family, route }
    }

    /// Get the route family
    pub fn family(&self) -> &RouteFamily {
        &self.family
    }

    /// Get the route
    pub fn route(&self) -> &Route {
        &self.route
    }

    /// Decompose into (family, route)
    pub fn into_parts(self) -> (RouteFamily, Route) {
        (self.family, self.route)
    }
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
        // Arrange & Act
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

        // Act & Assert
        assert_eq!(family1, family2);
        assert_ne!(family1, family3);
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
        // Arrange & Act
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

        // Act & Assert
        assert_eq!(route1, route2);
        assert_ne!(route1, route3);
    }

    #[test]
    fn should_create_route_address() {
        // Arrange
        let family = RouteFamily::new(100);
        let route = Route::new("/service/method");

        // Act
        let address = RouteAddress::new(family.clone(), route.clone());

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
        assert_ne!(addr1, addr2, "Same route in different families must be isolated");
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
        assert_eq!(map.len(), 2, "Same route in different families creates distinct keys");
        assert_eq!(map.get(&addr1), Some(&"value-a"));
        assert_eq!(map.get(&addr2), Some(&"value-b"));
    }
}
