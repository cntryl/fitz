//! Snapshot of permissions associated with a session
//!
//! This module keeps a read-only view of permissions that transports can capture
//! when the session is created. It's opaque to the transport layer.

use crate::auth::{Access, Permission};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

#[derive(Debug, Default)]
struct AuthorizationCache {
    entries: HashMap<u8, HashMap<String, bool>>,
    retained_route_bytes: usize,
}

const CHECK_CACHE_ENTRY_LIMIT: usize = 256;
const CHECK_CACHE_BYTE_LIMIT: usize = 64 * 1024;

/// Compiled permission used by runtime checks. This is created from an
/// `auth::Permission` by compiling the route-shaped string into a `Pattern`.
#[derive(Debug, Clone)]
struct CompiledPermission {
    pattern: crate::runtime::matcher::Pattern,
    access: Access,
}

impl CompiledPermission {
    fn from_permission(
        permission: &Permission,
        pattern_cache: Option<&mut HashMap<String, crate::runtime::matcher::Pattern>>,
    ) -> Self {
        let route_part = permission_route_part(&permission.raw);
        let pattern = match pattern_cache {
            Some(pattern_cache) => {
                if let Some(pattern) = pattern_cache.get(route_part) {
                    pattern.clone()
                } else {
                    let pattern = crate::runtime::matcher::Pattern::new(route_part);
                    pattern_cache.insert(route_part.to_string(), pattern.clone());
                    pattern
                }
            }
            None => crate::runtime::matcher::Pattern::new(route_part),
        };

        Self {
            pattern,
            access: permission.access,
        }
    }

    fn allows(&self, route: &str, access: Access) -> bool {
        self.pattern.matches_str(route) && access_grants(self.access, access)
    }
}

/// Opaque permission snapshot
#[derive(Debug, Clone)]
pub struct SessionPermissions {
    /// Generic key/value snapshot for compatibility with transports
    inner: Arc<HashMap<String, String>>,
    /// Compiled Fitz permissions used by authorization checks
    compiled: Arc<Vec<CompiledPermission>>,
    /// Cache for permission checks: `access_bits` -> route -> allowed
    /// Uses `RwLock` for thread-safe interior mutability
    check_cache: Arc<RwLock<AuthorizationCache>>,
}

impl SessionPermissions {
    #[must_use]
    pub fn new(map: HashMap<String, String>) -> Self {
        Self::from_parts(map, Vec::new())
    }

    /// Create permissions from parsed `auth::Permission` structs
    #[must_use]
    pub fn from_permissions(perms: Vec<Permission>) -> Self {
        Self::from_parts(HashMap::new(), Self::compile_permissions(perms))
    }

    #[must_use]
    pub fn empty() -> Self {
        Self::from_parts(HashMap::new(), Vec::new())
    }

    /// Create a permission set that allow all operations on all routes
    ///
    /// Used for unauthenticated sessions when `auth_required=false`
    #[must_use]
    pub fn all() -> Self {
        Self::from_permissions(vec![Permission {
            raw: "**#all".to_string(),
            access: Access::All,
        }])
    }

    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).map(String::as_str)
    }

    /// Check whether the permission set allows the given access to the route
    #[inline]
    #[must_use]
    pub fn allows(&self, route: &crate::runtime::routing::Route, access: Access) -> bool {
        self.allows_route(route.as_str(), access)
    }

    /// Check whether the permission set allows the given access to a raw route string.
    #[inline]
    #[must_use]
    pub fn allows_route(&self, route: &str, access: Access) -> bool {
        if crate::utils::route_shape::validate_route_shape(route).is_err() {
            return false;
        }

        let access_bits = access_to_bits(access);

        if let Some(result) = self.cached_authorization(route, access_bits) {
            return result;
        }

        let allowed = self.evaluate_route_access(route, access);
        self.cache_authorization(route, access_bits, allowed);

        allowed
    }

    fn from_parts(inner: HashMap<String, String>, compiled: Vec<CompiledPermission>) -> Self {
        Self {
            inner: Arc::new(inner),
            compiled: Arc::new(compiled),
            check_cache: Arc::new(RwLock::new(AuthorizationCache::default())),
        }
    }

    fn compile_permissions(perms: Vec<Permission>) -> Vec<CompiledPermission> {
        let mut pattern_cache = (perms.len() > 8).then(|| HashMap::with_capacity(perms.len()));

        perms
            .into_iter()
            .map(|permission| {
                CompiledPermission::from_permission(&permission, pattern_cache.as_mut())
            })
            .collect()
    }

    fn cached_authorization(&self, route: &str, access_bits: u8) -> Option<bool> {
        let cache = self.check_cache.read();
        cache
            .entries
            .get(&access_bits)
            .and_then(|routes| routes.get(route).copied())
    }

    fn evaluate_route_access(&self, route: &str, access: Access) -> bool {
        self.compiled
            .iter()
            .any(|permission| permission.allows(route, access))
    }

    fn cache_authorization(&self, route: &str, access_bits: u8, allowed: bool) {
        if route.len() > CHECK_CACHE_BYTE_LIMIT {
            return;
        }

        let mut cache = self.check_cache.write();
        let is_new = cache
            .entries
            .get(&access_bits)
            .is_none_or(|routes| !routes.contains_key(route));
        let should_clear = is_new
            && (Self::cached_entry_count(&cache) >= CHECK_CACHE_ENTRY_LIMIT
                || cache.retained_route_bytes.saturating_add(route.len()) > CHECK_CACHE_BYTE_LIMIT);

        if should_clear {
            cache.entries.clear();
            cache.retained_route_bytes = 0;
        }

        let routes = cache.entries.entry(access_bits).or_default();
        if let Some(cached) = routes.get_mut(route) {
            *cached = allowed;
        } else {
            routes.insert(route.to_string(), allowed);
            cache.retained_route_bytes = cache.retained_route_bytes.saturating_add(route.len());
        }
    }

    fn cached_entry_count(cache: &AuthorizationCache) -> usize {
        cache.entries.values().map(HashMap::len).sum()
    }
}
impl fmt::Display for SessionPermissions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SessionPermissions({} entries)", self.inner.len())
    }
}

#[inline]
fn permission_route_part(raw_permission: &str) -> &str {
    raw_permission
        .rsplit_once('#')
        .map_or(raw_permission, |(route, _access)| route)
}

#[inline]
fn access_grants(granted: Access, requested: Access) -> bool {
    matches!(granted, Access::All) || granted == requested
}

/// Convert Access to a compact bit representation for cache key
#[inline]
fn access_to_bits(access: Access) -> u8 {
    match access {
        Access::Read => 1,
        Access::Write => 2,
        Access::All => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Access, Permission};
    use crate::runtime::routing::Route;

    #[test]
    fn should_snapshot_survive_clone() {
        // Arrange
        let mut map = HashMap::new();
        map.insert("role".to_string(), "admin".to_string());
        let perms = SessionPermissions::new(map);

        // Act
        let clone = perms.clone();

        // Assert
        assert_eq!(clone.get("role"), Some("admin"));
    }

    #[test]
    fn should_check_allows_permissions() {
        // Arrange
        let p = Permission::parse("notice://prod/orders/**#write").unwrap();
        let perms = SessionPermissions::from_permissions(vec![p]);

        let route_allowed = Route::new("notice://prod/orders/create");
        let route_denied = Route::new("notice://prod/other/create");

        // Act
        let can_write_allowed = perms.allows(&route_allowed, Access::Write);
        let can_read_allowed = perms.allows(&route_allowed, Access::Read);
        let can_write_denied = perms.allows(&route_denied, Access::Write);

        // Assert
        assert!(can_write_allowed);
        assert!(!can_read_allowed);
        assert!(!can_write_denied);
    }

    #[test]
    fn should_not_authorize_same_path_on_different_scheme() {
        // Arrange
        let p = Permission::parse("notice://prod/orders/**#write").unwrap();
        let perms = SessionPermissions::from_permissions(vec![p]);

        // Act
        let notify_allowed =
            perms.allows(&Route::new("notify://prod/orders/create"), Access::Write);
        let queue_allowed = perms.allows(&Route::new("queue://prod/orders/create"), Access::Write);

        // Assert
        assert!(!notify_allowed);
        assert!(!queue_allowed);
    }

    #[test]
    fn should_allow_read_given_all_access_permission() {
        // Arrange
        let permission = Permission::parse("notice://prod/orders/**").unwrap();
        let perms = SessionPermissions::from_permissions(vec![permission]);
        let route = Route::new("notice://prod/orders/create");

        // Act
        let can_read = perms.allows(&route, Access::Read);

        // Assert
        assert!(can_read);
    }

    #[test]
    fn should_allow_write_given_all_access_permission() {
        // Arrange
        let permission = Permission::parse("notice://prod/orders/**").unwrap();
        let perms = SessionPermissions::from_permissions(vec![permission]);
        let route = Route::new("notice://prod/orders/create");

        // Act
        let can_write = perms.allows(&route, Access::Write);

        // Assert
        assert!(can_write);
    }

    #[test]
    fn should_cache_authorization_by_route_string() {
        // Arrange
        let permission = Permission::parse("notice://prod/orders/**#write").unwrap();
        let perms = SessionPermissions::from_permissions(vec![permission]);
        let first_route = "notice://prod/orders/create";
        let second_route = "notice://prod/orders/update";

        // Act
        let _ = perms.allows_route(first_route, Access::Write);
        let _ = perms.allows_route(second_route, Access::Write);

        // Assert
        let cache = perms.check_cache.read();
        let routes = cache.entries.get(&access_to_bits(Access::Write)).unwrap();
        assert!(routes.contains_key(first_route));
        assert!(routes.contains_key(second_route));
    }

    #[test]
    fn should_bound_cached_route_bytes() {
        // Arrange
        let permission = Permission::parse("notice://**#write").unwrap();
        let perms = SessionPermissions::from_permissions(vec![permission]);
        let route_body = "a".repeat(crate::utils::route_shape::MAX_ROUTE_BYTES - 32);

        // Act
        for index in 0..32 {
            let route = format!("notice://{index}/{route_body}");
            let _ = perms.allows_route(&route, Access::Write);
        }

        // Assert
        let cache = perms.check_cache.read();
        assert!(cache.retained_route_bytes <= CHECK_CACHE_BYTE_LIMIT);
        assert!(SessionPermissions::cached_entry_count(&cache) < 32);
    }

    #[test]
    fn should_not_cache_oversized_route() {
        // Arrange
        let permission = Permission::parse("notice://**#write").unwrap();
        let perms = SessionPermissions::from_permissions(vec![permission]);
        let route = format!(
            "notice://{}",
            "a".repeat(crate::utils::route_shape::MAX_ROUTE_BYTES)
        );

        // Act
        let allowed = perms.allows_route(&route, Access::Write);

        // Assert
        assert!(!allowed);
        assert_eq!(
            SessionPermissions::cached_entry_count(&perms.check_cache.read()),
            0
        );
    }
}
