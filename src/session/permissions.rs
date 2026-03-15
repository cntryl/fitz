//! Snapshot of permissions associated with a session
//!
//! This module keeps a read-only view of permissions that transports can capture
//! when the session is created. It's opaque to the transport layer.

use crate::auth::{Access, Permission};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Compiled permission used by runtime checks. This is created from an
/// auth::Permission by compiling the route-shaped string into a `Pattern`.
#[derive(Debug, Clone)]
struct CompiledPermission {
    pattern: crate::runtime::matcher::Pattern,
    access: Access,
}

/// Opaque permission snapshot
#[derive(Debug, Clone)]
pub struct SessionPermissions {
    /// Generic key/value snapshot for compatibility with transports
    inner: Arc<HashMap<String, String>>,
    /// Compiled Fitz permissions used by authorization checks
    compiled: Arc<Vec<CompiledPermission>>,
    /// Cache for permission checks: (route_hash, access_bits) -> allowed
    /// Uses RwLock for thread-safe interior mutability
    check_cache: Arc<RwLock<HashMap<(u64, u8), bool>>>,
}

impl SessionPermissions {
    pub fn new(map: HashMap<String, String>) -> Self {
        Self {
            inner: Arc::new(map),
            compiled: Arc::new(Vec::new()),
            check_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create permissions from parsed `auth::Permission` structs
    pub fn from_permissions(perms: Vec<Permission>) -> Self {
        // Compile auth::Permission.raw into runtime Pattern objects here so that
        // auth remains runtime-agnostic.
        let compiled: Vec<CompiledPermission> = perms
            .into_iter()
            .map(|p| {
                // Remove any fragment ("#access") before compiling into a route pattern
                let route_part = p.raw.split('#').next().unwrap_or("");
                CompiledPermission {
                    pattern: crate::runtime::matcher::Pattern::new(route_part),
                    access: p.access,
                }
            })
            .collect();

        Self {
            inner: Arc::new(HashMap::new()),
            compiled: Arc::new(compiled),
            check_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn empty() -> Self {
        Self {
            inner: Arc::new(HashMap::new()),
            compiled: Arc::new(Vec::new()),
            check_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a permission set that allow all operations on all routes
    ///
    /// Used for unauthenticated sessions when auth_required=false
    pub fn all() -> Self {
        Self::from_permissions(vec![Permission {
            raw: "**#all".to_string(),
            access: Access::All,
        }])
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).map(|s| s.as_str())
    }

    /// Check whether the permission set allows the given access to the route
    #[inline]
    pub fn allows(&self, route: &crate::runtime::routing::Route, access: Access) -> bool {
        // Create cache key from route and access
        let route_hash = route_to_hash(route.as_str());
        let access_bits = access_to_bits(access);
        let cache_key = (route_hash, access_bits);

        // Fast path: check cache first (read-lock, released immediately)
        {
            let cache = self.check_cache.read();
            if let Some(&result) = cache.get(&cache_key) {
                return result;
            }
        }

        // Slow path: evaluate permissions
        let mut allowed = false;
        for p in self.compiled.iter() {
            if !p.pattern.matches(route) {
                continue;
            }

            // Access semantics: All matches everything; Read matches Read/All; Write matches Write/All
            match (&p.access, &access) {
                (Access::All, _) => {
                    allowed = true;
                    break;
                }
                (Access::Read, Access::Read) => {
                    allowed = true;
                    break;
                }
                (Access::Write, Access::Write) => {
                    allowed = true;
                    break;
                }
                _ => continue,
            }
        }

        // Cache the result (write-lock, held briefly)
        {
            let mut cache = self.check_cache.write();
            // Simple LRU: if cache exceeds 256 entries, clear it
            if cache.len() >= 256 {
                cache.clear();
            }
            cache.insert(cache_key, allowed);
        }

        allowed
    }
}
impl fmt::Display for SessionPermissions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SessionPermissions({} entries)", self.inner.len())
    }
}

/// Convert route string to a hash for cache key
#[inline]
fn route_to_hash(route: &str) -> u64 {
    let mut hash: u64 = 5381;
    for byte in route.as_bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(*byte as u64);
    }
    hash
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
}
