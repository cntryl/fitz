//! Snapshot of permissions associated with a session
//!
//! This module keeps a read-only view of permissions that transports can capture
//! when the session is created. It's opaque to the transport layer.

use crate::auth::{Access, Permission};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Opaque permission snapshot
#[derive(Debug, Clone)]
pub struct SessionPermissions {
    /// Generic key/value snapshot for compatibility with transports
    inner: Arc<HashMap<String, String>>,
    /// Compiled Fitz permissions used by authorization checks
    compiled: Arc<Vec<Permission>>,
}

impl SessionPermissions {
    pub fn new(map: HashMap<String, String>) -> Self {
        Self {
            inner: Arc::new(map),
            compiled: Arc::new(Vec::new()),
        }
    }

    /// Create permissions from parsed `Permission` structs
    pub fn from_permissions(perms: Vec<Permission>) -> Self {
        Self {
            inner: Arc::new(HashMap::new()),
            compiled: Arc::new(perms),
        }
    }

    pub fn empty() -> Self {
        Self {
            inner: Arc::new(HashMap::new()),
            compiled: Arc::new(Vec::new()),
        }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).map(|s| s.as_str())
    }

    /// Check whether the permission set allows the given access to the route
    pub fn allows(&self, route: &crate::runtime::routing::Route, access: Access) -> bool {
        for p in self.compiled.iter() {
            if p.matches(route, &access) {
                return true;
            }
        }
        false
    }
}

impl fmt::Display for SessionPermissions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SessionPermissions({} entries)", self.inner.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Permission, Access};
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
}
