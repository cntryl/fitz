//! Snapshot of permissions associated with a session
//!
//! This module keeps a read-only view of permissions that transports can capture
//! when the session is created. It's opaque to the transport layer.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// Opaque permission snapshot
#[derive(Debug, Clone)]
pub struct SessionPermissions {
    inner: Arc<HashMap<String, String>>,
}

impl SessionPermissions {
    pub fn new(map: HashMap<String, String>) -> Self {
        Self { inner: Arc::new(map) }
    }

    pub fn empty() -> Self {
        Self { inner: Arc::new(HashMap::new()) }
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).map(|s| s.as_str())
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

    #[test]
    fn snapshot_survives_clone() {
        let mut map = HashMap::new();
        map.insert("role".to_string(), "admin".to_string());
        let perms = SessionPermissions::new(map);
        let clone = perms.clone();
        assert_eq!(clone.get("role"), Some("admin"));
    }
}
