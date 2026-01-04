use crate::auth::Access;
use crate::runtime::routing::Route;
use crate::session::session::SessionId;
use crate::session::permissions::SessionPermissions;
use std::sync::Arc;

/// Session-scoped actor helper that encapsulates authorization checks.
///
/// Responsibilities:
/// - Hold immutable `SessionPermissions` snapshot
/// - Provide `authorize` helper for domain-level checks
#[derive(Debug, Clone)]
pub struct SessionActor {
    pub session_id: SessionId,
    pub permissions: Arc<SessionPermissions>,
}

impl SessionActor {
    pub fn new(session_id: SessionId, permissions: SessionPermissions) -> Self {
        Self {
            session_id,
            permissions: Arc::new(permissions),
        }
    }

    /// Check whether this session is allowed to perform `access` on `route`.
    pub fn authorize(&self, route: &Route, access: Access) -> bool {
        self.permissions.allows(route, access)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Permission;
    use crate::runtime::routing::Route;

    #[test]
    fn should_session_actor_authorize_checks_permissions() {
        // Arrange
        let p = Permission::parse("notice://prod/orders/**#write").unwrap();
        let perms = crate::session::permissions::SessionPermissions::from_permissions(vec![p]);
        let actor = SessionActor::new(crate::session::session::SessionId(1), perms);

        // Act
        let can_write = actor.authorize(&Route::new("notice://prod/orders/create"), Access::Write);
        let can_read = actor.authorize(&Route::new("notice://prod/orders/create"), Access::Read);

        // Assert
        assert!(can_write);
        assert!(!can_read);
    }
}
