use crate::auth::{Access, Claims};
use crate::runtime::routing::Route;
use crate::session::session::SessionId;
use crate::session::permissions::SessionPermissions;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Session-scoped actor that enforces authorization.
///
/// **Responsibility:** Check every operation against compiled permissions
/// before calling domains. Domains are guaranteed to receive authorized requests only.
///
/// **Invariants:**
/// - Claims are immutable after authentication
/// - Permissions are compiled once, never reparsed
/// - Authorization is mandatory for every operation
#[derive(Debug, Clone)]
pub struct SessionActor {
    pub session_id: SessionId,
    /// Immutable authentication claims (set at auth time, may be None for pre-auth)
    pub claims: Option<Arc<Claims>>,
    /// Compiled and ready-to-match permissions
    pub permissions: Arc<SessionPermissions>,
}

impl SessionActor {
    /// Create a new unauthenticated session actor (pre-auth state)
    pub fn new(session_id: SessionId, permissions: SessionPermissions) -> Self {
        Self {
            session_id,
            claims: None,
            permissions: Arc::new(permissions),
        }
    }

    /// Authenticate this actor with claims (called after auth verification)
    pub fn authenticate(&mut self, claims: Claims, permissions: SessionPermissions) {
        self.claims = Some(Arc::new(claims));
        self.permissions = Arc::new(permissions);
    }

    /// Re-authenticate this actor with new claims (token refresh/rotation)
    ///
    /// Replaces existing claims and recompiles permissions atomically.
    /// Use this for token refresh flows without tearing down the session.
    pub fn reauth(&mut self, claims: Claims, permissions: SessionPermissions) {
        self.claims = Some(Arc::new(claims));
        self.permissions = Arc::new(permissions);
    }

    /// Check if the current token is expired
    ///
    /// Returns true if authenticated and token is expired, false otherwise.
    /// Unauthenticated sessions are never considered expired.
    pub fn is_token_expired(&self) -> bool {
        if let Some(claims) = &self.claims {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            return claims.exp <= now;
        }
        false
    }

    /// Get the current token expiration time (unix epoch seconds)
    pub fn token_expiration(&self) -> Option<u64> {
        self.claims.as_ref().map(|c| c.exp)
    }

    /// Core authorization gate: check if this session is allowed to perform
    /// `access` on `route`.
    ///
    /// **Every domain call must pass through this check.**
    /// If this returns false, the operation is rejected at the session layer
    /// and never reaches the domain.
    ///
    /// **Security:** This check includes token expiration validation.
    /// Expired tokens are automatically rejected.
    pub fn authorize(&self, route: &Route, access: Access) -> bool {
        // Reject if token is expired
        if self.is_token_expired() {
            return false;
        }
        self.permissions.allows(route, access)
    }

    /// Batch authorization check (useful for multi-operation requests)
    pub fn authorize_all(&self, checks: &[(Route, Access)]) -> bool {
        checks.iter().all(|(route, access)| self.authorize(route, *access))
    }

    /// Check if this session is authenticated
    pub fn is_authenticated(&self) -> bool {
        self.claims.is_some()
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
        let perms = crate::session::permissions::SessionPermissions::from_permissions(vec![p.clone()]);
        
        // Create minimal claims for testing
        let claims = crate::auth::Claims {
            sub: "user:42".to_string(),
            tenant: "prod".to_string(),
            roles: vec![],
            permissions: vec![p],
            exp: 9999999999,
        };
        
        let mut actor = SessionActor::new(crate::session::session::SessionId(1), perms.clone());
        actor.authenticate(claims, perms);

        // Act
        let can_write = actor.authorize(&Route::new("notice://prod/orders/create"), Access::Write);
        let can_read = actor.authorize(&Route::new("notice://prod/orders/create"), Access::Read);

        // Assert
        assert!(can_write);
        assert!(!can_read);
    }
}
