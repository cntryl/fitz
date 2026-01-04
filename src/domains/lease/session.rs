use crate::domains::lease::protocol::LeaseMessage;
use crate::domains::lease::lease_actor::LeaseActor;
use crate::runtime::actor::{Context, Actor};
use crate::session::permissions::SessionPermissions;
use crate::runtime::routing::Route;
use crate::auth::Access;
use crate::session::session::SessionId;
use crate::runtime::routing::RouteFamily;

/// Lightweight SessionActor helpers for the lease domain.
///
/// Responsibilities:
/// - Enforce session-level authorization for lease operations
/// - Forward authorized operations to the LeaseActor
///
/// This is intentionally small: a full actor system is out-of-scope for this change,
/// but tests rely on the SessionActor's semantic enforcement.
pub struct SessionActor {
    pub session_id: SessionId,
    pub permissions: SessionPermissions,
}

impl SessionActor {
    pub fn new(session_id: SessionId, permissions: SessionPermissions) -> Self {
        Self {
            session_id,
            permissions,
        }
    }

    /// Attempt to acquire a lease. Returns Err if authorization fails.
    pub fn acquire(
        &self,
        family: RouteFamily,
        route: Route,
        owner_id: String,
        ttl_secs: u64,
        lease_actor: &mut LeaseActor,
        ctx: &mut Context<LeaseActor>,
    ) -> Result<(), String> {
        // Acquire requires write access (taking a lock is a write operation)
        if !self.permissions.allows(&route, Access::Write) {
            return Err("unauthorized: acquire".to_string());
        }

        let msg = LeaseMessage::Acquire {
            family_id: family,
            route,
            owner_id,
            ttl_secs,
        };
        lease_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to renew a lease. Returns Err if authorization fails.
    pub fn renew(
        &self,
        family: RouteFamily,
        route: Route,
        owner_id: String,
        fencing_token: u64,
        ttl_secs: u64,
        lease_actor: &mut LeaseActor,
        ctx: &mut Context<LeaseActor>,
    ) -> Result<(), String> {
        // Renew requires write access (maintaining a lock is a write operation)
        if !self.permissions.allows(&route, Access::Write) {
            return Err("unauthorized: renew".to_string());
        }

        let msg = LeaseMessage::Renew {
            family_id: family,
            route,
            owner_id,
            fencing_token,
            ttl_secs,
        };
        lease_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to release a lease. Returns Err if authorization fails.
    pub fn release(
        &self,
        family: RouteFamily,
        route: Route,
        owner_id: String,
        fencing_token: u64,
        lease_actor: &mut LeaseActor,
        ctx: &mut Context<LeaseActor>,
    ) -> Result<(), String> {
        // Release requires write access (releasing a lock is a write operation)
        if !self.permissions.allows(&route, Access::Write) {
            return Err("unauthorized: release".to_string());
        }

        let msg = LeaseMessage::Release {
            family_id: family,
            route,
            owner_id,
            fencing_token,
        };
        lease_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to query lease status. Returns Err if authorization fails.
    pub fn query(
        &self,
        family: RouteFamily,
        route: Route,
        lease_actor: &mut LeaseActor,
        ctx: &mut Context<LeaseActor>,
    ) -> Result<(), String> {
        // Query requires read access (just inspecting state)
        if !self.permissions.allows(&route, Access::Read) {
            return Err("unauthorized: query".to_string());
        }

        let msg = LeaseMessage::Query {
            family_id: family,
            route,
        };
        lease_actor.receive(msg, ctx);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::actor::Context;
    use crate::runtime::router::Router;
    use crate::runtime::routing::{Route, RouteFamily, RouteAddress};
    use crate::session::permissions::SessionPermissions;
    use crate::auth::Permission;
    use std::sync::Arc;
    use crate::domains::lease::lease_actor::LeaseActor;

    fn make_ctx() -> Context<LeaseActor> {
        let router = Router::new();
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("lease://realm/session"));
        Context::new(addr, Arc::new(router))
    }

    #[test]
    fn should_reject_unauthenticated_acquire() {
        // Arrange
        let mut actor = LeaseActor::new();
        let mut ctx = make_ctx();

        let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

        // Act
        let res = session.acquire(
            RouteFamily::new(1),
            Route::new("lease://realm/locks/db-migration"),
            "owner1".to_string(),
            30,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_err());
        assert_eq!(actor.lease_count(), 0);
    }

    #[test]
    fn should_reject_unauthorized_acquire() {
        // Arrange
        let mut actor = LeaseActor::new();
        let mut ctx = make_ctx();

        // Create a session with read-only permission
        let perms = vec![Permission::parse("lease://realm/locks/**#read").unwrap()];
        let session_perms = SessionPermissions::from_permissions(perms);
        let session = SessionActor::new(SessionId(1), session_perms);

        // Act - try to acquire with only read permission
        let res = session.acquire(
            RouteFamily::new(1),
            Route::new("lease://realm/locks/db-migration"),
            "owner1".to_string(),
            30,
            &mut actor,
            &mut ctx,
        );

        // Assert - should fail because acquire requires write
        assert!(res.is_err());
    }

    #[test]
    fn should_allow_authorized_acquire() {
        // Arrange
        let mut actor = LeaseActor::new();
        let mut ctx = make_ctx();

        let perms = vec![Permission::parse("lease://realm/locks/**#write").unwrap()];
        let session_perms = SessionPermissions::from_permissions(perms);
        let session = SessionActor::new(SessionId(1), session_perms);

        // Act
        let res = session.acquire(
            RouteFamily::new(1),
            Route::new("lease://realm/locks/db-migration"),
            "owner1".to_string(),
            30,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_ok());
    }

    #[test]
    fn should_reject_unauthorized_renew() {
        // Arrange
        let mut actor = LeaseActor::new();
        let mut ctx = make_ctx();

        let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

        // Act
        let res = session.renew(
            RouteFamily::new(1),
            Route::new("lease://realm/locks/db-migration"),
            "owner1".to_string(),
            1,
            30,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_err());
    }

    #[test]
    fn should_reject_unauthorized_release() {
        // Arrange
        let mut actor = LeaseActor::new();
        let mut ctx = make_ctx();

        let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

        // Act
        let res = session.release(
            RouteFamily::new(1),
            Route::new("lease://realm/locks/db-migration"),
            "owner1".to_string(),
            1,
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_err());
    }

    #[test]
    fn should_allow_authorized_query_with_read_permission() {
        // Arrange
        let mut actor = LeaseActor::new();
        let mut ctx = make_ctx();

        let perms = vec![Permission::parse("lease://realm/locks/**#read").unwrap()];
        let session_perms = SessionPermissions::from_permissions(perms);
        let session = SessionActor::new(SessionId(1), session_perms);

        // Act
        let res = session.query(
            RouteFamily::new(1),
            Route::new("lease://realm/locks/db-migration"),
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_ok());
    }

    #[test]
    fn should_reject_unauthorized_query() {
        // Arrange
        let mut actor = LeaseActor::new();
        let mut ctx = make_ctx();

        let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

        // Act
        let res = session.query(
            RouteFamily::new(1),
            Route::new("lease://realm/locks/db-migration"),
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_err());
    }
}
