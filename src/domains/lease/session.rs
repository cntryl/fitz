use crate::auth::Access;
use crate::domains::lease::protocol::LeaseMessage;
use crate::domains::lease::LeaseActor;
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::Route;
use crate::runtime::routing::RouteFamily;
use crate::session::permissions::SessionPermissions;
use crate::session::session::SessionId;

/// Parameters for an acquire lease operation
pub struct AcquireRequest {
    pub family: RouteFamily,
    pub route: Route,
    pub owner_id: String,
    pub ttl_secs: u64,
    pub wait_seconds: u32,
}

/// Parameters for an extend lease operation
pub struct ExtendRequest {
    pub family: RouteFamily,
    pub route: Route,
    pub owner_id: String,
    pub fencing_token: u64,
    pub ttl_secs: u64,
}

/// Parameters for a release lease operation
pub struct ReleaseRequest {
    pub family: RouteFamily,
    pub route: Route,
    pub owner_id: String,
    pub fencing_token: u64,
}

/// Lightweight `SessionActor` helpers for the lease domain.
///
/// Responsibilities:
/// - Enforce session-level authorization for lease operations
/// - Forward authorized operations to the `LeaseActor`
///
/// This is intentionally small: a full actor system is out-of-scope for this change,
/// but tests rely on the `SessionActor`'s semantic enforcement.
pub struct SessionActor {
    pub session_id: SessionId,
    pub permissions: SessionPermissions,
}

impl SessionActor {
    #[must_use]
    pub fn new(session_id: SessionId, permissions: SessionPermissions) -> Self {
        Self {
            session_id,
            permissions,
        }
    }

    /// Attempt to acquire a lease.
    ///
    /// # Errors
    ///
    /// Returns an error when the session lacks write access to the lease route.
    pub fn acquire(
        &self,
        request: AcquireRequest,
        lease_actor: &mut LeaseActor,
        ctx: &mut Context<LeaseActor>,
    ) -> Result<(), String> {
        // Acquire requires write access (taking a lock is a write operation)
        if !self.permissions.allows(&request.route, Access::Write) {
            return Err("unauthorized: acquire".to_string());
        }

        let msg = LeaseMessage::Acquire {
            family_id: request.family,
            route: request.route,
            owner_id: request.owner_id,
            ttl_secs: request.ttl_secs,
            wait_seconds: request.wait_seconds,
        };
        lease_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to extend a lease.
    ///
    /// # Errors
    ///
    /// Returns an error when the session lacks write access to the lease route.
    pub fn extend(
        &self,
        request: ExtendRequest,
        lease_actor: &mut LeaseActor,
        ctx: &mut Context<LeaseActor>,
    ) -> Result<(), String> {
        // Extend requires write access (maintaining a lock is a write operation)
        if !self.permissions.allows(&request.route, Access::Write) {
            return Err("unauthorized: extend".to_string());
        }

        let msg = LeaseMessage::Extend {
            family_id: request.family,
            route: request.route,
            owner_id: request.owner_id,
            fencing_token: request.fencing_token,
            ttl_secs: request.ttl_secs,
        };
        lease_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to release a lease.
    ///
    /// # Errors
    ///
    /// Returns an error when the session lacks write access to the lease route.
    pub fn release(
        &self,
        request: ReleaseRequest,
        lease_actor: &mut LeaseActor,
        ctx: &mut Context<LeaseActor>,
    ) -> Result<(), String> {
        // Release requires write access (releasing a lock is a write operation)
        if !self.permissions.allows(&request.route, Access::Write) {
            return Err("unauthorized: release".to_string());
        }

        let msg = LeaseMessage::Release {
            family_id: request.family,
            route: request.route,
            owner_id: request.owner_id,
            fencing_token: request.fencing_token,
        };
        lease_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to query lease status.
    ///
    /// # Errors
    ///
    /// Returns an error when the session lacks read access to the lease route.
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
    use crate::auth::Permission;
    use crate::domains::lease::LeaseActor;
    use crate::runtime::actor::Context;
    use crate::runtime::router::Router;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use crate::session::permissions::SessionPermissions;
    use std::sync::Arc;

    fn make_ctx() -> Context<LeaseActor> {
        let router = Router::new();
        let addr = RouteAddress::new(RouteFamily::new(1), Route::new("lease://realm/session"));
        Context::new(addr, Arc::new(router))
    }

    #[test]
    fn should_reject_unauthenticated_acquire() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();

        let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

        // Act
        let res = session.acquire(
            AcquireRequest {
                family: RouteFamily::new(1),
                route: Route::new("lease://realm/locks/db-migration"),
                owner_id: "owner1".to_string(),
                ttl_secs: 30,
                wait_seconds: 0,
            },
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
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();

        // Create a session with read-only permission
        let perms = vec![Permission::parse("lease://realm/locks/**#read").unwrap()];
        let session_perms = SessionPermissions::from_permissions(perms);
        let session = SessionActor::new(SessionId(1), session_perms);

        // Act - try to acquire with only read permission
        let res = session.acquire(
            AcquireRequest {
                family: RouteFamily::new(1),
                route: Route::new("lease://realm/locks/db-migration"),
                owner_id: "owner1".to_string(),
                ttl_secs: 30,
                wait_seconds: 0,
            },
            &mut actor,
            &mut ctx,
        );

        // Assert - should fail because acquire requires write
        assert!(res.is_err());
    }

    #[test]
    fn should_allow_authorized_acquire() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();

        let perms = vec![Permission::parse("lease://realm/locks/**#write").unwrap()];
        let session_perms = SessionPermissions::from_permissions(perms);
        let session = SessionActor::new(SessionId(1), session_perms);

        // Act
        let res = session.acquire(
            AcquireRequest {
                family: RouteFamily::new(1),
                route: Route::new("lease://realm/locks/db-migration"),
                owner_id: "owner1".to_string(),
                ttl_secs: 30,
                wait_seconds: 0,
            },
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_ok());
    }

    #[test]
    fn should_reject_unauthorized_renew() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();

        let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

        // Act
        let res = session.extend(
            ExtendRequest {
                family: RouteFamily::new(1),
                route: Route::new("lease://realm/locks/db-migration"),
                owner_id: "owner1".to_string(),
                fencing_token: 1,
                ttl_secs: 30,
            },
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_err());
    }

    #[test]
    fn should_reject_unauthorized_release() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();

        let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

        // Act
        let res = session.release(
            ReleaseRequest {
                family: RouteFamily::new(1),
                route: Route::new("lease://realm/locks/db-migration"),
                owner_id: "owner1".to_string(),
                fencing_token: 1,
            },
            &mut actor,
            &mut ctx,
        );

        // Assert
        assert!(res.is_err());
    }

    #[test]
    fn should_allow_authorized_query_with_read_permission() {
        // Arrange
        let mut actor = LeaseActor::new(RouteFamily::new(1));
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
        let mut actor = LeaseActor::new(RouteFamily::new(1));
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
