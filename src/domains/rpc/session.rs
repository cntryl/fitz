//! Lightweight `SessionActor` helpers for the RPC domain
//!
//! Responsibilities:
//! - Enforce session-level authorization for RPC call and worker-registration operations
//! - Forward authorized operations to the `RpcRouteActor`
//!
//! This is intentionally small: a full actor system is out-of-scope for this change,
//! but tests rely on the `SessionActor`'s semantic enforcement.

use crate::auth::Access;
use crate::domains::rpc::RpcRouteActor;
use crate::domains::rpc::{RpcMessage, RpcRequest};
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::{Route, RouteAddress};
use crate::session::permissions::SessionPermissions;
use crate::session::session::SessionId;

/// Lightweight `SessionActor` for testing RPC authorization
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

    /// Attempt to send an RPC request. Returns `Err` if authorization fails.
    ///
    /// Requires "call" permission (represented as Write access) on the RPC route.
    ///
    /// # Errors
    ///
    /// Returns an error when the session lacks permission to call the requested route.
    pub fn call_rpc(
        &self,
        request: RpcRequest,
        rpc_actor: &mut RpcRouteActor,
        ctx: &mut Context<RpcRouteActor>,
    ) -> Result<(), String> {
        // RPC call requires write access to the route
        if !self.permissions.allows(&request.route, Access::Write) {
            return Err("unauthorized: rpc call".to_string());
        }

        let msg = RpcMessage::Request(request);
        rpc_actor.receive(msg, ctx);
        Ok(())
    }

    /// Attempt to register a worker. Returns `Err` if authorization fails.
    ///
    /// Requires worker-registration permission (represented as All access) on the RPC route.
    ///
    /// # Errors
    ///
    /// Returns an error when the session lacks permission to register a worker for the route.
    pub fn register_worker(
        &self,
        worker_addr: RouteAddress,
        route: &Route,
        rpc_actor: &mut RpcRouteActor,
        ctx: &mut Context<RpcRouteActor>,
    ) -> Result<(), String> {
        // Worker registration requires All access (permission to handle calls)
        if !self.permissions.allows(route, Access::All) {
            return Err("unauthorized: worker register".to_string());
        }

        let msg = RpcMessage::RegisterWorker { worker_addr };
        rpc_actor.receive(msg, ctx);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Permission;
    use crate::domains::rpc::RpcRouteActor;
    use crate::runtime::actor::Context;
    use crate::runtime::router::Router;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use crate::session::permissions::SessionPermissions;
    use bytes::Bytes;
    use std::sync::Arc;
    use uuid::Uuid;

    fn make_ctx() -> Context<RpcRouteActor> {
        let router = Router::new();
        let addr = RouteAddress::new(
            RouteFamily::new(1),
            Route::new("rpc://realm/area/resource/operation"),
        );
        Context::new(addr, Arc::new(router))
    }

    #[test]
    fn should_reject_unauthorized_rpc_call() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();

        let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

        let request = RpcRequest::new(
            RouteFamily::new(1),
            Uuid::new_v4(),
            Route::new("rpc://realm/area/resource/create"),
            Route::new("inbox://session/1"),
            Bytes::from(vec![1, 2, 3]),
        );

        // Act
        let res = session.call_rpc(request, &mut actor, &mut ctx);

        // Assert
        assert!(res.is_err());
        assert_eq!(actor.pending_count(), 0);
    }

    #[test]
    fn should_allow_authorized_rpc_call() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();

        // Worker registered
        let worker_addr =
            RouteAddress::new(RouteFamily::new(1), Route::new("worker://realm/worker1"));
        actor.receive(
            RpcMessage::RegisterWorker {
                worker_addr: worker_addr.clone(),
            },
            &mut ctx,
        );

        // Session with write permission
        let perms = vec![Permission::parse("rpc://realm/area/**#write").unwrap()];
        let session_perms = SessionPermissions::from_permissions(perms);
        let session = SessionActor::new(SessionId(1), session_perms);

        let request = RpcRequest::new(
            RouteFamily::new(1),
            Uuid::new_v4(),
            Route::new("rpc://realm/area/resource/create"),
            Route::new("inbox://session/1"),
            Bytes::from(vec![1, 2, 3]),
        );

        // Act
        let res = session.call_rpc(request, &mut actor, &mut ctx);

        // Assert
        assert!(res.is_ok());
        assert_eq!(actor.pending_count(), 0); // Dispatched to worker
    }

    #[test]
    fn should_reject_worker_registration_without_admin_permission() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();

        // Session with only write (call) permission, not admin
        let perms = vec![Permission::parse("rpc://realm/area/**#write").unwrap()];
        let session_perms = SessionPermissions::from_permissions(perms);
        let session = SessionActor::new(SessionId(1), session_perms);

        let worker_addr =
            RouteAddress::new(RouteFamily::new(1), Route::new("worker://realm/worker1"));
        let route = Route::new("rpc://realm/area/resource/operation");

        // Act
        let res = session.register_worker(worker_addr, &route, &mut actor, &mut ctx);

        // Assert
        assert!(res.is_err());
        assert_eq!(actor.worker_count(), 0);
    }

    #[test]
    fn should_allow_worker_registration_with_all_permission() {
        // Arrange
        let mut actor = RpcRouteActor::new(RouteFamily::new(1));
        let mut ctx = make_ctx();

        // Session with all permission
        let perms = vec![Permission::parse("rpc://realm/area/**#*").unwrap()];
        let session_perms = SessionPermissions::from_permissions(perms);
        let session = SessionActor::new(SessionId(1), session_perms);

        let worker_addr =
            RouteAddress::new(RouteFamily::new(1), Route::new("worker://realm/worker1"));
        let route = Route::new("rpc://realm/area/resource/operation");

        // Act
        let res = session.register_worker(worker_addr, &route, &mut actor, &mut ctx);

        // Assert
        assert!(res.is_ok());
        assert_eq!(actor.worker_count(), 1);
    }
}
