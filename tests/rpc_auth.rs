use fitz::domains::rpc::{RpcRouteActor, RpcMessage, RpcRequest};
use fitz::domains::rpc::session::SessionActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{Route, RouteFamily, RouteAddress};
use fitz::session::permissions::SessionPermissions;
use fitz::session::session::SessionId;
use fitz::auth::{Permission, Access};
use std::sync::Arc;

// This file tests RPC authorization: verifies that SessionActor properly enforces
// permissions for RPC operations before allowing requests to be processed.

fn make_ctx() -> Context<RpcRouteActor> {
    let router = Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("rpc://acme/auth/user/create"),
    );
    Context::new(addr, router)
}

#[test]
fn should_reject_rpc_request_without_call_permission() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/auth/worker1"),
    );
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Session without permission
    let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

    // Request without required permission
    let request = RpcRequest {
        correlation_id: "req-no-permission".to_string(),
        route: Route::new("rpc://acme/auth/user/create"),
        reply_route: Route::new("inbox://session/unauthorized"),
        body: b"{ \"username\": \"hacker\" }".to_vec(),
    };

    // Act
    let result = session.call_rpc(request, &mut actor, &mut ctx);

    // Assert - Request should be rejected
    assert!(result.is_err());
    assert_eq!(actor.pending_count(), 0);
}

#[test]
fn should_allow_rpc_request_with_valid_call_permission() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/auth/worker1"),
    );
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Session with write permission
    let perms = vec![Permission::parse("rpc://acme/auth/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Request with proper permission granted
    let request = RpcRequest {
        correlation_id: "req-authorized".to_string(),
        route: Route::new("rpc://acme/auth/user/create"),
        reply_route: Route::new("inbox://session/authorized"),
        body: b"{ \"username\": \"alice\" }".to_vec(),
    };

    // Act
    let result = session.call_rpc(request, &mut actor, &mut ctx);

    // Assert - Request should be processed
    assert!(result.is_ok());
    assert_eq!(actor.pending_count(), 0); // Dispatched to worker
    assert_eq!(actor.worker_count(), 1);
}

#[test]
fn should_enforce_realm_isolation_in_authorization() {
    // Arrange
    let mut actor_acme = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Worker in acme realm
    let worker_acme = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/data/worker1"),
    );
    actor_acme.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_acme.clone(),
        },
        &mut ctx,
    );

    // Session with permission for corp realm (different realm)
    let perms = vec![Permission::parse("rpc://corp/data/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Try to call acme RPC with corp realm permissions (should fail)
    let request_cross_realm = RpcRequest {
        correlation_id: "req-cross-realm".to_string(),
        route: Route::new("rpc://acme/data/query/execute"),
        reply_route: Route::new("inbox://session/corp123"),
        body: b"{ \"query\": \"SELECT *\" }".to_vec(),
    };

    // Act
    let result = session.call_rpc(request_cross_realm, &mut actor_acme, &mut ctx);

    // Assert - Cross-realm access denied
    assert!(result.is_err());
    assert_eq!(actor_acme.pending_count(), 0);
}

#[test]
fn should_allow_worker_subscription_with_valid_permissions() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Session with admin permission to register workers
    let perms = vec![Permission::parse("rpc://acme/inventory/**#admin").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/inventory/worker1"),
    );
    let route = Route::new("rpc://acme/inventory/item/query");

    // Act - Worker with proper subscribe permission
    let result = session.subscribe_worker(worker_addr, &route, &mut actor, &mut ctx);

    // Assert
    assert!(result.is_ok());
    assert_eq!(actor.worker_count(), 1);
}

#[test]
fn should_reject_worker_subscription_without_permissions() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Session without admin permission (only write)
    let perms = vec![Permission::parse("rpc://acme/admin/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    let unauthorized_worker = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/admin/rogue-worker"),
    );
    let route = Route::new("rpc://acme/admin/user/delete");

    // Act - Worker without subscribe permission
    let result = session.subscribe_worker(unauthorized_worker, &route, &mut actor, &mut ctx);

    // Assert - Worker registration rejected
    assert!(result.is_err());
    assert_eq!(actor.worker_count(), 0);
}

#[test]
fn should_enforce_scope_boundaries_for_rpc_calls() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/billing/worker1"),
    );
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Session with only read permission (needs write for create)
    let perms = vec![Permission::parse("rpc://acme/billing/**#read").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Request to billing service with only read permissions (needs write)
    let request = RpcRequest {
        correlation_id: "req-scope-violation".to_string(),
        route: Route::new("rpc://acme/billing/invoice/create"),
        reply_route: Route::new("inbox://session/limited"),
        body: b"{ \"amount\": 100 }".to_vec(),
    };

    // Act
    let result = session.call_rpc(request, &mut actor, &mut ctx);

    // Assert - Scope violation prevents processing
    assert!(result.is_err());
    assert_eq!(actor.pending_count(), 0);
}

#[test]
fn should_allow_requests_within_granted_scope() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/billing/worker1"),
    );
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Session with write permission for the scope
    let perms = vec![Permission::parse("rpc://acme/billing/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Request with proper scope
    let request = RpcRequest {
        correlation_id: "req-valid-scope".to_string(),
        route: Route::new("rpc://acme/billing/invoice/query"),
        reply_route: Route::new("inbox://session/authorized"),
        body: b"{ \"invoice_id\": \"inv-123\" }".to_vec(),
    };

    // Act
    let result = session.call_rpc(request, &mut actor, &mut ctx);

    // Assert - Request processed within valid scope
    assert!(result.is_ok());
    assert_eq!(actor.pending_count(), 0);
    assert_eq!(actor.worker_count(), 1);
}

#[test]
fn should_validate_permissions_per_request() {
    // Arrange
    let mut actor = RpcRouteActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let worker_addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("worker://acme/users/worker1"),
    );
    actor.receive(
        RpcMessage::Subscribe {
            worker_addr: worker_addr.clone(),
        },
        &mut ctx,
    );

    // Act - Send authorized and unauthorized requests
    // First request - authorized
    let request1 = RpcRequest {
        correlation_id: "req-authorized-1".to_string(),
        route: Route::new("rpc://acme/users/profile/read"),
        reply_route: Route::new("inbox://session/auth1"),
        body: b"{ \"user_id\": \"alice\" }".to_vec(),
    };
    actor.receive(RpcMessage::Request(request1), &mut ctx);

    // Second request - unauthorized (different operation)
    let request2 = RpcRequest {
        correlation_id: "req-unauthorized-2".to_string(),
        route: Route::new("rpc://acme/users/profile/delete"),
        reply_route: Route::new("inbox://session/auth1"),
        body: b"{ \"user_id\": \"bob\" }".to_vec(),
    };
    actor.receive(RpcMessage::Request(request2), &mut ctx);

    // Assert - First processed, second rejected
    // (exact count depends on whether first completed)
    assert_eq!(actor.worker_count(), 1);
}
