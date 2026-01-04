use fitz::auth::Permission;
use fitz::domains::lease::session::SessionActor;
use fitz::domains::lease::LeaseActor;
use fitz::runtime::actor::Context;
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::session::permissions::SessionPermissions;
use fitz::session::session::SessionId;
use std::sync::Arc;

fn make_ctx() -> Context<LeaseActor> {
    let router = Router::new();
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("lease://realm/locks/session"),
    );
    Context::new(addr, Arc::new(router))
}

#[test]
fn should_reject_unauthenticated_lease_acquire() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
    assert_eq!(res.unwrap_err(), "unauthorized: acquire");
}

#[test]
fn should_reject_lease_acquire_with_read_only_permission() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let perms = vec![Permission::parse("lease://realm/locks/**#read").unwrap()];
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
    assert!(res.is_err());
    assert_eq!(res.unwrap_err(), "unauthorized: acquire");
}

#[test]
fn should_allow_lease_acquire_with_write_permission() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
fn should_reject_unauthorized_lease_renew() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
    assert_eq!(res.unwrap_err(), "unauthorized: renew");
}

#[test]
fn should_reject_unauthorized_lease_release() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
    assert_eq!(res.unwrap_err(), "unauthorized: release");
}

#[test]
fn should_allow_lease_query_with_read_permission() {
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
fn should_reject_unauthorized_lease_query() {
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
    assert_eq!(res.unwrap_err(), "unauthorized: query");
}

#[test]
fn should_enforce_realm_boundary_for_lease_operations() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    // Permission for prod realm only
    let perms = vec![Permission::parse("lease://prod/locks/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(1), session_perms);

    // Act - try to acquire in different realm
    let res = session.acquire(
        RouteFamily::new(1),
        Route::new("lease://dev/locks/db-migration"),
        "owner1".to_string(),
        30,
        &mut actor,
        &mut ctx,
    );

    // Assert
    assert!(res.is_err());
    assert_eq!(res.unwrap_err(), "unauthorized: acquire");
}
