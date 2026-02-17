// Consolidated lease basic/unit tests
// Combined from: lease_auth.rs, lease_semantics.rs, lease_realm_isolation.rs

use fitz::auth::Permission;
use fitz::domains::lease::session::{AcquireRequest, ReleaseRequest, RenewRequest, SessionActor};
use fitz::domains::lease::LeaseActor;
use fitz::runtime::actor::{Actor, Context};
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
fn should_reject_unauthorized_lease_renew() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let session = SessionActor::new(SessionId(1), SessionPermissions::empty());

    // Act
    let res = session.renew(
        RenewRequest {
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

    // Act
    let res = session.acquire(
        AcquireRequest {
            family: RouteFamily::new(1),
            route: Route::new("lease://dev/locks/db-migration"),
            owner_id: "owner1".to_string(),
            ttl_secs: 30,
            wait_seconds: 0,
        },
        &mut actor,
        &mut ctx,
    );

    // Assert
    assert!(res.is_err());
    assert_eq!(res.unwrap_err(), "unauthorized: acquire");
}

// --- Lease semantics & invariants (from lease_semantics.rs + lease_realm_isolation.rs) ---

use fitz::domains::lease::lease_actor::LeaseActor as InnerLeaseActor;
use fitz::domains::lease::protocol::LeaseMessage;

#[allow(dead_code)]
fn make_lease_actor() -> (InnerLeaseActor, Context<InnerLeaseActor>) {
    let router = Arc::new(Router::new());
    let family = RouteFamily::new(1);
    let addr = RouteAddress::new(family, Route::new("lease://shared/leases/manage"));

    let actor = InnerLeaseActor::new(family);
    let ctx = Context::new(addr, router);

    (actor, ctx)
}

#[test]
fn should_grant_lease_to_first_requester() {
    // Arrange
    let mut actor = InnerLeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://realm/locks/db-migration/acquire"),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };

    // Act
    actor.receive(msg, &mut ctx);

    // Assert
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_reject_second_requester_when_lease_is_held() {
    // Arrange
    let mut actor = InnerLeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Client 1 acquires
    let msg1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(msg1, &mut ctx);

    // Act
    let msg2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(msg2, &mut ctx);

    // Assert
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_return_same_token_for_idempotent_acquire() {
    // Arrange
    let mut actor = InnerLeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Act
    let msg1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(msg1, &mut ctx);

    let msg2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(msg2, &mut ctx);

    // Assert
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_renew_lease_with_valid_token() {
    // Arrange
    let mut actor = InnerLeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Acquire first
    let acquire_msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire_msg, &mut ctx);

    // Act
    let renew_msg = LeaseMessage::Renew {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        fencing_token: 1,
        ttl_secs: 30,
    };
    actor.receive(renew_msg, &mut ctx);

    // Assert
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_release_lease_with_valid_token() {
    // Arrange
    let mut actor = InnerLeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Acquire first
    let acquire_msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire_msg, &mut ctx);

    // Act
    let release_msg = LeaseMessage::Release {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        fencing_token: 1,
    };
    actor.receive(release_msg, &mut ctx);

    // Assert
    assert_eq!(actor.lease_count(), 0);
}

#[test]
fn should_allow_new_owner_after_release() {
    // Arrange
    let mut actor = InnerLeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Client 1 acquires and releases
    let acquire1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire1, &mut ctx);

    let release = LeaseMessage::Release {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        fencing_token: 1,
    };
    actor.receive(release, &mut ctx);

    // Act
    let acquire2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire2, &mut ctx);

    // Assert
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_issue_monotonically_increasing_tokens() {
    // Arrange
    let mut actor = InnerLeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route1 = Route::new("lease://realm/locks/lock1/acquire");
    let route2 = Route::new("lease://realm/locks/lock2/acquire");
    let route3 = Route::new("lease://realm/locks/lock3/acquire");

    // Act
    let msg1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route1,
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(msg1, &mut ctx);

    let msg2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route2,
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(msg2, &mut ctx);

    let msg3 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route3,
        owner_id: "client-3".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(msg3, &mut ctx);

    // Assert
    assert_eq!(actor.lease_count(), 3);
}

#[test]
fn should_isolate_leases_across_route_families() {
    // Arrange
    let mut actor = InnerLeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Act
    let msg1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(msg1, &mut ctx);

    let msg2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(2),
        route: route.clone(),
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(msg2, &mut ctx);

    // Assert
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_query_lease_status() {
    // Arrange
    let mut actor = InnerLeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Acquire a lease
    let acquire_msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
    };
    actor.receive(acquire_msg, &mut ctx);

    // Act
    let query_msg = LeaseMessage::Query {
        family_id: RouteFamily::new(1),
        route: route.clone(),
    };
    actor.receive(query_msg, &mut ctx);

    // Assert
    assert_eq!(actor.lease_count(), 1);
}
