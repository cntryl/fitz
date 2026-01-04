use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::actor::{Actor, Context};
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

// This file asserts the basic golden path for leases: acquire, renew, release.
// Keep tests simple â€“ no complex expiration or conflict scenarios here.

fn make_ctx() -> Context<LeaseActor> {
    let router = Router::new();
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("lease://realm/locks/test/acquire"),
    );
    Context::new(addr, Arc::new(router))
}

/// E2E basic test: acquire a lease successfully
#[test]
fn should_acquire_lease_successfully() {
    // Arrange
    let mut actor = LeaseActor::new();
    let mut ctx = make_ctx();

    let msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://realm/locks/db-migration/acquire"),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };

    // Act
    actor.receive(msg, &mut ctx);

    // Assert
    assert_eq!(actor.lease_count(), 1);
}

/// E2E basic test: renew an acquired lease
#[test]
fn should_renew_lease_successfully() {
    // Arrange
    let mut actor = LeaseActor::new();
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // First acquire the lease
    let acquire_msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire_msg, &mut ctx);

    // Act - Renew the lease
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

/// E2E basic test: release a lease
#[test]
fn should_release_lease_successfully() {
    // Arrange
    let mut actor = LeaseActor::new();
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // First acquire the lease
    let acquire_msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire_msg, &mut ctx);

    // Act - Release the lease
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
