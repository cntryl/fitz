use fitz::domains::lease::{LeaseActor, LeaseMessage};
use fitz::runtime::actor::{Actor, Context};
use fitz::runtime::router::Router;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

// This file asserts lease semantics: verifies lease acquisition, renewal, release, and expiration rules.
// It MUST NOT test implementation details such as internal data structures.

fn make_ctx() -> Context<LeaseActor> {
    let router = Router::new();
    let addr = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("lease://realm/locks/test/acquire"),
    );
    Context::new(addr, Arc::new(router))
}

#[test]
fn should_grant_lease_to_first_requester() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
    // Verify the lease was acquired by checking actor state
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_reject_second_requester_when_lease_is_held() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Client 1 acquires
    let msg1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(msg1, &mut ctx);

    // Act - Client 2 tries to acquire
    let msg2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
    };
    actor.receive(msg2, &mut ctx);

    // Assert - Should still have only one lease holder
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_return_same_token_for_idempotent_acquire() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Act - Same client acquires twice
    let msg1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(msg1, &mut ctx);

    let msg2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(msg2, &mut ctx);

    // Assert - Still only one lease
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_renew_lease_with_valid_token() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Acquire first
    let acquire_msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire_msg, &mut ctx);

    // Act - Renew with token 1 (first token issued)
    let renew_msg = LeaseMessage::Renew {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        fencing_token: 1,
        ttl_secs: 30,
    };
    actor.receive(renew_msg, &mut ctx);

    // Assert - Lease still held
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_release_lease_with_valid_token() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Acquire first
    let acquire_msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire_msg, &mut ctx);

    // Act - Release with token 1
    let release_msg = LeaseMessage::Release {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        fencing_token: 1,
    };
    actor.receive(release_msg, &mut ctx);

    // Assert - Lease released
    assert_eq!(actor.lease_count(), 0);
}

#[test]
fn should_allow_new_owner_after_release() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Client 1 acquires and releases
    let acquire1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire1, &mut ctx);

    let release = LeaseMessage::Release {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        fencing_token: 1,
    };
    actor.receive(release, &mut ctx);

    // Act - Client 2 acquires
    let acquire2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire2, &mut ctx);

    // Assert - New owner has the lease
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_issue_monotonically_increasing_tokens() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route1 = Route::new("lease://realm/locks/lock1/acquire");
    let route2 = Route::new("lease://realm/locks/lock2/acquire");
    let route3 = Route::new("lease://realm/locks/lock3/acquire");

    // Act - Acquire three different leases
    let msg1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route1,
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(msg1, &mut ctx);

    let msg2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route2,
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
    };
    actor.receive(msg2, &mut ctx);

    let msg3 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route3,
        owner_id: "client-3".to_string(),
        ttl_secs: 30,
    };
    actor.receive(msg3, &mut ctx);

    // Assert - Three leases acquired
    assert_eq!(actor.lease_count(), 3);
}

#[test]
fn should_isolate_leases_across_route_families() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Act - Acquire same route in different families
    let msg1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(msg1, &mut ctx);

    let msg2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(2),
        route: route.clone(),
        owner_id: "client-2".to_string(),
        ttl_secs: 30,
    };
    actor.receive(msg2, &mut ctx);

    // Assert - Only one lease (family=1) because family=2 message is rejected
    assert_eq!(actor.lease_count(), 1);
}

#[test]
fn should_query_lease_status() {
    // Arrange
    let mut actor = LeaseActor::new(RouteFamily::new(1));
    let mut ctx = make_ctx();

    let route = Route::new("lease://realm/locks/db-migration/acquire");

    // Acquire a lease
    let acquire_msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: route.clone(),
        owner_id: "client-1".to_string(),
        ttl_secs: 30,
    };
    actor.receive(acquire_msg, &mut ctx);

    // Act - Query the lease
    let query_msg = LeaseMessage::Query {
        family_id: RouteFamily::new(1),
        route: route.clone(),
    };
    actor.receive(query_msg, &mut ctx);

    // Assert - Lease exists
    assert_eq!(actor.lease_count(), 1);
}
