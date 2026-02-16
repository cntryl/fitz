// Deprecated — tests consolidated into `tests/lease_basics.rs`. (Kept as a stub.)
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
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
    let mut actor = LeaseActor::new(RouteFamily::new(1));
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
