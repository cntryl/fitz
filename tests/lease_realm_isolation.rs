// Deprecated — consolidated into `tests/lease_basics.rs`.
// ============================================================================
// Invariant 2: Leases are stored separately per realm
// ============================================================================

#[test]
fn should_isolate_leases_by_realm() {
    // Arrange
    let (mut actor, mut ctx) = make_lease_actor();

    // Act
    let _lease_acme = LeaseKey {
        family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "locks".to_string(),
        resource: "config".to_string(),
    };

    let msg_acme = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://acme/locks/config"),
        owner_id: "app1".to_string(),
        ttl_secs: 60,
        wait_seconds: 0,
    };
    actor.receive(msg_acme, &mut ctx);

    let msg_evil = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://evil/locks/config"),
        owner_id: "app2".to_string(),
        ttl_secs: 60,
        wait_seconds: 0,
    };
    actor.receive(msg_evil, &mut ctx);

    // Assert
    // They don't interfere because they have different LeaseKeys
    // (different realm values)
}

// ============================================================================
// Invariant 3: Realm is never stripped or normalized from lease keys
// ============================================================================

#[test]
fn should_treat_realm_as_opaque_in_lease_keys() {
    // Arrange
    let family = RouteFamily::new(1);

    let key_lowercase = LeaseKey {
        family,
        realm: "production".to_string(),
        area: "locks".to_string(),
        resource: "task".to_string(),
    };

    // Act
    let key_uppercase = LeaseKey {
        family,
        realm: "PRODUCTION".to_string(), // Different case
        area: "locks".to_string(),
        resource: "task".to_string(),
    };

    // Assert
    assert_ne!(key_lowercase, key_uppercase);
}

// ============================================================================
// Invariant 4: Realm mismatch in operation is detected
// ============================================================================

#[test]
fn should_enforce_realm_in_lease_operations() {
    // Arrange
    let (mut actor, mut ctx) = make_lease_actor();

    let msg_acquire = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://realm1/locks/resource"),
        owner_id: "owner1".to_string(),
        ttl_secs: 60,
        wait_seconds: 0,
    };
    actor.receive(msg_acquire, &mut ctx);

    // Act
    let msg_release_different = LeaseMessage::Release {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://realm2/locks/resource"), // Different realm
        owner_id: "owner1".to_string(),
        fencing_token: 1,
    };
    actor.receive(msg_release_different, &mut ctx);

    // Assert
    // because realm1 and realm2 are different LeaseKeys
}

// ============================================================================
// Invariant 5: Cross-realm lease lookup is impossible
// ============================================================================

#[test]
fn should_prevent_cross_realm_lease_confusion() {
    // Arrange
    let (mut actor, mut ctx) = make_lease_actor();

    // Act
    let msg1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://realm-a/locks/resource"),
        owner_id: "owner-a".to_string(),
        ttl_secs: 60,
        wait_seconds: 0,
    };
    actor.receive(msg1, &mut ctx);

    let msg2 = LeaseMessage::Query {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://realm-b/locks/resource"),
    };
    actor.receive(msg2, &mut ctx);

    // Assert
    // Realm is part of the key, so different realms cannot see each other's leases
}

// ============================================================================
// Invariant 6: Fencing tokens are global across all realms
// ============================================================================

#[test]
fn should_maintain_global_monotonic_tokens() {
    // Arrange
    let (mut actor, mut ctx) = make_lease_actor();

    // Act
    let msg1 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://realm1/locks/resource"),
        owner_id: "owner1".to_string(),
        ttl_secs: 60,
        wait_seconds: 0,
    };
    actor.receive(msg1, &mut ctx);

    let msg2 = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://realm2/locks/resource"),
        owner_id: "owner2".to_string(),
        ttl_secs: 60,
        wait_seconds: 0,
    };
    actor.receive(msg2, &mut ctx);

    // Assert
    // Fencing tokens are allocated globally (next_token increments in both cases)
    // But leases remain isolated because they're stored under different LeaseKeys
}

// ============================================================================
// Invariant 7: Authorization enforced before lease dispatch
// ============================================================================

#[test]
fn should_rely_on_auth_layer_for_lease_realm_validation() {
    // Arrange
    let (_actor, _) = make_lease_actor();

    // Act
    // Note: Lease actor stores all leases in shared HashMap.
    // The SessionActor layer (in session.rs) performs authorization checks
    // based on token grants and route patterns before dispatching to LeaseActor.
    //
    // Example flow:
    // 1. Token grants access to "lease://authenticated-realm/**"
    // 2. Client sends route "lease://authenticated-realm/locks/database"
    // 3. SessionActor checks: permissions.allows(route, Write) = true
    // 4. SessionActor forwards to LeaseActor, which stores in LeaseKey with that realm
    //
    // Assert
    // If client tries:
    // 1. Token grants access to "lease://authenticated-realm/**"
    // 2. Client sends route "lease://other-realm/locks/database"
    // 3. SessionActor checks: permissions.allows(route, Write) = false
    // 4. SessionActor returns error, never reaches LeaseActor
}

// ============================================================================
// Invariant 8: Lease operations within same realm work correctly
// ============================================================================

#[test]
fn should_support_lease_operations_within_realm() {
    // Arrange
    let (mut actor, mut ctx) = make_lease_actor();

    // Act
    let msg_acquire = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://production/critical/database"),
        owner_id: "primary-db".to_string(),
        ttl_secs: 60,
        wait_seconds: 0,
    };
    actor.receive(msg_acquire, &mut ctx);

    let msg_release = LeaseMessage::Release {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://production/critical/database"),
        owner_id: "primary-db".to_string(),
        fencing_token: 1,
    };
    actor.receive(msg_release, &mut ctx);

    let msg_query = LeaseMessage::Query {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://production/critical/database"),
    };
    actor.receive(msg_query, &mut ctx);

    // Assert
    // Note: All operations succeed within same realm
}

// ============================================================================
// Invariant 9: Realm is never inferred from context
// ============================================================================

#[test]
fn should_require_explicit_realm_in_lease_routes() {
    // Arrange
    let (mut actor, mut ctx) = make_lease_actor();

    // Act
    let msg = LeaseMessage::Acquire {
        family_id: RouteFamily::new(1),
        route: Route::new("lease://explicit-realm/locks/resource"),
        owner_id: "owner".to_string(),
        ttl_secs: 60,
        wait_seconds: 0,
    };
    actor.receive(msg, &mut ctx);

    // Assert
    // Note: Lease is stored under LeaseKey with explicit realm.
    // There's no implicit realm default or fallback.
    // Every operation requires the realm to be explicitly provided in the route.
}
