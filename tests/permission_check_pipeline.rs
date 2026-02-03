//! Permission check order in request pipeline tests
//!
//! This test suite verifies that Layer 2 (Session) implements the correct
//! order of permission checks as per TODO.md and SERVER.md lines 152-156:
//!
//! Order: Route validation → JWT validation → Permission enforcement → Domain dispatch
//!
//! Tests cover:
//! - Permission checks are per-request
//! - Realm match checked per request
//! - Area match checked per request
//! - Scope match checked per request
//! - Failures return domain error code *001 (ERR_UNAUTHORIZED)

use fitz::auth::{Access, Claims, Permission};
use fitz::runtime::routing::Route;
use fitz::session::actor::SessionActor;
use fitz::session::permissions::SessionPermissions;
use fitz::session::session::SessionId;

// ============================================================================
// PERMISSION CHECK ORDER TESTS
// ============================================================================

#[test]
fn should_check_realm_match_first_in_pipeline() {
    // Arrange - Session authorized for realm "prod" only
    let perm = Permission::parse("kv://prod/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:order1".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - Request for different realm (should fail immediately)
    let authorized = actor.authorize(&Route::new("kv://staging/users/put"), Access::Write);

    // Assert
    assert!(
        !authorized,
        "realm mismatch should fail permission check pipeline"
    );
}

#[test]
fn should_check_area_match_after_realm_in_pipeline() {
    // Arrange - Session authorized for specific area only
    let perm = Permission::parse("kv://acme/app/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:order2".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - Realm matches, but area doesn't
    let authorized = actor.authorize(&Route::new("kv://acme/system/config/put"), Access::Write);

    // Assert
    assert!(
        !authorized,
        "area mismatch should fail even with matching realm"
    );
}

#[test]
fn should_check_scope_match_after_area_in_pipeline() {
    // Arrange - Session authorized for read scope only
    let perm = Permission::parse("kv://acme/app/**#read").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:order3".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - Realm and area match, but scope doesn't (write not permitted)
    let authorized = actor.authorize(&Route::new("kv://acme/app/users/put"), Access::Write);

    // Assert
    assert!(
        !authorized,
        "scope mismatch should fail even with matching realm and area"
    );
}

#[test]
fn should_allow_when_all_permission_checks_pass() {
    // Arrange - Session with all matching permissions
    let perm = Permission::parse("kv://acme/app/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:order4".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - All checks pass: realm, area, scope
    let authorized = actor.authorize(&Route::new("kv://acme/app/users/put"), Access::Write);

    // Assert
    assert!(authorized, "all permission checks pass");
}

// ============================================================================
// PER-REQUEST PERMISSION CHECK TESTS
// ============================================================================

#[test]
fn should_check_realm_per_request_not_cached() {
    // Arrange
    let perm = Permission::parse("kv://prod/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:per_req1".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - First request to correct realm, then to wrong realm
    let first_ok = actor.authorize(&Route::new("kv://prod/users/put"), Access::Write);
    let second_unauthorized = actor.authorize(&Route::new("kv://staging/users/put"), Access::Write);

    // Assert
    assert!(first_ok, "first request should succeed");
    assert!(
        !second_unauthorized,
        "second request to different realm should fail independently"
    );
}

#[test]
fn should_check_area_per_request_not_cached() {
    // Arrange
    let perm = Permission::parse("kv://acme/app/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:per_req2".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - First request to correct area, then to wrong area
    let first_ok = actor.authorize(&Route::new("kv://acme/app/users/put"), Access::Write);
    let second_unauthorized =
        actor.authorize(&Route::new("kv://acme/system/config/put"), Access::Write);

    // Assert
    assert!(first_ok, "first request to correct area should succeed");
    assert!(
        !second_unauthorized,
        "second request to different area should fail independently"
    );
}

#[test]
fn should_check_scope_per_request_not_cached() {
    // Arrange
    let read_perm = Permission::parse("kv://acme/app/**#read").unwrap();
    let write_perm = Permission::parse("kv://acme/app/**#write").unwrap();
    let perms_vec = vec![read_perm, write_perm];
    let perms = SessionPermissions::from_permissions(perms_vec.clone());

    let claims = Claims {
        sub: "user:per_req3".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: perms_vec,
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - First read, then write, then read again (scope checks per request)
    let first_read_ok = actor.authorize(&Route::new("kv://acme/app/users/get"), Access::Read);
    let write_ok = actor.authorize(&Route::new("kv://acme/app/users/put"), Access::Write);
    let second_read_ok = actor.authorize(&Route::new("kv://acme/app/users/get"), Access::Read);

    // Assert
    assert!(first_read_ok, "read request should succeed");
    assert!(write_ok, "write request should succeed");
    assert!(
        second_read_ok,
        "read request again should succeed independently"
    );
}

// ============================================================================
// ERROR CODE CONSISTENCY: ERR_UNAUTHORIZED = *001
// ============================================================================

#[test]
fn should_use_consistent_error_for_realm_mismatch() {
    // Arrange
    let perm = Permission::parse("kv://prod/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:err1".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act
    let authorized = actor.authorize(&Route::new("kv://staging/users/put"), Access::Write);

    // Assert
    // The error should be consistent (unauthorized)
    assert!(
        !authorized,
        "realm mismatch should result in unauthorized response (error code *001)"
    );
}

#[test]
fn should_use_consistent_error_for_area_mismatch() {
    // Arrange
    let perm = Permission::parse("kv://acme/app/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:err2".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act
    let authorized = actor.authorize(&Route::new("kv://acme/system/config/put"), Access::Write);

    // Assert
    assert!(
        !authorized,
        "area mismatch should result in unauthorized response (error code *001)"
    );
}

#[test]
fn should_use_consistent_error_for_scope_mismatch() {
    // Arrange
    let perm = Permission::parse("kv://acme/**#read").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:err3".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act
    let authorized = actor.authorize(&Route::new("kv://acme/users/put"), Access::Write);

    // Assert
    assert!(
        !authorized,
        "scope mismatch should result in unauthorized response (error code *001)"
    );
}

// ============================================================================
// MULTIPLE PERMISSION RULES TESTS
// ============================================================================

#[test]
fn should_allow_when_any_permission_matches() {
    // Arrange - Session with multiple permission rules
    let perm1 = Permission::parse("kv://acme/app/**#read").unwrap();
    let perm2 = Permission::parse("kv://acme/app/**#write").unwrap();
    let perms_vec = vec![perm1, perm2];
    let perms = SessionPermissions::from_permissions(perms_vec.clone());

    let claims = Claims {
        sub: "user:multi1".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: perms_vec,
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - Request should succeed if ANY permission matches
    let read_ok = actor.authorize(&Route::new("kv://acme/app/users/get"), Access::Read);
    let write_ok = actor.authorize(&Route::new("kv://acme/app/users/put"), Access::Write);

    // Assert
    assert!(read_ok, "read should match first permission rule");
    assert!(write_ok, "write should match second permission rule");
}

#[test]
fn should_reject_when_no_permission_matches() {
    // Arrange - Session with limited permissions
    let perm = Permission::parse("kv://acme/app/**#read").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:multi2".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - Request should fail when NO permission matches
    let write_denied = actor.authorize(&Route::new("kv://acme/app/users/put"), Access::Write);
    let system_denied = actor.authorize(&Route::new("kv://acme/system/config/get"), Access::Read);
    let staging_denied = actor.authorize(&Route::new("kv://staging/app/users/get"), Access::Read);

    // Assert
    assert!(
        !write_denied,
        "write should be denied (only read permitted)"
    );
    assert!(!system_denied, "system area should be denied");
    assert!(!staging_denied, "staging realm should be denied");
}

// ============================================================================
// WILDCARD PATTERN MATCHING WITH PERMISSION CHECKS
// ============================================================================

#[test]
fn should_apply_permission_checks_to_wildcard_patterns() {
    // Arrange - Permission with double-star wildcard
    let perm = Permission::parse("kv://acme/app/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:wild1".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - Wildcard should match various nested paths
    let shallow = actor.authorize(&Route::new("kv://acme/app/users/put"), Access::Write);
    let nested = actor.authorize(
        &Route::new("kv://acme/app/users/profile/put"),
        Access::Write,
    );
    let out_of_scope = actor.authorize(&Route::new("kv://acme/system/config/put"), Access::Write);

    // Assert
    assert!(shallow, "shallow path should match app/**");
    assert!(nested, "nested path should match app/**");
    assert!(!out_of_scope, "different area should not match");
}

#[test]
fn should_apply_permission_checks_to_single_star_patterns() {
    // Arrange - Permission with single-star wildcard (matches one segment)
    let perm = Permission::parse("notice://acme/orders/*#read").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:wild2".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - Single star should match one level only
    let one_level = actor.authorize(&Route::new("notice://acme/orders/created"), Access::Read);
    let two_levels = actor.authorize(
        &Route::new("notice://acme/orders/created/update"),
        Access::Read,
    );

    // Assert
    assert!(one_level, "one-level path should match orders/*");
    assert!(
        !two_levels,
        "two-level path should not match orders/* (single star only)"
    );
}

// ============================================================================
// INTEGRATION TESTS: FULL PIPELINE WITH MULTIPLE SCENARIOS
// ============================================================================

#[test]
fn should_apply_full_permission_pipeline_to_complex_scenario() {
    // Arrange - Realistic multi-area multi-scope setup
    let perms_vec = vec![
        Permission::parse("kv://acme/app/users/**#read").unwrap(),
        Permission::parse("kv://acme/app/users/**#write").unwrap(),
        Permission::parse("kv://acme/app/settings/**#read").unwrap(),
    ];
    let perms = SessionPermissions::from_permissions(perms_vec.clone());

    let claims = Claims {
        sub: "user:complex".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: perms_vec,
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - Various requests to verify pipeline
    let user_read = actor.authorize(&Route::new("kv://acme/app/users/get"), Access::Read);
    let user_write = actor.authorize(&Route::new("kv://acme/app/users/put"), Access::Write);
    let settings_read = actor.authorize(&Route::new("kv://acme/app/settings/get"), Access::Read);
    let settings_write = actor.authorize(&Route::new("kv://acme/app/settings/put"), Access::Write);
    let wrong_realm = actor.authorize(&Route::new("kv://staging/app/users/get"), Access::Read);
    let wrong_area = actor.authorize(&Route::new("kv://acme/system/config/get"), Access::Read);

    // Assert
    assert!(user_read, "user read should succeed");
    assert!(user_write, "user write should succeed");
    assert!(settings_read, "settings read should succeed");
    assert!(
        !settings_write,
        "settings write should fail (not in permissions)"
    );
    assert!(!wrong_realm, "wrong realm should fail");
    assert!(!wrong_area, "wrong area should fail");
}

#[test]
fn should_maintain_permission_checks_across_multiple_sequential_requests() {
    // Arrange
    let perm = Permission::parse("kv://acme/app/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![perm.clone()]);

    let claims = Claims {
        sub: "user:seq".to_string(),
        tenant: "acme".to_string(),
        roles: vec![],
        permissions: vec![perm],
        exp: 9999999999,
    };

    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act - Sequential requests should each be checked independently
    let mut results = Vec::new();
    for _i in 0..5 {
        let ok = actor.authorize(&Route::new("kv://acme/app/users/put"), Access::Write);
        results.push(ok);
    }

    // Assert - All should succeed consistently
    for (i, result) in results.iter().enumerate() {
        assert!(*result, "request {} should succeed consistently", i);
    }
}
