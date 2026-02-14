//! Schedule domain authorization tests
//!
//! Ensures that Schedule's authorization layer enforces realm-based access control
//! and prevents unauthorized realm access.

use fitz::auth::Permission;
use fitz::runtime::routing::Route;
use fitz::session::permissions::SessionPermissions;

// ============================================================================
// INVARIANT: Session authorization checked before Schedule domain execution
// ============================================================================

#[test]
fn should_reject_unauthorized_realm_for_schedule() {
    // Arrange - Session authorized for prod realm only
    let permissions = vec![Permission::parse("schedule://prod/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(permissions);

    // Act - Try to schedule in staging realm (unauthorized)
    let route = Route::new("schedule://staging/jobs/cleanup/create".to_string());

    // Assert - Session should not have write permission for staging realm
    assert!(
        !session_perms.allows(&route, fitz::auth::Access::Write),
        "Session should not have write permission for staging realm"
    );
}

#[test]
fn should_allow_authorized_realm_for_schedule() {
    // Arrange - Session authorized for prod realm
    let permissions = vec![Permission::parse("schedule://prod/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(permissions);

    // Act - Check permission for prod realm
    let route = Route::new("schedule://prod/jobs/cleanup/create".to_string());

    // Assert
    assert!(
        session_perms.allows(&route, fitz::auth::Access::Write),
        "Session should have write permission for prod realm"
    );
}

#[test]
fn should_enforce_realm_equality_strictly_for_schedule() {
    // Arrange - Session authorized for "prod" realm only
    let permissions = vec![Permission::parse("schedule://prod/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(permissions);

    // Act - Check realm variations that should NOT match
    let invalid_realms = vec![
        "PROD",    // Case-sensitive: different case
        "prod-2",  // Different realm (similar name)
        "xprod",   // Prefix doesn't match
        "staging", // Completely different
    ];

    for invalid_realm in invalid_realms {
        let route = Route::new(format!("schedule://{}/jobs/test/create", invalid_realm));

        // Assert - Should not match
        assert!(
            !session_perms.allows(&route, fitz::auth::Access::Write),
            "Realm '{}' should not match 'prod'",
            invalid_realm
        );
    }
}

#[test]
fn should_allow_read_permission_for_status_check() {
    // Arrange - Session has read-only permission
    let permissions = vec![Permission::parse("schedule://analytics/**#read").unwrap()];
    let session_perms = SessionPermissions::from_permissions(permissions);

    // Act - Check read permission
    let route = Route::new("schedule://analytics/jobs/report/list".to_string());

    // Assert
    assert!(
        session_perms.allows(&route, fitz::auth::Access::Read),
        "Session should have read permission"
    );
}

#[test]
fn should_enforce_realm_isolation_across_different_realms() {
    // Arrange
    let perms_acme = vec![Permission::parse("schedule://acme/**#write").unwrap()];
    let perms_evil = vec![Permission::parse("schedule://evil/**#write").unwrap()];

    let session_acme = SessionPermissions::from_permissions(perms_acme);
    let session_evil = SessionPermissions::from_permissions(perms_evil);

    // Act - Check that each session can only access its realm
    let acme_route = Route::new("schedule://acme/jobs/backup/create".to_string());
    let evil_route = Route::new("schedule://evil/jobs/cleanup/create".to_string());

    // Assert
    assert!(
        session_acme.allows(&acme_route, fitz::auth::Access::Write),
        "acme session should access acme realm"
    );
    assert!(
        !session_acme.allows(&evil_route, fitz::auth::Access::Write),
        "acme session should NOT access evil realm"
    );

    assert!(
        session_evil.allows(&evil_route, fitz::auth::Access::Write),
        "evil session should access evil realm"
    );
    assert!(
        !session_evil.allows(&acme_route, fitz::auth::Access::Write),
        "evil session should NOT access acme realm"
    );
}

#[test]
fn should_support_wildcard_patterns_for_schedule_realms() {
    // Arrange - Session with specific realm permission
    let permissions = vec![Permission::parse("schedule://prod/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(permissions);

    // Act - Check that matching works for that realm
    let prod_route = Route::new("schedule://prod/jobs/test/create".to_string());
    let staging_route = Route::new("schedule://staging/jobs/test/create".to_string());

    // Assert
    assert!(
        session_perms.allows(&prod_route, fitz::auth::Access::Write),
        "Should match prod realm with permission schedule://prod/**"
    );
    assert!(
        !session_perms.allows(&staging_route, fitz::auth::Access::Write),
        "Should not match staging realm with permission schedule://prod/**"
    );
}

#[test]
fn should_distinguish_between_read_write_permissions() {
    // Arrange
    let read_perms = vec![Permission::parse("schedule://monitoring/**#read").unwrap()];
    let session_read = SessionPermissions::from_permissions(read_perms);

    let route = Route::new("schedule://monitoring/jobs/alert/list".to_string());

    // Act
    let can_read = session_read.allows(&route, fitz::auth::Access::Read);
    let can_write = session_read.allows(&route, fitz::auth::Access::Write);

    // Assert
    assert!(can_read, "Should allow read access");
    assert!(
        !can_write,
        "Should NOT allow write access with read-only permission"
    );
}

#[test]
fn should_support_write_permission_level() {
    // Arrange
    let write_perms = vec![Permission::parse("schedule://admin/**#write").unwrap()];
    let session_write = SessionPermissions::from_permissions(write_perms);

    let route = Route::new("schedule://admin/jobs/critical/create".to_string());

    // Act
    let can_write = session_write.allows(&route, fitz::auth::Access::Write);
    let can_read = session_write.allows(&route, fitz::auth::Access::Read);

    // Assert
    assert!(can_write, "Should allow write with write permission");
    assert!(
        !can_read,
        "Should NOT allow read with write-only permission"
    );
}
