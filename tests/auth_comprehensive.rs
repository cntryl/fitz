//! Comprehensive auth tests for token expiration, reauth flow, and edge cases
//!
//! Tests the complete auth → session → authorization pipeline with focus on:
//! - Token expiration enforcement in authorize()
//! - Token refresh via reauth()
//! - Edge cases and security boundaries

use fitz::auth::{Access, Claims, Permission};
use fitz::runtime::routing::Route;
use fitz::session::permissions::SessionPermissions;
use fitz::session::actor::SessionActor;
use fitz::session::session::SessionId;

#[test]
fn should_reject_expired_token_in_authorize() {
    // Arrange
    let p = Permission::parse("notice://prod/orders/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![p.clone()]);
    
    let claims = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p],
        exp: 1, // Expired in 1970
    };
    
    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act
    let authorized = actor.authorize(&Route::new("notice://prod/orders/create"), Access::Write);

    // Assert
    assert!(!authorized, "expired token should be rejected");
}

#[test]
fn should_allow_valid_token_in_authorize() {
    // Arrange
    let p = Permission::parse("notice://prod/orders/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![p.clone()]);
    
    let claims = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p],
        exp: 9999999999, // Far future
    };
    
    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act
    let authorized = actor.authorize(&Route::new("notice://prod/orders/create"), Access::Write);

    // Assert
    assert!(authorized, "valid token should be accepted");
}

#[test]
fn should_detect_expired_token_with_is_token_expired() {
    // Arrange
    let p = Permission::parse("notice://prod/orders/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![p.clone()]);
    
    let claims = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p],
        exp: 1,
    };
    
    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act
    let expired = actor.is_token_expired();

    // Assert
    assert!(expired);
}

#[test]
fn should_not_consider_unauthenticated_session_expired() {
    // Arrange
    let perms = SessionPermissions::empty();
    let actor = SessionActor::new(SessionId(1), perms);

    // Act
    let expired = actor.is_token_expired();

    // Assert
    assert!(!expired, "unauthenticated sessions are never expired");
}

#[test]
fn should_reauth_replace_claims_and_permissions() {
    // Arrange
    let p1 = Permission::parse("notice://prod/orders/**#read").unwrap();
    let perms1 = SessionPermissions::from_permissions(vec![p1.clone()]);
    
    let claims1 = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p1],
        exp: 9999999999,
    };
    
    let mut actor = SessionActor::new(SessionId(1), perms1.clone());
    actor.authenticate(claims1, perms1);

    // Verify initial state
    assert!(actor.authorize(&Route::new("notice://prod/orders/create"), Access::Read));
    assert!(!actor.authorize(&Route::new("notice://prod/orders/create"), Access::Write));

    // Act: Reauth with new permissions
    let p2 = Permission::parse("notice://prod/orders/**#write").unwrap();
    let perms2 = SessionPermissions::from_permissions(vec![p2.clone()]);
    
    let claims2 = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p2],
        exp: 9999999999,
    };
    
    actor.reauth(claims2, perms2);

    // Assert: New permissions active
    assert!(!actor.authorize(&Route::new("notice://prod/orders/create"), Access::Read));
    assert!(actor.authorize(&Route::new("notice://prod/orders/create"), Access::Write));
}

#[test]
fn should_reauth_update_expiration_time() {
    // Arrange
    let p = Permission::parse("notice://prod/orders/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![p.clone()]);
    
    let claims_old = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p.clone()],
        exp: 1000,
    };
    
    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims_old, perms.clone());

    assert_eq!(actor.token_expiration(), Some(1000));

    // Act: Reauth with new expiration
    let claims_new = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p],
        exp: 2000,
    };
    
    actor.reauth(claims_new, perms);

    // Assert
    assert_eq!(actor.token_expiration(), Some(2000));
}

#[test]
fn should_return_none_for_token_expiration_when_unauthenticated() {
    // Arrange
    let perms = SessionPermissions::empty();
    let actor = SessionActor::new(SessionId(1), perms);

    // Act
    let exp = actor.token_expiration();

    // Assert
    assert_eq!(exp, None);
}

#[test]
fn should_batch_authorize_reject_all_on_expired_token() {
    // Arrange
    let p = Permission::parse("notice://prod/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![p.clone()]);
    
    let claims = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p],
        exp: 1,
    };
    
    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act
    let checks = vec![
        (Route::new("notice://prod/orders/create"), Access::Write),
        (Route::new("notice://prod/events/publish"), Access::Write),
    ];
    let authorized = actor.authorize_all(&checks);

    // Assert
    assert!(!authorized, "batch authorize should fail if token expired");
}

#[test]
fn should_authenticate_transition_from_unauthenticated() {
    // Arrange
    let perms = SessionPermissions::empty();
    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    
    assert!(!actor.is_authenticated());

    // Act
    let p = Permission::parse("notice://prod/orders/**#write").unwrap();
    let perms_auth = SessionPermissions::from_permissions(vec![p.clone()]);
    
    let claims = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p],
        exp: 9999999999,
    };
    
    actor.authenticate(claims, perms_auth);

    // Assert
    assert!(actor.is_authenticated());
}

#[test]
fn should_wildcard_permissions_match_multiple_routes() {
    // Arrange
    let p = Permission::parse("notice://prod/**#write").unwrap();
    let perms = SessionPermissions::from_permissions(vec![p.clone()]);
    
    let claims = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p],
        exp: 9999999999,
    };
    
    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act & Assert
    assert!(actor.authorize(&Route::new("notice://prod/orders/create"), Access::Write));
    assert!(actor.authorize(&Route::new("notice://prod/events/publish"), Access::Write));
    assert!(actor.authorize(&Route::new("notice://prod/any/nested/route"), Access::Write));
}

#[test]
fn should_access_all_permission_match_read_and_write() {
    // Arrange
    let p = Permission::parse("notice://prod/orders/**").unwrap(); // No #access = All
    let perms = SessionPermissions::from_permissions(vec![p.clone()]);
    
    let claims = Claims {
        sub: "user:42".to_string(),
        tenant: "prod".to_string(),
        roles: vec![],
        permissions: vec![p],
        exp: 9999999999,
    };
    
    let mut actor = SessionActor::new(SessionId(1), perms.clone());
    actor.authenticate(claims, perms);

    // Act & Assert
    assert!(actor.authorize(&Route::new("notice://prod/orders/create"), Access::Read));
    assert!(actor.authorize(&Route::new("notice://prod/orders/create"), Access::Write));
    assert!(actor.authorize(&Route::new("notice://prod/orders/create"), Access::All));
}
