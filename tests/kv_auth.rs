//! KV domain authorization tests
//!
//! Ensures that KV's SessionActor enforces realm-based authorization
//! and prevents unauthorized realm access.

use bytes::Bytes;
use fitz::auth::Permission;
use fitz::domains::kv::{KvActor, KvMessage, SessionActor, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::session::permissions::SessionPermissions;
use fitz::session::session::SessionId;
use fitz::testkit::create_test_engine_with_cfs;

fn create_kv_actor() -> KvActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    KvActor::new(store)
}

// ============================================================================
// INVARIANT: Session authorization checked before KV domain execution
// ============================================================================

#[test]
fn should_reject_unauthorized_realm_access() {
    // Arrange - Session authorized for realm1 only
    let permissions = vec![
        Permission::parse("kv://realm1/**#read").unwrap(),
        Permission::parse("kv://realm1/**#write").unwrap(),
    ];
    let session_perms = SessionPermissions::from_permissions(permissions);
    let session_actor = SessionActor::new(SessionId(1), session_perms);

    let mut kv_actor = create_kv_actor();

    // Act - Try to access realm2 (unauthorized)
    let msg = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "realm2".to_string(),
        area: "kv".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    let result = session_actor.begin(msg, &mut kv_actor);

    // Assert
    assert!(result.is_err(), "Should reject unauthorized realm");
    assert!(
        result.unwrap_err().contains("unauthorized"),
        "Error should mention unauthorized"
    );
}

#[test]
fn should_allow_authorized_realm_access() {
    // Arrange - Session authorized for mycompany realm
    let permissions = vec![
        Permission::parse("kv://mycompany/**#read").unwrap(),
        Permission::parse("kv://mycompany/**#write").unwrap(),
    ];
    let session_perms = SessionPermissions::from_permissions(permissions);
    let session_actor = SessionActor::new(SessionId(1), session_perms);

    let mut kv_actor = create_kv_actor();

    // Act - Access authorized realm
    let msg = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "mycompany".to_string(),
        area: "kv".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    let result = session_actor.begin(msg, &mut kv_actor);

    // Assert
    assert!(result.is_ok(), "Should allow authorized realm");
}

#[test]
fn should_enforce_realm_equality_strictly() {
    // Arrange - Session authorized for "acme" realm only
    let permissions = vec![Permission::parse("kv://acme/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(permissions);
    let session_actor = SessionActor::new(SessionId(1), session_perms);

    let mut kv_actor = create_kv_actor();

    // Act - Try realm variations that should NOT match
    let invalid_realms = vec![
        "ACME",   // Case-sensitive: different case
        "acme-2", // Different realm (similar name)
        "xacme",  // Prefix doesn't match
    ];

    for invalid_realm in invalid_realms {
        let msg = KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: invalid_realm.to_string(),
            area: "kv".to_string(),
            resource: "data".to_string(),
            mode: TxMode::ReadOnly,
            write_options: cntryl_midge::WriteOptions::buffered(),
        };

        let result = session_actor.begin(msg, &mut kv_actor);

        // Assert - Should be rejected
        assert!(
            result.is_err(),
            "Realm '{}' should not match 'acme'",
            invalid_realm
        );
    }
}

#[test]
fn should_accept_valid_message_type() {
    // Arrange
    let permissions = vec![Permission::parse("kv://tenant/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(permissions);
    let session_actor = SessionActor::new(SessionId(1), session_perms);

    let mut kv_actor = create_kv_actor();

    // Act - Valid Begin message
    let msg = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "tenant".to_string(),
        area: "kv".to_string(),
        resource: "table".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    let result = session_actor.begin(msg, &mut kv_actor);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_invalid_message_type() {
    // Arrange
    let permissions = vec![Permission::parse("kv://tenant/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(permissions);
    let session_actor = SessionActor::new(SessionId(1), session_perms);

    let mut kv_actor = create_kv_actor();

    // Act - Try to begin with non-Begin message
    // Get a tx_id first (we need one to create a Commit message)
    let begin_msg = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "test".to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };
    let begin_result = kv_actor.handle(begin_msg.clone());
    let tx_id = match begin_result {
        fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    let msg = KvMessage::Commit { tx_id }; // Not a Begin message

    let result = session_actor.begin(msg, &mut kv_actor);

    // Assert
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("invalid message type"));
}

#[test]
fn should_allow_subsequent_operations_after_begin() {
    // Arrange
    let permissions = vec![Permission::parse("kv://authed/**#write").unwrap()];
    let session_perms = SessionPermissions::from_permissions(permissions);
    let session_actor = SessionActor::new(SessionId(1), session_perms);

    let mut kv_actor = create_kv_actor();

    // Act - Begin with authorized realm
    let begin_msg = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "authed".to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    let result = session_actor.begin(begin_msg, &mut kv_actor);
    assert!(result.is_ok());

    // Extract tx_id from successful Begin
    let tx_id = match kv_actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "authed".to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    }) {
        fitz::domains::kv::KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    // Act - Subsequent Put operation (realm already validated)
    let put_msg = KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"key1"),
        value: Bytes::from_static(b"value1"),
    };

    let result = session_actor.operation(&mut kv_actor, put_msg);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_allow_read_permission_for_transactions() {
    // Arrange - Session has read-only permission
    let permissions = vec![Permission::parse("kv://analytics/**#read").unwrap()];
    let session_perms = SessionPermissions::from_permissions(permissions);
    let session_actor = SessionActor::new(SessionId(1), session_perms);

    let mut kv_actor = create_kv_actor();

    // Act - Begin read-only transaction with read permission
    let msg = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "analytics".to_string(),
        area: "kv".to_string(),
        resource: "reports".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    let result = session_actor.begin(msg, &mut kv_actor);

    // Assert - Should succeed (read permission grants access)
    assert!(result.is_ok());
}

#[test]
fn should_enforce_realm_isolation_across_sessions() {
    // Arrange
    let perms1 = vec![Permission::parse("kv://acme/**#write").unwrap()];
    let perms2 = vec![Permission::parse("kv://evil/**#write").unwrap()];

    let session1 = SessionActor::new(SessionId(1), SessionPermissions::from_permissions(perms1));
    let session2 = SessionActor::new(SessionId(2), SessionPermissions::from_permissions(perms2));

    let mut actor1 = create_kv_actor();
    let mut actor2 = create_kv_actor();

    // Act - Session1 writes to acme realm
    let msg1 = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "secrets".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    let result1 = session1.begin(msg1, &mut actor1);
    assert!(result1.is_ok(), "Session1 should access acme");

    // Act - Session2 tries to access acme realm (not authorized)
    let msg2 = KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "secrets".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    };

    let result2 = session2.begin(msg2, &mut actor2);

    // Assert - Session2 should be rejected
    assert!(
        result2.is_err(),
        "Session2 should not access realm outside its permissions"
    );
}
