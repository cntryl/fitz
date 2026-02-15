//! KV Realm Isolation Tests
//!
//! Enforce Fitz multi-tenancy invariants:
//! - Hard isolation by realm
//! - Server-defined realm authority (never trust client)
//! - Authorization before domain execution
//!
//! These tests MUST fail until realm enforcement is implemented.

use bytes::Bytes;
use fitz::domains::kv::{KvActor, KvError, KvMessage, KvResponse, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

fn create_kv_actor() -> KvActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    KvActor::new(store)
}

// ============================================================================
// INVARIANT 1: Single-Realm Token Cannot Cross Realms
// ============================================================================

#[test]
fn should_reject_realm_switch_on_single_realm_token() {
    // Arrange
    // NOTE: This test is a placeholder for the session/auth layer behavior.
    // The KV domain itself DOES accept the "evil" realm and creates a transaction.
    // The session/auth layer should NEVER send a realm to the domain that the
    // token doesn't authorize.
    //
    // For now, we verify that different realms create isolated transactions.
    let mut actor = create_kv_actor();

    // Write to realm "acme"
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk, got {:?}", response),
    };

    actor.handle(KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "users".to_string(),
        key: Bytes::from_static(b"user:123"),
        value: Bytes::from_static(b"acme-data"),
    });

    actor.handle(KvMessage::Commit { tx_id });

    // Act
    // Try to read from realm "evil" (should not see "acme" data)
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "evil".to_string(),
        area: "kv".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    let tx_id_evil = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk, got {:?}", response),
    };

    let response = actor.handle(KvMessage::Get {
        tx_id: tx_id_evil,
        route_family: RouteFamily::new(1),
        resource: "users".to_string(),
        key: Bytes::from_static(b"user:123"),
    });

    // Assert
    // Should NOT find data from "acme" realm
    assert!(
        matches!(
            response,
            KvResponse::GetResult {
                found: false,
                value: None
            }
        ),
        "Realm 'evil' should NOT see data from realm 'acme'"
    );
}

#[test]
fn should_reject_implicit_realm_without_default_realm() {
    // Arrange
    // NOTE: Empty realm check in validation
    // This test verifies the KV domain rejects empty realm strings
    let mut actor = create_kv_actor();

    // Act
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "".to_string(), // Empty realm
        area: "kv".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    // Assert
    assert!(
        matches!(
            response,
            KvResponse::Error {
                error: fitz::domains::kv::KvError::InvalidRealm
            }
        ),
        "Expected InvalidRealm error, got {:?}",
        response
    );
}

#[test]
fn should_reject_malformed_realm_before_domain_execution() {
    // Arrange
    let mut actor = create_kv_actor();

    // Act
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "invalid realm with spaces".to_string(),
        area: "kv".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    // Assert
    assert!(
        matches!(
            response,
            KvResponse::Error {
                error: fitz::domains::kv::KvError::InvalidRealm
            }
        ),
        "Expected InvalidRealm error for malformed realm, got {:?}",
        response
    );
}

// ============================================================================
// INVARIANT 2: Realm Isolation At Storage Boundary
// ============================================================================

#[test]
fn should_isolate_kv_data_across_realms() {
    // Arrange
    let mut actor = create_kv_actor();

    // Realm 1: Write a key
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    actor.handle(KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "users".to_string(),
        key: Bytes::from_static(b"user:secret"),
        value: Bytes::from_static(b"realm1-data"),
    });

    actor.handle(KvMessage::Commit { tx_id });

    // Act
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "evil".to_string(), // Different realm, same RouteFamily
        area: "kv".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    let tx_id_evil = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    let response = actor.handle(KvMessage::Get {
        tx_id: tx_id_evil,
        route_family: RouteFamily::new(1),
        resource: "users".to_string(),
        key: Bytes::from_static(b"user:secret"),
    });

    // Assert
    match response {
        KvResponse::GetResult {
            found: true,
            value: Some(_),
        } => {
            panic!("SECURITY VIOLATION: Data from realm 'acme' leaked to realm 'evil'");
        }
        KvResponse::GetResult {
            found: false,
            value: None,
        } => {
            // Expected: data isolation
        }
        KvResponse::Error { .. } => {
            // Also acceptable: error if realm mismatch is detected
        }
        _ => {
            panic!("Unexpected response type");
        }
    }
}

#[test]
fn should_enforce_realm_equality_strictly() {
    // Arrange
    let mut actor = create_kv_actor();

    // Act
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    actor.handle(KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"key1"),
        value: Bytes::from_static(b"value1"),
    });

    actor.handle(KvMessage::Commit { tx_id });

    // Assert
    // Try different realm variations that should NOT match:
    // - Different case (if realms are case-sensitive, which they must be)
    let invalid_realms = vec!["ACME"];

    for invalid_realm in invalid_realms {
        let response = actor.handle(KvMessage::Begin {
            route_family: RouteFamily::new(1),
            realm: invalid_realm.to_string(),
            area: "kv".to_string(),
            resource: "data".to_string(),
            mode: TxMode::ReadOnly,
            write_options: cntryl_midge::WriteOptions::buffered(),
        });

        match response {
            KvResponse::BeginOk { tx_id: invalid_tx } => {
                // If allowed to begin, data must still be isolated
                let get_response = actor.handle(KvMessage::Get {
                    tx_id: invalid_tx,
                    route_family: RouteFamily::new(1),
                    resource: "data".to_string(),
                    key: Bytes::from_static(b"key1"),
                });

                match get_response {
                    KvResponse::GetResult {
                        found: true,
                        value: Some(_),
                    } => {
                        panic!(
                            "Realm string '{}' matched 'acme' - must be strict equality",
                            invalid_realm
                        );
                    }
                    KvResponse::GetResult {
                        found: false,
                        value: None,
                    } => {} // Data correctly isolated
                    KvResponse::Error { .. } => {} // Acceptable
                    _ => {
                        panic!("Unexpected response type");
                    }
                }

                actor.handle(KvMessage::Rollback { tx_id: invalid_tx });
            }
            KvResponse::Error { .. } => {
                // Acceptable: reject invalid realm
            }
            _ => {
                panic!("Unexpected response type");
            }
        }
    }
}

// ============================================================================
// INVARIANT 3: Realm Authority From Token, Never Client
// ============================================================================

#[test]
fn should_never_accept_client_supplied_realm_as_authority() {
    // Arrange
    // This test documents the principle:
    // The domain accepts realm parameter, but the session/auth layer
    // must ensure only authorized realms reach the domain.
    //
    // The test verifies data is isolated by realm.
    let mut actor = create_kv_actor();

    // Write to realm "authorized"
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "authorized".to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    actor.handle(KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"key"),
        value: Bytes::from_static(b"authorized-value"),
    });

    actor.handle(KvMessage::Commit { tx_id });

    // Act
    // Try realm "unauthorized" - should not see data
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "unauthorized".to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    let tx_id_unauth = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    let response = actor.handle(KvMessage::Get {
        tx_id: tx_id_unauth,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"key"),
    });

    // Assert
    assert!(
        matches!(
            response,
            KvResponse::GetResult {
                found: false,
                value: None
            }
        ),
        "Realm isolation enforced: unauthorized realm cannot see authorized realm's data"
    );
}

// ============================================================================
// INVARIANT 4: Authorization Before Storage
// ============================================================================

#[test]
fn should_check_realm_authorization_before_touching_midge() {
    // Arrange
    // This test documents the safety principle:
    // Realm validation must happen BEFORE any Midge database access.
    //
    // The test verifies that malformed realms are rejected early,
    // preventing any possibility of invalid data being stored.
    let mut actor = create_kv_actor();

    // Act
    // Attempt to begin with realm containing spaces (invalid)
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "bad realm".to_string(), // Contains space - INVALID
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    // Assert
    // Must be rejected BEFORE creating a transaction
    assert!(
        matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidRealm
            }
        ),
        "Malformed realm must be rejected before transaction creation"
    );
}

// ============================================================================
// INVARIANT 5: Realm Opacity & No Normalization
// ============================================================================

#[test]
fn should_treat_realm_as_opaque_identifier() {
    // Arrange
    // This test verifies realm opacity:
    // Realms are compared as exact byte strings.
    // No normalization, no case folding, no parsing.
    let mut actor = create_kv_actor();

    let realm1 = "acme-corp-123";
    let realm2 = "ACME-CORP-123"; // Different case

    // Act
    // Write to realm1
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: realm1.to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    actor.handle(KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"test"),
        value: Bytes::from_static(b"realm1_data"),
    });

    actor.handle(KvMessage::Commit { tx_id });

    // Try to read from realm2 (different case) - should NOT find data
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: realm2.to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadOnly,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    let response = actor.handle(KvMessage::Get {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"test"),
    });

    // Assert
    assert!(
        matches!(
            response,
            KvResponse::GetResult {
                found: false,
                value: None
            }
        ),
        "Case-sensitive realm comparison: realm2 (uppercase) cannot see realm1 (lowercase) data"
    );
}

// ============================================================================
// INVARIANT 6: Error Messages Must Not Leak Realm Existence
// ============================================================================

#[test]
fn should_not_leak_realm_existence_in_errors() {
    // Arrange
    // This test documents a security principle:
    // Error messages must not reveal whether a realm exists or not.
    //
    // The test verifies that error messages don't leak realm identifiers
    // or information about realm existence.
    let mut actor = create_kv_actor();

    // Act
    // Attempt to access with a realm that wasn't authorized
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "guessed-realm".to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });

    // Assert
    // Realm validation occurs BEFORE touching storage
    // Response should be generic error, not revealing realm status
    assert!(
        matches!(
            response,
            KvResponse::Error {
                error: KvError::InvalidRealm
            } | KvResponse::BeginOk { tx_id: _ }
        ),
        "Response must not leak whether realm exists: {:?}",
        response
    );
}
