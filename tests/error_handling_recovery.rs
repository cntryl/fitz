//! Error Handling & Recovery validation tests
//!
//! **STATUS: These are CLIENT-SIDE requirements, not server requirements**
//!
//! This file previously contained marker tests for client-side behavior:
//! - Connection retry with exponential backoff
//! - Timeout handling
//! - Frame validation
//!
//! These features belong in CLIENT SDKs, not in the Fitz server.
//! The server correctly returns errors; clients must handle reconnection.
//!
//! Server-side error handling IS tested in:
//! - tests/auth_comprehensive.rs - Auth error handling
//! - tests/standard_error_codes.rs - Domain error codes
//! - tests/*_auth.rs - Authorization failures
//!
//! This file now contains server-side error handling integration tests.

use bytes::Bytes;
use cntryl_midge::WriteOptions;
use fitz::domains::kv::{KvActor, KvError, KvMessage, KvResponse, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

// ============================================================================
// SERVER-SIDE ERROR HANDLING
// ============================================================================

#[test]
fn should_return_error_for_invalid_transaction_id() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);
    let route_family = RouteFamily::new(1);

    // Act - Use invalid transaction ID
    let response = actor.handle(KvMessage::Get {
        tx_id: 999,
        route_family,
        resource: "users".to_string(),
        key: Bytes::from_static(b"key1"),
    });

    // Assert
    match response {
        KvResponse::Error { error } => {
            assert_eq!(error, KvError::InvalidTxId);
        }
        _ => panic!("Expected error response"),
    }
}

#[test]
fn should_return_error_for_invalid_realm_format() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);
    let route_family = RouteFamily::new(1);

    // Act - Use invalid realm (contains invalid characters)
    let response = actor.handle(KvMessage::Begin {
        route_family,
        realm: "invalid realm!".to_string(), // Spaces and special chars not allowed
        area: "area".to_string(),
        resource: "resource".to_string(),
        mode: TxMode::ReadWrite,
        write_options: WriteOptions::buffered(),
    });

    // Assert
    match response {
        KvResponse::Error { error } => {
            assert_eq!(error, KvError::InvalidRealm);
        }
        _ => panic!("Expected error for invalid realm"),
    }
}

#[test]
fn should_handle_get_on_nonexistent_key_gracefully() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);
    let route_family = RouteFamily::new(1);

    // Begin transaction
    let begin_response = actor.handle(KvMessage::Begin {
        route_family,
        realm: "realm".to_string(),
        area: "area".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadOnly,
        write_options: WriteOptions::buffered(),
    });

    let tx_id = match begin_response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    // Act - GET on nonexistent key
    let response = actor.handle(KvMessage::Get {
        tx_id,
        route_family,
        resource: "users".to_string(),
        key: Bytes::from_static(b"nonexistent"),
    });

    // Assert - Should return found=false
    match response {
        KvResponse::GetResult { found, .. } => {
            assert!(!found, "Should not find nonexistent key");
        }
        _ => panic!("Expected GetResult for nonexistent key"),
    }
}

#[test]
fn should_handle_commit_on_invalid_transaction() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);

    // Act - Try to commit non-existent transaction
    let response = actor.handle(KvMessage::Commit { tx_id: 999 });

    // Assert
    match response {
        KvResponse::Error { error } => {
            assert_eq!(error, KvError::InvalidTxId);
        }
        _ => panic!("Expected error for invalid tx_id"),
    }
}

#[test]
fn should_handle_rollback_on_invalid_transaction() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);

    // Act - Try to rollback non-existent transaction
    let response = actor.handle(KvMessage::Rollback { tx_id: 999 });

    // Assert
    match response {
        KvResponse::Error { error } => {
            assert_eq!(error, KvError::InvalidTxId);
        }
        _ => panic!("Expected error for invalid tx_id"),
    }
}

#[test]
fn should_handle_operations_after_transaction_closed() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let mut actor = KvActor::new(store);
    let route_family = RouteFamily::new(1);

    // Begin and commit transaction
    let begin_response = actor.handle(KvMessage::Begin {
        route_family,
        realm: "realm".to_string(),
        area: "area".to_string(),
        resource: "users".to_string(),
        mode: TxMode::ReadWrite,
        write_options: WriteOptions::buffered(),
    });

    let tx_id = match begin_response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    actor.handle(KvMessage::Commit { tx_id });

    // Act - Try to use transaction after commit
    let response = actor.handle(KvMessage::Get {
        tx_id,
        route_family,
        resource: "users".to_string(),
        key: Bytes::from_static(b"key1"),
    });

    // Assert - Should return error (transaction no longer valid)
    match response {
        KvResponse::Error { error } => {
            assert_eq!(error, KvError::InvalidTxId);
        }
        _ => panic!("Expected error for closed transaction"),
    }
}
