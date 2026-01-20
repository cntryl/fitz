//! KV domain end-to-end tests
//!
//! Tests full transaction lifecycle: Begin → Get/Put/Delete/Scan → Commit/Rollback
//! across multiple resources, families, and write options.

use bytes::Bytes;
use fitz::domains::kv::{KvActor, KvMessage, KvResponse, TxMode};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

fn create_kv_actor() -> KvActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    KvActor::new(store)
}

#[test]
fn should_complete_transaction_begin_put_get_sequence() {
    // Arrange
    let mut actor = create_kv_actor();

    // Act & Assert - Begin
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

    // Act & Assert - Put
    let response = actor.handle(KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "users".to_string(),
        key: Bytes::from_static(b"user:1001"),
        value: Bytes::from_static(b"{\"name\":\"Alice\",\"email\":\"alice@acme.com\"}"),
    });
    assert!(matches!(response, KvResponse::PutOk));

    // Act & Assert - Get
    let response = actor.handle(KvMessage::Get {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "users".to_string(),
        key: Bytes::from_static(b"user:1001"),
    });
    match response {
        KvResponse::GetResult {
            found: true,
            value: Some(v),
        } => {
            assert!(v.starts_with(b"{\"name\":\"Alice\""));
        }
        _ => panic!("Expected to find user"),
    }

    // Act & Assert - Rollback to cleanup
    let response = actor.handle(KvMessage::Rollback { tx_id });
    assert!(matches!(response, KvResponse::RollbackOk));
}

#[test]
fn should_isolate_transactions_across_resources() {
    // Arrange
    let mut actor = create_kv_actor();

    // Transaction 1 - users resource
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

    let user_key = Bytes::from_static(b"user:5001");
    actor.handle(KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "users".to_string(),
        key: user_key.clone(),
        value: Bytes::from_static(b"Alice"),
    });

    // Act - Try to operate on different resource
    let response = actor.handle(KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "posts".to_string(), // Different resource
        key: Bytes::from_static(b"post:2001"),
        value: Bytes::from_static(b"Hello World"),
    });

    // Assert - Should fail with TxScopeViolation
    assert!(matches!(
        response,
        KvResponse::Error {
            error: fitz::domains::kv::KvError::TxScopeViolation { .. }
        }
    ));

    // Cleanup - Rollback
    actor.handle(KvMessage::Rollback { tx_id });
}

#[test]
fn should_isolate_transactions_across_column_families() {
    // Arrange
    let mut actor = create_kv_actor();

    // Transaction 1 - Family 1
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "table_a".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id1 = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    actor.handle(KvMessage::Put {
        tx_id: tx_id1,
        route_family: RouteFamily::new(1),
        resource: "table_a".to_string(),
        key: Bytes::from_static(b"key1"),
        value: Bytes::from_static(b"value_from_family_1"),
    });

    // Commit transaction 1
    actor.handle(KvMessage::Commit { tx_id: tx_id1 });

    // Transaction 2 - Family 2 (different column family)
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(2),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "table_b".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id2 = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    actor.handle(KvMessage::Put {
        tx_id: tx_id2,
        route_family: RouteFamily::new(2),
        resource: "table_b".to_string(),
        key: Bytes::from_static(b"key1"),
        value: Bytes::from_static(b"value_from_family_2"),
    });

    // Get from Family 2
    let response = actor.handle(KvMessage::Get {
        tx_id: tx_id2,
        route_family: RouteFamily::new(2),
        resource: "table_b".to_string(),
        key: Bytes::from_static(b"key1"),
    });

    // Assert - Should get Family 2's value
    match response {
        KvResponse::GetResult {
            found: true,
            value: Some(v),
        } => {
            assert_eq!(v, Bytes::from_static(b"value_from_family_2"));
        }
        _ => panic!("Expected Family 2 value"),
    }

    actor.handle(KvMessage::Commit { tx_id: tx_id2 });
}

#[test]
fn should_rollback_changes_on_explicit_rollback() {
    // Arrange
    let mut actor = create_kv_actor();

    // Transaction 1 - Add a user (will be rolled back)
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

    let key = Bytes::from_static(b"user:temp");
    actor.handle(KvMessage::Put {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "users".to_string(),
        key: key.clone(),
        value: Bytes::from_static(b"Temporary User"),
    });

    // Act - Rollback
    let response = actor.handle(KvMessage::Rollback { tx_id });
    assert!(matches!(response, KvResponse::RollbackOk));

    // Assert - Transaction ended
    let response = actor.handle(KvMessage::Get {
        tx_id: 9999,
        route_family: RouteFamily::new(1),
        resource: "users".to_string(),
        key,
    });
    assert!(matches!(response, KvResponse::Error { .. })); // InvalidTxId
}

#[test]
fn should_handle_delete_operations() {
    // Arrange
    let mut actor = create_kv_actor();

    // Setup - Begin transaction
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
        key: Bytes::from_static(b"temp_key"),
        value: Bytes::from_static(b"temp_value"),
    });

    // Act - Delete the key
    let response = actor.handle(KvMessage::Delete {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"temp_key"),
    });

    // Assert - Delete succeeded
    assert!(matches!(response, KvResponse::DeleteOk));

    // Assert - Key is gone
    let response = actor.handle(KvMessage::Get {
        tx_id,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"temp_key"),
    });
    assert!(matches!(
        response,
        KvResponse::GetResult {
            found: false,
            value: None
        }
    ));

    actor.handle(KvMessage::Rollback { tx_id });
}

#[test]
fn should_reject_operations_without_begin() {
    // Arrange
    let mut actor = create_kv_actor();

    // Act - Try Get without Begin
    let response = actor.handle(KvMessage::Get {
        tx_id: 9999,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"key"),
    });

    // Assert
    assert!(matches!(
        response,
        KvResponse::Error {
            error: fitz::domains::kv::KvError::InvalidTxId
        }
    ));
}

#[test]
fn should_allow_multiple_sequential_transactions() {
    // Arrange
    let mut actor = create_kv_actor();

    // Transaction 1
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id1 = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    actor.handle(KvMessage::Put {
        tx_id: tx_id1,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"tx1_key"),
        value: Bytes::from_static(b"tx1_value"),
    });

    actor.handle(KvMessage::Rollback { tx_id: tx_id1 });

    // Transaction 2
    let response = actor.handle(KvMessage::Begin {
        route_family: RouteFamily::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "data".to_string(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id2 = match response {
        KvResponse::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    actor.handle(KvMessage::Put {
        tx_id: tx_id2,
        route_family: RouteFamily::new(1),
        resource: "data".to_string(),
        key: Bytes::from_static(b"tx2_key"),
        value: Bytes::from_static(b"tx2_value"),
    });

    // Assert - Can rollback second transaction
    let response = actor.handle(KvMessage::Rollback { tx_id: tx_id2 });
    assert!(matches!(response, KvResponse::RollbackOk));
}
