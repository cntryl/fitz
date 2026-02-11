//! Integration test: KV domain over TCP transport
//! This test connects to a real Fitz server running on 127.0.0.1:4091
//! and executes a complete KV transaction sequence

use cntryl::protocol::TransactionMode;
use cntryl::FitzClient;

/// Note: This test requires Fitz server running on 127.0.0.1:4091
/// Start with: cargo run --manifest-path ../Cargo.toml -F boot
#[test]
fn should_execute_kv_transaction_over_tcp() {
    // Arrange
    let client = FitzClient::connect_tcp("127.0.0.1", 4091, "test-realm", "secret-key")
        .expect("Failed to connect to Fitz server");

    let kv = client.kv();

    // Act - Begin transaction
    let tx = kv
        .begin("kv://test-realm/app/users", TransactionMode::ReadWrite)
        .expect("Failed to begin transaction");

    // Act - Put a value
    let key = b"username:alice";
    let value = b"Alice Johnson";
    tx.put(key, value)
        .expect("Failed to put value");

    // Act - Get the value back
    let retrieved = tx
        .get(key)
        .expect("Failed to get value")
        .expect("Value not found after put");

    // Assert
    assert_eq!(retrieved, value, "Retrieved value should match put value");

    // Act - Delete the key
    tx.delete(key)
        .expect("Failed to delete key");

    // Assert - Key should not exist after delete
    let after_delete = tx
        .get(key)
        .expect("Failed to get after delete");

    assert!(after_delete.is_none(), "Value should not exist after delete");

    // Act - Commit transaction
    tx.commit()
        .expect("Failed to commit transaction");

    // Act - Verify in new transaction that changes persisted
    let verify_tx = kv
        .begin("kv://test-realm/app/users", TransactionMode::ReadOnly)
        .expect("Failed to begin verify transaction");

    let verify = verify_tx
        .get(key)
        .expect("Failed to verify");

    assert!(verify.is_none(), "Value should not exist after commit");

    verify_tx
        .commit()
        .expect("Failed to commit verify transaction");

    // Cleanup
    client.close()
        .expect("Failed to close connection");
}

/// Test rollback behavior
#[test]
fn should_rollback_kv_transaction_over_tcp() {
    // Arrange
    let client = FitzClient::connect_tcp("127.0.0.1", 4091, "test-realm", "secret-key")
        .expect("Failed to connect");

    let kv = client.kv();

    // Put initial value
    let setup_tx = kv
        .begin("kv://test-realm/app/test", TransactionMode::ReadWrite)
        .expect("Failed to begin setup");

    let key = b"rollback_test";
    let original_value = b"original";
    setup_tx.put(key, original_value)
        .expect("Failed to put setup value");
    setup_tx.commit()
        .expect("Failed to commit setup");

    // Act - Begin transaction, modify value, then rollback
    let tx = kv
        .begin("kv://test-realm/app/test", TransactionMode::ReadWrite)
        .expect("Failed to begin");

    let new_value = b"modified";
    tx.put(key, new_value)
        .expect("Failed to put");

    // Verify we see the new value in the transaction
    let in_tx = tx
        .get(key)
        .expect("Failed to get in tx")
        .expect("Value not found in tx");
    assert_eq!(in_tx, new_value, "Should see updated value in same transaction");

    // Act - Rollback instead of commit
    tx.rollback()
        .expect("Failed to rollback");

    // Assert - New transaction should see original value
    let verify_tx = kv
        .begin("kv://test-realm/app/test", TransactionMode::ReadOnly)
        .expect("Failed to begin verify");

    let after_rollback = verify_tx
        .get(key)
        .expect("Failed to get after rollback")
        .expect("Original value should still exist");

    assert_eq!(after_rollback, original_value, "Rollback should restore original value");

    verify_tx.commit()
        .expect("Failed to commit verify");

    client.close()
        .expect("Failed to close");
}

/// Test multiple concurrent transactions (same client, sequential ops)
#[test]
fn should_isolate_multiple_kv_transactions_over_tcp() {
    // Arrange
    let client = FitzClient::connect_tcp("127.0.0.1", 4091, "test-realm", "secret-key")
        .expect("Failed to connect");

    let kv = client.kv();

    // Act - Begin first transaction
    let tx1 = kv
        .begin("kv://test-realm/app/data", TransactionMode::ReadWrite)
        .expect("Failed to begin tx1");

    tx1.put(b"key1", b"value1")
        .expect("Failed to put in tx1");

    // Act - Begin second transaction (should not see tx1's changes)
    let tx2 = kv
        .begin("kv://test-realm/app/data", TransactionMode::ReadOnly)
        .expect("Failed to begin tx2");

    let tx2_view = tx2
        .get(b"key1")
        .expect("Failed to get in tx2");

    assert!(tx2_view.is_none(), "tx2 should not see uncommitted changes from tx1");

    // Act - Commit tx1
    tx1.commit()
        .expect("Failed to commit tx1");

    // Act - Begin new transaction and verify changes visible
    let tx3 = kv
        .begin("kv://test-realm/app/data", TransactionMode::ReadOnly)
        .expect("Failed to begin tx3");

    let tx3_view = tx3
        .get(b"key1")
        .expect("Failed to get in tx3")
        .expect("Value should exist after commit");

    assert_eq!(tx3_view, b"value1", "New transaction should see committed value");

    tx3.commit()
        .expect("Failed to commit tx3");
    tx2.commit()
        .expect("Failed to commit tx2");

    client.close()
        .expect("Failed to close");
}
