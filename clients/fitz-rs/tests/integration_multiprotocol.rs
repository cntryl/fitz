//! Multiprotocol integration tests
//! 
//! This module provides parameterized tests that run identical test scenarios
//! against both TCP and WebSocket transports to ensure transport-agnostic behavior.
//!
//! Tests can be configured to run against specific transports or both.

use cntryl::protocol::TransactionMode;
use cntryl::{FitzClient, FitzError};

/// Enum to parameterize tests by transport
#[derive(Debug, Clone, Copy)]
enum Transport {
    Tcp,
    WebSocket,
}

impl Transport {
    fn name(&self) -> &'static str {
        match self {
            Transport::Tcp => "TCP",
            Transport::WebSocket => "WebSocket",
        }
    }
}

/// Helper to connect to the appropriate transport
fn connect_client(transport: Transport, realm: &str, secret: &str) -> Result<FitzClient, FitzError> {
    match transport {
        Transport::Tcp => FitzClient::connect_tcp("127.0.0.1", 4091, realm, secret),
        Transport::WebSocket => FitzClient::connect_ws("ws://127.0.0.1:4090/ws", realm, secret),
    }
}

/// Parameterized test: Basic KV put/get/delete over transport
fn run_kv_crud_operations(transport: Transport) {
    println!("Running KV CRUD test over {}", transport.name());

    let client = connect_client(transport, "test-realm", "secret-key")
        .expect("Failed to connect");

    let kv = client.kv();

    // Begin transaction
    let tx = kv
        .begin("kv://test-realm/app/data", TransactionMode::ReadWrite)
        .expect("Failed to begin");

    let test_key = b"crud_test_key";
    let test_value = b"crud_test_value";

    // PUT
    tx.put(test_key, test_value)
        .expect("Failed to put");

    // GET
    let retrieved = tx
        .get(test_key)
        .expect("Failed to get")
        .expect("Key not found after put");

    assert_eq!(retrieved, test_value, "Retrieved value should match put value");

    // DELETE
    tx.delete(test_key)
        .expect("Failed to delete");

    let after_delete = tx
        .get(test_key)
        .expect("Failed to get after delete");

    assert!(after_delete.is_none(), "Key should not exist after delete");

    tx.commit()
        .expect("Failed to commit");

    client.close()
        .expect("Failed to close");

    println!("✓ KV CRUD test passed over {}", transport.name());
}

/// Parameterized test: Transaction isolation
fn run_transaction_isolation(transport: Transport) {
    println!("Running transaction isolation test over {}", transport.name());

    let client = connect_client(transport, "test-realm", "secret-key")
        .expect("Failed to connect");

    let kv = client.kv();

    // Setup initial value
    let setup = kv
        .begin("kv://test-realm/app/isolation_test", TransactionMode::ReadWrite)
        .expect("Failed to begin setup");

    let key = b"isolation_key";
    let initial_value = b"initial";
    setup.put(key, initial_value)
        .expect("Failed to put");
    setup.commit()
        .expect("Failed to commit setup");

    // Begin read-write transaction
    let tx_rw = kv
        .begin("kv://test-realm/app/isolation_test", TransactionMode::ReadWrite)
        .expect("Failed to begin rw");

    let new_value = b"modified";
    tx_rw.put(key, new_value)
        .expect("Failed to modify");

    // Begin read-only transaction in parallel (conceptually)
    let tx_ro = kv
        .begin("kv://test-realm/app/isolation_test", TransactionMode::ReadOnly)
        .expect("Failed to begin ro");

    // Read-only transaction should NOT see uncommitted changes
    let ro_view = tx_ro
        .get(key)
        .expect("Failed to read in ro tx")
        .expect("Key should exist");

    assert_eq!(ro_view, initial_value, "Read-only tx should not see uncommitted changes");

    // Commit the read-write transaction
    tx_rw.commit()
        .expect("Failed to commit rw");

    // Read-only transaction can now be used for verification (though it has the old snapshot)
    let old_value = tx_ro
        .get(key)
        .expect("Failed to read again in ro tx")
        .expect("Key should still exist");

    assert_eq!(old_value, initial_value, "Ro transaction snapshot should be consistent");

    tx_ro.commit()
        .expect("Failed to commit ro");

    client.close()
        .expect("Failed to close");

    println!("✓ Transaction isolation test passed over {}", transport.name());
}

/// Parameterized test: Rollback behavior
fn run_rollback_behavior(transport: Transport) {
    println!("Running rollback test over {}", transport.name());

    let client = connect_client(transport, "test-realm", "secret-key")
        .expect("Failed to connect");

    let kv = client.kv();

    // Setup initial value
    let setup = kv
        .begin("kv://test-realm/app/rollback_test", TransactionMode::ReadWrite)
        .expect("Failed to begin setup");

    let key = b"rollback_key";
    let initial = b"initial";
    setup.put(key, initial)
        .expect("Failed to put");
    setup.commit()
        .expect("Failed to commit setup");

    // Begin transaction, make changes, rollback
    let tx = kv
        .begin("kv://test-realm/app/rollback_test", TransactionMode::ReadWrite)
        .expect("Failed to begin");

    let changed = b"changed";
    tx.put(key, changed)
        .expect("Failed to modify");

    // Verify we see the change in the transaction
    let in_tx = tx
        .get(key)
        .expect("Failed to read in tx")
        .expect("Key should exist");

    assert_eq!(in_tx, changed, "Should see changes within same transaction");

    // Rollback
    tx.rollback()
        .expect("Failed to rollback");

    // Verify rollback in new transaction
    let verify = kv
        .begin("kv://test-realm/app/rollback_test", TransactionMode::ReadOnly)
        .expect("Failed to begin verify");

    let after_rollback = verify
        .get(key)
        .expect("Failed to verify")
        .expect("Key should still exist");

    assert_eq!(after_rollback, initial, "Rollback should restore original value");

    verify.commit()
        .expect("Failed to commit verify");

    client.close()
        .expect("Failed to close");

    println!("✓ Rollback test passed over {}", transport.name());
}

/// Parameterized test: Large value handling
fn run_large_values(transport: Transport) {
    println!("Running large value test over {}", transport.name());

    let client = connect_client(transport, "test-realm", "secret-key")
        .expect("Failed to connect");

    let kv = client.kv();

    let tx = kv
        .begin("kv://test-realm/app/large_data", TransactionMode::ReadWrite)
        .expect("Failed to begin");

    // Create a large value (1 MB)
    let large_value = vec![0xAB; 1024 * 1024];
    let key = b"large_key";

    tx.put(key, &large_value)
        .expect("Failed to put large value");

    let retrieved = tx
        .get(key)
        .expect("Failed to get large value")
        .expect("Large value not found");

    assert_eq!(retrieved.len(), large_value.len(), "Retrieved size should match");
    assert_eq!(retrieved, large_value, "Retrieved value should match");

    tx.commit()
        .expect("Failed to commit");

    client.close()
        .expect("Failed to close");

    println!("✓ Large value test passed over {}", transport.name());
}

// Test runners for each transport
#[test]
fn should_execute_kv_crud_over_tcp() {
    run_kv_crud_operations(Transport::Tcp);
}

#[test]
fn should_execute_kv_crud_over_websocket() {
    run_kv_crud_operations(Transport::WebSocket);
}

#[test]
fn should_isolate_transactions_over_tcp() {
    run_transaction_isolation(Transport::Tcp);
}

#[test]
fn should_isolate_transactions_over_websocket() {
    run_transaction_isolation(Transport::WebSocket);
}

#[test]
fn should_rollback_over_tcp() {
    run_rollback_behavior(Transport::Tcp);
}

#[test]
fn should_rollback_over_websocket() {
    run_rollback_behavior(Transport::WebSocket);
}

#[test]
fn should_handle_large_values_over_tcp() {
    run_large_values(Transport::Tcp);
}

#[test]
fn should_handle_large_values_over_websocket() {
    run_large_values(Transport::WebSocket);
}
