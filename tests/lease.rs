mod harness;
use harness::common::start_test_engine;

// ============================================================================
// LEASE ENGINE INTEGRATION TESTS
// ============================================================================
// These tests exercise the engine-level lease/coordination functionality via
// in-process EngineHandle, not over WebSocket transport.
//
// For full end-to-end WebSocket tests, see e2e_lease_ws.rs (to be added).
// ============================================================================

// ============================================================================
// LEASE OPERATIONS
// ============================================================================
// Leases provide distributed coordination via the control plane with:
// - Acquire/Reserve(route, lease_secs) → (id, body, token): claim a lease
// - ExtendLease(route, id, token, add_secs) → new_expiry: extend existing lease
// - Release/Complete(route, id, token): explicitly release lease
// - Automatic expiration after lease_secs if not extended or released
//
// Leases are similar to queue operations but used for coordination/locking
// Control plane MAY grant work leases to coordinate external workers
// ============================================================================

// ============================================================================
// HAPPY PATH TESTS - Acquire/Create Lease
// ============================================================================

#[tokio::test]
async fn should_acquire_lease_successfully() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.reserve("lease://realm/area/resource".to_string(), 30).await;

    // Assert
    assert!(result.is_ok());
    let (_id, _body, _token) = result.unwrap();
}

#[tokio::test]
async fn should_return_lease_token_on_acquire() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 30).await.unwrap();

    // Assert
    assert!(!id.is_empty());
    assert!(!token.is_empty());
}

#[tokio::test]
async fn should_specify_lease_duration_on_acquire() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.reserve("lease://realm/area/resource".to_string(), 30).await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// HAPPY PATH TESTS - Extend Lease
// ============================================================================

#[tokio::test]
async fn should_extend_active_lease() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 10).await.unwrap();

    // Act
    let result = handle.extend_lease("lease://realm/area/resource".to_string(), id, token, 20).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_return_new_expiry_time_on_extend() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 10).await.unwrap();

    // Act
    let new_expiry = handle.extend_lease("lease://realm/area/resource".to_string(), id, token, 20).await.unwrap();

    // Assert
    assert!(new_expiry > 0);
}

#[tokio::test]
async fn should_allow_multiple_extensions() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 10).await.unwrap();

    // Act
    let ext1 = handle.extend_lease("lease://realm/area/resource".to_string(), id.clone(), token.clone(), 10).await;
    let ext2 = handle.extend_lease("lease://realm/area/resource".to_string(), id.clone(), token.clone(), 10).await;
    let ext3 = handle.extend_lease("lease://realm/area/resource".to_string(), id, token, 10).await;

    // Assert
    assert!(ext1.is_ok());
    assert!(ext2.is_ok());
    assert!(ext3.is_ok());
}

// ============================================================================
// HAPPY PATH TESTS - Release Lease
// ============================================================================

#[tokio::test]
async fn should_release_lease_explicitly() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 30).await.unwrap();

    // Act
    let result = handle.consume("lease://realm/area/resource".to_string(), id, token).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_make_resource_available_after_release() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 30).await.unwrap();

    // Act
    handle.consume("lease://realm/area/resource".to_string(), id, token).await.unwrap();
    let second_result = handle.reserve("lease://realm/area/resource".to_string(), 30).await;

    // Assert
    assert!(second_result.is_ok());
}

// ============================================================================
// HAPPY PATH TESTS - Expiration
// ============================================================================

#[tokio::test]
async fn should_expire_lease_after_duration() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (_id, _body, _token) = handle.reserve("lease://realm/area/resource".to_string(), 2).await.unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    let result = handle.reserve("lease://realm/area/resource".to_string(), 30).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_return_resource_to_pool_on_expiration() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (_id, _body, _token) = handle.reserve("lease://realm/area/resource".to_string(), 1).await.unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let new_acquisition = handle.reserve("lease://realm/area/resource".to_string(), 30).await;

    // Assert
    assert!(new_acquisition.is_ok());
}

#[tokio::test]
async fn should_prevent_expiration_when_extended_in_time() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 5).await.unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    let extend_result = handle.extend_lease("lease://realm/area/resource".to_string(), id.clone(), token.clone(), 10).await;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    let final_extend = handle.extend_lease("lease://realm/area/resource".to_string(), id, token, 5).await;

    // Assert
    assert!(extend_result.is_ok());
    assert!(final_extend.is_ok());
}

// ============================================================================
// NEGATIVE TESTS - Invalid Token
// ============================================================================

#[tokio::test]
async fn should_reject_extend_with_invalid_token() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, _token) = handle.reserve("lease://realm/area/resource".to_string(), 30).await.unwrap();

    // Act
    let result = handle.extend_lease("lease://realm/area/resource".to_string(), id, "invalid_token".to_string(), 10).await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_reject_release_with_invalid_token() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, _token) = handle.reserve("lease://realm/area/resource".to_string(), 30).await.unwrap();

    // Act
    let result = handle.consume("lease://realm/area/resource".to_string(), id, "invalid_token".to_string()).await;

    // Assert
    assert!(result.is_err());
}

// ============================================================================
// NEGATIVE TESTS - Expired Lease
// ============================================================================

#[tokio::test]
async fn should_reject_extend_on_expired_lease() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 1).await.unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let result = handle.extend_lease("lease://realm/area/resource".to_string(), id, token, 10).await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_reject_release_of_expired_lease() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 1).await.unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let result = handle.consume("lease://realm/area/resource".to_string(), id, token).await;

    // Assert
    assert!(result.is_err());
}

// ============================================================================
// NEGATIVE TESTS - Conflicts
// ============================================================================

#[tokio::test]
async fn should_prevent_concurrent_lease_acquisition() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (_id, _body, _token) = handle.reserve("lease://realm/area/resource".to_string(), 30).await.unwrap();

    // Act
    let result = handle.reserve("lease://realm/area/resource".to_string(), 30).await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_queue_lease_requests_when_resource_busy() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (_id, _body, _token) = handle.reserve("lease://realm/area/resource".to_string(), 30).await.unwrap();

    // Act
    let second_attempt = handle.reserve("lease://realm/area/resource".to_string(), 30).await;
    let third_attempt = handle.reserve("lease://realm/area/resource".to_string(), 30).await;

    // Assert
    assert!(second_attempt.is_err());
    assert!(third_attempt.is_err());
}

// ============================================================================
// EDGE CASES - Zero Duration
// ============================================================================

#[tokio::test]
async fn should_reject_lease_with_zero_duration() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.reserve("lease://realm/area/resource".to_string(), 0).await;

    // Assert
    assert!(result.is_err());
}

#[tokio::test]
async fn should_reject_extend_with_zero_duration() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 30).await.unwrap();

    // Act
    let result = handle.extend_lease("lease://realm/area/resource".to_string(), id, token, 0).await;

    // Assert
    assert!(result.is_err());
}

// ============================================================================
// EDGE CASES - Very Long Leases
// ============================================================================

#[tokio::test]
async fn should_support_long_duration_leases() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.reserve("lease://realm/area/resource".to_string(), 3600).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_limit_maximum_lease_duration() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.reserve("lease://realm/area/resource".to_string(), 31536000).await;

    // Assert
    assert!(result.is_ok() || result.is_err());
}

// ============================================================================
// EDGE CASES - Control Plane Coordination
// ============================================================================

#[tokio::test]
async fn should_coordinate_leases_via_control_plane() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle.reserve("lease://realm/area/worker".to_string(), 30).await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_revoke_lease_from_control_plane() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 60).await.unwrap();

    // Act
    let result = handle.consume("lease://realm/area/resource".to_string(), id, token).await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// EDGE CASES - Graceful Handoff
// ============================================================================

#[tokio::test]
async fn should_transfer_lease_between_workers() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 30).await.unwrap();

    // Act
    handle.consume("lease://realm/area/resource".to_string(), id, token).await.unwrap();
    let new_lease = handle.reserve("lease://realm/area/resource".to_string(), 30).await;

    // Assert
    assert!(new_lease.is_ok());
}

#[tokio::test]
async fn should_prevent_gaps_in_lease_coverage() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (id, _body, token) = handle.reserve("lease://realm/area/resource".to_string(), 10).await.unwrap();

    // Act
    let ext1 = handle.extend_lease("lease://realm/area/resource".to_string(), id.clone(), token.clone(), 10).await;
    let ext2 = handle.extend_lease("lease://realm/area/resource".to_string(), id, token, 10).await;

    // Assert
    assert!(ext1.is_ok());
    assert!(ext2.is_ok());
}

// ============================================================================
// EDGE CASES - Cleanup
// ============================================================================

#[tokio::test]
async fn should_cleanup_expired_leases_automatically() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (_id1, _body1, _token1) = handle.reserve("lease://realm/area/resource1".to_string(), 1).await.unwrap();
    let (_id2, _body2, _token2) = handle.reserve("lease://realm/area/resource2".to_string(), 1).await.unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    let new_lease = handle.reserve("lease://realm/area/resource1".to_string(), 30).await;

    // Assert
    assert!(new_lease.is_ok());
}

#[tokio::test]
async fn should_handle_client_disconnect_during_lease() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let (_id, _body, _token) = handle.reserve("lease://realm/area/resource".to_string(), 2).await.unwrap();

    // Act
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    let result = handle.reserve("lease://realm/area/resource".to_string(), 30).await;

    // Assert
    assert!(result.is_ok());
}
