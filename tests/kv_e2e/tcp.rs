use super::scenarios::*;
use crate::fixtures::transport::*;

// ===== TCP wrapper tests (added AAA comments) =====

#[tokio::test]
async fn should_complete_begin_put_commit_over_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_complete_begin_put_commit_over_transport::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_receive_responses_within_reasonable_time::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_transactions_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_concurrent_connections_with_separate_transactions::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_assign_unique_tx_ids_within_single_session_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_assign_unique_tx_ids_within_single_session::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_operations_on_invalid_transaction_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_reject_operations_on_invalid_transaction::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_preserve_transaction_scope_integrity_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_preserve_transaction_scope_integrity::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_require_connect_message_when_auth_enabled_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_require_connect_message_when_auth_enabled::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_accept_valid_jwt_in_connect_message_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_accept_valid_jwt_in_connect_message::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_expired_jwt_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_reject_expired_jwt::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_invalid_jwt_signature_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_reject_invalid_jwt_signature::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_jwt_for_wrong_realm_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_reject_jwt_for_wrong_realm::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_create_separate_sessions_for_each_connection_with_auth_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_create_separate_sessions_for_each_connection_with_auth::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_commit_before_begin_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_reject_commit_before_begin::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_put_after_commit_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_reject_put_after_commit::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_rollback_transaction_successfully_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_rollback_transaction_successfully::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_empty_key_and_value_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_empty_key_and_value::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_large_values_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_large_values::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_isolate_transactions_across_resources_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_isolate_transactions_across_resources::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_timeout_on_malformed_frame_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_timeout_on_malformed_frame::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_connection_drop_during_transaction_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_connection_drop_during_transaction::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_kv_operation_after_disconnect_given_old_transaction_id() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_reject_stale_transaction_id_after_client_reconnect::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_deliver_kv_watch_notification_after_committed_put_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_deliver_kv_watch_notification_after_committed_put::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_unregister_tcp_inbox_route_on_disconnect() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    let baseline_routes = server.runtime.registered_route_count();

    let client = server.connect().await.expect("failed to connect");
    server
        .wait_for_session_count(1)
        .await
        .expect("session open");
    server
        .wait_for_route_count(baseline_routes + 1)
        .await
        .expect("route registration");

    drop(client);

    server
        .wait_for_session_count(0)
        .await
        .expect("session cleanup");
    server
        .wait_for_route_count(baseline_routes)
        .await
        .expect("route cleanup");
}

#[tokio::test]
async fn should_put_and_get_same_key_in_transaction_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_put_and_get_same_key_in_transaction::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_execute_multiple_puts_in_transaction_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_execute_multiple_puts_in_transaction::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_execute_put_get_get_sequence_in_transaction_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_execute_put_get_get_sequence_in_transaction::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_verify_get_succeeds_after_put_commit_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_verify_get_succeeds_after_put_commit::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_large_batch_writes_in_transaction_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_large_batch_writes_in_transaction::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

// ===== WebSocket wrapper tests (added AAA comments) =====
