use super::scenarios::*;
use crate::fixtures::transport::*;

#[tokio::test]
async fn should_complete_begin_put_commit_over_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_complete_begin_put_commit_over_transport::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_receive_responses_within_reasonable_time::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_transactions_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_concurrent_connections_with_separate_transactions::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_assign_unique_tx_ids_within_single_session_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_assign_unique_tx_ids_within_single_session::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_connection_drop_during_transaction_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_connection_drop_during_transaction::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_stale_transaction_id_after_client_reconnect_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_reject_stale_transaction_id_after_client_reconnect::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_operations_on_invalid_transaction_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_reject_operations_on_invalid_transaction::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_require_connect_message_when_auth_enabled_ws() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_require_connect_message_when_auth_enabled::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_put_and_get_same_key_in_transaction_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_put_and_get_same_key_in_transaction::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_execute_multiple_puts_in_transaction_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_execute_multiple_puts_in_transaction::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_execute_put_get_get_sequence_in_transaction_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_execute_put_get_get_sequence_in_transaction::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_verify_get_succeeds_after_put_commit_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_verify_get_succeeds_after_put_commit::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_large_batch_writes_in_transaction_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_large_batch_writes_in_transaction::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_deliver_kv_watch_notification_after_committed_put_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_deliver_kv_watch_notification_after_committed_put::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_unregister_websocket_inbox_route_on_disconnect() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    let baseline_routes = server.runtime.registered_route_count();

    let socket = server.connect_ws().await.expect("failed to connect ws");
    server
        .wait_for_session_count(1)
        .await
        .expect("session open");
    server
        .wait_for_route_count(baseline_routes + 1)
        .await
        .expect("route registration");

    drop(socket);

    server
        .wait_for_session_count(0)
        .await
        .expect("session cleanup");
    server
        .wait_for_route_count(baseline_routes)
        .await
        .expect("route cleanup");
}
