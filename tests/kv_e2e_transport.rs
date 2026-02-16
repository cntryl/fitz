//! KV domain transport-layer end-to-end tests
//!
//! These tests verify the COMPLETE request-response cycle:
//! Client → TCP/WebSocket → Session → Routing → KV Actor → Response → Client

mod fixtures;
use fixtures::transport::*;

// helper functions moved to tests::fixtures::transport

// ===== TCP tests =====

#[tokio::test]
async fn should_complete_begin_put_commit_over_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_complete_begin_put_commit_over_transport::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_transactions_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_concurrent_connections_with_separate_transactions::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_assign_unique_tx_ids_within_single_session_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_assign_unique_tx_ids_within_single_session::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_operations_on_invalid_transaction_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_operations_on_invalid_transaction::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_require_connect_message_when_auth_enabled_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_require_connect_message_when_auth_enabled::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_accept_valid_jwt_in_connect_message_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_accept_valid_jwt_in_connect_message::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_expired_jwt_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_expired_jwt::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_invalid_jwt_signature_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_invalid_jwt_signature::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_jwt_for_wrong_realm_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_jwt_for_wrong_realm::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_create_separate_sessions_for_each_connection_with_auth_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_create_separate_sessions_for_each_connection_with_auth::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_commit_before_begin_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_commit_before_begin::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_put_after_commit_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_put_after_commit::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_rollback_transaction_successfully_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_rollback_transaction_successfully::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_empty_key_and_value_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_empty_key_and_value::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_large_values_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_large_values::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_transactions_across_resources_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_isolate_transactions_across_resources::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_timeout_on_malformed_frame_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_timeout_on_malformed_frame::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_connection_drop_during_transaction_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_connection_drop_during_transaction::<TcpConnector>(&server).await;
}

// ===== WebSocket tests =====

#[tokio::test]
async fn should_complete_begin_put_commit_over_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_complete_begin_put_commit_over_transport::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_transactions_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_concurrent_connections_with_separate_transactions::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_assign_unique_tx_ids_within_single_session_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_assign_unique_tx_ids_within_single_session::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_operations_on_invalid_transaction_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_operations_on_invalid_transaction::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_require_connect_message_when_auth_enabled_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_require_connect_message_when_auth_enabled::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_accept_valid_jwt_in_connect_message_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_accept_valid_jwt_in_connect_message::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_expired_jwt_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_expired_jwt::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_invalid_jwt_signature_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_invalid_jwt_signature::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_jwt_for_wrong_realm_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_jwt_for_wrong_realm::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_create_separate_sessions_for_each_connection_with_auth_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_create_separate_sessions_for_each_connection_with_auth::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_commit_before_begin_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_commit_before_begin::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_put_after_commit_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_put_after_commit::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_rollback_transaction_successfully_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_rollback_transaction_successfully::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_empty_key_and_value_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_empty_key_and_value::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_large_values_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_large_values::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_transactions_across_resources_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_isolate_transactions_across_resources::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_timeout_on_malformed_frame_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_timeout_on_malformed_frame::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_connection_drop_during_transaction_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_connection_drop_during_transaction::<WsConnector>(&server).await;
}
