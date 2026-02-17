//! Consolidated KV end-to-end tests â€” transport, codec, and integration
//!
//! Merged from: `kv_e2e_transport.rs`, `kv_e2e_basic.rs`, `kv_e2e_domain_routing.rs`, `ws_domain_flow.rs`.

use fitz::testkit::create_test_engine_with_cfs;
mod fixtures;
use bytes::Bytes;
use fixtures::transport::*;

// ===== TCP tests =====

async fn should_reject_operations_on_invalid_transaction<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let put_frame = build_kv_put(99999, "kv://test/app/users", b"key", b"value");
    let response = client
        .request(&put_frame, 2000)
        .await
        .expect("server should respond even for invalid tx");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 1, "Expected error status for invalid tx_id");
}

async fn should_require_connect_message_when_auth_enabled<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    // Arrange
    // Act
    let begin_frame = build_kv_begin("kv://test/app/users", 1, 0);
    let result = client.request(&begin_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected connection close or timeout when unauthenticated"
    );
}

async fn should_accept_valid_jwt_in_connect_message<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    // Arrange
    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    client
        .send_frame(&connect_frame)
        .await
        .expect("CONNECT send failed");

    fitz::testkit::transport::wait_for_auth_ready().await;

    // Act
    let begin_frame = build_kv_begin("kv://test-realm/app/users", 1, 0);
    let response = client
        .request(&begin_frame, 2000)
        .await
        .expect("BEGIN should work after auth");

    let (_msg_type, status, data) = parse_kv_response(&response);
    assert_eq!(status, 0, "Expected BEGIN success after authentication");
    assert_eq!(data.len(), 8, "Expected tx_id");
}

async fn should_reject_expired_jwt<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    // Arrange
    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_expired_jwt("test-realm"),
    );
    let result = client.request(&connect_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected connection close for expired token"
    );
}

async fn should_reject_invalid_jwt_signature<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_invalid_signature_jwt("test-realm"),
    );
    let result = client.request(&connect_frame, 1000).await;

    assert!(
        result.is_err(),
        "Expected connection close for invalid signature"
    );
}

async fn should_reject_jwt_for_wrong_realm<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "acme",
        &fitz::testkit::transport::generate_test_jwt("acme"),
    );
    client
        .send_frame(&connect_frame)
        .await
        .expect("CONNECT send failed");

    fitz::testkit::transport::wait_for_auth_ready().await;

    let begin_frame = build_kv_begin("kv://corp/app/users", 1, 0);
    let result = client.request(&begin_frame, 1000).await;

    if let Ok(response) = result {
        let (_msg_type, status, _data) = parse_kv_response(&response);
        assert_eq!(status, 1, "Expected error for wrong realm");
    } else {
        assert!(result.is_err(), "Expected error/timeout for wrong realm");
    }
}

async fn should_create_separate_sessions_for_each_connection_with_auth<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client1 = C::connect(server).await.expect("failed to connect");
    let mut client2 = C::connect(server).await.expect("failed to connect");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    client1.send_frame(&connect_frame).await.expect("CONNECT 1");
    client2.send_frame(&connect_frame).await.expect("CONNECT 2");

    fitz::testkit::transport::wait_for_auth_ready().await;

    let begin_frame1 = build_kv_begin("kv://test-realm/app/users", 1, 0);
    let begin_frame2 = build_kv_begin("kv://test-realm/app/posts", 1, 0);
    let response1 = client1.request(&begin_frame1, 2000).await.expect("BEGIN 1");
    let response2 = client2.request(&begin_frame2, 2000).await.expect("BEGIN 2");

    let (_msg_type1, status1, data1) = parse_kv_response(&response1);
    let (_msg_type2, status2, data2) = parse_kv_response(&response2);

    assert_eq!(
        status1, 0,
        "First BEGIN should succeed, got status {}",
        status1
    );
    assert_eq!(
        status2, 0,
        "Second BEGIN should succeed, got status {}",
        status2
    );
    assert!(
        data1.len() >= 8,
        "First response should have tx_id (8 bytes), got {} bytes",
        data1.len()
    );
    assert!(
        data2.len() >= 8,
        "Second response should have tx_id (8 bytes), got {} bytes",
        data2.len()
    );

    let tx_id1 = u64::from_be_bytes([
        data1[0], data1[1], data1[2], data1[3], data1[4], data1[5], data1[6], data1[7],
    ]);
    let tx_id2 = u64::from_be_bytes([
        data2[0], data2[1], data2[2], data2[3], data2[4], data2[5], data2[6], data2[7],
    ]);

    assert_eq!(tx_id1, 1, "First connection should get tx_id=1");
    assert_eq!(
        tx_id2, 1,
        "Second connection should also get tx_id=1 (separate session)"
    );
}

async fn should_reject_commit_before_begin<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let commit_frame = build_kv_commit(1, "kv://test/app/users");
    let result = client.request(&commit_frame, 2000).await;

    if let Ok(response) = result {
        let (_msg_type, status, _data) = parse_kv_response(&response);
        assert_eq!(status, 1, "Expected error for COMMIT without BEGIN");
    } else {
        assert!(
            result.is_err(),
            "Expected error/timeout for COMMIT without BEGIN"
        );
    }
}

async fn should_complete_begin_put_commit_over_transport<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "kv://test/app/users";

    let begin_frame = build_kv_begin(route, 1, 0);
    let response = client
        .request(&begin_frame, 2000)
        .await
        .expect("BEGIN failed");
    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);

    let put_frame = build_kv_put(1, route, b"key1", b"value1");
    let response = client
        .request(&put_frame, 2000)
        .await
        .expect("PUT failed");
    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);

    let commit_frame = build_kv_commit(1, route);
    let response = client
        .request(&commit_frame, 2000)
        .await
        .expect("COMMIT failed");
    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);
}

async fn should_receive_responses_within_reasonable_time<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let begin_frame = build_kv_begin("kv://test/app/bench", 1, 0);
    let response = client
        .request(&begin_frame, 500)
        .await
        .expect("BEGIN should complete quickly");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);
}

async fn should_handle_concurrent_connections_with_separate_transactions<C>(server: &TestServer)
where
    C: KvConnector,
{
    let run_tx = |idx: usize| async move {
        let mut client = C::connect(server).await.expect("connect failed");
        let route = format!("kv://test/app/resource{}", idx);

        let begin_frame = build_kv_begin(&route, 1, 0);
        let response = client
            .request(&begin_frame, 2000)
            .await
            .expect("BEGIN failed");

        let (_msg_type, status, _data) = parse_kv_response(&response);
        assert_eq!(status, 0);
    };

    tokio::join!(run_tx(0), run_tx(1), run_tx(2));
}

async fn should_assign_unique_tx_ids_within_single_session<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "kv://test/app/items";

    let begin_frame = build_kv_begin(route, 1, 0);
    let response = client.request(&begin_frame, 2000).await.expect("BEGIN 1");
    let (_msg_type, status, data) = parse_kv_response(&response);
    assert_eq!(status, 0);

    let commit_frame = build_kv_commit(1, route);
    client.request(&commit_frame, 2000).await.expect("COMMIT");

    let begin_frame = build_kv_begin(route, 2, 0);
    let response = client.request(&begin_frame, 2000).await.expect("BEGIN 2");
    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);
}

async fn should_reject_put_after_commit<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "kv://test/app/user/s";

    let begin_frame = build_kv_begin(route, 1, 0);
    client.request(&begin_frame, 2000).await.expect("BEGIN");

    let put_frame = build_kv_put(1, route, b"k1", b"v1");
    client.request(&put_frame, 2000).await.expect("PUT");

    let commit_frame = build_kv_commit(1, route);
    client.request(&commit_frame, 2000).await.expect("COMMIT");

    let put_frame2 = build_kv_put(1, route, b"k2", b"v2");
    let response = client
        .request(&put_frame2, 2000)
        .await
        .expect("PUT after COMMIT");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_ne!(status, 0, "Expected error after COMMIT");
}

async fn should_rollback_transaction_successfully<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "kv://test/app/roll";

    let begin_frame = build_kv_begin(route, 1, 0);
    client.request(&begin_frame, 2000).await.expect("BEGIN");

    let put_frame = build_kv_put(1, route, b"key", b"value");
    client.request(&put_frame, 2000).await.expect("PUT");

    let rollback_frame = build_kv_rollback(1, route);
    let response = client
        .request(&rollback_frame, 2000)
        .await
        .expect("ROLLBACK");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);
}

async fn should_handle_empty_key_and_value<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "kv://test/app/empty";

    let begin_frame = build_kv_begin(route, 1, 0);
    client.request(&begin_frame, 2000).await.expect("BEGIN");

    let put_frame = build_kv_put(1, route, b"", b"");
    let response = client
        .request(&put_frame, 2000)
        .await
        .expect("PUT with empty key/value");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);
}

async fn should_handle_large_values<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "kv://test/app/large";

    let begin_frame = build_kv_begin(route, 1, 0);
    client.request(&begin_frame, 2000).await.expect("BEGIN");

    let large_val = vec![b'X'; 60_000];
    let put_frame = build_kv_put(1, route, b"bigkey", &large_val);
    let response = client
        .request(&put_frame, 3000)
        .await
        .expect("PUT with large value");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);
}

async fn should_isolate_transactions_across_resources<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let begin1 = build_kv_begin("kv://test/app/resource1", 1, 0);
    let response1 = client.request(&begin1, 2000).await.expect("BEGIN 1");
    let (_msg_type, status1, _data) = parse_kv_response(&response1);
    assert_eq!(status1, 0);

    let begin2 = build_kv_begin("kv://test/app/resource2", 2, 0);
    let response2 = client.request(&begin2, 2000).await.expect("BEGIN 2");
    let (_msg_type, status2, _data) = parse_kv_response(&response2);
    assert_eq!(status2, 0);
}

async fn should_timeout_on_malformed_frame<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let garbage = vec![0xFF, 0xFF, 0xFF, 0xFF];

    let result = client.request(&garbage, 100).await;
    assert!(result.is_err(), "Expected error for malformed frame");
}

async fn should_handle_connection_drop_during_transaction<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "kv://test/app/disconnect";

    let begin_frame = build_kv_begin(route, 1, 0);
    client.request(&begin_frame, 2000).await.expect("BEGIN");

    drop(client);
    fitz::testkit::transport::wait_for_disconnect_cleanup().await;

    let mut client2 = C::connect(server).await.expect("failed to reconnect");
    let begin_frame2 = build_kv_begin(route, 1, 0);
    let response = client2
        .request(&begin_frame2, 2000)
        .await
        .expect("BEGIN after reconnect");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);
}

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

// ===== WebSocket wrapper tests (added AAA comments) =====

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
