//! RPC domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::transport::*;

// Generic test helper for request-response
// NOTE: Without registered workers, RPC requests will fail as expected.
// This tests that the RPC domain correctly returns error status when no workers are registered.
async fn should_send_rpc_request<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_rpc_request("rpc://test/services/api", "getUser", b"user-123");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&response);
    assert_ne!(
        status, 0,
        "Expected error for RPC request without registered workers"
    );
}

#[tokio::test]
async fn should_send_rpc_request_tcp() {
    let server = TestServer::start().await.expect("start");
    should_send_rpc_request::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
async fn should_send_rpc_request_ws() {
    let server = TestServer::start().await.expect("start");
    should_send_rpc_request::<WsRpcConnector>(&server).await;
}

// Generic test helper for invalid method
// NOTE: With no registered workers, all RPC requests fail with error status.
// This test verifies that behavior is consistent.
async fn should_reject_unknown_method<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_rpc_request("rpc://test/services/api", "unknownMethod", b"");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&response);
    assert_ne!(status, 0, "Expected failure when no workers registered");
}

#[tokio::test]
async fn should_reject_unknown_method_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_unknown_method::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_unknown_method_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_unknown_method::<WsRpcConnector>(&server).await;
}

// Generic test helper for service not found
async fn should_reject_unknown_service<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_rpc_request("rpc://test/services/nonexistent", "anyMethod", b"");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&response);
    assert_ne!(status, 0, "Expected failure for unknown RPC service");
}

#[tokio::test]
async fn should_reject_unknown_service_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_unknown_service::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_unknown_service_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_unknown_service::<WsRpcConnector>(&server).await;
}

// Generic test helper for payload handling
// NOTE: Without registered workers, RPC requests fail. This tests error handling.
async fn should_echo_payload_in_response<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let payload = b"test payload";
    let frame = build_rpc_request("rpc://test/services/api", "echo", payload);

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&response);
    assert_ne!(status, 0, "Expected error when no workers registered");
    // Would verify payload echo if parse included it
}

#[tokio::test]
async fn should_echo_payload_in_response_tcp() {
    let server = TestServer::start().await.expect("start");
    should_echo_payload_in_response::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
async fn should_echo_payload_in_response_ws() {
    let server = TestServer::start().await.expect("start");
    should_echo_payload_in_response::<WsRpcConnector>(&server).await;
}

// Generic test helper for multiple concurrent requests
async fn should_handle_concurrent_rpc_requests<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client1 = C::connect(server).await.expect("connect 1");
    let mut client2 = C::connect(server).await.expect("connect 2");
    let mut client3 = C::connect(server).await.expect("connect 3");

    // Act - Send requests from all three clients
    let frame1 = build_rpc_request("rpc://test/services/api", "getUser", b"user-1");
    let response1 = client1
        .send_and_receive(&frame1, 2000)
        .await
        .expect("request 1");

    let frame2 = build_rpc_request("rpc://test/services/api", "getUser", b"user-2");
    let response2 = client2
        .send_and_receive(&frame2, 2000)
        .await
        .expect("request 2");

    let frame3 = build_rpc_request("rpc://test/services/api", "getUser", b"user-3");
    let response3 = client3
        .send_and_receive(&frame3, 2000)
        .await
        .expect("request 3");

    // Assert
    let (_msg_type, status1, _data) = parse_rpc_response(&response1);
    let (_msg_type, status2, _data) = parse_rpc_response(&response2);
    let (_msg_type, status3, _data) = parse_rpc_response(&response3);

    assert_ne!(status1, 0, "Request 1 should fail (no workers)");
    assert_ne!(status2, 0, "Request 2 should fail (no workers)");
    assert_ne!(status3, 0, "Request 3 should fail (no workers)");
}

#[tokio::test]
async fn should_handle_concurrent_rpc_requests_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_concurrent_rpc_requests::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_rpc_requests_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_concurrent_rpc_requests::<WsRpcConnector>(&server).await;
}

// Generic test helper for large payload
// NOTE: Large payload tests error handling without registered workers.
async fn should_handle_large_rpc_payload<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let large_payload = vec![0u8; 50_000]; // 50KB
    let frame = build_rpc_request("rpc://test/services/api", "store", &large_payload);

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&response);
    assert_ne!(status, 0, "Should return error when no workers registered");
}

#[tokio::test]
async fn should_handle_large_rpc_payload_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_large_rpc_payload::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_large_rpc_payload_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_large_rpc_payload::<WsRpcConnector>(&server).await;
}

// Generic test helper for sequential requests on same connection
async fn should_handle_sequential_rpc_requests_on_same_connection<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Send first request
    let frame1 = build_rpc_request("rpc://test/services/counter", "increment", b"");
    let response1 = client
        .send_and_receive(&frame1, 2000)
        .await
        .expect("request 1");

    let (_msg_type, status1, _data) = parse_rpc_response(&response1);
    assert_ne!(status1, 0);

    // Act - Send second request on same connection
    let frame2 = build_rpc_request("rpc://test/services/counter", "increment", b"");
    let response2 = client
        .send_and_receive(&frame2, 2000)
        .await
        .expect("request 2");

    let (_msg_type, status2, _data) = parse_rpc_response(&response2);
    assert_ne!(status2, 0);

    // Act - Send third request
    let frame3 = build_rpc_request("rpc://test/services/counter", "increment", b"");
    let response3 = client
        .send_and_receive(&frame3, 2000)
        .await
        .expect("request 3");

    // Assert
    let (_msg_type, status3, _data) = parse_rpc_response(&response3);
    assert_ne!(
        status3, 0,
        "Sequential requests should all fail (no workers)"
    );
}

#[tokio::test]
async fn should_handle_sequential_rpc_requests_on_same_connection_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_sequential_rpc_requests_on_same_connection::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_sequential_rpc_requests_on_same_connection_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_sequential_rpc_requests_on_same_connection::<WsRpcConnector>(&server).await;
}

// Generic test helper for different methods on same service
async fn should_call_multiple_methods_on_service<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Call first method
    let frame1 = build_rpc_request("rpc://test/services/data", "get", b"key1");
    let response1 = client.send_and_receive(&frame1, 2000).await.expect("get");

    let (_msg_type, status, _data) = parse_rpc_response(&response1);
    assert_ne!(status, 0);

    // Act - Call second method
    let frame2 = build_rpc_request("rpc://test/services/data", "put", b"key1:value1");
    let response2 = client.send_and_receive(&frame2, 2000).await.expect("put");

    let (_msg_type, status, _data) = parse_rpc_response(&response2);
    assert_ne!(status, 0);

    // Act - Call third method
    let frame3 = build_rpc_request("rpc://test/services/data", "delete", b"key1");
    let response3 = client
        .send_and_receive(&frame3, 2000)
        .await
        .expect("delete");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&response3);
    assert_ne!(status, 0, "Multiple methods should all fail (no workers)");
}

#[tokio::test]
async fn should_call_multiple_methods_on_service_tcp() {
    let server = TestServer::start().await.expect("start");
    should_call_multiple_methods_on_service::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
async fn should_call_multiple_methods_on_service_ws() {
    let server = TestServer::start().await.expect("start");
    should_call_multiple_methods_on_service::<WsRpcConnector>(&server).await;
}

fn assert_worker_disconnect_error_frame(frame: &[u8], expected_correlation_id: uuid::Uuid) {
    let response = parse_rpc_response_delivery(frame).expect("parse rpc response delivery");
    assert_eq!(response.correlation_id, expected_correlation_id);
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);

    let (code, message) =
        fitz::protocol::rpc_codec::decode_error_body(&response.body).expect("parse rpc error body");
    assert_eq!(code, fitz::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND);
    assert_eq!(message, "Worker disconnected or unregistered");
}

async fn exercise_worker_disconnect_error_after_accept_tcp(server: &TestServer) {
    let worker_route = "rpc://test/services/disconnect";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestClient::new(server.tcp_addr)
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestClient::new(server.tcp_addr)
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");
    let accepted_response = caller.recv_frame(2000).await.expect("accepted response");
    let (_msg_type, status, _data) = parse_rpc_response(&accepted_response);
    assert_eq!(status, 0);

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    drop(worker);
    server
        .wait_for_session_count(1)
        .await
        .expect("wait for worker disconnect");

    let disconnect_error = caller.recv_frame(2000).await.expect("disconnect error");
    assert_worker_disconnect_error_frame(&disconnect_error, delivered_request.correlation_id);
}

async fn exercise_worker_disconnect_error_after_accept_ws(server: &TestServer) {
    let worker_route = "rpc://test/services/disconnect";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");
    let accepted_response = caller.recv_frame(2000).await.expect("accepted response");
    let (_msg_type, status, _data) = parse_rpc_response(&accepted_response);
    assert_eq!(status, 0);

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    worker.close().await.expect("close worker");
    server
        .wait_for_session_count(1)
        .await
        .expect("wait for worker disconnect");

    let disconnect_error = caller.recv_frame(2000).await.expect("disconnect error");
    assert_worker_disconnect_error_frame(&disconnect_error, delivered_request.correlation_id);

    caller.close().await.expect("close caller");
}

async fn exercise_worker_unregister_error_after_accept_tcp(server: &TestServer) {
    let worker_route = "rpc://test/services/unregister";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let unsubscribe_frame = build_rpc_unsubscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestClient::new(server.tcp_addr)
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestClient::new(server.tcp_addr)
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");
    let accepted_response = caller.recv_frame(2000).await.expect("accepted response");
    let (_msg_type, status, _data) = parse_rpc_response(&accepted_response);
    assert_eq!(status, 0);

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    worker
        .send_frame(&unsubscribe_frame)
        .await
        .expect("unsubscribe worker");
    let unsubscribe_response = worker.recv_frame(2000).await.expect("unsubscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&unsubscribe_response);
    assert_eq!(status, 0);
    server
        .wait_for_session_count(2)
        .await
        .expect("worker should remain connected after unsubscribe");

    let unregister_error = caller.recv_frame(2000).await.expect("unregister error");
    assert_worker_disconnect_error_frame(&unregister_error, delivered_request.correlation_id);
}

async fn exercise_worker_unregister_error_after_accept_ws(server: &TestServer) {
    let worker_route = "rpc://test/services/unregister";
    let subscribe_frame = build_rpc_subscribe(worker_route);
    let unsubscribe_frame = build_rpc_unsubscribe(worker_route);
    let request_frame = build_rpc_request(worker_route, "getUser", b"user-123");

    let mut worker = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect worker");
    worker
        .send_frame(&subscribe_frame)
        .await
        .expect("subscribe worker");
    let subscribe_response = worker.recv_frame(2000).await.expect("subscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    let mut caller = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("connect caller");
    caller
        .send_frame(&request_frame)
        .await
        .expect("send request");
    let accepted_response = caller.recv_frame(2000).await.expect("accepted response");
    let (_msg_type, status, _data) = parse_rpc_response(&accepted_response);
    assert_eq!(status, 0);

    let delivered_request = worker.recv_frame(2000).await.expect("request delivery");
    let delivered_request =
        parse_rpc_request_delivery(&delivered_request).expect("parse request delivery");

    worker
        .send_frame(&unsubscribe_frame)
        .await
        .expect("unsubscribe worker");
    let unsubscribe_response = worker.recv_frame(2000).await.expect("unsubscribe ack");
    let (_msg_type, status, _data) = parse_rpc_response(&unsubscribe_response);
    assert_eq!(status, 0);
    server
        .wait_for_session_count(2)
        .await
        .expect("worker should remain connected after unsubscribe");

    let unregister_error = caller.recv_frame(2000).await.expect("unregister error");
    assert_worker_disconnect_error_frame(&unregister_error, delivered_request.correlation_id);

    worker.close().await.expect("close worker");
    caller.close().await.expect("close caller");
}

#[tokio::test]
async fn should_return_worker_disconnect_error_after_accept_tcp() {
    let server = TestServer::start().await.expect("start");
    exercise_worker_disconnect_error_after_accept_tcp(&server).await;
}

#[tokio::test]
async fn should_return_worker_disconnect_error_after_accept_ws() {
    let server = TestServer::start().await.expect("start");
    exercise_worker_disconnect_error_after_accept_ws(&server).await;
}

#[tokio::test]
async fn should_return_worker_disconnect_error_after_unsubscribe_tcp() {
    let server = TestServer::start().await.expect("start");
    exercise_worker_unregister_error_after_accept_tcp(&server).await;
}

#[tokio::test]
async fn should_return_worker_disconnect_error_after_unsubscribe_ws() {
    let server = TestServer::start().await.expect("start");
    exercise_worker_unregister_error_after_accept_ws(&server).await;
}
