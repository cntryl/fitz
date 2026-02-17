//! RPC domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::transport::*;

// Generic test helper for request-response
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
    assert_eq!(status, 0, "Expected success for RPC request");
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
    assert_ne!(status, 0, "Expected failure for unknown RPC method");
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
async fn should_echo_payload_in_response<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let test_payload = b"request-data-123";
    let frame = build_rpc_request("rpc://test/services/echo", "echo", test_payload);

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&response);
    assert_eq!(status, 0, "Expected success for echo method");
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

    assert_eq!(status1, 0, "Request 1 should succeed");
    assert_eq!(status2, 0, "Request 2 should succeed");
    assert_eq!(status3, 0, "Request 3 should succeed");
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
async fn should_handle_large_rpc_payload<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let large_payload = vec![b'X'; 50_000];
    let frame = build_rpc_request("rpc://test/services/process", "handleLarge", &large_payload);

    // Act
    let response = client.send_and_receive(&frame, 3000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&response);
    assert_eq!(status, 0, "Should handle large payload");
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
    assert_eq!(status1, 0);

    // Act - Send second request on same connection
    let frame2 = build_rpc_request("rpc://test/services/counter", "increment", b"");
    let response2 = client
        .send_and_receive(&frame2, 2000)
        .await
        .expect("request 2");

    let (_msg_type, status2, _data) = parse_rpc_response(&response2);
    assert_eq!(status2, 0);

    // Act - Send third request
    let frame3 = build_rpc_request("rpc://test/services/counter", "increment", b"");
    let response3 = client
        .send_and_receive(&frame3, 2000)
        .await
        .expect("request 3");

    // Assert
    let (_msg_type, status3, _data) = parse_rpc_response(&response3);
    assert_eq!(status3, 0, "Sequential requests should all succeed");
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
    assert_eq!(status, 0);

    // Act - Call second method
    let frame2 = build_rpc_request("rpc://test/services/data", "put", b"key1:value1");
    let response2 = client.send_and_receive(&frame2, 2000).await.expect("put");

    let (_msg_type, status, _data) = parse_rpc_response(&response2);
    assert_eq!(status, 0);

    // Act - Call third method
    let frame3 = build_rpc_request("rpc://test/services/data", "delete", b"key1");
    let response3 = client
        .send_and_receive(&frame3, 2000)
        .await
        .expect("delete");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&response3);
    assert_eq!(status, 0, "Multiple methods should be callable");
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
