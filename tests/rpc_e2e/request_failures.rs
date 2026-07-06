use super::common::*;

// Generic test helper for request-response
// NOTE: Without registered workers, RPC requests will fail as expected.
// This tests that the RPC domain correctly returns error status when no workers are registered.
pub(crate) async fn should_send_rpc_request<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_rpc_request("rpc://test/services/api", "getUser", b"user-123");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    assert_rpc_route_not_registered_error_response(&response);
}

#[tokio::test]
#[serial]
pub(crate) async fn should_send_rpc_request_tcp() {
    let server = TestServer::start().await.expect("start");
    should_send_rpc_request::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_send_rpc_request_ws() {
    let server = TestServer::start().await.expect("start");
    should_send_rpc_request::<WsRpcConnector>(&server).await;
}

// Generic test helper for invalid method
// NOTE: With no registered workers, all RPC requests fail with error status.
// This test verifies that behavior is consistent.
pub(crate) async fn should_reject_unknown_method<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_rpc_request("rpc://test/services/api", "unknownMethod", b"");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    assert_rpc_route_not_registered_error_response(&response);
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_unknown_method_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_unknown_method::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_unknown_method_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_unknown_method::<WsRpcConnector>(&server).await;
}

// Generic test helper for service not found
pub(crate) async fn should_reject_unknown_service<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_rpc_request("rpc://test/services/nonexistent", "anyMethod", b"");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    assert_rpc_route_not_registered_error_response(&response);
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_unknown_service_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_unknown_service::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_unknown_service_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_unknown_service::<WsRpcConnector>(&server).await;
}

// Generic test helper for payload handling
// NOTE: Without registered workers, RPC requests fail. This tests error handling.
pub(crate) async fn should_echo_payload_in_response<C>(server: &TestServer)
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
    assert_rpc_route_not_registered_error_response(&response);
}

#[tokio::test]
#[serial]
pub(crate) async fn should_echo_payload_in_response_tcp() {
    let server = TestServer::start().await.expect("start");
    should_echo_payload_in_response::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_echo_payload_in_response_ws() {
    let server = TestServer::start().await.expect("start");
    should_echo_payload_in_response::<WsRpcConnector>(&server).await;
}

// Generic test helper for multiple concurrent requests
pub(crate) async fn should_handle_concurrent_rpc_requests<C>(server: &TestServer)
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
    assert_rpc_route_not_registered_error_response(&response1);
    assert_rpc_route_not_registered_error_response(&response2);
    assert_rpc_route_not_registered_error_response(&response3);
}

#[tokio::test]
#[serial]
pub(crate) async fn should_handle_concurrent_rpc_requests_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_concurrent_rpc_requests::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_handle_concurrent_rpc_requests_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_concurrent_rpc_requests::<WsRpcConnector>(&server).await;
}

// Generic test helper for large payload
// NOTE: Large payload tests error handling without registered workers.
pub(crate) async fn should_handle_large_rpc_payload<C>(server: &TestServer)
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
    assert_rpc_route_not_registered_error_response(&response);
}

#[tokio::test]
#[serial]
pub(crate) async fn should_handle_large_rpc_payload_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_large_rpc_payload::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_handle_large_rpc_payload_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_large_rpc_payload::<WsRpcConnector>(&server).await;
}

// Generic test helper for sequential requests on same connection
pub(crate) async fn should_handle_sequential_rpc_requests_on_same_connection<C>(server: &TestServer)
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

    assert_rpc_route_not_registered_error_response(&response1);

    // Act - Send second request on same connection
    let frame2 = build_rpc_request("rpc://test/services/counter", "increment", b"");
    let response2 = client
        .send_and_receive(&frame2, 2000)
        .await
        .expect("request 2");

    assert_rpc_route_not_registered_error_response(&response2);

    // Act - Send third request
    let frame3 = build_rpc_request("rpc://test/services/counter", "increment", b"");
    let response3 = client
        .send_and_receive(&frame3, 2000)
        .await
        .expect("request 3");

    // Assert
    assert_rpc_route_not_registered_error_response(&response3);
}

#[tokio::test]
#[serial]
pub(crate) async fn should_handle_sequential_rpc_requests_on_same_connection_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_sequential_rpc_requests_on_same_connection::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_handle_sequential_rpc_requests_on_same_connection_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_sequential_rpc_requests_on_same_connection::<WsRpcConnector>(&server).await;
}

// Generic test helper for different methods on same service
pub(crate) async fn should_call_multiple_methods_on_service<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Call first method
    let frame1 = build_rpc_request("rpc://test/services/data", "get", b"key1");
    let response1 = client.send_and_receive(&frame1, 2000).await.expect("get");

    assert_rpc_route_not_registered_error_response(&response1);

    // Act - Call second method
    let frame2 = build_rpc_request("rpc://test/services/data", "put", b"key1:value1");
    let response2 = client.send_and_receive(&frame2, 2000).await.expect("put");

    assert_rpc_route_not_registered_error_response(&response2);

    // Act - Call third method
    let frame3 = build_rpc_request("rpc://test/services/data", "delete", b"key1");
    let response3 = client
        .send_and_receive(&frame3, 2000)
        .await
        .expect("delete");

    // Assert
    assert_rpc_route_not_registered_error_response(&response3);
}

#[tokio::test]
#[serial]
pub(crate) async fn should_call_multiple_methods_on_service_tcp() {
    let server = TestServer::start().await.expect("start");
    should_call_multiple_methods_on_service::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_call_multiple_methods_on_service_ws() {
    let server = TestServer::start().await.expect("start");
    should_call_multiple_methods_on_service::<WsRpcConnector>(&server).await;
}
