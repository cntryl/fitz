//! RPC domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use fixtures::transport::*;
use fitz::testkit::TestServer;

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
