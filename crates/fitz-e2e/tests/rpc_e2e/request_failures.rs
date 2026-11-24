use super::common::*;

async fn check_call_without_registered_worker<C: RpcConnector>(server: &TestServer) {
    // Arrange
    let mut caller = C::connect(server).await.expect("connect caller");
    let frame = build_rpc_request("rpc://test/services/unregistered", "", b"request");
    let request_id = parse_rpc_request_delivery(&frame)
        .expect("decode caller request")
        .correlation_id;

    // Act
    let reply = caller.send_and_receive(&frame, 2000).await.expect("reply");

    // Assert
    assert_rpc_route_not_registered_error_response(&reply);
    assert_eq!(
        parse_rpc_response_delivery(&reply)
            .expect("decode terminal reply")
            .correlation_id,
        request_id,
    );
}

#[tokio::test]
#[serial]
async fn should_reject_call_without_registered_worker_tcp() {
    let server = TestServer::start().await.expect("start");
    check_call_without_registered_worker::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
async fn should_reject_call_without_registered_worker_ws() {
    let server = TestServer::start().await.expect("start");
    check_call_without_registered_worker::<WsRpcConnector>(&server).await;
}

pub(crate) async fn should_reject_wildcard_call_route_with_6011<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_rpc_request("rpc://test/services/*", "ignored", b"request");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    assert_rpc_terminal_validation_error_response(
        &response,
        fitz::protocol::error_codes::rpc::ERR_INVALID_ROUTE,
        "Invalid RPC call route",
    );
}

pub(crate) async fn should_reject_invalid_registration_pattern_with_6012<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_rpc_subscribe("rpc://test/serv*ces/api");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    assert_rpc_control_validation_error_response(
        &response,
        300,
        fitz::protocol::error_codes::rpc::ERR_INVALID_SUBSCRIPTION_PATTERN,
        "Invalid RPC registration pattern",
    );
}

pub(crate) async fn should_reject_wildcard_registration_overflow_with_6013<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    for index in 0..128 {
        let frame = build_rpc_subscribe(&format!("rpc://test/area{index}/*"));
        let response = client
            .send_and_receive(&frame, 2000)
            .await
            .expect("register wildcard");
        let (_, status, _) = parse_rpc_response(&response);
        assert_eq!(status, 0);
    }
    let overflow = build_rpc_subscribe("rpc://test/overflow/*");

    // Act
    let response = client
        .send_and_receive(&overflow, 2000)
        .await
        .expect("send overflow registration");

    // Assert
    assert_rpc_control_validation_error_response(
        &response,
        300,
        fitz::protocol::error_codes::rpc::ERR_SUBSCRIPTION_LIMIT,
        "RPC registration limit exceeded",
    );
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_wildcard_call_route_with_6011_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_wildcard_call_route_with_6011::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_wildcard_call_route_with_6011_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_wildcard_call_route_with_6011::<WsRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_invalid_registration_pattern_with_6012_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_invalid_registration_pattern_with_6012::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_invalid_registration_pattern_with_6012_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_invalid_registration_pattern_with_6012::<WsRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_wildcard_registration_overflow_with_6013_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_wildcard_registration_overflow_with_6013::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
pub(crate) async fn should_reject_wildcard_registration_overflow_with_6013_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_wildcard_registration_overflow_with_6013::<WsRpcConnector>(&server).await;
}
