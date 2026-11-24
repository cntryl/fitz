use super::common::*;
use uuid::Uuid;

const REPLY_TIMEOUT_MS: u64 = 2000;

async fn register_worker<C: RpcConnector>(server: &TestServer, route: &str, credit: u32) -> C {
    let mut worker = C::connect(server).await.expect("connect worker");
    let reply = worker
        .request(
            &build_rpc_subscribe_with_max_concurrent(route, credit),
            REPLY_TIMEOUT_MS,
        )
        .await
        .expect("register worker");
    let (_, status, _) = parse_rpc_response(&reply);
    assert_eq!(status, 0, "RPC worker registration should succeed");
    worker
}

fn request_id(frame: &[u8]) -> Uuid {
    parse_rpc_request_delivery(frame)
        .expect("decode caller's RPC request")
        .correlation_id
}

async fn receive_call(worker: &mut impl FrameReceivingConnector) -> RpcRequestDelivery {
    let frame = worker
        .recv_frame(REPLY_TIMEOUT_MS)
        .await
        .expect("worker receives dispatched call");
    parse_rpc_request_delivery(&frame).expect("decode dispatched call")
}

async fn echo_call(worker: &mut impl TestConnectorClient, call: &RpcRequestDelivery) {
    worker
        .send_frame(&build_rpc_response_delivery(
            call.correlation_id,
            0,
            true,
            &call.body,
        ))
        .await
        .expect("worker echoes payload");
}

async fn relay_echo_call<C>(caller: &mut C, worker: &mut C, frame: &[u8]) -> (String, Vec<u8>)
where
    C: FrameReceivingConnector,
{
    caller.send_frame(frame).await.expect("send caller request");
    let delivery = receive_call(worker).await;
    echo_call(worker, &delivery).await;
    let reply = caller
        .recv_frame(REPLY_TIMEOUT_MS)
        .await
        .expect("caller receives terminal reply");
    (delivery.route, reply)
}

async fn check_rpc_echo<C>(server: &TestServer, body: &[u8])
where
    C: RpcConnector + FrameReceivingConnector,
{
    // Arrange
    let route = "rpc://test/services/echo";
    let mut caller = C::connect(server).await.expect("connect caller");
    let mut worker = register_worker::<C>(server, route, 1).await;
    let frame = build_rpc_request(route, "", body);

    // Act
    let (worker_route, reply) = relay_echo_call(&mut caller, &mut worker, &frame).await;

    // Assert
    assert_eq!(worker_route, route);
    assert_forwarded_rpc_response_frame(&reply, request_id(&frame), body);
}

async fn check_overlapping_rpc_calls<C>(server: &TestServer)
where
    C: RpcConnector + FrameReceivingConnector,
{
    // Arrange
    let route = "rpc://test/services/concurrent";
    let mut first = C::connect(server).await.expect("connect first caller");
    let mut second = C::connect(server).await.expect("connect second caller");
    let mut worker = register_worker::<C>(server, route, 2).await;
    let first_frame = build_rpc_request(route, "", b"first");
    let second_frame = build_rpc_request(route, "", b"second");

    // Act
    tokio::try_join!(
        first.send_frame(&first_frame),
        second.send_frame(&second_frame)
    )
    .expect("submit calls on both sessions");
    // Both requests must reach the worker while both credits are occupied.
    let first_delivery = receive_call(&mut worker).await;
    let second_delivery = receive_call(&mut worker).await;
    echo_call(&mut worker, &second_delivery).await;
    echo_call(&mut worker, &first_delivery).await;
    let (first_reply, second_reply) = tokio::try_join!(
        first.recv_frame(REPLY_TIMEOUT_MS),
        second.recv_frame(REPLY_TIMEOUT_MS),
    )
    .expect("receive replies on both sessions");

    // Assert
    assert_ne!(
        first_delivery.correlation_id, second_delivery.correlation_id,
        "distinct live requests need distinct worker IDs"
    );
    assert_eq!(first_delivery.route, route);
    assert_eq!(second_delivery.route, route);
    assert_forwarded_rpc_response_frame(&first_reply, request_id(&first_frame), b"first");
    assert_forwarded_rpc_response_frame(&second_reply, request_id(&second_frame), b"second");
}

async fn check_rpc_calls_on_same_connection<C>(server: &TestServer)
where
    C: RpcConnector + FrameReceivingConnector,
{
    // Arrange
    let mut caller = C::connect(server).await.expect("connect caller");
    let mut worker = register_worker::<C>(server, "rpc://test/services/*", 1).await;
    let cases = [
        ("rpc://test/services/get", b"key".as_slice()),
        ("rpc://test/services/put", b"key:value".as_slice()),
    ];
    let frames = cases.map(|(route, body)| build_rpc_request(route, "", body));

    // Act
    let mut replies = Vec::new();
    for frame in &frames {
        replies.push(relay_echo_call(&mut caller, &mut worker, frame).await);
    }

    // Assert
    for (((route, body), frame), (worker_route, reply)) in
        cases.into_iter().zip(frames).zip(replies)
    {
        assert_eq!(worker_route, route);
        assert_forwarded_rpc_response_frame(&reply, request_id(&frame), body);
    }
}

#[tokio::test]
#[serial]
async fn should_echo_rpc_payload_tcp() {
    let server = TestServer::start().await.expect("start");
    check_rpc_echo::<TcpRpcConnector>(&server, b"test payload").await;
}

#[tokio::test]
#[serial]
async fn should_echo_rpc_payload_ws() {
    let server = TestServer::start().await.expect("start");
    check_rpc_echo::<WsRpcConnector>(&server, b"test payload").await;
}

fn large_payload() -> Vec<u8> {
    (0_u32..12_288).flat_map(u32::to_be_bytes).collect()
}

#[tokio::test]
#[serial]
async fn should_echo_large_rpc_payload_tcp() {
    let server = TestServer::start().await.expect("start");
    check_rpc_echo::<TcpRpcConnector>(&server, &large_payload()).await;
}

#[tokio::test]
#[serial]
async fn should_echo_large_rpc_payload_ws() {
    let server = TestServer::start().await.expect("start");
    check_rpc_echo::<WsRpcConnector>(&server, &large_payload()).await;
}

#[tokio::test]
#[serial]
async fn should_correlate_overlapping_rpc_calls_tcp() {
    let server = TestServer::start().await.expect("start");
    check_overlapping_rpc_calls::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
async fn should_correlate_overlapping_rpc_calls_ws() {
    let server = TestServer::start().await.expect("start");
    check_overlapping_rpc_calls::<WsRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
async fn should_dispatch_sequential_rpc_calls_to_distinct_routes_on_same_connection_tcp() {
    let server = TestServer::start().await.expect("start");
    check_rpc_calls_on_same_connection::<TcpRpcConnector>(&server).await;
}

#[tokio::test]
#[serial]
async fn should_dispatch_sequential_rpc_calls_to_distinct_routes_on_same_connection_ws() {
    let server = TestServer::start().await.expect("start");
    check_rpc_calls_on_same_connection::<WsRpcConnector>(&server).await;
}
