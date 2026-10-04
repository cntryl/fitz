use super::common::*;
use bytes::BufMut;
use fitz::protocol::payload_codec::PayloadEncoder;
use uuid::Uuid;

fn frame(message_type: u16, payload: &[u8]) -> Vec<u8> {
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(message_type, payload);
    builder.build()
}

fn subscribe_cancellation_worker(route: &str) -> Vec<u8> {
    let mut payload = PayloadEncoder::new();
    payload.put_string(route);
    payload.put_u32(1);
    payload.put_u8(1);
    payload.put_u8(1);
    frame(300, &payload.finish())
}

fn rpc_request(correlation_id: Uuid, route: &str, budget_ms: u32) -> Vec<u8> {
    let mut payload = PayloadEncoder::new();
    payload.put_raw(correlation_id.as_bytes());
    payload.put_string(route);
    payload.put_bytes(b"request");
    payload.put_u8(1);
    payload.put_u8(1);
    payload.put_u32(budget_ms);
    frame(302, &payload.finish())
}

fn caller_cancel(correlation_id: Uuid) -> Vec<u8> {
    let mut payload = Vec::with_capacity(18);
    payload.put_u8(1);
    payload.put_slice(correlation_id.as_bytes());
    payload.put_u8(1);
    frame(304, &payload)
}

fn worker_cleanup_ack(correlation_id: Uuid) -> Vec<u8> {
    let mut payload = Vec::with_capacity(17);
    payload.put_u8(3);
    payload.put_slice(correlation_id.as_bytes());
    frame(304, &payload)
}

fn assert_lifecycle(frame_bytes: &[u8], kind: u8, correlation_id: Uuid, value: u8) {
    let mut parser = TlvFrameParser::new(frame_bytes);
    let (message_type, payload) = parser.next_field().expect("lifecycle frame");
    assert_eq!(message_type, 305);
    assert!(parser.next_field().is_none());
    assert_eq!(
        payload,
        [kind]
            .into_iter()
            .chain(correlation_id.into_bytes())
            .chain([value])
            .collect::<Vec<_>>()
    );
}

async fn connect_worker<C>(server: &TestServer, route: &str) -> C
where
    C: RpcConnector,
{
    let mut worker = C::connect(server).await.expect("connect worker");
    let response = worker
        .request(&subscribe_cancellation_worker(route), 2000)
        .await
        .expect("subscribe worker");
    let (_, status, _) = parse_rpc_response(&response);
    assert_eq!(status, 0);
    worker
}

pub(crate) async fn should_forward_rpc_cancellation_over_transport<C>(server: &TestServer)
where
    C: RpcConnector + FrameReceivingConnector,
{
    // Arrange
    let route = "rpc://cancel/service/work";
    let mut worker = connect_worker::<C>(server, route).await;
    let mut caller = C::connect(server).await.expect("connect caller");
    let correlation_id = Uuid::new_v4();

    // Act
    caller
        .send_frame(&rpc_request(correlation_id, route, 5000))
        .await
        .expect("send request");
    let delivery = worker.recv_frame(2000).await.expect("request delivery");
    let request = parse_rpc_request_delivery(&delivery).expect("parse request delivery");
    assert_ne!(request.correlation_id, correlation_id);
    assert!(request
        .remaining_budget_ms
        .is_some_and(|budget| budget <= 5000));
    caller
        .send_frame(&caller_cancel(correlation_id))
        .await
        .expect("send cancellation");
    let worker_cancellation = worker.recv_frame(2000).await.expect("worker cancellation");
    let caller_result = caller
        .recv_frame(2000)
        .await
        .expect("caller cancellation result");
    worker
        .send_frame(&worker_cleanup_ack(request.correlation_id))
        .await
        .expect("acknowledge worker cleanup");

    // Assert
    assert_lifecycle(&worker_cancellation, 2, request.correlation_id, 1);
    assert_lifecycle(&caller_result, 4, correlation_id, 2);
}

pub(crate) async fn should_propagate_rpc_cancellation_through_downstream_call<C>(
    server: &TestServer,
) where
    C: RpcConnector + FrameReceivingConnector,
{
    // Arrange
    let parent_route = "rpc://cancel/chain/parent";
    let child_route = "rpc://cancel/chain/child";
    let mut caller_a = C::connect(server).await.expect("connect caller A");
    let mut worker_b = connect_worker::<C>(server, parent_route).await;
    let mut worker_c = connect_worker::<C>(server, child_route).await;
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();

    // Act
    caller_a
        .send_frame(&rpc_request(parent_id, parent_route, 5000))
        .await
        .expect("send parent request");
    let parent_delivery = worker_b.recv_frame(2000).await.expect("parent delivery");
    let parent_request =
        parse_rpc_request_delivery(&parent_delivery).expect("parse parent delivery");
    assert_ne!(parent_request.correlation_id, parent_id);
    let parent_budget = parent_request.remaining_budget_ms.expect("parent budget");
    let child_budget = parent_budget.saturating_sub(1);
    worker_b
        .send_frame(&rpc_request(child_id, child_route, child_budget))
        .await
        .expect("send explicitly linked child request");
    let child_delivery = worker_c.recv_frame(2000).await.expect("child delivery");
    let child_request = parse_rpc_request_delivery(&child_delivery).expect("parse child delivery");
    assert_ne!(child_request.correlation_id, child_id);
    assert!(child_request
        .remaining_budget_ms
        .is_some_and(|budget| budget <= child_budget && budget <= parent_budget));

    caller_a
        .send_frame(&caller_cancel(parent_id))
        .await
        .expect("cancel parent call");
    let parent_worker_signal = worker_b
        .recv_frame(2000)
        .await
        .expect("parent cancellation");
    worker_b
        .send_frame(&caller_cancel(child_id))
        .await
        .expect("propagate cancellation to child call");
    let child_caller_result = worker_b
        .recv_frame(2000)
        .await
        .expect("child cancellation result");
    let child_worker_signal = worker_c
        .recv_frame(2000)
        .await
        .expect("child worker cancellation");
    let parent_caller_result = caller_a
        .recv_frame(2000)
        .await
        .expect("parent cancellation result");
    worker_b
        .send_frame(&worker_cleanup_ack(parent_request.correlation_id))
        .await
        .expect("acknowledge parent cleanup");
    worker_c
        .send_frame(&worker_cleanup_ack(child_request.correlation_id))
        .await
        .expect("acknowledge child cleanup");

    // Assert
    assert_lifecycle(&parent_worker_signal, 2, parent_request.correlation_id, 1);
    assert_lifecycle(&child_caller_result, 4, child_id, 2);
    assert_lifecycle(&child_worker_signal, 2, child_request.correlation_id, 1);
    assert_lifecycle(&parent_caller_result, 4, parent_id, 2);
}

pub(crate) async fn should_cancel_worker_when_caller_disconnects_over_transport<C>(
    server: &TestServer,
) where
    C: RpcConnector + FrameReceivingConnector,
{
    // Arrange
    let route = "rpc://cancel/service/disconnect";
    let mut worker = connect_worker::<C>(server, route).await;
    let mut caller = C::connect(server).await.expect("connect caller");
    let correlation_id = Uuid::new_v4();
    caller
        .send_frame(&rpc_request(correlation_id, route, 5000))
        .await
        .expect("send request");
    let delivery = worker.recv_frame(2000).await.expect("request delivery");
    let worker_id = parse_rpc_request_delivery(&delivery)
        .expect("parse request delivery")
        .correlation_id;
    assert_ne!(worker_id, correlation_id);

    // Act
    drop(caller);
    let worker_cancellation = worker.recv_frame(2000).await.expect("worker cancellation");
    worker
        .send_frame(&worker_cleanup_ack(worker_id))
        .await
        .expect("acknowledge worker cleanup");

    // Assert
    assert_lifecycle(&worker_cancellation, 2, worker_id, 3);
}

pub(crate) async fn should_cancel_worker_when_rpc_budget_expires_over_transport<C>(
    server: &TestServer,
) where
    C: RpcConnector + FrameReceivingConnector,
{
    // Arrange
    let route = "rpc://cancel/service/deadline";
    let mut worker = connect_worker::<C>(server, route).await;
    let mut caller = C::connect(server).await.expect("connect caller");
    let correlation_id = Uuid::new_v4();
    caller
        .send_frame(&rpc_request(correlation_id, route, 250))
        .await
        .expect("send request");
    let delivery = worker.recv_frame(2000).await.expect("request delivery");
    let worker_id = parse_rpc_request_delivery(&delivery)
        .expect("parse request delivery")
        .correlation_id;
    assert_ne!(worker_id, correlation_id);

    // Act
    let caller_timeout = caller
        .recv_frame(2000)
        .await
        .expect("caller timeout response");
    let worker_cancellation = worker.recv_frame(2000).await.expect("worker cancellation");
    worker
        .send_frame(&worker_cleanup_ack(worker_id))
        .await
        .expect("acknowledge worker cleanup");

    // Assert
    assert_rpc_timeout_error_frame(&caller_timeout, correlation_id);
    assert_lifecycle(&worker_cancellation, 2, worker_id, 4);
}

#[tokio::test]
#[serial]
async fn should_forward_rpc_cancellation_over_tcp() {
    // Arrange
    let server = TestServer::start().await.expect("start");
    // Act
    should_forward_rpc_cancellation_over_transport::<TcpRpcConnector>(&server).await;
    // Assert
}

#[tokio::test]
#[serial]
async fn should_forward_rpc_cancellation_over_websocket() {
    // Arrange
    let server = TestServer::start().await.expect("start");
    // Act
    should_forward_rpc_cancellation_over_transport::<WsRpcConnector>(&server).await;
    // Assert
}

#[tokio::test]
#[serial]
async fn should_propagate_rpc_cancellation_through_downstream_call_over_tcp() {
    // Arrange
    let server = TestServer::start().await.expect("start");
    // Act
    should_propagate_rpc_cancellation_through_downstream_call::<TcpRpcConnector>(&server).await;
    // Assert
}

#[tokio::test]
#[serial]
async fn should_propagate_rpc_cancellation_through_downstream_call_over_websocket() {
    // Arrange
    let server = TestServer::start().await.expect("start");
    // Act
    should_propagate_rpc_cancellation_through_downstream_call::<WsRpcConnector>(&server).await;
    // Assert
}

#[tokio::test]
#[serial]
async fn should_cancel_worker_when_caller_disconnects_over_tcp() {
    // Arrange
    let server = TestServer::start().await.expect("start");
    // Act
    should_cancel_worker_when_caller_disconnects_over_transport::<TcpRpcConnector>(&server).await;
    // Assert
}

#[tokio::test]
#[serial]
async fn should_cancel_worker_when_caller_disconnects_over_websocket() {
    // Arrange
    let server = TestServer::start().await.expect("start");
    // Act
    should_cancel_worker_when_caller_disconnects_over_transport::<WsRpcConnector>(&server).await;
    // Assert
}

#[tokio::test]
#[serial]
async fn should_cancel_worker_when_rpc_budget_expires_over_tcp() {
    // Arrange
    let server = TestServer::start().await.expect("start");
    // Act
    should_cancel_worker_when_rpc_budget_expires_over_transport::<TcpRpcConnector>(&server).await;
    // Assert
}

#[tokio::test]
#[serial]
async fn should_cancel_worker_when_rpc_budget_expires_over_websocket() {
    // Arrange
    let server = TestServer::start().await.expect("start");
    // Act
    should_cancel_worker_when_rpc_budget_expires_over_transport::<WsRpcConnector>(&server).await;
    // Assert
}

async fn should_close_uncooperative_worker_after_cancellation_grace<C>(server: &TestServer)
where
    C: RpcConnector + FrameReceivingConnector,
{
    // Arrange
    let route = "rpc://cancel/service/ignored";
    let mut worker = connect_worker::<C>(server, route).await;
    let mut caller = C::connect(server).await.expect("connect caller");
    let id = Uuid::new_v4();
    caller
        .send_frame(&rpc_request(id, route, 30_000))
        .await
        .expect("send request");
    let delivery = worker.recv_frame(2000).await.expect("worker delivery");
    let worker_id = parse_rpc_request_delivery(&delivery)
        .expect("parse delivery")
        .correlation_id;
    assert_ne!(worker_id, id);

    // Act
    caller
        .send_frame(&caller_cancel(id))
        .await
        .expect("send cancellation");
    let signal = worker.recv_frame(2000).await.expect("worker cancellation");
    let result = caller
        .recv_frame(2000)
        .await
        .expect("caller cancellation result");
    let close_error = worker
        .recv_frame(8000)
        .await
        .expect_err("worker must close without cleanup ack");
    server
        .wait_for_session_count(1)
        .await
        .expect("worker transport finalizes session");

    // Assert
    assert_lifecycle(&signal, 2, worker_id, 1);
    assert_lifecycle(&result, 4, id, 2);
    assert!(
        !close_error.contains("timeout"),
        "transport closed rather than read timeout: {close_error}"
    );
    assert!(server.runtime.rpc_list_pending(None).is_empty());
    assert!(server.runtime.rpc_list_workers(None).is_empty());
}

#[tokio::test]
#[serial]
async fn should_close_uncooperative_worker_after_rpc_cancellation_grace_over_tcp() {
    // Arrange
    let server = TestServer::start().await.expect("start");
    // Act
    should_close_uncooperative_worker_after_cancellation_grace::<TcpRpcConnector>(&server).await;
    // Assert
}

#[tokio::test]
#[serial]
async fn should_close_uncooperative_worker_after_rpc_cancellation_grace_over_websocket() {
    // Arrange
    let server = TestServer::start().await.expect("start");
    // Act
    should_close_uncooperative_worker_after_cancellation_grace::<WsRpcConnector>(&server).await;
    // Assert
}
