//! Queue domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::transport::*;

// Generic test helper for enqueue
async fn should_enqueue_message<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_queue_enqueue("queue://test/jobs", b"task-1");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Expected success for enqueue");
}

#[tokio::test]
async fn should_enqueue_message_tcp() {
    let server = TestServer::start().await.expect("start");
    should_enqueue_message::<TcpQueueConnector>(&server).await;
}

#[tokio::test]
async fn should_enqueue_message_ws() {
    let server = TestServer::start().await.expect("start");
    should_enqueue_message::<WsQueueConnector>(&server).await;
}

// Generic test helper for dequeue
async fn should_dequeue_message<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let data = b"task-data";
    let enqueue_frame = build_queue_enqueue("queue://test/jobs", data);
    let _ = client
        .send_and_receive(&enqueue_frame, 2000)
        .await
        .expect("enqueue");

    // Act
    let dequeue_frame = build_queue_dequeue("queue://test/jobs");
    let response = client
        .send_and_receive(&dequeue_frame, 2000)
        .await
        .expect("dequeue");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Expected success for dequeue");
}

#[tokio::test]
async fn should_dequeue_message_tcp() {
    let server = TestServer::start().await.expect("start");
    should_dequeue_message::<TcpQueueConnector>(&server).await;
}

#[tokio::test]
async fn should_dequeue_message_ws() {
    let server = TestServer::start().await.expect("start");
    should_dequeue_message::<WsQueueConnector>(&server).await;
}

// Generic test helper for error on dequeue empty
async fn should_reject_dequeue_empty_queue<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_queue_dequeue("queue://test/empty");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_ne!(status, 0, "Expected failure for dequeue on empty queue");
}

#[tokio::test]
async fn should_reject_dequeue_empty_queue_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_dequeue_empty_queue::<TcpQueueConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_dequeue_empty_queue_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_dequeue_empty_queue::<WsQueueConnector>(&server).await;
}

// Generic test helper for queue isolation by name
async fn should_isolate_separate_queues<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let queue1_data = b"q1-task";
    let enqueue_q1 = build_queue_enqueue("queue://test/q1", queue1_data);
    let _ = client
        .send_and_receive(&enqueue_q1, 2000)
        .await
        .expect("enqueue q1");

    // Act - dequeue from different queue
    let dequeue_q2 = build_queue_dequeue("queue://test/q2");
    let response = client
        .send_and_receive(&dequeue_q2, 2000)
        .await
        .expect("dequeue q2");

    // Assert - should fail (different queue is empty)
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_ne!(
        status, 0,
        "Expected failure for dequeue from different queue"
    );
}

#[tokio::test]
async fn should_isolate_separate_queues_tcp() {
    let server = TestServer::start().await.expect("start");
    should_isolate_separate_queues::<TcpQueueConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_separate_queues_ws() {
    let server = TestServer::start().await.expect("start");
    should_isolate_separate_queues::<WsQueueConnector>(&server).await;
}
