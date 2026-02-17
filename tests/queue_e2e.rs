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

// Generic test helper for message payload preservation
async fn should_preserve_message_payload<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let payload = b"important-data-123";
    let enqueue_frame = build_queue_enqueue("queue://test/data", payload);
    let _ = client
        .send_and_receive(&enqueue_frame, 2000)
        .await
        .expect("enqueue");

    // Act
    let dequeue_frame = build_queue_dequeue("queue://test/data");
    let response = client
        .send_and_receive(&dequeue_frame, 2000)
        .await
        .expect("dequeue");

    // Assert
    let (_msg_type, status, data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Dequeue should succeed");
    assert_eq!(data, payload, "Payload should be preserved");
}

#[tokio::test]
async fn should_preserve_message_payload_tcp() {
    let server = TestServer::start().await.expect("start");
    should_preserve_message_payload::<TcpQueueConnector>(&server).await;
}

#[tokio::test]
async fn should_preserve_message_payload_ws() {
    let server = TestServer::start().await.expect("start");
    should_preserve_message_payload::<WsQueueConnector>(&server).await;
}

// Generic test helper for multiple enqueue operations
async fn should_handle_batch_enqueue_operations<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Enqueue multiple messages
    for i in 0..10 {
        let data = format!("task-{}", i).into_bytes();
        let frame = build_queue_enqueue("queue://test/batch", &data);
        let response = client
            .send_and_receive(&frame, 2000)
            .await
            .expect(&format!("enqueue {}", i));

        let (_msg_type, status, _data) = parse_queue_response(&response);
        assert_eq!(status, 0, "Enqueue {} should succeed", i);
    }

    // Assert - Dequeue to verify all were stored
    let dequeue_frame = build_queue_dequeue("queue://test/batch");
    let response = client
        .send_and_receive(&dequeue_frame, 2000)
        .await
        .expect("dequeue");

    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Should be able to dequeue after batch enqueue");
}

#[tokio::test]
async fn should_handle_batch_enqueue_operations_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_batch_enqueue_operations::<TcpQueueConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_batch_enqueue_operations_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_batch_enqueue_operations::<WsQueueConnector>(&server).await;
}

// Generic test helper for concurrent enqueue from multiple clients
async fn should_handle_concurrent_enqueue_from_multiple_clients<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client1 = C::connect(server).await.expect("connect 1");
    let mut client2 = C::connect(server).await.expect("connect 2");

    // Act - Both clients enqueue
    let frame1 = build_queue_enqueue("queue://test/concurrent", b"client-1-task");
    let response1 = client1
        .send_and_receive(&frame1, 2000)
        .await
        .expect("enqueue 1");

    let frame2 = build_queue_enqueue("queue://test/concurrent", b"client-2-task");
    let response2 = client2
        .send_and_receive(&frame2, 2000)
        .await
        .expect("enqueue 2");

    // Assert
    let (_msg_type, status1, _data) = parse_queue_response(&response1);
    let (_msg_type, status2, _data) = parse_queue_response(&response2);
    assert_eq!(status1, 0, "Client 1 enqueue should succeed");
    assert_eq!(status2, 0, "Client 2 enqueue should succeed");
}

#[tokio::test]
async fn should_handle_concurrent_enqueue_from_multiple_clients_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_concurrent_enqueue_from_multiple_clients::<TcpQueueConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_enqueue_from_multiple_clients_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_concurrent_enqueue_from_multiple_clients::<WsQueueConnector>(&server).await;
}

// Generic test helper for enqueue-dequeue-enqueue sequence
async fn should_handle_mixed_enqueue_dequeue_sequence<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Enqueue first message
    let frame1 = build_queue_enqueue("queue://test/mixed", b"msg-1");
    let response1 = client
        .send_and_receive(&frame1, 2000)
        .await
        .expect("enqueue 1");
    let (_msg_type, status, _data) = parse_queue_response(&response1);
    assert_eq!(status, 0);

    // Act - Dequeue
    let dequeue_frame = build_queue_dequeue("queue://test/mixed");
    let response = client
        .send_and_receive(&dequeue_frame, 2000)
        .await
        .expect("dequeue");
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0);

    // Act - Enqueue again
    let frame2 = build_queue_enqueue("queue://test/mixed", b"msg-2");
    let response2 = client
        .send_and_receive(&frame2, 2000)
        .await
        .expect("enqueue 2");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response2);
    assert_eq!(status, 0, "Should allow enqueue after dequeue");
}

#[tokio::test]
async fn should_handle_mixed_enqueue_dequeue_sequence_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_mixed_enqueue_dequeue_sequence::<TcpQueueConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_mixed_enqueue_dequeue_sequence_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_mixed_enqueue_dequeue_sequence::<WsQueueConnector>(&server).await;
}
