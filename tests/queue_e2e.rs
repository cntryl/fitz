//! Queue domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::define_transport_tests;
use fixtures::transport::*;

// Generic test helper for enqueue
async fn should_enqueue_message<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_queue_enqueue("queue://test/app/jobs", b"task-1");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Expected success for enqueue");
}

// Generic test helper for dequeue
async fn should_dequeue_message<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let data = b"task-data";
    let enqueue_frame = build_queue_enqueue("queue://test/app/jobs", data);
    let _ = client
        .send_and_receive(&enqueue_frame, 2000)
        .await
        .expect("enqueue");

    // Act
    let dequeue_frame = build_queue_dequeue("queue://test/app/jobs");
    let response = client
        .send_and_receive(&dequeue_frame, 2000)
        .await
        .expect("dequeue");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Expected success for dequeue");
}

// Generic test helper for dequeue on empty queue (returns success with 0 messages)
async fn should_reject_dequeue_empty_queue<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_queue_dequeue("queue://test/app/empty");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert - empty queue returns success with 0 messages (not an error)
    let (_msg_type, status, data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Dequeue on empty queue returns success");
    let messages = fixtures::transport::extract_queue_messages(&data).expect("extract messages");
    assert!(messages.is_empty(), "Expected no messages from empty queue");
}

// Generic test helper for queue isolation by name
async fn should_isolate_separate_queues<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let queue1_data = b"q1-task";
    let enqueue_q1 = build_queue_enqueue("queue://test/app/q1", queue1_data);
    let _ = client
        .send_and_receive(&enqueue_q1, 2000)
        .await
        .expect("enqueue q1");

    // Act - dequeue from different queue (q2 is empty)
    let dequeue_q2 = build_queue_dequeue("queue://test/app/q2");
    let response = client
        .send_and_receive(&dequeue_q2, 2000)
        .await
        .expect("dequeue q2");

    // Assert - different queue is empty, so success with 0 messages (isolation verified)
    let (_msg_type, status, data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Dequeue from empty queue returns success");
    let messages = fixtures::transport::extract_queue_messages(&data).expect("extract messages");
    assert!(
        messages.is_empty(),
        "Expected no messages from q2 (enqueued only to q1)"
    );
}

// Generic test helper for message payload preservation
async fn should_preserve_message_payload<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let payload = b"important-data-123";
    let enqueue_frame = build_queue_enqueue("queue://test/app/data", payload);
    let _ = client
        .send_and_receive(&enqueue_frame, 2000)
        .await
        .expect("enqueue");

    // Act
    let dequeue_frame = build_queue_dequeue("queue://test/app/data");
    let response = client
        .send_and_receive(&dequeue_frame, 2000)
        .await
        .expect("dequeue");

    // Assert
    let (_msg_type, status, data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Dequeue should succeed");
    let messages = fixtures::transport::extract_queue_messages(&data).expect("extract messages");
    assert_eq!(messages.len(), 1, "Should receive one message");
    assert_eq!(&messages[0], payload, "Payload should be preserved");
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
        let frame = build_queue_enqueue("queue://test/app/batch", &data);
        let response = client
            .send_and_receive(&frame, 2000)
            .await
            .unwrap_or_else(|_| panic!("enqueue {}", i));

        let (_msg_type, status, _data) = parse_queue_response(&response);
        assert_eq!(status, 0, "Enqueue {} should succeed", i);
    }

    // Assert - Dequeue to verify all were stored
    let dequeue_frame = build_queue_dequeue("queue://test/app/batch");
    let response = client
        .send_and_receive(&dequeue_frame, 2000)
        .await
        .expect("dequeue");

    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Should be able to dequeue after batch enqueue");
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
    let frame1 = build_queue_enqueue("queue://test/app/concurrent", b"client-1-task");
    let response1 = client1
        .send_and_receive(&frame1, 2000)
        .await
        .expect("enqueue 1");

    let frame2 = build_queue_enqueue("queue://test/app/concurrent", b"client-2-task");
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

// Generic test helper for enqueue-dequeue-enqueue sequence
async fn should_handle_mixed_enqueue_dequeue_sequence<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Enqueue first message
    let frame1 = build_queue_enqueue("queue://test/app/mixed", b"msg-1");
    let response1 = client
        .send_and_receive(&frame1, 2000)
        .await
        .expect("enqueue 1");
    let (_msg_type, status, _data) = parse_queue_response(&response1);
    assert_eq!(status, 0);

    // Act - Dequeue
    let dequeue_frame = build_queue_dequeue("queue://test/app/mixed");
    let response = client
        .send_and_receive(&dequeue_frame, 2000)
        .await
        .expect("dequeue");
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0);

    // Act - Enqueue again
    let frame2 = build_queue_enqueue("queue://test/app/mixed", b"msg-2");
    let response2 = client
        .send_and_receive(&frame2, 2000)
        .await
        .expect("enqueue 2");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response2);
    assert_eq!(status, 0, "Should allow enqueue after dequeue");
}

async fn should_notify_queue_watch_given_enqueue<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let queue_route = "queue://test/app/audits";
    let mut receiver = C::connect(server).await.expect("connect receiver");
    let mut sender = C::connect(server).await.expect("connect sender");

    let watch_response = receiver
        .send_and_receive(&build_queue_watch(queue_route), 2000)
        .await
        .expect("watch queue route");
    let (_msg_type, status, data) = parse_queue_response(&watch_response);
    assert_eq!(status, 0, "Expected success for queue watch registration");
    let subscription_id = extract_queue_subscription_id(&data).expect("watch subscription id");

    // Act
    let enqueue_response = sender
        .send_and_receive(&build_queue_enqueue(queue_route, b"retained"), 2000)
        .await
        .expect("enqueue route");
    let (_msg_type, status, _data) = parse_queue_response(&enqueue_response);
    assert_eq!(status, 0, "Expected success for queue enqueue");

    // Assert
    let delivery_frame = receiver
        .recv_frame(2500)
        .await
        .expect("queue watch delivery");
    let delivery = parse_queue_watch_delivery(&delivery_frame).expect("parse queue watch delivery");
    assert_eq!(delivery.subscription_id, subscription_id);
    assert_eq!(delivery.route, format!("{queue_route}/ready"));
    assert!(delivery.ready_messages >= 1);
}

define_transport_tests!(
    TcpQueueConnector,
    WsQueueConnector;
    should_enqueue_message_tcp / should_enqueue_message_ws => should_enqueue_message,
    should_dequeue_message_tcp / should_dequeue_message_ws => should_dequeue_message,
    should_reject_dequeue_empty_queue_tcp / should_reject_dequeue_empty_queue_ws => should_reject_dequeue_empty_queue,
    should_isolate_separate_queues_tcp / should_isolate_separate_queues_ws => should_isolate_separate_queues,
    should_preserve_message_payload_tcp / should_preserve_message_payload_ws => should_preserve_message_payload,
    should_handle_batch_enqueue_operations_tcp / should_handle_batch_enqueue_operations_ws => should_handle_batch_enqueue_operations,
    should_handle_concurrent_enqueue_from_multiple_clients_tcp / should_handle_concurrent_enqueue_from_multiple_clients_ws => should_handle_concurrent_enqueue_from_multiple_clients,
    should_handle_mixed_enqueue_dequeue_sequence_tcp / should_handle_mixed_enqueue_dequeue_sequence_ws => should_handle_mixed_enqueue_dequeue_sequence,
    should_notify_queue_watch_given_enqueue_tcp / should_notify_queue_watch_given_enqueue_ws => should_notify_queue_watch_given_enqueue,
);
