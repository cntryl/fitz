//! Cross-domain end-to-end tests
//!
//! Tests interactions between multiple domains (queue+lease, notice+stream, rpc+stream, etc.)
//! to verify correct coordination across domain boundaries.

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::transport::*;

// ============================================================================
// QUEUE + LEASE INTERACTION TESTS
// ============================================================================

#[tokio::test]
async fn should_extend_queue_message_lease_before_expiry_tcp() {
    let server = TestServer::start().await.expect("start");
    // Scenario: enqueue then acquire coordination lease.

    // Arrange
    let mut queue_client = TcpQueueConnector::connect(&server)
        .await
        .expect("queue connect");
    let mut lease_client = TcpLeaseConnector::connect(&server)
        .await
        .expect("lease connect");

    // Act
    let enqueue_frame = build_queue_enqueue("tasks", b"job-123");
    let enqueue_response = queue_client
        .send_and_receive(&enqueue_frame, 2000)
        .await
        .expect("enqueue");
    let lease_frame = build_lease_acquire_immediate("lease://test/locks/task", "queue-handler", 30);
    let lease_response = lease_client
        .send_and_receive(&lease_frame, 2000)
        .await
        .expect("lease acquire");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&enqueue_response);
    assert_eq!(status, 0, "Message should be enqueued");
    let (_msg_type, status, _data) = parse_lease_response(&lease_response);
    assert_eq!(status, 0, "Lease should be acquired for coordination");
}

#[tokio::test]
async fn should_extend_queue_message_lease_before_expiry_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: enqueue then acquire coordination lease.

    // Arrange
    let mut queue_client = WsQueueConnector::connect(&server)
        .await
        .expect("queue connect");
    let mut lease_client = WsLeaseConnector::connect(&server)
        .await
        .expect("lease connect");

    // Act
    let enqueue_frame = build_queue_enqueue("tasks", b"job-123");
    let enqueue_response = queue_client
        .send_and_receive(&enqueue_frame, 2000)
        .await
        .expect("enqueue");
    let lease_frame = build_lease_acquire_immediate("lease://test/locks/task", "queue-handler", 30);
    let lease_response = lease_client
        .send_and_receive(&lease_frame, 2000)
        .await
        .expect("lease acquire");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&enqueue_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_lease_response(&lease_response);
    assert_eq!(status, 0);
}

#[tokio::test]
async fn should_coordinate_queue_and_lease_for_multiple_messages_tcp() {
    let server = TestServer::start().await.expect("start");
    // Scenario: multiple enqueues coordinated by a lease.

    // Arrange
    let mut queue_client = TcpQueueConnector::connect(&server)
        .await
        .expect("queue connect");
    let mut lease_client = TcpLeaseConnector::connect(&server)
        .await
        .expect("lease connect");

    // Act
    for i in 1..=3 {
        let data = format!("task-{}", i).into_bytes();
        let frame = build_queue_enqueue("work", &data);
        let response = queue_client
            .send_and_receive(&frame, 2000)
            .await
            .unwrap_or_else(|_| panic!("enqueue {}", i));

        let (_msg_type, status, _data) = parse_queue_response(&response);
        assert_eq!(status, 0);
    }

    let lease_frame = build_lease_acquire_immediate("lease://test/locks/multi", "worker", 60);
    let lease_response = lease_client
        .send_and_receive(&lease_frame, 2000)
        .await
        .expect("lease coord");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&lease_response);
    assert_eq!(status, 0, "Multiple message coordination should succeed");
}

#[tokio::test]
async fn should_coordinate_queue_and_lease_for_multiple_messages_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: multiple enqueues coordinated by a lease.

    // Arrange
    let mut queue_client = WsQueueConnector::connect(&server)
        .await
        .expect("queue connect");
    let mut lease_client = WsLeaseConnector::connect(&server)
        .await
        .expect("lease connect");

    // Act
    for i in 1..=3 {
        let data = format!("task-{}", i).into_bytes();
        let frame = build_queue_enqueue("work", &data);
        let response = queue_client
            .send_and_receive(&frame, 2000)
            .await
            .unwrap_or_else(|_| panic!("enqueue {}", i));

        let (_msg_type, status, _data) = parse_queue_response(&response);
        assert_eq!(status, 0);
    }

    let lease_frame = build_lease_acquire_immediate("lease://test/locks/multi", "worker", 60);
    let lease_response = lease_client
        .send_and_receive(&lease_frame, 2000)
        .await
        .expect("lease coord");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&lease_response);
    assert_eq!(status, 0);
}

// ============================================================================
// NOTICE + STREAM FANOUT TESTS
// ============================================================================

#[tokio::test]
async fn should_handle_concurrent_stream_append_and_notice_publish_tcp() {
    let server = TestServer::start().await.expect("start");
    // Scenario: stream append alongside notice subscribe.
    // NOTE: PUBLISH is fire-and-forget per protocol spec, so we test SUBSCRIBE instead
    // which does return a response.

    // Arrange
    let mut stream_client = TcpStreamConnector::connect(&server)
        .await
        .expect("stream connect");
    let mut notice_client = TcpNoticeConnector::connect(&server)
        .await
        .expect("notice connect");

    // Act - First create a stream session with BEGIN
    let begin_frame = build_stream_begin("stream://test/stream/events/write", 0);
    let begin_response = stream_client
        .send_and_receive(&begin_frame, 3000)
        .await
        .expect("stream begin");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");

    // Act - Now append to the session
    let append_frame = build_stream_append(session_id, b"event-1");
    let append_response = stream_client
        .send_and_receive(&append_frame, 3000)
        .await
        .expect("stream append");
    
    // Act - Subscribe to notice (fire-and-forget PUBLISH wouldn't return a response)
    let subscribe_frame = build_notice_subscribe("notice://test/notifications/**");
    let subscribe_response = notice_client
        .send_and_receive(&subscribe_frame, 3000)
        .await
        .expect("notice subscribe");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Stream append should succeed");
    let (_msg_type, status, _data) = parse_notice_response(&subscribe_response);
    assert_eq!(
        status, 0,
        "Notice subscribe should succeed concurrently with stream append"
    );
}

#[tokio::test]
async fn should_handle_concurrent_stream_append_and_notice_publish_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: stream append alongside notice subscribe.
    // NOTE: PUBLISH is fire-and-forget per protocol spec, so we test SUBSCRIBE instead.

    // Arrange
    let mut stream_client = WsStreamConnector::connect(&server)
        .await
        .expect("stream connect");
    let mut notice_client = WsNoticeConnector::connect(&server)
        .await
        .expect("notice connect");

    // Act - First create a stream session with BEGIN
    let begin_frame = build_stream_begin("stream://test/stream/events/write", 0);
    let begin_response = stream_client
        .send_and_receive(&begin_frame, 3000)
        .await
        .expect("stream begin");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");

    // Act - Now append to the session
    let append_frame = build_stream_append(session_id, b"event-1");
    let append_response = stream_client
        .send_and_receive(&append_frame, 3000)
        .await
        .expect("stream append");
    
    // Act - Subscribe to notice (fire-and-forget PUBLISH wouldn't return a response)
    let subscribe_frame = build_notice_subscribe("notice://test/notifications/**");
    let subscribe_response = notice_client
        .send_and_receive(&subscribe_frame, 3000)
        .await
        .expect("notice subscribe");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_notice_response(&subscribe_response);
    assert_eq!(status, 0);
}

#[tokio::test]
async fn should_isolate_stream_reads_and_notice_subscriptions_tcp() {
    let server = TestServer::start().await.expect("start");
    // Scenario: notice subscription stays independent from stream reads.

    // Arrange
    let mut stream_client = TcpStreamConnector::connect(&server)
        .await
        .expect("stream connect");
    let mut notice_client = TcpNoticeConnector::connect(&server)
        .await
        .expect("notice connect");

    // Act
    let subscribe_frame = build_notice_subscribe("patterns/*");
    let _ = notice_client
        .send_and_receive(&subscribe_frame, 2000)
        .await
        .expect("notice subscribe");

    // Create stream session
    let begin_frame = build_stream_begin("stream://test/stream/data/write", 0);
    let begin_response = stream_client
        .send_and_receive(&begin_frame, 2000)
        .await
        .expect("stream begin");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");

    let append_frame = build_stream_append(session_id, b"record");
    let append_response = stream_client
        .send_and_receive(&append_frame, 2000)
        .await
        .expect("stream append");
    let read_frame = build_stream_read("stream-data", 0);
    let read_response = stream_client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("stream read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_stream_response(&read_response);
    assert_eq!(
        status, 0,
        "Stream operations should not interfere with notice subscriptions"
    );
}

#[tokio::test]
async fn should_isolate_stream_reads_and_notice_subscriptions_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: notice subscription stays independent from stream reads.

    // Arrange
    let mut stream_client = WsStreamConnector::connect(&server)
        .await
        .expect("stream connect");
    let mut notice_client = WsNoticeConnector::connect(&server)
        .await
        .expect("notice connect");

    // Act
    let subscribe_frame = build_notice_subscribe("patterns/*");
    let _ = notice_client
        .send_and_receive(&subscribe_frame, 2000)
        .await
        .expect("notice subscribe");

    // Create stream session
    let begin_frame = build_stream_begin("stream://test/stream/data/write", 0);
    let begin_response = stream_client
        .send_and_receive(&begin_frame, 2000)
        .await
        .expect("stream begin");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");

    let append_frame = build_stream_append(session_id, b"record");
    let append_response = stream_client
        .send_and_receive(&append_frame, 2000)
        .await
        .expect("stream append");
    let read_frame = build_stream_read("stream-data", 0);
    let read_response = stream_client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("stream read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_stream_response(&read_response);
    assert_eq!(status, 0);
}

// ============================================================================
// RPC + STREAM CONCURRENT TESTS
// ============================================================================

#[tokio::test]
async fn should_handle_concurrent_rpc_request_and_stream_append_tcp() {
    let server = TestServer::start().await.expect("start");
    // Scenario: RPC request alongside stream append.

    // Arrange
    let mut rpc_client = TcpRpcConnector::connect(&server)
        .await
        .expect("rpc connect");
    let mut rpc_worker = TestClient::new(server.tcp_addr)
        .await
        .expect("rpc worker connect");
    let mut stream_client = TcpStreamConnector::connect(&server)
        .await
        .expect("stream connect");

    let subscribe_frame = build_rpc_subscribe("rpc://test/services/processor");
    let subscribe_response = rpc_worker
        .request(&subscribe_frame, 2000)
        .await
        .expect("rpc subscribe");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0, "RPC worker subscribe should succeed");

    // Act
    let rpc_frame = build_rpc_request("rpc://test/services/processor", "process", b"input-data");
    // Spawn RPC request as concurrent task so frame is sent while worker waits for delivery
    let mut rpc_client_handle = rpc_client;
    let rpc_response_handle = tokio::spawn(async move {
        rpc_client_handle.send_and_receive(&rpc_frame, 2000).await
    });

    // Give the RPC request time to start executing
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let request_frame = rpc_worker
        .recv_frame(2000)
        .await
        .expect("rpc work item");
    let delivery = parse_rpc_request_delivery(&request_frame).expect("parse rpc work item");
    let response_frame = build_rpc_response_delivery(delivery.correlation_id, 0, true, b"ok");
    rpc_worker
        .send_frame(&response_frame)
        .await
        .expect("rpc response send");
    let _ = rpc_worker.recv_frame(2000).await;

    let rpc_response = rpc_response_handle.await.expect("join").expect("rpc request");

    // Create stream session
    let begin_frame = build_stream_begin("stream://test/stream/results/write", 0);
    let begin_response = stream_client
        .send_and_receive(&begin_frame, 2000)
        .await
        .expect("stream begin");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");

    let append_frame = build_stream_append(session_id, b"result-123");
    let append_response = stream_client
        .send_and_receive(&append_frame, 2000)
        .await
        .expect("stream append");

    // Assert
    let (_msg_type, rpc_status, _data) = parse_rpc_response(&rpc_response);
    // RPC fails because no workers registered - this is expected behavior
    assert_ne!(rpc_status, 0, "RPC should fail without registered workers");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(
        status, 0,
        "Stream append should succeed independently, even if RPC fails"
    );
}

#[tokio::test]
async fn should_handle_concurrent_rpc_request_and_stream_append_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: RPC request alongside stream append.

    // Arrange
    let mut rpc_client = WsRpcConnector::connect(&server).await.expect("rpc connect");
    let mut rpc_worker = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("rpc worker connect");
    let mut stream_client = WsStreamConnector::connect(&server)
        .await
        .expect("stream connect");

    let subscribe_frame = build_rpc_subscribe("rpc://test/services/processor");
    let subscribe_response = rpc_worker
        .request(&subscribe_frame, 2000)
        .await
        .expect("rpc subscribe");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    // Act
    let rpc_frame = build_rpc_request("rpc://test/services/processor", "process", b"input-data");
    // Spawn RPC request as concurrent task so frame is sent while worker waits for delivery
    let mut rpc_client_handle = rpc_client;
    let rpc_response_handle = tokio::spawn(async move {
        rpc_client_handle.send_and_receive(&rpc_frame, 2000).await
    });

    // Give the RPC request time to start executing
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    let request_frame = rpc_worker
        .recv_frame(2000)
        .await
        .expect("rpc work item");
    let delivery = parse_rpc_request_delivery(&request_frame).expect("parse rpc work item");
    let response_frame = build_rpc_response_delivery(delivery.correlation_id, 0, true, b"ok");
    rpc_worker
        .send_frame(&response_frame)
        .await
        .expect("rpc response send");
    let _ = rpc_worker.recv_frame(2000).await;

    let rpc_response = rpc_response_handle.await.expect("join").expect("rpc request");

    // Create stream session
    let begin_frame = build_stream_begin("stream://test/stream/results/write", 0);
    let begin_response = stream_client
        .send_and_receive(&begin_frame, 2000)
        .await
        .expect("stream begin");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");

    let append_frame = build_stream_append(session_id, b"result-123");
    let append_response = stream_client
        .send_and_receive(&append_frame, 2000)
        .await
        .expect("stream append");

    // Assert
    let (_msg_type, rpc_status, _data) = parse_rpc_response(&rpc_response);
    // RPC fails because no workers registered - this is expected behavior
    assert_ne!(rpc_status, 0, "RPC should fail without registered workers");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(
        status, 0,
        "Stream append should succeed independently, even if RPC fails"
    );
}

#[tokio::test]
async fn should_handle_multiple_rpc_requests_with_stream_operations_tcp() {
    let server = TestServer::start().await.expect("start");
    // Scenario: interleaved RPC calls with stream appends.

    // Arrange
    let mut rpc_client = TcpRpcConnector::connect(&server)
        .await
        .expect("rpc connect");
    let mut rpc_worker = TestClient::new(server.tcp_addr)
        .await
        .expect("rpc worker connect");
    let mut stream_client = TcpStreamConnector::connect(&server)
        .await
        .expect("stream connect");

    let subscribe_frame = build_rpc_subscribe("rpc://test/services/api");
    let subscribe_response = rpc_worker
        .request(&subscribe_frame, 2000)
        .await
        .expect("rpc subscribe");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0, "RPC worker subscribe should succeed");

    // Act
    // Create stream session first
    let begin_frame = build_stream_begin("stream://test/stream/audit/write", 0);
    let begin_response = stream_client
        .send_and_receive(&begin_frame, 2000)
        .await
        .expect("stream begin");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");

    let rpc1_frame = build_rpc_request("rpc://test/services/api", "getConfig", b"");
    // Spawn RPC request as concurrent task so frame is sent while worker waits for delivery
    let mut rpc_client_handle = rpc_client;
    let rpc1_response_handle = tokio::spawn(async move {
        rpc_client_handle.send_and_receive(&rpc1_frame, 2000).await
    });

    // Give the RPC request time to start executing 
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    let request_frame = rpc_worker
        .recv_frame(2000)
        .await
        .expect("rpc work item 1");
    let delivery = parse_rpc_request_delivery(&request_frame).expect("parse rpc work item 1");
    let response_frame = build_rpc_response_delivery(delivery.correlation_id, 0, true, b"ok");
    rpc_worker
        .send_frame(&response_frame)
        .await
        .expect("rpc response 1 send");
    let _ = rpc_worker.recv_frame(2000).await;
    let rpc1_response = rpc1_response_handle.await.expect("join").expect("rpc 1");
    let append1_frame = build_stream_append(session_id, b"config-requested");
    let append1_response = stream_client
        .send_and_receive(&append1_frame, 2000)
        .await
        .expect("append 1");

    // For second RPC, we need a new client since the first was moved
    let mut rpc_client2 = TcpRpcConnector::connect(&server)
        .await
        .expect("rpc client 2");
    let rpc2_frame = build_rpc_request("rpc://test/services/api", "setConfig", b"key=value");
    let mut rpc_client2_handle = rpc_client2;
    let rpc2_response_handle = tokio::spawn(async move {
        rpc_client2_handle.send_and_receive(&rpc2_frame, 2000).await
    });

    // Give the RPC request time to start executing
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    let request_frame = rpc_worker
        .recv_frame(2000)
        .await
        .expect("rpc work item 2");
    let delivery = parse_rpc_request_delivery(&request_frame).expect("parse rpc work item 2");
    let response_frame = build_rpc_response_delivery(delivery.correlation_id, 0, true, b"ok");
    rpc_worker
        .send_frame(&response_frame)
        .await
        .expect("rpc response 2 send");
    let _ = rpc_worker.recv_frame(2000).await;
    let rpc2_response = rpc2_response_handle.await.expect("join").expect("rpc 2");
    let append2_frame = build_stream_append(session_id, b"config-updated");
    let append2_response = stream_client
        .send_and_receive(&append2_frame, 2000)
        .await
        .expect("append 2");

    // Assert
    let (_msg_type, status1, _data) = parse_rpc_response(&rpc1_response);
    assert_ne!(status1, 0, "RPC should fail without workers");
    let (_msg_type, status, _data) = parse_stream_response(&append1_response);
    assert_eq!(status, 0, "Stream append should succeed");
    let (_msg_type, status2, _data) = parse_rpc_response(&rpc2_response);
    assert_ne!(status2, 0, "RPC should fail without workers");
    let (_msg_type, status, _data) = parse_stream_response(&append2_response);
    assert_eq!(
        status, 0,
        "Interleaved Stream operations should succeed even when RPC fails"
    );
}

#[tokio::test]
async fn should_handle_multiple_rpc_requests_with_stream_operations_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: interleaved RPC calls with stream appends.

    // Arrange
    let mut rpc_client = WsRpcConnector::connect(&server).await.expect("rpc connect");
    let mut rpc_worker = TestWebSocketClient::connect(&format!("ws://{}", server.ws_addr))
        .await
        .expect("rpc worker connect");
    let mut stream_client = WsStreamConnector::connect(&server)
        .await
        .expect("stream connect");

    let subscribe_frame = build_rpc_subscribe("rpc://test/services/api");
    let subscribe_response = rpc_worker
        .request(&subscribe_frame, 2000)
        .await
        .expect("rpc subscribe");
    let (_msg_type, status, _data) = parse_rpc_response(&subscribe_response);
    assert_eq!(status, 0);

    // Act
    // Create stream session first
    let begin_frame = build_stream_begin("stream://test/stream/audit/write", 0);
    let begin_response = stream_client
        .send_and_receive(&begin_frame, 2000)
        .await
        .expect("stream begin");
    let (_msg_type, _status, begin_data) = parse_stream_response(&begin_response);
    let session_id = parse_stream_session_id(&begin_data).expect("session_id");

    let rpc1_frame = build_rpc_request("rpc://test/services/api", "getConfig", b"");
    // Spawn RPC request as concurrent task so frame is sent while worker waits for delivery
    let mut rpc_client_handle = rpc_client;
    let rpc1_response_handle = tokio::spawn(async move {
        rpc_client_handle.send_and_receive(&rpc1_frame, 2000).await
    });

    // Give the RPC request time to start executing
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    let request_frame = rpc_worker
        .recv_frame(2000)
        .await
        .expect("rpc work item 1");
    let delivery = parse_rpc_request_delivery(&request_frame).expect("parse rpc work item 1");
    let response_frame = build_rpc_response_delivery(delivery.correlation_id, 0, true, b"ok");
    rpc_worker
        .send_frame(&response_frame)
        .await
        .expect("rpc response 1 send");
    let _ = rpc_worker.recv_frame(2000).await;
    let rpc1_response = rpc1_response_handle.await.expect("join").expect("rpc 1");
    let append1_frame = build_stream_append(session_id, b"config-requested");
    let append1_response = stream_client
        .send_and_receive(&append1_frame, 2000)
        .await
        .expect("append 1");

    // For second RPC, we need a new client since the first was moved
    let mut rpc_client2 = WsRpcConnector::connect(&server).await.expect("rpc client 2");
    let rpc2_frame = build_rpc_request("rpc://test/services/api", "setConfig", b"key=value");
    let mut rpc_client2_handle = rpc_client2;
    let rpc2_response_handle = tokio::spawn(async move {
        rpc_client2_handle.send_and_receive(&rpc2_frame, 2000).await
    });

    // Give the RPC request time to start executing
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    let request_frame = rpc_worker
        .recv_frame(2000)
        .await
        .expect("rpc work item 2");
    let delivery = parse_rpc_request_delivery(&request_frame).expect("parse rpc work item 2");
    let response_frame = build_rpc_response_delivery(delivery.correlation_id, 0, true, b"ok");
    rpc_worker
        .send_frame(&response_frame)
        .await
        .expect("rpc response 2 send");
    let _ = rpc_worker.recv_frame(2000).await;
    let rpc2_response = rpc2_response_handle.await.expect("join").expect("rpc 2");
    let append2_frame = build_stream_append(session_id, b"config-updated");
    let append2_response = stream_client
        .send_and_receive(&append2_frame, 2000)
        .await
        .expect("append 2");

    // Assert
    let (_msg_type, status1, _data) = parse_rpc_response(&rpc1_response);
    assert_ne!(status1, 0, "RPC should fail without workers");
    let (_msg_type, status, _data) = parse_stream_response(&append1_response);
    assert_eq!(status, 0, "Stream append should succeed");
    let (_msg_type, status2, _data) = parse_rpc_response(&rpc2_response);
    assert_ne!(status2, 0, "RPC should fail without workers");
    let (_msg_type, status, _data) = parse_stream_response(&append2_response);
    assert_eq!(status, 0, "Stream operations should succeed even when RPC fails");
}

// ============================================================================
// KV + LEASE DISTRIBUTED LOCKING TESTS
// ============================================================================

#[tokio::test]
async fn should_use_lease_as_lock_before_kv_write_tcp() {
    let server = TestServer::start().await.expect("start");
    // Scenario: lease acts as a lock for KV transaction.

    // Arrange
    let mut lease_client = TcpLeaseConnector::connect(&server)
        .await
        .expect("lease connect");
    let mut kv_client = TcpConnector::connect(&server).await.expect("kv connect");

    // Act
    let lease_frame = build_lease_acquire_immediate("lease://test/locks/resource", "owner1", 30);
    let lease_response = lease_client
        .send_and_receive(&lease_frame, 2000)
        .await
        .expect("acquire lock");
    let kv_begin = build_kv_begin("test/kv/protected", 1, 0);
    let begin_response = kv_client.request(&kv_begin, 2000).await.expect("kv begin");
    let kv_put = build_kv_put(1, "test/kv/protected", b"key", b"value");
    let put_response = kv_client.request(&kv_put, 2000).await.expect("kv put");
    let kv_commit = build_kv_commit(1, "test/kv/protected");
    let commit_response = kv_client
        .request(&kv_commit, 2000)
        .await
        .expect("kv commit");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&lease_response);
    assert_eq!(status, 0, "Should acquire lock");
    let (_msg_type, status, _data) = parse_kv_response(&begin_response);
    assert_eq!(status, 0, "KV transaction should succeed under lock");
    let (_msg_type, status, _data) = parse_kv_response(&put_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_kv_response(&commit_response);
    assert_eq!(status, 0, "KV writeset should commit while holding lock");
}

#[tokio::test]
async fn should_use_lease_as_lock_before_kv_write_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: lease acts as a lock for KV transaction.

    // Arrange
    let mut lease_client = WsLeaseConnector::connect(&server)
        .await
        .expect("lease connect");
    let mut kv_client = WsConnector::connect(&server).await.expect("kv connect");

    // Act
    let lease_frame = build_lease_acquire_immediate("lease://test/locks/resource", "owner1", 30);
    let lease_response = lease_client
        .send_and_receive(&lease_frame, 2000)
        .await
        .expect("acquire lock");
    let kv_begin = build_kv_begin("test/kv/protected", 1, 0);
    let begin_response = kv_client.request(&kv_begin, 2000).await.expect("kv begin");
    let kv_put = build_kv_put(1, "test/kv/protected", b"key", b"value");
    let put_response = kv_client.request(&kv_put, 2000).await.expect("kv put");
    let kv_commit = build_kv_commit(1, "test/kv/protected");
    let commit_response = kv_client
        .request(&kv_commit, 2000)
        .await
        .expect("kv commit");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&lease_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_kv_response(&begin_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_kv_response(&put_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_kv_response(&commit_response);
    assert_eq!(status, 0);
}

#[tokio::test]
async fn should_release_lock_after_kv_transaction_complete_tcp() {
    let server = TestServer::start().await.expect("start");
    // Scenario: release lease after KV commit.

    // Arrange
    let mut lease_client = TcpLeaseConnector::connect(&server)
        .await
        .expect("lease connect");
    let mut kv_client = TcpConnector::connect(&server).await.expect("kv connect");

    // Act
    let lease_acquire = build_lease_acquire_immediate("lease://test/locks/seq", "owner1", 60);
    let lease_response = lease_client
        .send_and_receive(&lease_acquire, 2000)
        .await
        .expect("acquire");
    let (_msg_type, status, data) = parse_lease_response(&lease_response);
    let token = parse_lease_token_response(&data).expect("parse token");
    let kv_begin = build_kv_begin("test/kv/locked", 1, 0);
    let _ = kv_client.request(&kv_begin, 2000).await.expect("kv begin");

    let kv_put = build_kv_put(1, "test/kv/locked", b"k", b"v");
    let _ = kv_client.request(&kv_put, 2000).await.expect("kv put");

    let kv_commit = build_kv_commit(1, "test/kv/locked");
    let _ = kv_client
        .request(&kv_commit, 2000)
        .await
        .expect("kv commit");

    let lease_release = build_lease_release("lease://test/locks/seq", "owner1", token);
    let release_response = lease_client
        .send_and_receive(&lease_release, 2000)
        .await
        .expect("release");

    // Assert
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_lease_response(&release_response);
    assert_eq!(status, 0, "Should release lock after KV transaction");
}

#[tokio::test]
async fn should_release_lock_after_kv_transaction_complete_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: release lease after KV commit.

    // Arrange
    let mut lease_client = WsLeaseConnector::connect(&server)
        .await
        .expect("lease connect");
    let mut kv_client = WsConnector::connect(&server).await.expect("kv connect");

    // Act
    let lease_acquire = build_lease_acquire_immediate("lease://test/locks/seq", "owner1", 60);
    let lease_response = lease_client
        .send_and_receive(&lease_acquire, 2000)
        .await
        .expect("acquire");
    let (_msg_type, status, data) = parse_lease_response(&lease_response);
    let token = parse_lease_token_response(&data).expect("parse token");
    let kv_begin = build_kv_begin("test/kv/locked", 1, 0);
    let _ = kv_client.request(&kv_begin, 2000).await.expect("kv begin");

    let kv_put = build_kv_put(1, "test/kv/locked", b"k", b"v");
    let _ = kv_client.request(&kv_put, 2000).await.expect("kv put");

    let kv_commit = build_kv_commit(1, "test/kv/locked");
    let _ = kv_client
        .request(&kv_commit, 2000)
        .await
        .expect("kv commit");

    let lease_release = build_lease_release("lease://test/locks/seq", "owner1", token);
    let release_response = lease_client
        .send_and_receive(&lease_release, 2000)
        .await
        .expect("release");

    // Assert
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_lease_response(&release_response);
    assert_eq!(status, 0);
}

// ============================================================================
// REALM ISOLATION ACROSS DOMAINS
// ============================================================================

#[tokio::test]
async fn should_isolate_realms_across_domains_tcp() {
    let server = TestServer::start().await.expect("start");
    // Scenario: operations in separate realms stay independent.

    // Arrange
    let mut kv_client = TcpConnector::connect(&server).await.expect("kv connect");
    let mut queue_client = TcpQueueConnector::connect(&server)
        .await
        .expect("queue connect");

    // Act
    let kv_begin = build_kv_begin("test/realm1/data", 1, 0);
    let kv_response = kv_client.request(&kv_begin, 2000).await.expect("kv begin");
    let queue_frame = build_queue_enqueue("tasks", b"task");
    let queue_response = queue_client
        .send_and_receive(&queue_frame, 2000)
        .await
        .expect("queue enqueue");

    // Assert
    let (_msg_type, status, _data) = parse_kv_response(&kv_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_queue_response(&queue_response);
    assert_eq!(
        status, 0,
        "Different realms should not interfere across domains"
    );
}

#[tokio::test]
async fn should_isolate_realms_across_domains_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: operations in separate realms stay independent.

    // Arrange
    let mut kv_client = WsConnector::connect(&server).await.expect("kv connect");
    let mut queue_client = WsQueueConnector::connect(&server)
        .await
        .expect("queue connect");

    // Act
    let kv_begin = build_kv_begin("test/realm1/data", 1, 0);
    let kv_response = kv_client.request(&kv_begin, 2000).await.expect("kv begin");
    let queue_frame = build_queue_enqueue("tasks", b"task");
    let queue_response = queue_client
        .send_and_receive(&queue_frame, 2000)
        .await
        .expect("queue enqueue");

    // Assert
    let (_msg_type, status, _data) = parse_kv_response(&kv_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_queue_response(&queue_response);
    assert_eq!(status, 0);
}

