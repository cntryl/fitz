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
    let lease_frame = build_lease_acquire_immediate("task-coordination", "queue-handler", 30);
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
    let lease_frame = build_lease_acquire_immediate("task-coordination", "queue-handler", 30);
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
            .expect(&format!("enqueue {}", i));

        let (_msg_type, status, _data) = parse_queue_response(&response);
        assert_eq!(status, 0);
    }

    let lease_frame = build_lease_acquire_immediate("multi-coordination", "worker", 60);
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
            .expect(&format!("enqueue {}", i));

        let (_msg_type, status, _data) = parse_queue_response(&response);
        assert_eq!(status, 0);
    }

    let lease_frame = build_lease_acquire_immediate("multi-coordination", "worker", 60);
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
    // Scenario: stream append alongside notice publish.

    // Arrange
    let mut stream_client = TcpStreamConnector::connect(&server)
        .await
        .expect("stream connect");
    let mut notice_client = TcpNoticeConnector::connect(&server)
        .await
        .expect("notice connect");

    // Act
    let append_frame = build_stream_append("events", b"event-1");
    let append_response = stream_client
        .send_and_receive(&append_frame, 2000)
        .await
        .expect("stream append");
    let publish_frame = build_notice_publish("notifications", "realm", b"event-1-notification");
    let publish_response = notice_client
        .send_and_receive(&publish_frame, 2000)
        .await
        .expect("notice publish");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Stream append should succeed");
    let (_msg_type, status, _data) = parse_notice_response(&publish_response);
    assert_eq!(
        status, 0,
        "Notice publish should succeed concurrently with stream append"
    );
}

#[tokio::test]
async fn should_handle_concurrent_stream_append_and_notice_publish_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: stream append alongside notice publish.

    // Arrange
    let mut stream_client = WsStreamConnector::connect(&server)
        .await
        .expect("stream connect");
    let mut notice_client = WsNoticeConnector::connect(&server)
        .await
        .expect("notice connect");

    // Act
    let append_frame = build_stream_append("events", b"event-1");
    let append_response = stream_client
        .send_and_receive(&append_frame, 2000)
        .await
        .expect("stream append");
    let publish_frame = build_notice_publish("notifications", "realm", b"event-1-notification");
    let publish_response = notice_client
        .send_and_receive(&publish_frame, 2000)
        .await
        .expect("notice publish");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_notice_response(&publish_response);
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
    let append_frame = build_stream_append("stream-data", b"record");
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
    let append_frame = build_stream_append("stream-data", b"record");
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
    let mut stream_client = TcpStreamConnector::connect(&server)
        .await
        .expect("stream connect");

    // Act
    let rpc_frame = build_rpc_request("services/processor", "process", b"input-data");
    let rpc_response = rpc_client
        .send_and_receive(&rpc_frame, 2000)
        .await
        .expect("rpc request");
    let append_frame = build_stream_append("results", b"result-123");
    let append_response = stream_client
        .send_and_receive(&append_frame, 2000)
        .await
        .expect("stream append");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&rpc_response);
    assert_eq!(status, 0, "RPC request should succeed");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(
        status, 0,
        "Stream append should succeed concurrently with RPC"
    );
}

#[tokio::test]
async fn should_handle_concurrent_rpc_request_and_stream_append_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: RPC request alongside stream append.

    // Arrange
    let mut rpc_client = WsRpcConnector::connect(&server).await.expect("rpc connect");
    let mut stream_client = WsStreamConnector::connect(&server)
        .await
        .expect("stream connect");

    // Act
    let rpc_frame = build_rpc_request("services/processor", "process", b"input-data");
    let rpc_response = rpc_client
        .send_and_receive(&rpc_frame, 2000)
        .await
        .expect("rpc request");
    let append_frame = build_stream_append("results", b"result-123");
    let append_response = stream_client
        .send_and_receive(&append_frame, 2000)
        .await
        .expect("stream append");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response(&rpc_response);
    assert_eq!(status, 0);
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0);
}

#[tokio::test]
async fn should_handle_multiple_rpc_requests_with_stream_operations_tcp() {
    let server = TestServer::start().await.expect("start");
    // Scenario: interleaved RPC calls with stream appends.

    // Arrange
    let mut rpc_client = TcpRpcConnector::connect(&server)
        .await
        .expect("rpc connect");
    let mut stream_client = TcpStreamConnector::connect(&server)
        .await
        .expect("stream connect");

    // Act
    let rpc1_frame = build_rpc_request("services/api", "getConfig", b"");
    let rpc1_response = rpc_client
        .send_and_receive(&rpc1_frame, 2000)
        .await
        .expect("rpc 1");
    let append1_frame = build_stream_append("audit", b"config-requested");
    let append1_response = stream_client
        .send_and_receive(&append1_frame, 2000)
        .await
        .expect("append 1");
    let rpc2_frame = build_rpc_request("services/api", "setConfig", b"key=value");
    let rpc2_response = rpc_client
        .send_and_receive(&rpc2_frame, 2000)
        .await
        .expect("rpc 2");
    let append2_frame = build_stream_append("audit", b"config-updated");
    let append2_response = stream_client
        .send_and_receive(&append2_frame, 2000)
        .await
        .expect("append 2");

    // Assert
    let (_msg_type, status1, _data) = parse_rpc_response(&rpc1_response);
    assert_eq!(status1, 0);
    let (_msg_type, status, _data) = parse_stream_response(&append1_response);
    assert_eq!(status, 0);
    let (_msg_type, status2, _data) = parse_rpc_response(&rpc2_response);
    assert_eq!(status2, 0);
    let (_msg_type, status, _data) = parse_stream_response(&append2_response);
    assert_eq!(
        status, 0,
        "Interleaved RPC and stream operations should succeed"
    );
}

#[tokio::test]
async fn should_handle_multiple_rpc_requests_with_stream_operations_ws() {
    let server = TestServer::start().await.expect("start");
    // Scenario: interleaved RPC calls with stream appends.

    // Arrange
    let mut rpc_client = WsRpcConnector::connect(&server).await.expect("rpc connect");
    let mut stream_client = WsStreamConnector::connect(&server)
        .await
        .expect("stream connect");

    // Act
    let rpc1_frame = build_rpc_request("services/api", "getConfig", b"");
    let rpc1_response = rpc_client
        .send_and_receive(&rpc1_frame, 2000)
        .await
        .expect("rpc 1");
    let append1_frame = build_stream_append("audit", b"config-requested");
    let append1_response = stream_client
        .send_and_receive(&append1_frame, 2000)
        .await
        .expect("append 1");
    let rpc2_frame = build_rpc_request("services/api", "setConfig", b"key=value");
    let rpc2_response = rpc_client
        .send_and_receive(&rpc2_frame, 2000)
        .await
        .expect("rpc 2");
    let append2_frame = build_stream_append("audit", b"config-updated");
    let append2_response = stream_client
        .send_and_receive(&append2_frame, 2000)
        .await
        .expect("append 2");

    // Assert
    let (_msg_type, status1, _data) = parse_rpc_response(&rpc1_response);
    assert_eq!(status1, 0);
    let (_msg_type, status, _data) = parse_stream_response(&append1_response);
    assert_eq!(status, 0);
    let (_msg_type, status2, _data) = parse_rpc_response(&rpc2_response);
    assert_eq!(status2, 0);
    let (_msg_type, status, _data) = parse_stream_response(&append2_response);
    assert_eq!(status, 0);
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
    let lease_frame = build_lease_acquire_immediate("resource-lock", "owner1", 30);
    let lease_response = lease_client
        .send_and_receive(&lease_frame, 2000)
        .await
        .expect("acquire lock");
    let kv_begin = build_kv_begin("protected-data", 1, 0);
    let begin_response = kv_client.request(&kv_begin, 2000).await.expect("kv begin");
    let kv_put = build_kv_put(1, "protected-data", b"key", b"value");
    let put_response = kv_client.request(&kv_put, 2000).await.expect("kv put");
    let kv_commit = build_kv_commit(1, "protected-data");
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
    let lease_frame = build_lease_acquire_immediate("resource-lock", "owner1", 30);
    let lease_response = lease_client
        .send_and_receive(&lease_frame, 2000)
        .await
        .expect("acquire lock");
    let kv_begin = build_kv_begin("protected-data", 1, 0);
    let begin_response = kv_client.request(&kv_begin, 2000).await.expect("kv begin");
    let kv_put = build_kv_put(1, "protected-data", b"key", b"value");
    let put_response = kv_client.request(&kv_put, 2000).await.expect("kv put");
    let kv_commit = build_kv_commit(1, "protected-data");
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
    let lease_acquire = build_lease_acquire_immediate("seq-lock", "owner1", 60);
    let lease_response = lease_client
        .send_and_receive(&lease_acquire, 2000)
        .await
        .expect("acquire");
    let (_msg_type, status, data) = parse_lease_response(&lease_response);
    let token = parse_lease_token_response(&data).expect("parse token");
    let kv_begin = build_kv_begin("locked-data", 1, 0);
    let _ = kv_client.request(&kv_begin, 2000).await.expect("kv begin");

    let kv_put = build_kv_put(1, "locked-data", b"k", b"v");
    let _ = kv_client.request(&kv_put, 2000).await.expect("kv put");

    let kv_commit = build_kv_commit(1, "locked-data");
    let _ = kv_client
        .request(&kv_commit, 2000)
        .await
        .expect("kv commit");

    let lease_release = build_lease_release("seq-lock", "owner1", token);
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
    let lease_acquire = build_lease_acquire_immediate("seq-lock", "owner1", 60);
    let lease_response = lease_client
        .send_and_receive(&lease_acquire, 2000)
        .await
        .expect("acquire");
    let (_msg_type, status, data) = parse_lease_response(&lease_response);
    let token = parse_lease_token_response(&data).expect("parse token");
    let kv_begin = build_kv_begin("locked-data", 1, 0);
    let _ = kv_client.request(&kv_begin, 2000).await.expect("kv begin");

    let kv_put = build_kv_put(1, "locked-data", b"k", b"v");
    let _ = kv_client.request(&kv_put, 2000).await.expect("kv put");

    let kv_commit = build_kv_commit(1, "locked-data");
    let _ = kv_client
        .request(&kv_commit, 2000)
        .await
        .expect("kv commit");

    let lease_release = build_lease_release("seq-lock", "owner1", token);
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
    let kv_begin = build_kv_begin("data", 1, 0);
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
    let kv_begin = build_kv_begin("data", 1, 0);
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
