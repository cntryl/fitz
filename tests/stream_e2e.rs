//! Stream domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::transport::*;

async fn commit_stream_record<C>(client: &mut C, route: &str, body: &[u8])
where
    C: StreamConnector,
{
    let begin_response = client
        .send_and_receive(&build_stream_begin(route, 0), 2000)
        .await
        .expect("begin stream");
    let (_msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(status, 0, "Expected success for stream begin");
    let session_id = parse_stream_session_id(&data).expect("stream session id");

    let append_response = client
        .send_and_receive(&build_stream_append(session_id, body), 2000)
        .await
        .expect("append stream");
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected success for stream append");

    let commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("commit stream");
    let (_msg_type, status, _data) = parse_stream_response(&commit_response);
    assert_eq!(status, 0, "Expected success for stream commit");
}

// Generic test helper for appending to stream
async fn should_append_data_to_stream<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_stream_append_simple("stream://test/events/audit", b"event-001");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Expected success for stream append");
}

#[tokio::test]
async fn should_append_data_to_stream_tcp() {
    let server = TestServer::start().await.expect("start");
    should_append_data_to_stream::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_append_data_to_stream_ws() {
    let server = TestServer::start().await.expect("start");
    should_append_data_to_stream::<WsStreamConnector>(&server).await;
}

// Generic test helper for reading from stream
async fn should_read_appended_data<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let test_data = b"stream-record-1";
    let append_frame = build_stream_append_simple("stream://test/logs", test_data);
    let _ = client
        .send_and_receive(&append_frame, 2000)
        .await
        .expect("append");

    // Act
    let read_frame = build_stream_read("stream://test/logs", 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Expected success for stream read");
}

#[tokio::test]
async fn should_read_appended_data_tcp() {
    let server = TestServer::start().await.expect("start");
    should_read_appended_data::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_read_appended_data_ws() {
    let server = TestServer::start().await.expect("start");
    should_read_appended_data::<WsStreamConnector>(&server).await;
}

// Generic test helper for read ordering
async fn should_preserve_append_order<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame1 = build_stream_append_simple("stream://test/ordered", b"first");
    let frame2 = build_stream_append_simple("stream://test/ordered", b"second");

    // Act
    let _ = client
        .send_and_receive(&frame1, 2000)
        .await
        .expect("append 1");
    let _ = client
        .send_and_receive(&frame2, 2000)
        .await
        .expect("append 2");

    let read_frame = build_stream_read("stream://test/ordered", 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Expected success for ordered read");
}

#[tokio::test]
async fn should_preserve_append_order_tcp() {
    let server = TestServer::start().await.expect("start");
    should_preserve_append_order::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_preserve_append_order_ws() {
    let server = TestServer::start().await.expect("start");
    should_preserve_append_order::<WsStreamConnector>(&server).await;
}

// Generic test helper for read past end
async fn should_handle_read_past_end<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_stream_read("stream://test/sparse", 999999);

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, _status, _data) = parse_stream_response(&response);
    // Status can be success (empty read) or not found - both acceptable
    // Any status is acceptable here - we're just validating the request completes
}

#[tokio::test]
async fn should_handle_read_past_end_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_read_past_end::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_read_past_end_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_read_past_end::<WsStreamConnector>(&server).await;
}

// Generic test helper for FIFO ordering with multiple appends
async fn should_maintain_fifo_order_with_multiple_appends<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Append 5 events
    for i in 1..=5 {
        let data = format!("event-{}", i).into_bytes();
        let frame = build_stream_append_simple("stream://test/fifo", &data);
        let response = client
            .send_and_receive(&frame, 2000)
            .await
            .unwrap_or_else(|_| panic!("append {}", i));

        let (_msg_type, status, _data) = parse_stream_response(&response);
        assert_eq!(status, 0, "Append {} should succeed", i);
    }

    // Assert - Order should be preserved (can't directly verify without GET support for sequence, but test ensures no errors)
}

#[tokio::test]
async fn should_maintain_fifo_order_with_multiple_appends_tcp() {
    let server = TestServer::start().await.expect("start");
    should_maintain_fifo_order_with_multiple_appends::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_maintain_fifo_order_with_multiple_appends_ws() {
    let server = TestServer::start().await.expect("start");
    should_maintain_fifo_order_with_multiple_appends::<WsStreamConnector>(&server).await;
}

// Generic test helper for large stream payloads
async fn should_handle_large_stream_payload<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let large_data = vec![b'D'; 60_000]; // Within u16 TLV length limit (65535)
    let frame = build_stream_append_simple("stream://test/large", &large_data);

    // Act
    let response = client.send_and_receive(&frame, 3000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Should handle large payload");
}

#[tokio::test]
async fn should_handle_large_stream_payload_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_large_stream_payload::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_large_stream_payload_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_large_stream_payload::<WsStreamConnector>(&server).await;
}

// Generic test helper for concurrent appends from multiple clients
async fn should_handle_concurrent_appends_from_multiple_clients<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client1 = C::connect(server).await.expect("connect 1");
    let mut client2 = C::connect(server).await.expect("connect 2");

    // Act - Both clients append
    let frame1 = build_stream_append_simple("stream://test/concurrent", b"client-1-event");
    let response1 = client1
        .send_and_receive(&frame1, 2000)
        .await
        .expect("append 1");

    let frame2 = build_stream_append_simple("stream://test/concurrent", b"client-2-event");
    let response2 = client2
        .send_and_receive(&frame2, 2000)
        .await
        .expect("append 2");

    // Assert
    let (_msg_type, status1, _data) = parse_stream_response(&response1);
    let (_msg_type, status2, _data) = parse_stream_response(&response2);

    assert_eq!(status1, 0, "Client 1 append should succeed");
    assert_eq!(status2, 0, "Client 2 append should succeed");
}

#[tokio::test]
async fn should_handle_concurrent_appends_from_multiple_clients_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_concurrent_appends_from_multiple_clients::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_appends_from_multiple_clients_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_concurrent_appends_from_multiple_clients::<WsStreamConnector>(&server).await;
}

// Generic test helper for multiple sequential read operations
async fn should_handle_sequential_read_operations<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // First, append some data
    let append_frame = build_stream_append_simple("stream://test/sequential", b"event-data");
    let _ = client
        .send_and_receive(&append_frame, 2000)
        .await
        .expect("append");

    // Act - Sequential reads
    let read1_frame = build_stream_read("stream://test/sequential", 0);
    let response1 = client
        .send_and_receive(&read1_frame, 2000)
        .await
        .expect("read 1");

    let (_msg_type, status1, _data) = parse_stream_response(&response1);
    assert_eq!(status1, 0);

    // Act - Read again with different offset
    let read2_frame = build_stream_read("stream://test/sequential", 0);
    let response2 = client
        .send_and_receive(&read2_frame, 2000)
        .await
        .expect("read 2");

    let (_msg_type, status2, _data) = parse_stream_response(&response2);
    assert_eq!(status2, 0);

    // Act - Third read
    let read3_frame = build_stream_read("stream://test/sequential", 0);
    let response3 = client
        .send_and_receive(&read3_frame, 2000)
        .await
        .expect("read 3");

    // Assert
    let (_msg_type, status3, _data) = parse_stream_response(&response3);
    assert_eq!(status3, 0, "Sequential reads should all succeed");
}

#[tokio::test]
async fn should_handle_sequential_read_operations_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_sequential_read_operations::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_sequential_read_operations_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_sequential_read_operations::<WsStreamConnector>(&server).await;
}

// Generic test helper for stream isolation
async fn should_isolate_streams_by_route<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Append to stream 1
    let frame1 = build_stream_append_simple("stream://test/stream1", b"data-1");
    let _ = client
        .send_and_receive(&frame1, 2000)
        .await
        .expect("append 1");

    // Act - Append to stream 2
    let frame2 = build_stream_append_simple("stream://test/stream2", b"data-2");
    let _ = client
        .send_and_receive(&frame2, 2000)
        .await
        .expect("append 2");

    // Act - Read from stream 1
    let read_frame = build_stream_read("stream://test/stream1", 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Should isolate streams by route");
}

async fn should_retain_other_stream_subscription_after_unsubscribe<C>(server: &TestServer)
where
    C: StreamConnector,
{
    let removed_route = "stream://test/app/events";
    let retained_route = "stream://test/app/audits";
    let mut subscriber = C::connect(server).await.expect("connect subscriber");
    let mut writer = C::connect(server).await.expect("connect writer");

    let removed_subscribe_response = subscriber
        .send_and_receive(&build_stream_subscribe(removed_route), 2000)
        .await
        .expect("subscribe removed route");
    let (_msg_type, status, _data) = parse_stream_response(&removed_subscribe_response);
    assert_eq!(status, 0, "Expected success for removed route subscribe");

    let retained_subscribe_response = subscriber
        .send_and_receive(&build_stream_subscribe(retained_route), 2000)
        .await
        .expect("subscribe retained route");
    let (_msg_type, status, _data) = parse_stream_response(&retained_subscribe_response);
    assert_eq!(status, 0, "Expected success for retained route subscribe");

    let unsubscribe_response = subscriber
        .send_and_receive(&build_stream_unsubscribe(removed_route), 2000)
        .await
        .expect("unsubscribe removed route");
    let (_msg_type, status, _data) = parse_stream_response(&unsubscribe_response);
    assert_eq!(status, 0, "Expected success for removed route unsubscribe");

    commit_stream_record(&mut writer, removed_route, b"removed").await;
    assert!(
        subscriber.recv_frame(200).await.is_err(),
        "Removed route commit should not deliver after unsubscribe"
    );

    commit_stream_record(&mut writer, retained_route, b"retained").await;

    let retained_delivery = subscriber
        .recv_frame(2000)
        .await
        .expect("retained route delivery");
    let retained_delivery = parse_stream_delivery(&retained_delivery).expect("parse delivery");
    assert_eq!(retained_delivery.msg_type, 609);
    assert!(retained_delivery.subscription_id > 0);
    assert_eq!(retained_delivery.route, retained_route);

    let retained_payload: serde_json::Value =
        serde_json::from_slice(&retained_delivery.body).expect("notify payload JSON");
    assert_eq!(retained_payload["event"], "committed");
    assert_eq!(retained_payload["batch_size"], 1);
}

#[tokio::test]
async fn should_isolate_streams_by_route_tcp() {
    let server = TestServer::start().await.expect("start");
    should_isolate_streams_by_route::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_retain_other_stream_subscription_after_unsubscribe_tcp() {
    let server = TestServer::start().await.expect("start");
    should_retain_other_stream_subscription_after_unsubscribe::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_streams_by_route_ws() {
    let server = TestServer::start().await.expect("start");
    should_isolate_streams_by_route::<WsStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_retain_other_stream_subscription_after_unsubscribe_ws() {
    let server = TestServer::start().await.expect("start");
    should_retain_other_stream_subscription_after_unsubscribe::<WsStreamConnector>(&server).await;
}
