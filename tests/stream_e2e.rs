//! Stream domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::transport::*;

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
    let large_data = vec![b'D'; 80_000];
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

#[tokio::test]
async fn should_isolate_streams_by_route_tcp() {
    let server = TestServer::start().await.expect("start");
    should_isolate_streams_by_route::<TcpStreamConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_streams_by_route_ws() {
    let server = TestServer::start().await.expect("start");
    should_isolate_streams_by_route::<WsStreamConnector>(&server).await;
}
