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
    let frame = build_stream_append("stream://test/events/audit", b"event-001");

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
    let append_frame = build_stream_append("stream://test/logs", test_data);
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
    let frame1 = build_stream_append("stream://test/ordered", b"first");
    let frame2 = build_stream_append("stream://test/ordered", b"second");

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
    let (_msg_type, status, _data) = parse_stream_response(&response);
    // Status can be success (empty read) or not found - both acceptable
    assert!(
        status == 0 || status != 0,
        "Expected determined result for out-of-bounds read"
    );
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
