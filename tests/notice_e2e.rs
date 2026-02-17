//! Notice end-to-end transport tests
//!
//! Tests notice (pub/sub) domain functionality across TCP and WebSocket transports.

mod fixtures;
use fixtures::transport::*;
use fitz::testkit::TestServer;

// ===== GENERIC TEST IMPLEMENTATIONS =====

async fn should_publish_to_subscribers<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Subscribe first
    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let _sub_response = client.send_and_receive(&subscribe_frame, 2000).await.expect("subscribe");

    // Act - Then publish
    let publish_frame = build_notice_publish("notice://test/events", "test-realm", b"hello");
    let pub_response = client.send_and_receive(&publish_frame, 2000).await.expect("publish");

    // Assert
    let (_msg_type, status, _data) = parse_notice_response(&pub_response);
    assert_eq!(status, 0, "Expected success for publish");
}

async fn should_reject_invalid_pattern<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Try to subscribe with badly formed pattern
    let bad_frame = build_notice_subscribe("");
    let response = client.send_and_receive(&bad_frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_notice_response(&response);
    // Should either error or timeout - either way, something's wrong with empty pattern
    assert!(
        status != 0,
        "Should reject empty subscription pattern"
    );
}

// ===== TCP TESTS =====

#[tokio::test]
async fn should_publish_to_subscribers_tcp() {
    let server = TestServer::start().await.expect("start");
    should_publish_to_subscribers::<TcpNoticeConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_invalid_pattern_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_invalid_pattern::<TcpNoticeConnector>(&server).await;
}

// ===== WEBSOCKET TESTS =====

#[tokio::test]
async fn should_publish_to_subscribers_ws() {
    let server = TestServer::start().await.expect("start");
    should_publish_to_subscribers::<WsNoticeConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_invalid_pattern_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_invalid_pattern::<WsNoticeConnector>(&server).await;
}
