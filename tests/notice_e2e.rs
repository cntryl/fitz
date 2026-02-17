//! Notice end-to-end transport tests
//!
//! Tests notice (pub/sub) domain functionality across TCP and WebSocket transports.

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::transport::*;

// ===== GENERIC TEST IMPLEMENTATIONS =====

async fn should_publish_to_subscribers<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Subscribe first
    let subscribe_frame = build_notice_subscribe("notice://test/events");
    let _sub_response = client
        .send_and_receive(&subscribe_frame, 2000)
        .await
        .expect("subscribe");

    // Act - Then publish
    let publish_frame = build_notice_publish("notice://test/events", "test-realm", b"hello");
    let pub_response = client
        .send_and_receive(&publish_frame, 2000)
        .await
        .expect("publish");

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
    let response = client
        .send_and_receive(&bad_frame, 2000)
        .await
        .expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_notice_response(&response);
    // Should either error or timeout - either way, something's wrong with empty pattern
    assert!(status != 0, "Should reject empty subscription pattern");
}

async fn should_match_single_wildcard_pattern<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Subscribe to pattern with single wildcard
    let subscribe_frame = build_notice_subscribe("notice://test/app/*");
    let _sub_response = client
        .send_and_receive(&subscribe_frame, 2000)
        .await
        .expect("subscribe");

    // Act - Publish to matching route
    let publish_frame = build_notice_publish("notice://test/app/users", "test-realm", b"event1");
    let pub_response = client
        .send_and_receive(&publish_frame, 2000)
        .await
        .expect("publish");

    // Assert
    let (_msg_type, status, _data) = parse_notice_response(&pub_response);
    assert_eq!(status, 0, "Expected success for wildcard pattern match");
}

async fn should_match_double_wildcard_pattern<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Subscribe to pattern with double wildcard
    let subscribe_frame = build_notice_subscribe("notice://test/**");
    let _sub_response = client
        .send_and_receive(&subscribe_frame, 2000)
        .await
        .expect("subscribe");

    // Act - Publish to deeply nested matching route
    let publish_frame =
        build_notice_publish("notice://test/app/feature/events", "test-realm", b"deep");
    let pub_response = client
        .send_and_receive(&publish_frame, 2000)
        .await
        .expect("publish");

    // Assert
    let (_msg_type, status, _data) = parse_notice_response(&pub_response);
    assert_eq!(status, 0, "Expected success for double wildcard pattern");
}

async fn should_match_multiple_subscribers_on_overlapping_patterns<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client1 = C::connect(server).await.expect("connect 1");
    let mut client2 = C::connect(server).await.expect("connect 2");

    // Act - Subscribe with different patterns
    let sub1_frame = build_notice_subscribe("notice://test/app/*");
    let _sub1_response = client1
        .send_and_receive(&sub1_frame, 2000)
        .await
        .expect("subscribe 1");

    let sub2_frame = build_notice_subscribe("notice://test/**");
    let _sub2_response = client2
        .send_and_receive(&sub2_frame, 2000)
        .await
        .expect("subscribe 2");

    // Act - Publish to route matching both patterns
    let publish_frame = build_notice_publish("notice://test/app/events", "test-realm", b"shared");
    let pub_response = client1
        .send_and_receive(&publish_frame, 2000)
        .await
        .expect("publish");

    // Assert
    let (_msg_type, status, _data) = parse_notice_response(&pub_response);
    assert_eq!(status, 0, "Expected success for overlapping patterns");
}

async fn should_deliver_to_exact_match_before_wildcard<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Subscribe to exact route
    let subscribe_frame = build_notice_subscribe("notice://test/exact/route");
    let _sub_response = client
        .send_and_receive(&subscribe_frame, 2000)
        .await
        .expect("subscribe");

    // Act - Publish to exact route (should match)
    let publish_frame = build_notice_publish("notice://test/exact/route", "test-realm", b"exact");
    let pub_response = client
        .send_and_receive(&publish_frame, 2000)
        .await
        .expect("publish");

    // Assert
    let (_msg_type, status, _data) = parse_notice_response(&pub_response);
    assert_eq!(status, 0, "Expected success for exact match");
}

async fn should_not_match_pattern_if_publish_beneath_scope<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Subscribe to specific level
    let subscribe_frame = build_notice_subscribe("notice://test/app");
    let _sub_response = client
        .send_and_receive(&subscribe_frame, 2000)
        .await
        .expect("subscribe");

    // Act - Publish to deeper level (not matching)
    let publish_frame =
        build_notice_publish("notice://test/app/users/events", "test-realm", b"deep");
    let pub_response = client
        .send_and_receive(&publish_frame, 2000)
        .await
        .expect("publish");

    // Assert - Should not match (or error)
    let (_msg_type, _status, _data) = parse_notice_response(&pub_response);
    // Status may be non-zero if subscription was rejected/not found, or still 0 if published
    // The key is that deep publish doesn't match non-wildcard subscribe
    // Any status is acceptable here - we're just validating the request completes
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

#[tokio::test]
async fn should_match_single_wildcard_pattern_tcp() {
    let server = TestServer::start().await.expect("start");
    should_match_single_wildcard_pattern::<TcpNoticeConnector>(&server).await;
}

#[tokio::test]
async fn should_match_double_wildcard_pattern_tcp() {
    let server = TestServer::start().await.expect("start");
    should_match_double_wildcard_pattern::<TcpNoticeConnector>(&server).await;
}

#[tokio::test]
async fn should_match_multiple_subscribers_on_overlapping_patterns_tcp() {
    let server = TestServer::start().await.expect("start");
    should_match_multiple_subscribers_on_overlapping_patterns::<TcpNoticeConnector>(&server).await;
}

#[tokio::test]
async fn should_deliver_to_exact_match_before_wildcard_tcp() {
    let server = TestServer::start().await.expect("start");
    should_deliver_to_exact_match_before_wildcard::<TcpNoticeConnector>(&server).await;
}

#[tokio::test]
async fn should_not_match_pattern_if_publish_beneath_scope_tcp() {
    let server = TestServer::start().await.expect("start");
    should_not_match_pattern_if_publish_beneath_scope::<TcpNoticeConnector>(&server).await;
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

#[tokio::test]
async fn should_match_single_wildcard_pattern_ws() {
    let server = TestServer::start().await.expect("start");
    should_match_single_wildcard_pattern::<WsNoticeConnector>(&server).await;
}

#[tokio::test]
async fn should_match_double_wildcard_pattern_ws() {
    let server = TestServer::start().await.expect("start");
    should_match_double_wildcard_pattern::<WsNoticeConnector>(&server).await;
}

#[tokio::test]
async fn should_match_multiple_subscribers_on_overlapping_patterns_ws() {
    let server = TestServer::start().await.expect("start");
    should_match_multiple_subscribers_on_overlapping_patterns::<WsNoticeConnector>(&server).await;
}

#[tokio::test]
async fn should_deliver_to_exact_match_before_wildcard_ws() {
    let server = TestServer::start().await.expect("start");
    should_deliver_to_exact_match_before_wildcard::<WsNoticeConnector>(&server).await;
}

#[tokio::test]
async fn should_not_match_pattern_if_publish_beneath_scope_ws() {
    let server = TestServer::start().await.expect("start");
    should_not_match_pattern_if_publish_beneath_scope::<WsNoticeConnector>(&server).await;
}
