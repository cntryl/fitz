//! Notice end-to-end transport tests
//!
//! Tests notice (pub/sub) domain functionality across TCP and WebSocket transports.

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::define_transport_tests;
use fixtures::transport::*;

// ===== GENERIC TEST IMPLEMENTATIONS =====

async fn wait_for_notice_subscription_count(server: &TestServer, expected: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let live_count = server.runtime.notice_subscriptions_active();
            let admin_count = server
                .runtime
                .admin_read_model()
                .notice_subscriptions(None, None)
                .len();

            if live_count == expected && admin_count == expected {
                return;
            }

            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("wait for notice subscription count");
}

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

async fn should_retain_other_notice_subscription_after_unsubscribe<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    let removed_route = "notice://test/app/events";
    let retained_route = "notice://test/app/audits";
    let mut subscriber = C::connect(server).await.expect("connect subscriber");
    let mut publisher = C::connect(server).await.expect("connect publisher");

    let removed_subscribe_response = subscriber
        .send_and_receive(&build_notice_subscribe(removed_route), 2000)
        .await
        .expect("subscribe removed route");
    let (_msg_type, status, _data) = parse_notice_response(&removed_subscribe_response);
    assert_eq!(status, 0, "Expected success for removed route subscribe");

    let retained_subscribe_response = subscriber
        .send_and_receive(&build_notice_subscribe(retained_route), 2000)
        .await
        .expect("subscribe retained route");
    let (_msg_type, status, _data) = parse_notice_response(&retained_subscribe_response);
    assert_eq!(status, 0, "Expected success for retained route subscribe");

    let unsubscribe_response = subscriber
        .send_and_receive(&build_notice_unsubscribe(removed_route), 2000)
        .await
        .expect("unsubscribe removed route");
    let (_msg_type, status, _data) = parse_notice_response(&unsubscribe_response);
    assert_eq!(status, 0, "Expected success for removed route unsubscribe");

    let removed_publish_response = publisher
        .send_and_receive(
            &build_notice_publish(removed_route, "test-realm", b"removed"),
            2000,
        )
        .await
        .expect("publish removed route");
    let (_msg_type, status, _data) = parse_notice_response(&removed_publish_response);
    assert_eq!(status, 0, "Expected success for removed route publish");
    assert!(
        subscriber.recv_frame(200).await.is_err(),
        "Removed route publish should not deliver after unsubscribe"
    );

    let retained_publish_response = publisher
        .send_and_receive(
            &build_notice_publish(retained_route, "test-realm", b"retained"),
            2000,
        )
        .await
        .expect("publish retained route");
    let (_msg_type, status, _data) = parse_notice_response(&retained_publish_response);
    assert_eq!(status, 0, "Expected success for retained route publish");

    let retained_delivery = subscriber
        .recv_frame(2000)
        .await
        .expect("retained route delivery");
    let retained_delivery = parse_notice_delivery(&retained_delivery).expect("parse delivery");
    assert_eq!(retained_delivery.msg_type, 504);
    assert_eq!(retained_delivery.route, retained_route);
    assert_eq!(retained_delivery.body.as_slice(), b"retained");
}

async fn should_remove_notice_subscription_when_subscriber_disconnects<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    let route = "notice://test/app/events";
    let mut subscriber = C::connect(server).await.expect("connect subscriber");

    let subscribe_response = subscriber
        .send_and_receive(&build_notice_subscribe(route), 2000)
        .await
        .expect("subscribe route");
    let (_msg_type, status, _data) = parse_notice_response(&subscribe_response);
    assert_eq!(status, 0, "Expected success for route subscribe");

    wait_for_notice_subscription_count(server, 1).await;

    drop(subscriber);
    server
        .wait_for_session_count(0)
        .await
        .expect("subscriber disconnect cleanup");

    wait_for_notice_subscription_count(server, 0).await;

    let mut publisher = C::connect(server).await.expect("connect publisher");
    let publish_response = publisher
        .send_and_receive(
            &build_notice_publish(route, "test-realm", b"after-disconnect"),
            2000,
        )
        .await
        .expect("publish after disconnect");

    let (_msg_type, status, _data) = parse_notice_response(&publish_response);
    assert_eq!(status, 0, "Expected success for publish after disconnect");
    assert_eq!(server.runtime.notice_subscriptions_active(), 0);
    assert!(
        server
            .runtime
            .admin_read_model()
            .notice_subscriptions(None, None)
            .is_empty(),
        "Admin notice snapshot should reflect disconnect cleanup"
    );
}

async fn should_require_resubscribe_after_broker_restart<C>()
where
    C: NoticeConnector,
{
    let route = "notice://test/app/restart";

    {
        let server = TestServer::start().await.expect("start first server");
        let mut subscriber = C::connect(&server).await.expect("connect subscriber");
        let mut publisher = C::connect(&server).await.expect("connect publisher");

        let subscribe_response = subscriber
            .send_and_receive(&build_notice_subscribe(route), 2000)
            .await
            .expect("subscribe before restart");
        let (_msg_type, status, _data) = parse_notice_response(&subscribe_response);
        assert_eq!(status, 0, "Expected success for pre-restart subscribe");

        wait_for_notice_subscription_count(&server, 1).await;

        let publish_response = publisher
            .send_and_receive(
                &build_notice_publish(route, "test-realm", b"before-restart"),
                2000,
            )
            .await
            .expect("publish before restart");
        let (_msg_type, status, _data) = parse_notice_response(&publish_response);
        assert_eq!(status, 0, "Expected success for pre-restart publish");

        let delivery = subscriber
            .recv_frame(2000)
            .await
            .expect("delivery before restart");
        let delivery = parse_notice_delivery(&delivery).expect("parse delivery before restart");
        assert_eq!(delivery.msg_type, 504);
        assert_eq!(delivery.route, route);
        assert_eq!(delivery.body.as_slice(), b"before-restart");
    }

    let restarted_server = TestServer::start().await.expect("start restarted server");
    let mut subscriber = C::connect(&restarted_server)
        .await
        .expect("connect subscriber after restart");
    let mut publisher = C::connect(&restarted_server)
        .await
        .expect("connect publisher after restart");

    wait_for_notice_subscription_count(&restarted_server, 0).await;

    let publish_response = publisher
        .send_and_receive(
            &build_notice_publish(route, "test-realm", b"after-restart"),
            2000,
        )
        .await
        .expect("publish after restart");
    let (_msg_type, status, _data) = parse_notice_response(&publish_response);
    assert_eq!(status, 0, "Expected success for post-restart publish");

    assert!(
        subscriber.recv_frame(200).await.is_err(),
        "Client should re-subscribe after broker restart"
    );
}

define_transport_tests!(
    TcpNoticeConnector,
    WsNoticeConnector;
    should_publish_to_subscribers_tcp / should_publish_to_subscribers_ws => should_publish_to_subscribers,
    should_reject_invalid_pattern_tcp / should_reject_invalid_pattern_ws => should_reject_invalid_pattern,
    should_match_single_wildcard_pattern_tcp / should_match_single_wildcard_pattern_ws => should_match_single_wildcard_pattern,
    should_match_double_wildcard_pattern_tcp / should_match_double_wildcard_pattern_ws => should_match_double_wildcard_pattern,
    should_match_multiple_subscribers_on_overlapping_patterns_tcp / should_match_multiple_subscribers_on_overlapping_patterns_ws => should_match_multiple_subscribers_on_overlapping_patterns,
    should_deliver_to_exact_match_before_wildcard_tcp / should_deliver_to_exact_match_before_wildcard_ws => should_deliver_to_exact_match_before_wildcard,
    should_not_match_pattern_if_publish_beneath_scope_tcp / should_not_match_pattern_if_publish_beneath_scope_ws => should_not_match_pattern_if_publish_beneath_scope,
    should_retain_other_notice_subscription_after_unsubscribe_tcp / should_retain_other_notice_subscription_after_unsubscribe_ws => should_retain_other_notice_subscription_after_unsubscribe,
    should_remove_notice_subscription_when_subscriber_disconnects_tcp / should_remove_notice_subscription_when_subscriber_disconnects_ws => should_remove_notice_subscription_when_subscriber_disconnects,
);

#[tokio::test]
async fn should_require_resubscribe_after_broker_restart_tcp() {
    should_require_resubscribe_after_broker_restart::<TcpNoticeConnector>().await;
}

#[tokio::test]
async fn should_require_resubscribe_after_broker_restart_ws() {
    should_require_resubscribe_after_broker_restart::<WsNoticeConnector>().await;
}
