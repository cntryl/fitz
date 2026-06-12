//! Notice end-to-end transport tests
//!
//! Tests notice (pub/sub) domain functionality across TCP and WebSocket transports.

mod fixtures;
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestServer;
use fixtures::define_transport_tests;
use fixtures::transport::*;

const MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION: usize = 128;

// ===== GENERIC TEST IMPLEMENTATIONS =====

async fn wait_for_notice_subscription_count(server: &TestServer, expected: usize) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let live_count = server.runtime.notice_subscriptions_active();
            let admin_count = server.runtime.notice_list_subscriptions(None, None).len();

            if live_count == expected && admin_count == expected {
                return;
            }

            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("wait for notice subscription count");
}

fn decode_notice_subscription_id(data: &[u8]) -> Option<u64> {
    let mut decoder = PayloadDecoder::new(data);
    let subscription_id = decoder.get_optional_u64().expect("notice subscription id");
    assert!(decoder.is_complete());
    subscription_id
}

fn decode_notice_error_message(data: &[u8]) -> String {
    let mut decoder = PayloadDecoder::new(data);
    let _error_code = decoder.get_u32().expect("notice error code");
    let error = decoder.get_string().expect("notice error message");
    assert!(decoder.is_complete());
    error
}

async fn subscribe_notice_pattern_expect_ok<C>(client: &mut C, pattern: &str) -> Option<u64>
where
    C: NoticeConnector,
{
    let response = client
        .send_and_receive(&build_notice_subscribe(pattern), 2000)
        .await
        .expect("subscribe notice pattern");
    let (_msg_type, status, data) = parse_notice_response(&response);
    assert_eq!(status, 0, "Expected success for notice subscribe");
    decode_notice_subscription_id(&data)
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
    client.send_frame(&publish_frame).await.expect("publish");
    let delivery = client.recv_frame(2000).await.expect("notice delivery");
    let delivery = parse_notice_delivery(&delivery).expect("parse notice delivery");

    // Assert
    assert_eq!(delivery.msg_type, 504);
    assert_eq!(delivery.route, "notice://test/events");
    assert_eq!(delivery.body.as_slice(), b"hello");
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
    client.send_frame(&publish_frame).await.expect("publish");
    let delivery = client.recv_frame(2000).await.expect("wildcard delivery");
    let delivery = parse_notice_delivery(&delivery).expect("parse wildcard delivery");

    // Assert
    assert_eq!(delivery.msg_type, 504);
    assert_eq!(delivery.route, "notice://test/app/users");
    assert_eq!(delivery.body.as_slice(), b"event1");
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
    client.send_frame(&publish_frame).await.expect("publish");
    let delivery = client
        .recv_frame(2000)
        .await
        .expect("double wildcard delivery");
    let delivery = parse_notice_delivery(&delivery).expect("parse double wildcard delivery");

    // Assert
    assert_eq!(delivery.msg_type, 504);
    assert_eq!(delivery.route, "notice://test/app/feature/events");
    assert_eq!(delivery.body.as_slice(), b"deep");
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
    client1.send_frame(&publish_frame).await.expect("publish");
    let delivery1 = client1.recv_frame(2000).await.expect("delivery 1");
    let delivery1 = parse_notice_delivery(&delivery1).expect("parse delivery 1");
    let delivery2 = client2.recv_frame(2000).await.expect("delivery 2");
    let delivery2 = parse_notice_delivery(&delivery2).expect("parse delivery 2");

    // Assert
    assert_eq!(delivery1.route, "notice://test/app/events");
    assert_eq!(delivery1.body.as_slice(), b"shared");
    assert_eq!(delivery2.route, "notice://test/app/events");
    assert_eq!(delivery2.body.as_slice(), b"shared");
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
    client.send_frame(&publish_frame).await.expect("publish");
    let delivery = client.recv_frame(2000).await.expect("exact delivery");
    let delivery = parse_notice_delivery(&delivery).expect("parse exact delivery");

    // Assert
    assert_eq!(delivery.route, "notice://test/exact/route");
    assert_eq!(delivery.body.as_slice(), b"exact");
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
    client.send_frame(&publish_frame).await.expect("publish");

    // Assert - Should not match (or error)
    assert!(
        client.recv_frame(200).await.is_err(),
        "Deep publish should not deliver when subscribe route is exact"
    );
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
    let (_msg_type, status, data) = parse_notice_response(&removed_subscribe_response);
    assert_eq!(status, 0, "Expected success for removed route subscribe");
    let removed_subscription_id =
        decode_notice_subscription_id(&data).expect("removed subscription id");

    let retained_subscribe_response = subscriber
        .send_and_receive(&build_notice_subscribe(retained_route), 2000)
        .await
        .expect("subscribe retained route");
    let (_msg_type, status, data) = parse_notice_response(&retained_subscribe_response);
    assert_eq!(status, 0, "Expected success for retained route subscribe");
    let retained_subscription_id =
        decode_notice_subscription_id(&data).expect("retained subscription id");

    let unsubscribe_response = subscriber
        .send_and_receive(&build_notice_unsubscribe(removed_subscription_id), 2000)
        .await
        .expect("unsubscribe removed route");
    let (_msg_type, status, _data) = parse_notice_response(&unsubscribe_response);
    assert_eq!(status, 0, "Expected success for removed route unsubscribe");

    publisher
        .send_frame(&build_notice_publish(
            removed_route,
            "test-realm",
            b"removed",
        ))
        .await
        .expect("publish removed route");
    assert!(
        subscriber.recv_frame(200).await.is_err(),
        "Removed route publish should not deliver after unsubscribe"
    );

    publisher
        .send_frame(&build_notice_publish(
            retained_route,
            "test-realm",
            b"retained",
        ))
        .await
        .expect("publish retained route");

    let retained_delivery = subscriber
        .recv_frame(2000)
        .await
        .expect("retained route delivery");
    let retained_delivery = parse_notice_delivery(&retained_delivery).expect("parse delivery");
    assert_eq!(retained_delivery.msg_type, 504);
    assert_eq!(retained_delivery.subscription_id, retained_subscription_id);
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
    publisher
        .send_frame(&build_notice_publish(
            route,
            "test-realm",
            b"after-disconnect",
        ))
        .await
        .expect("publish after disconnect");
    assert_eq!(server.runtime.notice_subscriptions_active(), 0);
    assert!(
        server
            .runtime
            .notice_list_subscriptions(None, None)
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

        publisher
            .send_frame(&build_notice_publish(
                route,
                "test-realm",
                b"before-restart",
            ))
            .await
            .expect("publish before restart");

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

    publisher
        .send_frame(&build_notice_publish(route, "test-realm", b"after-restart"))
        .await
        .expect("publish after restart");

    assert!(
        subscriber.recv_frame(200).await.is_err(),
        "Client should re-subscribe after broker restart"
    );
}

async fn should_reject_wildcard_subscription_when_session_limit_is_exceeded<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect subscriber");

    for pattern_index in 0..MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION {
        let pattern = format!("notice://test/limit/{pattern_index}/*");
        let subscription_id = subscribe_notice_pattern_expect_ok(&mut client, &pattern).await;
        assert!(
            subscription_id.is_some(),
            "Wildcard subscribe should return an id"
        );
    }

    wait_for_notice_subscription_count(server, MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION).await;

    // Act
    let overflow_response = client
        .send_and_receive(
            &build_notice_subscribe("notice://test/limit/overflow/*"),
            2000,
        )
        .await
        .expect("subscribe overflow wildcard pattern");

    // Assert
    let (_msg_type, status, data) = parse_notice_response(&overflow_response);
    assert_eq!(status, 1, "Expected wildcard limit rejection");
    assert_eq!(
        decode_notice_error_message(&data),
        "wildcard subscription limit exceeded (128 per session)"
    );
    wait_for_notice_subscription_count(server, MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION).await;
}

async fn should_allow_exact_subscription_when_wildcard_limit_is_reached<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect subscriber");

    for pattern_index in 0..MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION {
        let pattern = format!("notice://test/exact-limit/{pattern_index}/*");
        let subscription_id = subscribe_notice_pattern_expect_ok(&mut client, &pattern).await;
        assert!(
            subscription_id.is_some(),
            "Wildcard subscribe should return an id"
        );
    }

    wait_for_notice_subscription_count(server, MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION).await;

    // Act
    let exact_response = client
        .send_and_receive(
            &build_notice_subscribe("notice://test/exact-limit/final"),
            2000,
        )
        .await
        .expect("subscribe exact pattern after wildcard limit");

    // Assert
    let (_msg_type, status, data) = parse_notice_response(&exact_response);
    assert_eq!(
        status, 0,
        "Expected exact subscribe to bypass wildcard limit"
    );
    assert!(
        decode_notice_subscription_id(&data).is_some(),
        "Exact subscribe should return an id"
    );
    wait_for_notice_subscription_count(server, MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION + 1).await;
}

async fn should_allow_wildcard_subscription_after_unsubscribe_releases_session_budget<C>(
    server: &TestServer,
) where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect subscriber");
    let mut first_subscription_id = None;

    for pattern_index in 0..MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION {
        let pattern = format!("notice://test/release/{pattern_index}/*");
        let subscription_id = subscribe_notice_pattern_expect_ok(&mut client, &pattern).await;
        assert!(
            subscription_id.is_some(),
            "Wildcard subscribe should return an id"
        );
        if pattern_index == 0 {
            first_subscription_id = subscription_id;
        }
    }

    wait_for_notice_subscription_count(server, MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION).await;

    let unsubscribe_response = client
        .send_and_receive(
            &build_notice_unsubscribe(first_subscription_id.expect("first subscription id")),
            2000,
        )
        .await
        .expect("unsubscribe wildcard pattern");
    let (_msg_type, status, _data) = parse_notice_response(&unsubscribe_response);
    assert_eq!(status, 0, "Expected unsubscribe to succeed");
    wait_for_notice_subscription_count(server, MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION - 1).await;

    // Act
    let replacement_response = client
        .send_and_receive(
            &build_notice_subscribe("notice://test/release/replacement/*"),
            2000,
        )
        .await
        .expect("subscribe replacement wildcard pattern");

    // Assert
    let (_msg_type, status, data) = parse_notice_response(&replacement_response);
    assert_eq!(
        status, 0,
        "Expected wildcard subscribe after unsubscribe to succeed"
    );
    assert!(
        decode_notice_subscription_id(&data).is_some(),
        "Replacement wildcard subscribe should return an id"
    );
    wait_for_notice_subscription_count(server, MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION).await;
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
    should_reject_wildcard_subscription_when_session_limit_is_exceeded_tcp / should_reject_wildcard_subscription_when_session_limit_is_exceeded_ws => should_reject_wildcard_subscription_when_session_limit_is_exceeded,
    should_allow_exact_subscription_when_wildcard_limit_is_reached_tcp / should_allow_exact_subscription_when_wildcard_limit_is_reached_ws => should_allow_exact_subscription_when_wildcard_limit_is_reached,
    should_allow_wildcard_subscription_after_unsubscribe_releases_session_budget_tcp / should_allow_wildcard_subscription_after_unsubscribe_releases_session_budget_ws => should_allow_wildcard_subscription_after_unsubscribe_releases_session_budget,
);

#[tokio::test]
async fn should_require_resubscribe_after_broker_restart_tcp() {
    should_require_resubscribe_after_broker_restart::<TcpNoticeConnector>().await;
}

#[tokio::test]
async fn should_require_resubscribe_after_broker_restart_ws() {
    should_require_resubscribe_after_broker_restart::<WsNoticeConnector>().await;
}
