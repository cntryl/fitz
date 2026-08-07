use super::common::*;

pub(crate) async fn should_retain_other_stream_subscription_after_unsubscribe<C>(
    server: &TestServer,
) where
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

pub(crate) async fn should_not_recover_stream_subscription_given_disconnect<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let route = "stream://test/app/events";
    let mut subscriber = C::connect(server).await.expect("connect subscriber");

    // Act
    let subscribe_response = subscriber
        .send_and_receive(&build_stream_subscribe(route), 2000)
        .await
        .expect("subscribe route");
    let (_msg_type, status, _data) = parse_stream_response(&subscribe_response);
    assert_eq!(status, 0, "Expected success for route subscribe");
    wait_for_stream_subscription_count(server, 1).await;

    drop(subscriber);
    server
        .wait_for_session_count(0)
        .await
        .expect("subscriber disconnect cleanup");

    // Assert
    wait_for_stream_subscription_count(server, 0).await;
    assert_eq!(server.runtime.stream_subscriptions_active(), 0);
}

pub(crate) async fn should_not_treat_stream_subscription_as_replay_cursor_given_shared_route<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let route = "stream://test/app/shared";
    let mut writer = C::connect(server).await.expect("connect writer");
    let mut subscriber = C::connect(server).await.expect("connect subscriber");

    // Act
    commit_stream_record(&mut writer, route, b"before-subscribe").await;

    let subscribe_response = subscriber
        .send_and_receive(&build_stream_subscribe(route), 2000)
        .await
        .expect("subscribe route");
    let (_msg_type, status, _data) = parse_stream_response(&subscribe_response);
    assert_eq!(status, 0, "Expected success for stream subscribe");

    commit_stream_record_with_offset(&mut writer, route, 1, b"after-subscribe").await;
    let live_delivery = subscriber
        .recv_frame(2000)
        .await
        .expect("live stream delivery");
    let live_delivery = parse_stream_delivery(&live_delivery).expect("parse stream delivery");

    let read_response = writer
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read full stream history");

    // Assert
    assert_eq!(live_delivery.msg_type, 609);
    assert_eq!(live_delivery.route, route);
    let delivery_payload: serde_json::Value =
        serde_json::from_slice(&live_delivery.body).expect("stream notify payload JSON");
    assert_eq!(delivery_payload["last_resource_offset"], 1);
    assert!(
        subscriber.recv_frame(200).await.is_err(),
        "stream subscribe should not replay committed history"
    );

    let records = parse_stream_read_records(&read_response);
    let bodies: Vec<Vec<u8>> = records.into_iter().map(|(_, body)| body).collect();
    assert_eq!(
        bodies,
        vec![b"before-subscribe".to_vec(), b"after-subscribe".to_vec()]
    );
}

pub(crate) async fn should_abort_uncommitted_stream_session_on_disconnect<C>(server: &TestServer)
where
    C: StreamConnector,
{
    let route = "stream://test/events/disconnect";

    let session_id = {
        let mut client = C::connect(server).await.expect("connect staging client");
        let begin_response = client
            .send_and_receive(&build_stream_begin(route), 2000)
            .await
            .expect("begin staging stream session");
        let (_msg_type, status, data) = parse_stream_response(&begin_response);
        assert_eq!(status, 0, "expected success for stream begin");
        let session_id = parse_stream_session_id(&data).expect("stream session id");

        let append_response = client
            .send_and_receive(&build_stream_append(session_id, 0, b"staged"), 2000)
            .await
            .expect("append staged stream event");
        let (_msg_type, status, _data) = parse_stream_response(&append_response);
        assert_eq!(status, 0, "expected success for staged append");
        session_id
    };

    server
        .wait_for_session_count(0)
        .await
        .expect("wait for disconnect cleanup");

    let mut client = C::connect(server)
        .await
        .expect("connect replacement client");

    let stale_commit_response = client
        .send_and_receive(&build_stream_commit(session_id), 2000)
        .await
        .expect("send stale commit");
    let (_msg_type, status, _data) = parse_stream_response(&stale_commit_response);
    assert_ne!(
        status, 0,
        "stale stream session should be gone after disconnect"
    );

    commit_stream_record(&mut client, route, b"committed").await;

    let read_response = client
        .send_and_receive(&build_stream_read(route, 0), 2000)
        .await
        .expect("read committed stream");
    let records = parse_stream_read_records(&read_response);
    assert_eq!(records, vec![(0, b"committed".to_vec())]);
}

pub(crate) async fn should_reject_invalid_stream_subscription_patterns<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let invalid_patterns = [
        "notice://test/app/events",
        "stream://test//events",
        "stream://test/app/event*",
        "stream://test/app",
        "stream://test/app/events/extra",
    ];

    // Act / Assert
    for pattern in invalid_patterns {
        for frame in [
            build_stream_subscribe(pattern),
            build_stream_unsubscribe(pattern),
        ] {
            let response = client
                .send_and_receive(&frame, 2000)
                .await
                .expect("invalid Stream subscription response");
            let (_message_type, status, data) = parse_stream_response(&response);
            assert_eq!(status, 1, "invalid pattern must fail: {pattern}");
            let mut decoder = PayloadDecoder::new(&data[1..]);
            let message = decoder
                .get_string()
                .expect("Stream subscription plain error envelope");
            assert!(!message.is_empty());
            assert!(decoder.is_complete());
        }
    }
}

pub(crate) async fn should_enforce_stream_wildcard_registration_limit<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let mut first_subscription_id = None;
    for index in 0..128 {
        let pattern = format!("stream://*/limit/{index}");
        let response = client
            .send_and_receive(&build_stream_subscribe(&pattern), 2000)
            .await
            .expect("Stream wildcard registration");
        let (_, status, data) = parse_stream_response(&response);
        assert_eq!(status, 0, "wildcard registration {index} must succeed");
        let id = parse_stream_session_id(&data).expect("Stream subscription id");
        first_subscription_id.get_or_insert(id);
    }

    // Act
    let duplicate = client
        .send_and_receive(&build_stream_subscribe("stream://*/limit/0"), 2000)
        .await
        .expect("duplicate Stream registration");
    let exact = client
        .send_and_receive(&build_stream_subscribe("stream://test/limit/exact"), 2000)
        .await
        .expect("exact Stream registration at wildcard cap");
    let overflow = client
        .send_and_receive(&build_stream_subscribe("stream://*/limit/overflow"), 2000)
        .await
        .expect("Stream wildcard overflow response");

    // Assert
    let (_, duplicate_status, duplicate_data) = parse_stream_response(&duplicate);
    assert_eq!(duplicate_status, 0);
    assert_eq!(
        parse_stream_session_id(&duplicate_data).expect("duplicate subscription id"),
        first_subscription_id.expect("first subscription id")
    );
    assert_eq!(parse_stream_response(&exact).1, 0);
    let (_, overflow_status, overflow_data) = parse_stream_response(&overflow);
    assert_eq!(overflow_status, 1);
    let mut decoder = PayloadDecoder::new(&overflow_data[1..]);
    let message = decoder
        .get_string()
        .expect("Stream overflow plain error envelope");
    assert!(message.contains("subscription limit"));
    assert!(decoder.is_complete());
}

pub(crate) async fn should_deliver_stream_notification_for_overlapping_wildcards<C>(
    server: &TestServer,
) where
    C: StreamConnector,
{
    // Arrange
    let route = "stream://test/app/wildcard-events";
    let mut subscriber = C::connect(server).await.expect("connect subscriber");
    let mut writer = C::connect(server).await.expect("connect writer");
    let mut subscription_ids = Vec::new();
    for pattern in ["stream://*/app/*", "stream://test/**"] {
        let response = subscriber
            .send_and_receive(&build_stream_subscribe(pattern), 2000)
            .await
            .expect("register Stream wildcard");
        let (_, status, data) = parse_stream_response(&response);
        assert_eq!(status, 0);
        subscription_ids.push(parse_stream_session_id(&data).expect("Stream subscription id"));
    }

    // Act
    commit_stream_record(&mut writer, route, b"event").await;
    let first = subscriber
        .recv_frame(2000)
        .await
        .expect("first Stream delivery");
    let second = subscriber
        .recv_frame(2000)
        .await
        .expect("second Stream delivery");

    // Assert
    let mut deliveries =
        [first, second].map(|frame| parse_stream_delivery(&frame).expect("Stream delivery"));
    deliveries.sort_by_key(|delivery| delivery.subscription_id);
    subscription_ids.sort_unstable();
    let delivery_ids = [deliveries[0].subscription_id, deliveries[1].subscription_id];
    assert_eq!(delivery_ids.as_slice(), subscription_ids.as_slice());
    assert!(deliveries.iter().all(|delivery| delivery.route == route));
}
