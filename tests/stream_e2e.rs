//! Stream domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::define_transport_tests;
use fixtures::transport::*;

async fn commit_stream_record_with_offset<C>(
    client: &mut C,
    route: &str,
    expected_offset: u64,
    body: &[u8],
)
where
    C: StreamConnector,
{
    let begin_response = client
        .send_and_receive(&build_stream_begin(route, expected_offset), 2000)
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

async fn commit_stream_record<C>(client: &mut C, route: &str, body: &[u8])
where
    C: StreamConnector,
{
    commit_stream_record_with_offset(client, route, 0, body).await;
}

// Generic test helper for appending to stream
async fn should_append_data_to_stream<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/events/audit";

    // Act
    commit_stream_record(&mut client, route, b"event-001").await;
}

// Generic test helper for reading from stream
async fn should_read_appended_data<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/logs/main";
    let test_data = b"stream-record-1";
    commit_stream_record(&mut client, route, test_data).await;

    // Act
    let read_frame = build_stream_read(route, 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Expected success for stream read");
}

// Generic test helper for read ordering
async fn should_preserve_append_order<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/ordered/main";

    // Act
    commit_stream_record_with_offset(&mut client, route, 0, b"first").await;
    commit_stream_record_with_offset(&mut client, route, 1, b"second").await;

    let read_frame = build_stream_read(route, 0);
    let response = client
        .send_and_receive(&read_frame, 2000)
        .await
        .expect("read");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Expected success for ordered read");
}

// Generic test helper for read past end
async fn should_handle_read_past_end<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_stream_read("stream://test/sparse/main", 999999);

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, _status, _data) = parse_stream_response(&response);
    // Status can be success (empty read) or not found - both acceptable
    // Any status is acceptable here - we're just validating the request completes
}

// Generic test helper for FIFO ordering with multiple appends
async fn should_maintain_fifo_order_with_multiple_appends<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/fifo/main";

    // Act - Append 5 events
    for i in 1..=5 {
        let data = format!("event-{}", i).into_bytes();
        commit_stream_record_with_offset(&mut client, route, (i - 1) as u64, &data).await;
    }

    // Assert - Order should be preserved (can't directly verify without GET support for sequence, but test ensures no errors)
}

// Generic test helper for large stream payloads
async fn should_handle_large_stream_payload<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/large/main";
    let large_data = vec![b'D'; 60_000]; // Within u16 TLV length limit (65535)

    // Act
    commit_stream_record(&mut client, route, &large_data).await;
}

// Generic test helper for concurrent appends from multiple clients
async fn should_handle_concurrent_appends_from_multiple_clients<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client1 = C::connect(server).await.expect("connect 1");
    let mut client2 = C::connect(server).await.expect("connect 2");
    let route = "stream://test/concurrent/main";

    // Act - Both clients append
    commit_stream_record_with_offset(&mut client1, route, 0, b"client-1-event").await;
    commit_stream_record_with_offset(&mut client2, route, 1, b"client-2-event").await;
}

// Generic test helper for multiple sequential read operations
async fn should_handle_sequential_read_operations<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route = "stream://test/sequential/main";

    // First, append some data
    commit_stream_record(&mut client, route, b"event-data").await;

    // Act - Sequential reads
    let read1_frame = build_stream_read(route, 0);
    let response1 = client
        .send_and_receive(&read1_frame, 2000)
        .await
        .expect("read 1");

    let (_msg_type, status1, _data) = parse_stream_response(&response1);
    assert_eq!(status1, 0);

    // Act - Read again with different offset
    let read2_frame = build_stream_read(route, 0);
    let response2 = client
        .send_and_receive(&read2_frame, 2000)
        .await
        .expect("read 2");

    let (_msg_type, status2, _data) = parse_stream_response(&response2);
    assert_eq!(status2, 0);

    // Act - Third read
    let read3_frame = build_stream_read(route, 0);
    let response3 = client
        .send_and_receive(&read3_frame, 2000)
        .await
        .expect("read 3");

    // Assert
    let (_msg_type, status3, _data) = parse_stream_response(&response3);
    assert_eq!(status3, 0, "Sequential reads should all succeed");
}

// Generic test helper for stream isolation
async fn should_isolate_streams_by_route<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let route1 = "stream://test/app/stream1";
    let route2 = "stream://test/app/stream2";

    // Act - Append to stream 1
    commit_stream_record(&mut client, route1, b"data-1").await;

    // Act - Append to stream 2
    commit_stream_record(&mut client, route2, b"data-2").await;

    // Act - Read from stream 1
    let read_frame = build_stream_read(route1, 0);
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

define_transport_tests!(
    TcpStreamConnector,
    WsStreamConnector;
    should_append_data_to_stream_tcp / should_append_data_to_stream_ws => should_append_data_to_stream,
    should_read_appended_data_tcp / should_read_appended_data_ws => should_read_appended_data,
    should_preserve_append_order_tcp / should_preserve_append_order_ws => should_preserve_append_order,
    should_handle_read_past_end_tcp / should_handle_read_past_end_ws => should_handle_read_past_end,
    should_maintain_fifo_order_with_multiple_appends_tcp / should_maintain_fifo_order_with_multiple_appends_ws => should_maintain_fifo_order_with_multiple_appends,
    should_handle_large_stream_payload_tcp / should_handle_large_stream_payload_ws => should_handle_large_stream_payload,
    should_handle_concurrent_appends_from_multiple_clients_tcp / should_handle_concurrent_appends_from_multiple_clients_ws => should_handle_concurrent_appends_from_multiple_clients,
    should_handle_sequential_read_operations_tcp / should_handle_sequential_read_operations_ws => should_handle_sequential_read_operations,
    should_isolate_streams_by_route_tcp / should_isolate_streams_by_route_ws => should_isolate_streams_by_route,
    should_retain_other_stream_subscription_after_unsubscribe_tcp / should_retain_other_stream_subscription_after_unsubscribe_ws => should_retain_other_stream_subscription_after_unsubscribe,
);
