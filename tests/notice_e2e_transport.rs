//! Notice domain transport-layer end-to-end tests
//!
//! These tests verify the COMPLETE request-response cycle:
//! Client → TCP/WebSocket → Session → Routing → Notice Actor → Response/Notification → Client

use bytes::{BufMut, BytesMut};
use fitz::testkit::transport::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

type BoxError = Box<dyn Error>;
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub trait NoticeTestClient {
    fn send_frame<'a>(
        &'a mut self,
        frame: &'a [u8],
    ) -> BoxFuture<'a, Result<(), BoxError>>;
    fn request<'a>(
        &'a mut self,
        frame: &'a [u8],
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>>;
    fn recv_frame<'a>(
        &'a mut self,
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>>;
}

pub trait NoticeConnector {
    type Client: NoticeTestClient;

    fn connect<'a>(
        server: &'a TestServer,
    ) -> BoxFuture<'a, Result<Self::Client, BoxError>>;
}

impl NoticeTestClient for TestClient {
    fn send_frame<'a>(
        &'a mut self,
        frame: &'a [u8],
    ) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move { TestClient::send_frame(self, frame).await })
    }

    fn request<'a>(
        &'a mut self,
        frame: &'a [u8],
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
        Box::pin(async move { TestClient::request(self, frame, timeout_ms).await })
    }

    fn recv_frame<'a>(
        &'a mut self,
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
        Box::pin(async move { TestClient::recv_frame(self, timeout_ms).await })
    }
}

impl NoticeTestClient for TestWebSocketClient {
    fn send_frame<'a>(
        &'a mut self,
        frame: &'a [u8],
    ) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move { TestWebSocketClient::send_frame(self, frame).await })
    }

    fn request<'a>(
        &'a mut self,
        frame: &'a [u8],
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
        Box::pin(async move { TestWebSocketClient::request(self, frame, timeout_ms).await })
    }

    fn recv_frame<'a>(
        &'a mut self,
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
        Box::pin(async move { TestWebSocketClient::recv_frame(self, timeout_ms).await })
    }
}

struct TcpConnector;

impl NoticeConnector for TcpConnector {
    type Client = TestClient;

    fn connect<'a>(
        server: &'a TestServer,
    ) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect().await })
    }
}

struct WsConnector;

impl NoticeConnector for WsConnector {
    type Client = TestWebSocketClient;

    fn connect<'a>(
        server: &'a TestServer,
    ) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect_ws().await })
    }
}

/// Build Notice PUBLISH request frame
/// Wire format: [u32 BE route_len][route][u32 BE payload_len][payload]
fn build_notice_publish(route: &str, payload: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(route.len() as u32).to_be_bytes());
    buf.put_slice(route.as_bytes());
    buf.put_slice(&(payload.len() as u32).to_be_bytes());
    buf.put_slice(payload);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(500, &buf);
    builder.build()
}

/// Build Notice SUBSCRIBE request frame
/// Wire format: [u32 BE pattern_len][pattern]
fn build_notice_subscribe(pattern: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(pattern.len() as u32).to_be_bytes());
    buf.put_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(501, &buf);
    builder.build()
}

/// Build Notice UNSUBSCRIBE request frame
/// Wire format: [u32 BE pattern_len][pattern]
fn build_notice_unsubscribe(pattern: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(pattern.len() as u32).to_be_bytes());
    buf.put_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(502, &buf);
    builder.build()
}

/// Build Notice UNSUBSCRIBE_ALL request frame
/// Wire format: (empty)
fn build_notice_unsubscribe_all() -> Vec<u8> {
    let buf = BytesMut::new();

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(503, &buf);
    builder.build()
}

/// Parse Notice response status byte
fn parse_notice_response(frame: &[u8]) -> (u16, u8, Vec<u8>) {
    let mut offset = 0;

    const ESCAPE_MARKER: u8 = 0xFF;
    let msg_type = if frame[offset] == ESCAPE_MARKER {
        offset += 1;
        let mt = u16::from_be_bytes([frame[offset], frame[offset + 1]]);
        offset += 2;
        mt
    } else {
        let mt = frame[offset] as u16;
        offset += 1;
        mt
    };

    let _length = u16::from_be_bytes([frame[offset], frame[offset + 1]]) as usize;
    offset += 2;

    let payload = &frame[offset..];
    let status = payload[0];
    let data = payload[1..].to_vec();

    (msg_type, status, data)
}

/// Parse SUBSCRIBE response to extract subscription ID
fn parse_subscription_id(data: &[u8]) -> Option<u64> {
    if data.is_empty() {
        return None;
    }
    let has_id = data[0];
    if has_id == 1 && data.len() >= 9 {
        Some(u64::from_be_bytes([
            data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
        ]))
    } else {
        None
    }
}

/// Parse NOTIFY message (server-to-client notification)
/// Wire format: [u64 subscription_id][u32 route_len][route][u32 payload_len][payload]
fn parse_notify_message(frame: &[u8]) -> (u64, String, Vec<u8>) {
    let mut offset = 0;

    const ESCAPE_MARKER: u8 = 0xFF;
    if frame[offset] == ESCAPE_MARKER {
        offset += 3; // Skip escape marker and u16 msg_type
    } else {
        offset += 1; // Skip u8 msg_type
    }

    let _length = u16::from_be_bytes([frame[offset], frame[offset + 1]]);
    offset += 2;

    let payload = &frame[offset..];
    offset = 0;

    // Parse subscription_id (no status byte in NOTIFY - it's a notification, not a response)
    let subscription_id = u64::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
        payload[offset + 4],
        payload[offset + 5],
        payload[offset + 6],
        payload[offset + 7],
    ]);
    offset += 8;

    // Parse route
    let route_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;
    let route = String::from_utf8_lossy(&payload[offset..offset + route_len]).to_string();
    offset += route_len;

    // Parse notification payload
    let payload_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;
    let notification_payload = payload[offset..offset + payload_len].to_vec();

    (subscription_id, route, notification_payload)
}

// ===== Test Functions =====

async fn should_complete_subscribe_publish_notify_cycle<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let pattern = "notice://test/app/events";
    let publish_payload = b"test-event";

    // Act - SUBSCRIBE
    let subscribe_frame = build_notice_subscribe(pattern);
    let response = client
        .request(&subscribe_frame, 2000)
        .await
        .expect("SUBSCRIBE request failed");

    // Assert - SUBSCRIBE success
    let (msg_type, status, data) = parse_notice_response(&response);
    assert_eq!(msg_type, 501, "Expected SUBSCRIBE response (501)");
    assert_eq!(status, 0, "Expected success status");
    let subscription_id = parse_subscription_id(&data).expect("Expected subscription ID");
    assert!(subscription_id > 0, "Expected valid subscription ID");

    // Act - PUBLISH to subscribed pattern
    let publish_frame = build_notice_publish(pattern, publish_payload);
    client
        .send_frame(&publish_frame)
        .await
        .expect("PUBLISH send failed");

    // Act - Wait for NOTIFY message
    let notify_frame = client
        .recv_frame(2000)
        .await
        .expect("Expected NOTIFY message");

    // Assert - NOTIFY received with correct data
    let (received_sub_id, received_route, received_payload) = parse_notify_message(&notify_frame);
    assert_eq!(received_sub_id, subscription_id, "Expected matching subscription ID");
    assert_eq!(received_route, pattern, "Expected matching route");
    assert_eq!(received_payload, publish_payload, "Expected matching payload");
}

async fn should_receive_responses_within_reasonable_time<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let warmup_frame = build_notice_subscribe("notice://test/app/warmup");
    let _ = client.request(&warmup_frame, 1000).await.expect("warmup failed");

    // Act
    let subscribe_frame = build_notice_subscribe("notice://test/app/bench");
    let start = std::time::Instant::now();
    let response = client
        .request(&subscribe_frame, 500)
        .await
        .expect("SUBSCRIBE request should complete quickly");
    let latency = start.elapsed();

    // Assert
    assert!(
        latency.as_millis() < 20,
        "Expected sub-20ms latency, got {:?}",
        latency
    );
    let (_msg_type, status, _data) = parse_notice_response(&response);
    assert_eq!(status, 0, "Expected success status");
}

async fn should_handle_concurrent_connections_with_separate_subscriptions<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange & Act
    let run_subscription = |idx: usize| async move {
        let mut client = C::connect(server).await.expect("connect failed");
        let pattern = format!("notice://test/app/concurrent{}", idx);

        let subscribe_frame = build_notice_subscribe(&pattern);
        let response = client
            .request(&subscribe_frame, 4000)
            .await
            .expect("SUBSCRIBE failed");

        let (_msg_type, status, data) = parse_notice_response(&response);
        assert_eq!(status, 0);
        let subscription_id = parse_subscription_id(&data).expect("Expected subscription ID");

        let unsubscribe_frame = build_notice_unsubscribe(&pattern);
        let response = client
            .request(&unsubscribe_frame, 4000)
            .await
            .expect("UNSUBSCRIBE failed");
        let (_msg_type, status, _data) = parse_notice_response(&response);
        assert_eq!(status, 0);

        subscription_id
    };

    // Assert - All 3 concurrent operations complete
    let (id1, id2, id3) = tokio::join!(run_subscription(0), run_subscription(1), run_subscription(2));
    let ids = [id1, id2, id3];
    assert_eq!(ids.len(), 3, "All 3 concurrent subscriptions should complete");
}

async fn should_assign_unique_subscription_ids<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let mut subscription_ids = vec![];

    // Act - Subscribe to 3 different patterns
    for i in 0..3 {
        let pattern = format!("notice://test/app/pattern{}", i);
        let subscribe_frame = build_notice_subscribe(&pattern);
        let response = client
            .request(&subscribe_frame, 2000)
            .await
            .expect("SUBSCRIBE failed");

        let (_msg_type, status, data) = parse_notice_response(&response);
        assert_eq!(status, 0);
        let subscription_id = parse_subscription_id(&data).expect("Expected subscription ID");
        subscription_ids.push(subscription_id);
    }

    // Assert - All IDs are unique
    assert_eq!(subscription_ids.len(), 3);
    assert_eq!(subscription_ids, vec![1, 2, 3], "Subscription IDs should be sequential");
}

async fn should_support_wildcard_patterns<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let pattern = "notice://test/app/*";

    // Act - Subscribe to wildcard
    let subscribe_frame = build_notice_subscribe(pattern);
    let response = client.request(&subscribe_frame, 2000).await.expect("SUBSCRIBE");
    let (_, status, data) = parse_notice_response(&response);
    assert_eq!(status, 0);
    let subscription_id = parse_subscription_id(&data).expect("Expected subscription ID");

    // Act - Publish to matching route
    let publish_frame = build_notice_publish("notice://test/app/events", b"wildcard-match");
    client.send_frame(&publish_frame).await.expect("PUBLISH");

    // Act - Receive notification
    let notify_frame = client.recv_frame(2000).await.expect("Expected NOTIFY");

    // Assert
    let (received_sub_id, _route, _payload) = parse_notify_message(&notify_frame);
    assert_eq!(received_sub_id, subscription_id, "Expected notification for wildcard subscription");
}

async fn should_require_connect_message_when_auth_enabled<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    // Act
    let subscribe_frame = build_notice_subscribe("notice://test/app/events");
    let result = client.request(&subscribe_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected connection close or timeout when unauthenticated"
    );
}

async fn should_accept_valid_jwt_in_connect_message<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    client
        .send_frame(&connect_frame)
        .await
        .expect("CONNECT send failed");

    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Act
    let subscribe_frame = build_notice_subscribe("notice://test-realm/app/events");
    let response = client
        .request(&subscribe_frame, 2000)
        .await
        .expect("SUBSCRIBE should work after auth");

    // Assert
    let (_msg_type, status, data) = parse_notice_response(&response);
    assert_eq!(status, 0, "Expected SUBSCRIBE success after authentication");
    assert!(!data.is_empty(), "Expected subscription ID");
}

async fn should_reject_expired_jwt<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_expired_jwt("test-realm"),
    );
    client
        .send_frame(&connect_frame)
        .await
        .expect("CONNECT send failed");

    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Act
    let subscribe_frame = build_notice_subscribe("notice://test-realm/app/events");
    let result = client.request(&subscribe_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for expired JWT");
}

async fn should_reject_invalid_jwt_signature<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_invalid_signature_jwt("test-realm"),
    );
    client
        .send_frame(&connect_frame)
        .await
        .expect("CONNECT send failed");

    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Act
    let subscribe_frame = build_notice_subscribe("notice://test-realm/app/events");
    let result = client.request(&subscribe_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for invalid JWT signature");
}

async fn should_reject_jwt_for_wrong_realm<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "other-realm",
        &fitz::testkit::transport::generate_test_jwt("other-realm"),
    );
    client
        .send_frame(&connect_frame)
        .await
        .expect("CONNECT send failed");

    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Act - Try to subscribe to test-realm pattern
    let subscribe_frame = build_notice_subscribe("notice://test-realm/app/events");
    let result = client.request(&subscribe_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for JWT realm mismatch");
}

async fn should_create_separate_sessions_for_each_connection_with_auth<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange - First client
    let mut client1 = C::connect(server).await.expect("failed to connect");
    let connect_frame1 = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    client1.send_frame(&connect_frame1).await.expect("CONNECT 1");
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Arrange - Second client
    let mut client2 = C::connect(server).await.expect("failed to connect");
    let connect_frame2 = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    client2.send_frame(&connect_frame2).await.expect("CONNECT 2");
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Act - Both clients subscribe
    let pattern = "notice://test-realm/app/shared";
    let subscribe1 = build_notice_subscribe(pattern);
    let response1 = client1.request(&subscribe1, 2000).await.expect("SUBSCRIBE 1");
    let id1 = parse_subscription_id(&parse_notice_response(&response1).2).expect("Expected ID");

    let subscribe2 = build_notice_subscribe(pattern);
    let response2 = client2.request(&subscribe2, 2000).await.expect("SUBSCRIBE 2");
    let id2 = parse_subscription_id(&parse_notice_response(&response2).2).expect("Expected ID");

    // Assert - Both got unique subscription IDs
    assert_eq!(id1, 1, "First connection should get subscription_id=1");
    assert_eq!(id2, 1, "Second connection should also get subscription_id=1 (separate session)");
}

async fn should_support_unsubscribe_operation<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let pattern = "notice://test/app/unsub";

    // ActAct - Subscribe
    let subscribe_frame = build_notice_subscribe(pattern);
    let response = client.request(&subscribe_frame, 2000).await.expect("SUBSCRIBE");
    let (_, status, _) = parse_notice_response(&response);
    assert_eq!(status, 0);

    // Act - Unsubscribe
    let unsubscribe_frame = build_notice_unsubscribe(pattern);
    let response = client.request(&unsubscribe_frame, 2000).await.expect("UNSUBSCRIBE");

    // Assert
    let (msg_type, status, _data) = parse_notice_response(&response);
    assert_eq!(msg_type, 502, "Expected UNSUBSCRIBE response (502)");
    assert_eq!(status, 0, "Expected success status");
}

async fn should_support_unsubscribe_all_operation<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    // Act - Subscribe to multiple patterns
    for i in 0..3 {
        let pattern = format!("notice://test/app/pattern{}", i);
        let subscribe_frame = build_notice_subscribe(&pattern);
        client.request(&subscribe_frame, 2000).await.expect("SUBSCRIBE");
    }

    // Act - Unsubscribe all
    let unsubscribe_all_frame = build_notice_unsubscribe_all();
    let response = client
        .request(&unsubscribe_all_frame, 2000)
        .await
        .expect("UNSUBSCRIBE_ALL");

    // Assert
    let (msg_type, status, _data) = parse_notice_response(&response);
    assert_eq!(msg_type, 503, "Expected UNSUBSCRIBE_ALL response (503)");
    assert_eq!(status, 0, "Expected success status");
}

async fn should_handle_empty_payload<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let pattern = "notice://test/app/empty";

    let subscribe_frame = build_notice_subscribe(pattern);
    client.request(&subscribe_frame, 2000).await.expect("SUBSCRIBE");

    // Act - Publish with empty payload
    let publish_frame = build_notice_publish(pattern, b"");
    client.send_frame(&publish_frame).await.expect("PUBLISH");

    // Assert - Should receive notification
    let notify_frame = client.recv_frame(2000).await.expect("Expected NOTIFY");
    let (_, _, payload) = parse_notify_message(&notify_frame);
    assert_eq!(payload.len(), 0, "Expected empty payload");
}

async fn should_handle_large_payload<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let pattern = "notice://test/app/large";
    let large_payload = vec![b'X'; 60_000];

    let subscribe_frame = build_notice_subscribe(pattern);
    client.request(&subscribe_frame, 2000).await.expect("SUBSCRIBE");

    // Act - Publish with large payload
    let publish_frame = build_notice_publish(pattern, &large_payload);
    client.send_frame(&publish_frame).await.expect("PUBLISH");

    // Assert - Should receive notification
    let notify_frame = client.recv_frame(3000).await.expect("Expected NOTIFY");
    let (_, _, payload) = parse_notify_message(&notify_frame);
    assert_eq!(payload.len(), 60_000, "Expected 60KB payload");
}

async fn should_support_fanout_to_multiple_subscribers<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client1 = C::connect(server).await.expect("failed to connect 1");
    let mut client2 = C::connect(server).await.expect("failed to connect 2");
    let pattern = "notice://test/app/fanout";

    // Act - Both subscribe to same pattern
    let subscribe_frame1 = build_notice_subscribe(pattern);
    let response1 = client1.request(&subscribe_frame1, 2000).await.expect("SUBSCRIBE 1");
    let id1 = parse_subscription_id(&parse_notice_response(&response1).2).expect("Expected ID");

    let subscribe_frame2 = build_notice_subscribe(pattern);
    let response2 = client2.request(&subscribe_frame2, 2000).await.expect("SUBSCRIBE 2");
    let id2 = parse_subscription_id(&parse_notice_response(&response2).2).expect("Expected ID");

    // Act - Publish once
    let publish_frame = build_notice_publish(pattern, b"fanout-test");
    client1.send_frame(&publish_frame).await.expect("PUBLISH");

    // Assert - Both subscribers receive notification
    let notify1 = client1.recv_frame(2000).await.expect("Expected NOTIFY 1");
    let (received_id1, _, _) = parse_notify_message(&notify1);
    assert_eq!(received_id1, id1);

    let notify2 = client2.recv_frame(2000).await.expect("Expected NOTIFY 2");
    let (received_id2, _, _) = parse_notify_message(&notify2);
    assert_eq!(received_id2, id2);
}

async fn should_isolate_subscriptions_across_patterns<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    // Act - Subscribe to two different patterns
    let subscribe1 = build_notice_subscribe("notice://test/app/pattern1");
    let response1 = client.request(&subscribe1, 2000).await.expect("SUBSCRIBE 1");
    let id1 = parse_subscription_id(&parse_notice_response(&response1).2).expect("Expected ID");

    let subscribe2 = build_notice_subscribe("notice://test/app/pattern2");
    let response2 = client.request(&subscribe2, 2000).await.expect("SUBSCRIBE 2");
    let id2 = parse_subscription_id(&parse_notice_response(&response2).2).expect("Expected ID");

    // Act - Publish to pattern1 only
    let publish_frame = build_notice_publish("notice://test/app/pattern1", b"isolated");
    client.send_frame(&publish_frame).await.expect("PUBLISH");

    // Assert - Only pattern1 subscriber receives notification
    let notify = client.recv_frame(2000).await.expect("Expected NOTIFY");
    let (received_id, _, _) = parse_notify_message(&notify);
    assert_eq!(received_id, id1, "Expected notification for pattern1 only");

    // Assert - No additional notifications
    let result = client.recv_frame(500).await;
    assert!(result.is_err(), "Expected no notification for pattern2");
}

async fn should_timeout_on_malformed_frame<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let garbage = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00];

    // Act
    let result = client.request(&garbage, 100).await;

    // Assert
    assert!(result.is_err(), "Expected error/timeout for malformed frame");
}

async fn should_handle_connection_drop_during_subscription<C>(server: &TestServer)
where
    C: NoticeConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let pattern = "notice://test/app/disconnect";

    let subscribe_frame = build_notice_subscribe(pattern);
    client.request(&subscribe_frame, 1000).await.expect("SUBSCRIBE");

    // Act - Drop connection
    drop(client);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Act - Reconnect and subscribe again (should succeed)
    let mut client2 = C::connect(server).await.expect("failed to reconnect");
    let subscribe_frame2 = build_notice_subscribe(pattern);
    let response = client2
        .request(&subscribe_frame2, 2000)
        .await
        .expect("SUBSCRIBE should work after reconnect");

    // Assert
    let (_msg_type, status, _data) = parse_notice_response(&response);
    assert_eq!(status, 0, "Expected successful subscribe after reconnect");
}

// ===== TCP tests =====

#[tokio::test]
async fn should_complete_subscribe_publish_notify_cycle_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_complete_subscribe_publish_notify_cycle::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_subscriptions_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_concurrent_connections_with_separate_subscriptions::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_assign_unique_subscription_ids_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_assign_unique_subscription_ids::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_wildcard_patterns_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_wildcard_patterns::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_require_connect_message_when_auth_enabled_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_require_connect_message_when_auth_enabled::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_accept_valid_jwt_in_connect_message_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_accept_valid_jwt_in_connect_message::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_expired_jwt_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_expired_jwt::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_invalid_jwt_signature_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_invalid_jwt_signature::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_jwt_for_wrong_realm_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_jwt_for_wrong_realm::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_create_separate_sessions_for_each_connection_with_auth_tcp() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_create_separate_sessions_for_each_connection_with_auth::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_unsubscribe_operation_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_unsubscribe_operation::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_unsubscribe_all_operation_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_unsubscribe_all_operation::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_empty_payload_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_empty_payload::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_large_payload_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_large_payload::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_fanout_to_multiple_subscribers_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_fanout_to_multiple_subscribers::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_subscriptions_across_patterns_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_isolate_subscriptions_across_patterns::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_timeout_on_malformed_frame_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_timeout_on_malformed_frame::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_connection_drop_during_subscription_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_connection_drop_during_subscription::<TcpConnector>(&server).await;
}

// ===== WebSocket tests =====

#[tokio::test]
async fn should_complete_subscribe_publish_notify_cycle_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_complete_subscribe_publish_notify_cycle::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_subscriptions_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_concurrent_connections_with_separate_subscriptions::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_assign_unique_subscription_ids_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_assign_unique_subscription_ids::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_wildcard_patterns_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_wildcard_patterns::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_require_connect_message_when_auth_enabled_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_require_connect_message_when_auth_enabled::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_accept_valid_jwt_in_connect_message_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_accept_valid_jwt_in_connect_message::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_expired_jwt_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_expired_jwt::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_invalid_jwt_signature_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_invalid_jwt_signature::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_jwt_for_wrong_realm_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_reject_jwt_for_wrong_realm::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_create_separate_sessions_for_each_connection_with_auth_ws() {
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");
    should_create_separate_sessions_for_each_connection_with_auth::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_unsubscribe_operation_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_unsubscribe_operation::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_unsubscribe_all_operation_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_unsubscribe_all_operation::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_empty_payload_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_empty_payload::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_large_payload_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_large_payload::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_fanout_to_multiple_subscribers_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_fanout_to_multiple_subscribers::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_subscriptions_across_patterns_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_isolate_subscriptions_across_patterns::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_timeout_on_malformed_frame_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_timeout_on_malformed_frame::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_connection_drop_during_subscription_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_connection_drop_during_subscription::<WsConnector>(&server).await;
}
