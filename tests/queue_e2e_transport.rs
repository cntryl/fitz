//! Queue domain transport-layer end-to-end tests
//!
//! These tests verify the COMPLETE request-response cycle:
//! Client → TCP/WebSocket → Session → Routing → Queue Actor → Response → Client

use bytes::{BufMut, BytesMut};
use fitz::testkit::transport::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

type BoxError = Box<dyn Error>;
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub trait QueueTestClient {
    fn send_frame<'a>(&'a mut self, frame: &'a [u8]) -> BoxFuture<'a, Result<(), BoxError>>;
    fn request<'a>(
        &'a mut self,
        frame: &'a [u8],
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>>;
}

pub trait QueueConnector {
    type Client: QueueTestClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>>;
}

impl QueueTestClient for TestClient {
    fn send_frame<'a>(&'a mut self, frame: &'a [u8]) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move { TestClient::send_frame(self, frame).await })
    }

    fn request<'a>(
        &'a mut self,
        frame: &'a [u8],
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
        Box::pin(async move { TestClient::request(self, frame, timeout_ms).await })
    }
}

impl QueueTestClient for TestWebSocketClient {
    fn send_frame<'a>(&'a mut self, frame: &'a [u8]) -> BoxFuture<'a, Result<(), BoxError>> {
        Box::pin(async move { TestWebSocketClient::send_frame(self, frame).await })
    }

    fn request<'a>(
        &'a mut self,
        frame: &'a [u8],
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>> {
        Box::pin(async move { TestWebSocketClient::request(self, frame, timeout_ms).await })
    }
}

struct TcpConnector;

impl QueueConnector for TcpConnector {
    type Client = TestClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect().await })
    }
}

struct WsConnector;

impl QueueConnector for WsConnector {
    type Client = TestWebSocketClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect_ws().await })
    }
}

/// Build Queue ENQUEUE request frame
/// Wire format: [u32 BE route_len][route][u32 BE body_len][body][u8 has_delay][u64 delay?]
fn build_queue_enqueue(route: &str, body: &[u8], delay_seconds: Option<u64>) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());
    payload.put_slice(&(body.len() as u32).to_be_bytes());
    payload.put_slice(body);

    if let Some(delay) = delay_seconds {
        payload.put_u8(1);
        payload.put_slice(&delay.to_be_bytes());
    } else {
        payload.put_u8(0);
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(200, &payload);
    builder.build()
}

/// Build Queue RESERVE request frame
/// Wire format: [u32 BE route_len][route][u64 BE lease_seconds][u8 has_batch_size][u32 batch?][u8 has_wait][u64 wait?]
fn build_queue_reserve(
    route: &str,
    lease_seconds: u64,
    batch_size: Option<usize>,
    wait_seconds: Option<u64>,
) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());
    payload.put_slice(&lease_seconds.to_be_bytes());

    if let Some(batch) = batch_size {
        payload.put_u8(1);
        payload.put_slice(&(batch as u32).to_be_bytes());
    } else {
        payload.put_u8(0);
    }

    if let Some(wait) = wait_seconds {
        payload.put_u8(1);
        payload.put_slice(&wait.to_be_bytes());
    } else {
        payload.put_u8(0);
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(202, &payload);
    builder.build()
}

/// Build Queue EXTEND request frame
/// Wire format: [u32 BE route_len][route][u64 BE message_id][u64 BE token][u64 BE lease_seconds]
fn build_queue_extend(route: &str, message_id: u64, token: u64, lease_seconds: u64) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());
    payload.put_slice(&message_id.to_be_bytes());
    payload.put_slice(&token.to_be_bytes());
    payload.put_slice(&lease_seconds.to_be_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(203, &payload);
    builder.build()
}

/// Build Queue COMPLETE request frame
/// Wire format: [u32 BE route_len][route][u64 BE message_id][u64 BE token]
fn build_queue_complete(route: &str, message_id: u64, token: u64) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());
    payload.put_slice(&message_id.to_be_bytes());
    payload.put_slice(&token.to_be_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(204, &payload);
    builder.build()
}

/// Parse Queue response status byte and extract data
fn parse_queue_response(frame: &[u8]) -> (u16, u8, Vec<u8>) {
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

/// Parse ENQUEUE response to get message ID
fn parse_enqueue_response(data: &[u8]) -> u64 {
    u64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ])
}

/// Parse RESERVE response to get reserved messages
fn parse_reserve_response(data: &[u8]) -> Vec<(u64, u64, Vec<u8>)> {
    let mut offset = 0;
    let count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    offset += 4;

    let mut messages = Vec::new();
    for _ in 0..count {
        let id = u64::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        let token = u64::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
            data[offset + 5],
            data[offset + 6],
            data[offset + 7],
        ]);
        offset += 8;

        let body_len = u32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        let body = data[offset..offset + body_len].to_vec();
        offset += body_len;

        messages.push((id, token, body));
    }
    messages
}

// ===== Test Functions =====

async fn should_complete_enqueue_reserve_complete_cycle<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/jobs";
    let body = b"task-payload";

    // Act - ENQUEUE
    let enqueue_frame = build_queue_enqueue(route, body, None);
    let response = client
        .request(&enqueue_frame, 2000)
        .await
        .expect("ENQUEUE request failed");

    // Assert - ENQUEUE success
    let (msg_type, status, data) = parse_queue_response(&response);
    assert_eq!(msg_type, 200, "Expected ENQUEUE response (200)");
    assert_eq!(status, 0, "Expected success status");
    let message_id = parse_enqueue_response(&data);
    assert!(message_id > 0, "Expected valid message ID");

    // Act - RESERVE
    let reserve_frame = build_queue_reserve(route, 30, Some(1), None);
    let response = client
        .request(&reserve_frame, 2000)
        .await
        .expect("RESERVE request failed");

    // Assert - RESERVE success
    let (msg_type, status, data) = parse_queue_response(&response);
    assert_eq!(msg_type, 202, "Expected RESERVE response (202)");
    assert_eq!(status, 0, "Expected success status");
    let messages = parse_reserve_response(&data);
    assert_eq!(messages.len(), 1, "Expected 1 reserved message");
    assert_eq!(messages[0].0, message_id, "Expected same message ID");
    assert_eq!(messages[0].2, body, "Expected same body");
    let token = messages[0].1;

    // Act - COMPLETE
    let complete_frame = build_queue_complete(route, message_id, token);
    let response = client
        .request(&complete_frame, 2000)
        .await
        .expect("COMPLETE request failed");

    // Assert - COMPLETE success
    let (msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(msg_type, 204, "Expected COMPLETE response (204)");
    assert_eq!(status, 0, "Expected success status");
}

async fn should_receive_responses_within_reasonable_time<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let warmup_frame = build_queue_enqueue("queue://test/app/warmup", b"warmup", None);
    let _ = client
        .request(&warmup_frame, 1000)
        .await
        .expect("warmup failed");

    // Act
    let enqueue_frame = build_queue_enqueue("queue://test/app/bench", b"benchmark", None);
    let response = client
        .request(&enqueue_frame, 500)
        .await
        .expect("ENQUEUE request should complete quickly");
    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Expected success status");
}

async fn should_handle_concurrent_connections_with_separate_queues<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange & Act
    let run_queue = |idx: usize| async move {
        let mut client = C::connect(server).await.expect("connect failed");
        let route = format!("queue://test/app/concurrent{}", idx);
        let body = format!("message-{}", idx);

        let enqueue_frame = build_queue_enqueue(&route, body.as_bytes(), None);
        let response = client
            .request(&enqueue_frame, 4000)
            .await
            .expect("ENQUEUE failed");

        let (_msg_type, status, data) = parse_queue_response(&response);
        assert_eq!(status, 0);
        let message_id = parse_enqueue_response(&data);

        let reserve_frame = build_queue_reserve(&route, 30, Some(1), None);
        let response = client
            .request(&reserve_frame, 4000)
            .await
            .expect("RESERVE failed");
        let (_msg_type, status, data) = parse_queue_response(&response);
        assert_eq!(status, 0);
        let messages = parse_reserve_response(&data);
        assert_eq!(messages.len(), 1);

        let complete_frame = build_queue_complete(&route, message_id, messages[0].1);
        let response = client
            .request(&complete_frame, 4000)
            .await
            .expect("COMPLETE failed");
        let (_msg_type, status, _data) = parse_queue_response(&response);
        assert_eq!(status, 0);

        message_id
    };

    // Assert - All 3 concurrent operations complete
    let (id1, id2, id3) = tokio::join!(run_queue(0), run_queue(1), run_queue(2));
    let ids = [id1, id2, id3];
    assert_eq!(
        ids.len(),
        3,
        "All 3 concurrent queue operations should complete"
    );
}

async fn should_assign_unique_message_ids_within_single_queue<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/sequential";
    let mut message_ids = vec![];

    // Act - Enqueue 3 messages
    for i in 0..3 {
        let body = format!("message-{}", i);
        let enqueue_frame = build_queue_enqueue(route, body.as_bytes(), None);
        let response = client
            .request(&enqueue_frame, 2000)
            .await
            .expect("ENQUEUE failed");

        let (_msg_type, status, data) = parse_queue_response(&response);
        assert_eq!(status, 0);
        let message_id = parse_enqueue_response(&data);
        message_ids.push(message_id);
    }

    // Assert - All IDs are unique
    assert_eq!(message_ids.len(), 3);
    assert_eq!(
        message_ids,
        vec![1, 2, 3],
        "Message IDs should be sequential"
    );
}

async fn should_reject_operations_with_invalid_token<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/invalid";

    let enqueue_frame = build_queue_enqueue(route, b"test", None);
    let response = client.request(&enqueue_frame, 2000).await.expect("ENQUEUE");
    let message_id = parse_enqueue_response(&parse_queue_response(&response).2);

    let reserve_frame = build_queue_reserve(route, 30, Some(1), None);
    let _ = client.request(&reserve_frame, 2000).await.expect("RESERVE");

    // Act - Try to complete with wrong token
    let complete_frame = build_queue_complete(route, message_id, 99999);
    let response = client
        .request(&complete_frame, 2000)
        .await
        .expect("server should respond even for invalid token");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 1, "Expected error status for invalid token");
}

async fn should_require_connect_message_when_auth_enabled<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    // Act
    let enqueue_frame = build_queue_enqueue("queue://test/app/auth", b"test", None);
    let result = client.request(&enqueue_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected connection close or timeout when unauthenticated"
    );
}

async fn should_accept_valid_jwt_in_connect_message<C>(server: &TestServer)
where
    C: QueueConnector,
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
    let enqueue_frame = build_queue_enqueue("queue://test-realm/app/auth", b"test", None);
    let response = client
        .request(&enqueue_frame, 2000)
        .await
        .expect("ENQUEUE should work after auth");

    // Assert
    let (_msg_type, status, data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Expected ENQUEUE success after authentication");
    assert_eq!(data.len(), 8, "Expected message_id");
}

async fn should_reject_expired_jwt<C>(server: &TestServer)
where
    C: QueueConnector,
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
    let enqueue_frame = build_queue_enqueue("queue://test-realm/app/auth", b"test", None);
    let result = client.request(&enqueue_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for expired JWT");
}

async fn should_reject_invalid_jwt_signature<C>(server: &TestServer)
where
    C: QueueConnector,
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
    let enqueue_frame = build_queue_enqueue("queue://test-realm/app/auth", b"test", None);
    let result = client.request(&enqueue_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected rejection for invalid JWT signature"
    );
}

async fn should_reject_jwt_for_wrong_realm<C>(server: &TestServer)
where
    C: QueueConnector,
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

    // Act - Try to access test-realm queue
    let enqueue_frame = build_queue_enqueue("queue://test-realm/app/auth", b"test", None);
    let result = client.request(&enqueue_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for JWT realm mismatch");
}

async fn should_create_separate_sessions_for_each_connection_with_auth<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange - First client
    let mut client1 = C::connect(server).await.expect("failed to connect");
    let connect_frame1 = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    client1
        .send_frame(&connect_frame1)
        .await
        .expect("CONNECT 1");
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Arrange - Second client
    let mut client2 = C::connect(server).await.expect("failed to connect");
    let connect_frame2 = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    client2
        .send_frame(&connect_frame2)
        .await
        .expect("CONNECT 2");
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Act - Both clients enqueue to same queue
    let route = "queue://test-realm/app/shared";
    let enqueue1 = build_queue_enqueue(route, b"message1", None);
    let response1 = client1.request(&enqueue1, 2000).await.expect("ENQUEUE 1");
    let id1 = parse_enqueue_response(&parse_queue_response(&response1).2);

    let enqueue2 = build_queue_enqueue(route, b"message2", None);
    let response2 = client2.request(&enqueue2, 2000).await.expect("ENQUEUE 2");
    let id2 = parse_enqueue_response(&parse_queue_response(&response2).2);

    // Assert - Both messages accepted, different IDs
    assert_eq!(id1, 1, "First connection should get message_id=1");
    assert_eq!(
        id2, 2,
        "Second connection should get message_id=2 (same queue)"
    );
}

async fn should_reject_complete_without_reserve<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/noprep";

    let enqueue_frame = build_queue_enqueue(route, b"test", None);
    let response = client.request(&enqueue_frame, 2000).await.expect("ENQUEUE");
    let message_id = parse_enqueue_response(&parse_queue_response(&response).2);

    // Act - Try to complete without reserving
    let complete_frame = build_queue_complete(route, message_id, 12345);
    let result = client.request(&complete_frame, 2000).await;

    // Assert
    if let Ok(response) = result {
        let (_msg_type, status, _data) = parse_queue_response(&response);
        assert_eq!(status, 1, "Expected error for COMPLETE without RESERVE");
    } else {
        assert!(
            result.is_err(),
            "Expected error/timeout for COMPLETE without RESERVE"
        );
    }
}

async fn should_reject_extend_after_complete<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/lifecycle";

    let enqueue_frame = build_queue_enqueue(route, b"test", None);
    let response = client.request(&enqueue_frame, 2000).await.expect("ENQUEUE");
    let message_id = parse_enqueue_response(&parse_queue_response(&response).2);

    let reserve_frame = build_queue_reserve(route, 30, Some(1), None);
    let response = client.request(&reserve_frame, 2000).await.expect("RESERVE");
    let messages = parse_reserve_response(&parse_queue_response(&response).2);
    let token = messages[0].1;

    let complete_frame = build_queue_complete(route, message_id, token);
    client
        .request(&complete_frame, 2000)
        .await
        .expect("COMPLETE");

    // Act - Try to extend after complete
    let extend_frame = build_queue_extend(route, message_id, token, 60);
    let response = client
        .request(&extend_frame, 2000)
        .await
        .expect("server should respond");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 1, "Expected error for EXTEND after COMPLETE");
}

async fn should_handle_empty_body<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/empty";

    // Act
    let enqueue_frame = build_queue_enqueue(route, b"", None);
    let response = client.request(&enqueue_frame, 2000).await.expect("ENQUEUE");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Expected success with empty body");
}

async fn should_handle_large_body<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/large";
    let large_body = vec![b'X'; 60_000];

    // Act
    let enqueue_frame = build_queue_enqueue(route, &large_body, None);
    let response = client.request(&enqueue_frame, 3000).await.expect("ENQUEUE");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Expected success with 60KB body");
}

async fn should_support_delayed_messages<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/delayed";

    // Act - Enqueue with 10 second delay
    let enqueue_frame = build_queue_enqueue(route, b"delayed", Some(10));
    let response = client.request(&enqueue_frame, 2000).await.expect("ENQUEUE");
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0);

    // Act - Try to reserve immediately (should be empty)
    let reserve_frame = build_queue_reserve(route, 30, Some(1), None);
    let response = client.request(&reserve_frame, 2000).await.expect("RESERVE");

    // Assert - No messages available yet
    let (_msg_type, status, data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Expected success status");
    let messages = parse_reserve_response(&data);
    assert_eq!(messages.len(), 0, "Expected no messages (delayed)");
}

async fn should_support_batch_reserve<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/batch";

    // Act - Enqueue 3 messages
    for i in 0..3 {
        let body = format!("message-{}", i);
        let enqueue_frame = build_queue_enqueue(route, body.as_bytes(), None);
        client.request(&enqueue_frame, 2000).await.expect("ENQUEUE");
    }

    // Act - Reserve batch of 3
    let reserve_frame = build_queue_reserve(route, 30, Some(3), None);
    let response = client.request(&reserve_frame, 2000).await.expect("RESERVE");

    // Assert
    let (_msg_type, status, data) = parse_queue_response(&response);
    assert_eq!(status, 0);
    let messages = parse_reserve_response(&data);
    assert_eq!(messages.len(), 3, "Expected 3 reserved messages");
}

async fn should_extend_lease_successfully<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/extend";

    let enqueue_frame = build_queue_enqueue(route, b"test", None);
    let response = client.request(&enqueue_frame, 2000).await.expect("ENQUEUE");
    let message_id = parse_enqueue_response(&parse_queue_response(&response).2);

    let reserve_frame = build_queue_reserve(route, 30, Some(1), None);
    let response = client.request(&reserve_frame, 2000).await.expect("RESERVE");
    let messages = parse_reserve_response(&parse_queue_response(&response).2);
    let token = messages[0].1;

    // Act - Extend lease
    let extend_frame = build_queue_extend(route, message_id, token, 60);
    let response = client.request(&extend_frame, 2000).await.expect("EXTEND");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(status, 0, "Expected EXTEND success");
}

async fn should_isolate_queues_across_resources<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    // Act - Enqueue to two different queues
    let enqueue1 = build_queue_enqueue("queue://test/app/jobs", b"job1", None);
    let response1 = client.request(&enqueue1, 2000).await.expect("ENQUEUE 1");
    let id1 = parse_enqueue_response(&parse_queue_response(&response1).2);

    let enqueue2 = build_queue_enqueue("queue://test/app/tasks", b"task1", None);
    let response2 = client.request(&enqueue2, 2000).await.expect("ENQUEUE 2");
    let id2 = parse_enqueue_response(&parse_queue_response(&response2).2);

    // Assert - Different queues have independent IDs
    assert_eq!(id1, 1, "First queue should start at ID 1");
    assert_eq!(id2, 1, "Second queue should also start at ID 1 (isolated)");

    // Act - Reserve from first queue only
    let reserve1 = build_queue_reserve("queue://test/app/jobs", 30, Some(1), None);
    let response = client.request(&reserve1, 2000).await.expect("RESERVE 1");
    let messages = parse_reserve_response(&parse_queue_response(&response).2);

    // Assert - Only message from first queue
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].2, b"job1");
}

async fn should_timeout_on_malformed_frame<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let garbage = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00];

    // Act
    let result = client.request(&garbage, 100).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected error/timeout for malformed frame"
    );
}

async fn should_handle_connection_drop_during_lease<C>(server: &TestServer)
where
    C: QueueConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "queue://test/app/disconnect";

    let enqueue_frame = build_queue_enqueue(route, b"test", None);
    let response = client.request(&enqueue_frame, 1000).await.expect("ENQUEUE");
    let message_id = parse_enqueue_response(&parse_queue_response(&response).2);

    let reserve_frame = build_queue_reserve(route, 30, Some(1), None);
    let response = client.request(&reserve_frame, 1000).await.expect("RESERVE");
    let messages = parse_reserve_response(&parse_queue_response(&response).2);
    let token = messages[0].1;

    // Act - Drop connection
    drop(client);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Act - Reconnect and try to complete with old token
    let mut client2 = C::connect(server).await.expect("failed to reconnect");
    let complete_frame = build_queue_complete(route, message_id, token);
    let response = client2
        .request(&complete_frame, 2000)
        .await
        .expect("server should respond");

    // Assert
    let (_msg_type, status, _data) = parse_queue_response(&response);
    assert_eq!(
        status, 1,
        "Expected error for token from disconnected session"
    );
}

// ===== TCP tests =====

#[tokio::test]
async fn should_complete_enqueue_reserve_complete_cycle_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_complete_enqueue_reserve_complete_cycle::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_queues_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_concurrent_connections_with_separate_queues::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_assign_unique_message_ids_within_single_queue_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_assign_unique_message_ids_within_single_queue::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_operations_with_invalid_token_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_operations_with_invalid_token::<TcpConnector>(&server).await;
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
async fn should_reject_complete_without_reserve_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_complete_without_reserve::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_extend_after_complete_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_extend_after_complete::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_empty_body_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_empty_body::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_large_body_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_large_body::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_delayed_messages_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_support_delayed_messages::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_batch_reserve_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_support_batch_reserve::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_extend_lease_successfully_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_extend_lease_successfully::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_queues_across_resources_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_isolate_queues_across_resources::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_timeout_on_malformed_frame_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_timeout_on_malformed_frame::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_connection_drop_during_lease_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_connection_drop_during_lease::<TcpConnector>(&server).await;
}

// ===== WebSocket tests =====

#[tokio::test]
async fn should_complete_enqueue_reserve_complete_cycle_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_complete_enqueue_reserve_complete_cycle::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_queues_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_concurrent_connections_with_separate_queues::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_assign_unique_message_ids_within_single_queue_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_assign_unique_message_ids_within_single_queue::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_operations_with_invalid_token_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_operations_with_invalid_token::<WsConnector>(&server).await;
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
async fn should_reject_complete_without_reserve_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_complete_without_reserve::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_extend_after_complete_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_extend_after_complete::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_empty_body_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_empty_body::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_large_body_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_large_body::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_delayed_messages_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_support_delayed_messages::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_batch_reserve_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_support_batch_reserve::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_extend_lease_successfully_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_extend_lease_successfully::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_queues_across_resources_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_isolate_queues_across_resources::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_timeout_on_malformed_frame_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_timeout_on_malformed_frame::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_connection_drop_during_lease_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_connection_drop_during_lease::<WsConnector>(&server).await;
}
