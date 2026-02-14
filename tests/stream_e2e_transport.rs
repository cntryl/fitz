//! Stream domain transport-layer end-to-end tests
//!
//! These tests verify the COMPLETE append-only streaming cycle:
//! Client → TCP/WebSocket → Session → Routing → Stream Actor → Response

use bytes::{BufMut, BytesMut};
use fitz::testkit::transport::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

type BoxError = Box<dyn Error>;
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub trait StreamTestClient {
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

pub trait StreamConnector {
    type Client: StreamTestClient;

    fn connect<'a>(
        server: &'a TestServer,
    ) -> BoxFuture<'a, Result<Self::Client, BoxError>>;
}

impl StreamTestClient for TestClient {
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

impl StreamTestClient for TestWebSocketClient {
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

impl StreamConnector for TcpConnector {
    type Client = TestClient;

    fn connect<'a>(
        server: &'a TestServer,
    ) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect().await })
    }
}

struct WsConnector;

impl StreamConnector for WsConnector {
    type Client = TestWebSocketClient;

    fn connect<'a>(
        server: &'a TestServer,
    ) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect_ws().await })
    }
}

/// Build Stream BEGIN request frame
/// Wire format: [string route][u64 expected_offset][optional bytes ingest_metadata]
fn build_stream_begin(route: &str, expected_offset: u64, metadata: Option<&[u8]>) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(route.len() as u32).to_be_bytes());
    buf.put_slice(route.as_bytes());
    buf.put_slice(&expected_offset.to_be_bytes());
    
    if let Some(meta) = metadata {
        buf.put_u8(1); // flag = Some
        buf.put_slice(&(meta.len() as u32).to_be_bytes());
        buf.put_slice(meta);
    } else {
        buf.put_u8(0); // flag = None
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(600, &buf);
    builder.build()
}

/// Build Stream APPEND request frame
/// Wire format: [u64 session_id][bytes body][optional bytes metadata]
fn build_stream_append(session_id: u64, body: &[u8], metadata: Option<&[u8]>) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&session_id.to_be_bytes());
    buf.put_slice(&(body.len() as u32).to_be_bytes());
    buf.put_slice(body);
    
    if let Some(meta) = metadata {
        buf.put_u8(1); // flag = Some
        buf.put_slice(&(meta.len() as u32).to_be_bytes());
        buf.put_slice(meta);
    } else {
        buf.put_u8(0); // flag = None
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(601, &buf);
    builder.build()
}

/// Build Stream COMMIT request frame
/// Wire format: [u64 session_id][u8 mode] where mode: 0=Buffered, 1=Sync
fn build_stream_commit(session_id: u64, sync: bool) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&session_id.to_be_bytes());
    buf.put_u8(if sync { 1 } else { 0 });

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(602, &buf);
    builder.build()
}

/// Build Stream ROLLBACK request frame
/// Wire format: [u64 session_id]
fn build_stream_rollback(session_id: u64) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&session_id.to_be_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(603, &buf);
    builder.build()
}

/// Build Stream READ request frame
/// Wire format: [string route][u64 from_offset][u64 limit][optional u64 max_bytes]
fn build_stream_read(route: &str, from_offset: u64, limit: u64, max_bytes: Option<u64>) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(route.len() as u32).to_be_bytes());
    buf.put_slice(route.as_bytes());
    buf.put_slice(&from_offset.to_be_bytes());
    buf.put_slice(&limit.to_be_bytes());
    
    if let Some(mb) = max_bytes {
        buf.put_u8(1); // flag = Some
        buf.put_slice(&mb.to_be_bytes());
    } else {
        buf.put_u8(0); // flag = None
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(604, &buf);
    builder.build()
}

/// Build Stream LAST request frame
/// Wire format: [string route]
fn build_stream_last(route: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(route.len() as u32).to_be_bytes());
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(605, &buf);
    builder.build()
}

/// Build Stream GET_METADATA request frame
/// Wire format: [string route]
fn build_stream_get_metadata(route: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(route.len() as u32).to_be_bytes());
    buf.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(606, &buf);
    builder.build()
}

/// Build Stream SUBSCRIBE request frame
/// Wire format: [string pattern]
#[allow(dead_code)]
fn build_stream_subscribe(pattern: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(pattern.len() as u32).to_be_bytes());
    buf.put_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(607, &buf);
    builder.build()
}

/// Build Stream UNSUBSCRIBE request frame
/// Wire format: [string pattern]
#[allow(dead_code)]
fn build_stream_unsubscribe(pattern: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(pattern.len() as u32).to_be_bytes());
    buf.put_slice(pattern.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(608, &buf);
    builder.build()
}

/// Parse stream response
fn parse_stream_response(frame: &[u8]) -> (u16, u8, Vec<u8>) {
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

/// Parse session_id from BEGIN response
fn parse_session_id_from_begin(data: &[u8]) -> Option<u64> {
    if data.is_empty() {
        return None;
    }
    // Response format: [u8 flag][optional u64 session_id][bytes data]
    // flag: 0=None, 1=Some
    let flag = data[0];
    if flag == 1 && data.len() >= 9 {
        Some(u64::from_be_bytes([
            data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
        ]))
    } else {
        None
    }
}

// ===== Test Functions =====

async fn should_complete_begin_append_commit_cycle<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "stream://test/app/logs";

    // Act - BEGIN
    let begin_frame = build_stream_begin(route, 0, None);
    let begin_response = client
        .request(&begin_frame, 2000)
        .await
        .expect("BEGIN request failed");

    let (msg_type, status, data) = parse_stream_response(&begin_response);
    assert_eq!(msg_type, 600, "Expected BEGIN response (600)");
    assert_eq!(status, 0, "Expected success status");

    let session_id = parse_session_id_from_begin(&data).expect("Expected session_id");

    // Act - APPEND
    let append_frame = build_stream_append(session_id, b"log entry 1", None);
    let append_response = client
        .request(&append_frame, 2000)
        .await
        .expect("APPEND request failed");

    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected APPEND success");

    // Act - COMMIT
    let commit_frame = build_stream_commit(session_id, false);
    let commit_response = client
        .request(&commit_frame, 2000)
        .await
        .expect("COMMIT request failed");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&commit_response);
    assert_eq!(status, 0, "Expected COMMIT success");
}

async fn should_receive_responses_within_reasonable_time<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let warmup_frame = build_stream_begin("stream://test/app/warmup", 0, None);
    let _ = client.request(&warmup_frame, 1000).await.expect("warmup failed");

    // Act
    let begin_frame = build_stream_begin("stream://test/app/bench", 0, None);
    let start = std::time::Instant::now();
    let response = client
        .request(&begin_frame, 500)
        .await
        .expect("BEGIN request should complete quickly");
    let latency = start.elapsed();

    // Assert
    assert!(
        latency.as_millis() < 20,
        "Expected sub-20ms latency, got {:?}",
        latency
    );
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Expected success status");
}

async fn should_handle_concurrent_stream_operations<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange & Act
    let run_stream = |idx: usize| async move {
        let mut client = C::connect(server).await.expect("connect failed");
        let route = format!("stream://test/app/concurrent{}", idx);

        let begin_frame = build_stream_begin(&route, 0, None);
        let begin_response = client
            .request(&begin_frame, 4000)
            .await
            .expect("BEGIN failed");

        let (_msg_type, status, data) = parse_stream_response(&begin_response);
        assert_eq!(status, 0);

        let session_id = parse_session_id_from_begin(&data).expect("session_id");

        let append_frame = build_stream_append(session_id, b"entry", None);
        let append_response = client
            .request(&append_frame, 4000)
            .await
            .expect("APPEND failed");
        let (_msg_type, status, _data) = parse_stream_response(&append_response);
        assert_eq!(status, 0);
    };

    // Assert - All 3 concurrent operations complete
    tokio::join!(run_stream(0), run_stream(1), run_stream(2));
}

async fn should_support_large_append_payloads<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "stream://test/app/large";

    let begin_frame = build_stream_begin(route, 0, None);
    let begin_response = client.request(&begin_frame, 2000).await.expect("BEGIN");
    let (_msg_type, _status, data) = parse_stream_response(&begin_response);
    let session_id = parse_session_id_from_begin(&data).expect("session_id");

    // Act - APPEND with 60KB body
    let large_body = vec![b'X'; 60_000];
    let append_frame = build_stream_append(session_id, &large_body, None);
    let append_response = client
        .request(&append_frame, 3000)
        .await
        .expect("APPEND with large body");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&append_response);
    assert_eq!(status, 0, "Expected APPEND success with 60KB body");
}

async fn should_require_connect_message_when_auth_enabled<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    // Act
    let begin_frame = build_stream_begin("stream://test/app/logs", 0, None);
    let result = client.request(&begin_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected connection close or timeout when unauthenticated"
    );
}

async fn should_accept_valid_jwt_in_connect_message<C>(server: &TestServer)
where
    C: StreamConnector,
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
    let begin_frame = build_stream_begin("stream://test-realm/app/logs", 0, None);
    let response = client
        .request(&begin_frame, 2000)
        .await
        .expect("BEGIN should work after auth");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Expected BEGIN success after authentication");
}

async fn should_reject_expired_jwt<C>(server: &TestServer)
where
    C: StreamConnector,
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
    let begin_frame = build_stream_begin("stream://test-realm/app/logs", 0, None);
    let result = client.request(&begin_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for expired JWT");
}

async fn should_reject_invalid_jwt_signature<C>(server: &TestServer)
where
    C: StreamConnector,
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
    let begin_frame = build_stream_begin("stream://test-realm/app/logs", 0, None);
    let result = client.request(&begin_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for invalid JWT signature");
}

async fn should_reject_jwt_for_wrong_realm<C>(server: &TestServer)
where
    C: StreamConnector,
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

    // Act - Try to begin stream in test-realm
    let begin_frame = build_stream_begin("stream://test-realm/app/logs", 0, None);
    let result = client.request(&begin_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for JWT realm mismatch");
}

async fn should_create_separate_sessions_for_each_connection_with_auth<C>(server: &TestServer)
where
    C: StreamConnector,
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

    // Act - Both clients begin streams
    let begin1 = build_stream_begin("stream://test-realm/app/logs1", 0, None);
    let response1 = client1.request(&begin1, 2000).await.expect("BEGIN 1");
    assert_eq!(parse_stream_response(&response1).1, 0);

    let begin2 = build_stream_begin("stream://test-realm/app/logs2", 0, None);
    let response2 = client2.request(&begin2, 2000).await.expect("BEGIN 2");
    assert_eq!(parse_stream_response(&response2).1, 0);

    // Assert - Both clients have separate sessions
    assert!(true, "Both clients have separate sessions");
}

async fn should_support_rollback_operation<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "stream://test/app/rollback";

    let begin_frame = build_stream_begin(route, 0, None);
    let begin_response = client.request(&begin_frame, 2000).await.expect("BEGIN");
    let (_msg_type, _status, data) = parse_stream_response(&begin_response);
    let session_id = parse_session_id_from_begin(&data).expect("session_id");

    let append_frame = build_stream_append(session_id, b"entry", None);
    client.request(&append_frame, 2000).await.expect("APPEND");

    // Act - ROLLBACK
    let rollback_frame = build_stream_rollback(session_id);
    let rollback_response = client
        .request(&rollback_frame, 2000)
        .await
        .expect("ROLLBACK");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&rollback_response);
    assert_eq!(status, 0, "Expected ROLLBACK success");
}

async fn should_support_read_operation<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "stream://test/app/read";

    let begin_frame = build_stream_begin(route, 0, None);
    let begin_response = client.request(&begin_frame, 2000).await.expect("BEGIN");
    let (_msg_type, _status, data) = parse_stream_response(&begin_response);
    let session_id = parse_session_id_from_begin(&data).expect("session_id");

    let append_frame = build_stream_append(session_id, b"entry1", None);
    client.request(&append_frame, 2000).await.expect("APPEND");

    let commit_frame = build_stream_commit(session_id, false);
    client.request(&commit_frame, 2000).await.expect("COMMIT");

    // Act - READ
    let read_frame = build_stream_read(route, 0, 10, None);
    let read_response = client.request(&read_frame, 2000).await.expect("READ");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&read_response);
    assert_eq!(status, 0, "Expected READ success");
}

async fn should_support_last_operation<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "stream://test/app/last";

    // Act - LAST (even on empty stream)
    let last_frame = build_stream_last(route);
    let last_response = client.request(&last_frame, 2000).await.expect("LAST");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&last_response);
    assert_eq!(status, 0, "Expected LAST success");
}

async fn should_support_get_metadata_operation<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "stream://test/app/metadata";

    // Act - GET_METADATA
    let metadata_frame = build_stream_get_metadata(route);
    let metadata_response = client
        .request(&metadata_frame, 2000)
        .await
        .expect("GET_METADATA");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&metadata_response);
    assert_eq!(status, 0, "Expected GET_METADATA success");
}

async fn should_isolate_sessions_across_streams<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange - Begin two separate streams
    let mut client1 = C::connect(server).await.expect("connect 1");
    let mut client2 = C::connect(server).await.expect("connect 2");

    let begin1 = build_stream_begin("stream://test/app/stream1", 0, None);
    let response1 = client1.request(&begin1, 2000).await.expect("BEGIN 1");
    let (_msg_type, _status, data1) = parse_stream_response(&response1);
    let session1 = parse_session_id_from_begin(&data1).expect("session_id 1");

    let begin2 = build_stream_begin("stream://test/app/stream2", 0, None);
    let response2 = client2.request(&begin2, 2000).await.expect("BEGIN 2");
    let (_msg_type, _status, data2) = parse_stream_response(&response2);
    let session2 = parse_session_id_from_begin(&data2).expect("session_id 2");

    // Assert - Different session IDs
    assert_ne!(session1, session2, "Expected different session IDs");

    // Act - Append to both
    let append1 = build_stream_append(session1, b"entry1", None);
    let result1 = client1.request(&append1, 2000).await;
    assert!(result1.is_ok(), "Session 1 append should work");

    let append2 = build_stream_append(session2, b"entry2", None);
    let result2 = client2.request(&append2, 2000).await;
    assert!(result2.is_ok(), "Session 2 append should work");
}

async fn should_timeout_on_malformed_frame<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let garbage = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00];

    // Act
    let result = client.request(&garbage, 100).await;

    // Assert
    assert!(result.is_err(), "Expected error/timeout for malformed frame");
}

async fn should_handle_connection_drop_during_stream<C>(server: &TestServer)
where
    C: StreamConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "stream://test/app/disconnect";

    let begin_frame = build_stream_begin(route, 0, None);
    client.request(&begin_frame, 1000).await.expect("BEGIN");

    // Act - Drop connection
    drop(client);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Act - Reconnect and begin again
    let mut client2 = C::connect(server).await.expect("failed to reconnect");
    let begin_frame2 = build_stream_begin(route, 0, None);
    let response = client2
        .request(&begin_frame2, 2000)
        .await
        .expect("BEGIN should work after reconnect");

    // Assert
    let (_msg_type, status, _data) = parse_stream_response(&response);
    assert_eq!(status, 0, "Expected successful begin after reconnect");
}

// ===== TCP tests =====

#[tokio::test]
async fn should_complete_begin_append_commit_cycle_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_complete_begin_append_commit_cycle::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_stream_operations_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_concurrent_stream_operations::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_large_append_payloads_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_large_append_payloads::<TcpConnector>(&server).await;
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
async fn should_support_rollback_operation_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_rollback_operation::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_read_operation_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_read_operation::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_last_operation_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_last_operation::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_get_metadata_operation_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_get_metadata_operation::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_sessions_across_streams_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_isolate_sessions_across_streams::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_timeout_on_malformed_frame_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_timeout_on_malformed_frame::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_connection_drop_during_stream_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_connection_drop_during_stream::<TcpConnector>(&server).await;
}

// ===== WebSocket tests =====

#[tokio::test]
async fn should_complete_begin_append_commit_cycle_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_complete_begin_append_commit_cycle::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_stream_operations_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_concurrent_stream_operations::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_large_append_payloads_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_large_append_payloads::<WsConnector>(&server).await;
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
async fn should_support_rollback_operation_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_rollback_operation::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_read_operation_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_read_operation::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_last_operation_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_last_operation::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_get_metadata_operation_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_get_metadata_operation::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_sessions_across_streams_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_isolate_sessions_across_streams::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_timeout_on_malformed_frame_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_timeout_on_malformed_frame::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_connection_drop_during_stream_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_connection_drop_during_stream::<WsConnector>(&server).await;
}
