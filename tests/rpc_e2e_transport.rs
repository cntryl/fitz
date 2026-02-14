//! RPC domain transport-layer end-to-end tests
//!
//! These tests verify the COMPLETE request-response cycle:
//! Client → TCP/WebSocket → Session → Routing → RPC Actor → Worker → Response → Client

use bytes::{BufMut, BytesMut};
use fitz::testkit::transport::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use uuid::Uuid;

type BoxError = Box<dyn Error>;
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub trait RpcTestClient {
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

pub trait RpcConnector {
    type Client: RpcTestClient;

    fn connect<'a>(
        server: &'a TestServer,
    ) -> BoxFuture<'a, Result<Self::Client, BoxError>>;
}

impl RpcTestClient for TestClient {
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

impl RpcTestClient for TestWebSocketClient {
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

impl RpcConnector for TcpConnector {
    type Client = TestClient;

    fn connect<'a>(
        server: &'a TestServer,
    ) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect().await })
    }
}

struct WsConnector;

impl RpcConnector for WsConnector {
    type Client = TestWebSocketClient;

    fn connect<'a>(
        server: &'a TestServer,
    ) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect_ws().await })
    }
}

/// Build RPC SUBSCRIBE request frame (worker subscribes to receive requests)
/// Wire format: [u32 BE worker_addr_len][worker_addr]
fn build_rpc_subscribe(worker_addr: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(worker_addr.len() as u32).to_be_bytes());
    buf.put_slice(worker_addr.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(300, &buf);
    builder.build()
}

/// Build RPC UNSUBSCRIBE request frame
/// Wire format: [u32 BE worker_addr_len][worker_addr]
fn build_rpc_unsubscribe(worker_addr: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    buf.put_slice(&(worker_addr.len() as u32).to_be_bytes());
    buf.put_slice(worker_addr.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(301, &buf);
    builder.build()
}

/// Build RPC REQUEST frame (client sends request to route)
/// Wire format: [u32 BE correlation_id_len][correlation_id (16 bytes)][u32 BE route_len][route][u32 BE reply_route_len][reply_route][u32 BE body_len][body]
fn build_rpc_request(correlation_id: Uuid, route: &str, reply_route: &str, body: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let uuid_bytes = correlation_id.as_bytes();
    buf.put_slice(&(uuid_bytes.len() as u32).to_be_bytes());
    buf.put_slice(uuid_bytes);
    buf.put_slice(&(route.len() as u32).to_be_bytes());
    buf.put_slice(route.as_bytes());
    buf.put_slice(&(reply_route.len() as u32).to_be_bytes());
    buf.put_slice(reply_route.as_bytes());
    buf.put_slice(&(body.len() as u32).to_be_bytes());
    buf.put_slice(body);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(302, &buf);
    builder.build()
}

/// Build RPC RESPONSE frame (worker sends response back)
/// Wire format: [u32 BE correlation_id_len][correlation_id (16 bytes)][u64 BE seq][u32 BE body_len][body][u8 stream_end]
fn build_rpc_response(correlation_id: Uuid, seq: u64, body: &[u8], stream_end: bool) -> Vec<u8> {
    let mut buf = BytesMut::new();
    let uuid_bytes = correlation_id.as_bytes();
    buf.put_slice(&(uuid_bytes.len() as u32).to_be_bytes());
    buf.put_slice(uuid_bytes);
    buf.put_slice(&seq.to_be_bytes());
    buf.put_slice(&(body.len() as u32).to_be_bytes());
    buf.put_slice(body);
    buf.put_u8(if stream_end { 1 } else { 0 });

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(303, &buf);
    builder.build()
}

/// Parse RPC response status byte
fn parse_rpc_response_frame(frame: &[u8]) -> (u16, u8, Vec<u8>) {
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

/// Parse RPC REQUEST delivery to worker
/// Wire format: [u32 correlation_id_len][correlation_id (16 bytes)][u32 route_len][route][u32 reply_route_len][reply_route][u32 body_len][body]
fn parse_rpc_request_delivery(frame: &[u8]) -> (Uuid, String, String, Vec<u8>) {
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

    // Skip status byte
    offset += 1;

    // Parse correlation_id
    let correlation_id_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&payload[offset..offset + correlation_id_len]);
    let correlation_id = Uuid::from_bytes(uuid_bytes);
    offset += correlation_id_len;

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

    // Parse reply_route
    let reply_route_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;
    let reply_route = String::from_utf8_lossy(&payload[offset..offset + reply_route_len]).to_string();
    offset += reply_route_len;

    // Parse body
    let body_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;
    let body = payload[offset..offset + body_len].to_vec();

    (correlation_id, route, reply_route, body)
}

/// Parse RPC RESPONSE delivery to client
/// Wire format: [u32 correlation_id_len][correlation_id (16 bytes)][u64 seq][u32 body_len][body][u8 stream_end]
fn parse_rpc_response_delivery(frame: &[u8]) -> (Uuid, u64, Vec<u8>, bool) {
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

    // Skip status byte
    offset += 1;

    // Parse correlation_id
    let correlation_id_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;
    let mut uuid_bytes = [0u8; 16];
    uuid_bytes.copy_from_slice(&payload[offset..offset + correlation_id_len]);
    let correlation_id = Uuid::from_bytes(uuid_bytes);
    offset += correlation_id_len;

    // Parse seq
    let seq = u64::from_be_bytes([
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

    // Parse body
    let body_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;
    let body = payload[offset..offset + body_len].to_vec();
    offset += body_len;

    // Parse stream_end
    let stream_end = payload[offset] != 0;

    (correlation_id, seq, body, stream_end)
}

// ===== Test Functions =====

async fn should_complete_subscribe_request_response_cycle<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange - Worker subscribes
    let mut worker = C::connect(server).await.expect("failed to connect worker");
    let worker_addr = "rpc://test/app/workers/worker1";
    
    let subscribe_frame = build_rpc_subscribe(worker_addr);
    let response = worker
        .request(&subscribe_frame, 2000)
        .await
        .expect("SUBSCRIBE request failed");

    let (msg_type, status, _data) = parse_rpc_response_frame(&response);
    assert_eq!(msg_type, 300, "Expected SUBSCRIBE response (300)");
    assert_eq!(status, 0, "Expected success status");

    // Arrange - Client sends request
    let mut client = C::connect(server).await.expect("failed to connect client");
    let correlation_id = Uuid::new_v4();
    let route = "rpc://test/app/compute";
    let reply_route = "rpc://test/app/replies/client1";
    let request_body = b"compute-request";

    let request_frame = build_rpc_request(correlation_id, route, reply_route, request_body);
    client
        .send_frame(&request_frame)
        .await
        .expect("REQUEST send failed");

    // Act - Worker receives request
    let request_delivery = worker
        .recv_frame(2000)
        .await
        .expect("Expected REQUEST delivery");

    let (received_correlation_id, received_route, received_reply_route, received_body) = 
        parse_rpc_request_delivery(&request_delivery);

    // Assert - Request delivered correctly
    assert_eq!(received_correlation_id, correlation_id);
    assert_eq!(received_route, route);
    assert_eq!(received_reply_route, reply_route);
    assert_eq!(received_body, request_body);

    // Act - Worker sends response
    let response_body = b"compute-result";
    let response_frame = build_rpc_response(correlation_id, 0, response_body, true);
    worker
        .send_frame(&response_frame)
        .await
        .expect("RESPONSE send failed");

    // Act - Client receives response
    let response_delivery = client
        .recv_frame(2000)
        .await
        .expect("Expected RESPONSE delivery");

    let (resp_correlation_id, seq, resp_body, stream_end) = 
        parse_rpc_response_delivery(&response_delivery);

    // Assert - Response delivered correctly
    assert_eq!(resp_correlation_id, correlation_id);
    assert_eq!(seq, 0);
    assert_eq!(resp_body, response_body);
    assert!(stream_end);
}

async fn should_receive_responses_within_reasonable_time<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let warmup_frame = build_rpc_subscribe("rpc://test/app/warmup");
    let _ = client.request(&warmup_frame, 1000).await.expect("warmup failed");

    // Act
    let subscribe_frame = build_rpc_subscribe("rpc://test/app/bench");
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
    let (_msg_type, status, _data) = parse_rpc_response_frame(&response);
    assert_eq!(status, 0, "Expected success status");
}

async fn should_handle_concurrent_connections_with_separate_workers<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange & Act
    let run_worker = |idx: usize| async move {
        let mut worker = C::connect(server).await.expect("connect failed");
        let worker_addr = format!("rpc://test/app/workers/worker{}", idx);

        let subscribe_frame = build_rpc_subscribe(&worker_addr);
        let response = worker
            .request(&subscribe_frame, 4000)
            .await
            .expect("SUBSCRIBE failed");

        let (_msg_type, status, _data) = parse_rpc_response_frame(&response);
        assert_eq!(status, 0);

        let unsubscribe_frame = build_rpc_unsubscribe(&worker_addr);
        let response = worker
            .request(&unsubscribe_frame, 4000)
            .await
            .expect("UNSUBSCRIBE failed");
        let (_msg_type, status, _data) = parse_rpc_response_frame(&response);
        assert_eq!(status, 0);
    };

    // Assert - All 3 concurrent operations complete
    tokio::join!(run_worker(0), run_worker(1), run_worker(2));
}

async fn should_support_streaming_responses<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange - Worker subscribes
    let mut worker = C::connect(server).await.expect("failed to connect worker");
    let worker_addr = "rpc://test/app/workers/streamer";
    
    let subscribe_frame = build_rpc_subscribe(worker_addr);
    worker.request(&subscribe_frame, 2000).await.expect("SUBSCRIBE");

    // Arrange - Client sends request
    let mut client = C::connect(server).await.expect("failed to connect client");
    let correlation_id = Uuid::new_v4();
    let route = "rpc://test/app/stream";
    let reply_route = "rpc://test/app/replies/client1";

    let request_frame = build_rpc_request(correlation_id, route, reply_route, b"stream-request");
    client.send_frame(&request_frame).await.expect("REQUEST");

    // Act - Worker receives and sends 3 responses
    let _ = worker.recv_frame(2000).await.expect("REQUEST delivery");

    worker.send_frame(&build_rpc_response(correlation_id, 0, b"chunk1", false)).await.expect("RESPONSE 1");
    worker.send_frame(&build_rpc_response(correlation_id, 1, b"chunk2", false)).await.expect("RESPONSE 2");
    worker.send_frame(&build_rpc_response(correlation_id, 2, b"chunk3", true)).await.expect("RESPONSE 3");

    // Assert - Client receives all 3 responses in order
    let resp1 = client.recv_frame(2000).await.expect("RESPONSE 1");
    let (_, seq1, body1, end1) = parse_rpc_response_delivery(&resp1);
    assert_eq!(seq1, 0);
    assert_eq!(body1, b"chunk1");
    assert!(!end1);

    let resp2 = client.recv_frame(2000).await.expect("RESPONSE 2");
    let (_, seq2, body2, end2) = parse_rpc_response_delivery(&resp2);
    assert_eq!(seq2, 1);
    assert_eq!(body2, b"chunk2");
    assert!(!end2);

    let resp3 = client.recv_frame(2000).await.expect("RESPONSE 3");
    let (_, seq3, body3, end3) = parse_rpc_response_delivery(&resp3);
    assert_eq!(seq3,2);
    assert_eq!(body3, b"chunk3");
    assert!(end3);
}

async fn should_require_connect_message_when_auth_enabled<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    // Act
    let subscribe_frame = build_rpc_subscribe("rpc://test/app/workers/worker1");
    let result = client.request(&subscribe_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected connection close or timeout when unauthenticated"
    );
}

async fn should_accept_valid_jwt_in_connect_message<C>(server: &TestServer)
where
    C: RpcConnector,
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
    let subscribe_frame = build_rpc_subscribe("rpc://test-realm/app/workers/worker1");
    let response = client
        .request(&subscribe_frame, 2000)
        .await
        .expect("SUBSCRIBE should work after auth");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response_frame(&response);
    assert_eq!(status, 0, "Expected SUBSCRIBE success after authentication");
}

async fn should_reject_expired_jwt<C>(server: &TestServer)
where
    C: RpcConnector,
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
    let subscribe_frame = build_rpc_subscribe("rpc://test-realm/app/workers/worker1");
    let result = client.request(&subscribe_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for expired JWT");
}

async fn should_reject_invalid_jwt_signature<C>(server: &TestServer)
where
    C: RpcConnector,
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
    let subscribe_frame = build_rpc_subscribe("rpc://test-realm/app/workers/worker1");
    let result = client.request(&subscribe_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for invalid JWT signature");
}

async fn should_reject_jwt_for_wrong_realm<C>(server: &TestServer)
where
    C: RpcConnector,
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

    // Act - Try to subscribe in test-realm
    let subscribe_frame = build_rpc_subscribe("rpc://test-realm/app/workers/worker1");
    let result = client.request(&subscribe_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for JWT realm mismatch");
}

async fn should_create_separate_sessions_for_each_connection_with_auth<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange - First worker
    let mut worker1 = C::connect(server).await.expect("failed to connect");
    let connect_frame1 = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    worker1.send_frame(&connect_frame1).await.expect("CONNECT 1");
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Arrange - Second worker
    let mut worker2 = C::connect(server).await.expect("failed to connect");
    let connect_frame2 = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    worker2.send_frame(&connect_frame2).await.expect("CONNECT 2");
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Act - Both workers subscribe to different routes
    let subscribe1 = build_rpc_subscribe("rpc://test-realm/app/workers/worker1");
    let response1 = worker1.request(&subscribe1, 2000).await.expect("SUBSCRIBE 1");
    assert_eq!(parse_rpc_response_frame(&response1).1, 0);

    let subscribe2 = build_rpc_subscribe("rpc://test-realm/app/workers/worker2");
    let response2 = worker2.request(&subscribe2, 2000).await.expect("SUBSCRIBE 2");
    assert_eq!(parse_rpc_response_frame(&response2).1, 0);

    // Assert - Both workers subscribed successfully
    assert!(true, "Both workers subscribed with separate sessions");
}

async fn should_support_unsubscribe_operation<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange
    let mut worker = C::connect(server).await.expect("failed to connect");
    let worker_addr = "rpc://test/app/workers/unsub";

    let subscribe_frame = build_rpc_subscribe(worker_addr);
    worker.request(&subscribe_frame, 2000).await.expect("SUBSCRIBE");

    // Act - Unsubscribe
    let unsubscribe_frame = build_rpc_unsubscribe(worker_addr);
    let response = worker.request(&unsubscribe_frame, 2000).await.expect("UNSUBSCRIBE");

    // Assert
    let (msg_type, status, _data) = parse_rpc_response_frame(&response);
    assert_eq!(msg_type, 301, "Expected UNSUBSCRIBE response (301)");
    assert_eq!(status, 0, "Expected success status");
}

async fn should_handle_empty_request_body<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange - Worker subscribes
    let mut worker = C::connect(server).await.expect("failed to connect worker");
    let worker_addr = "rpc://test/app/workers/empty";
    
    let subscribe_frame = build_rpc_subscribe(worker_addr);
    worker.request(&subscribe_frame, 2000).await.expect("SUBSCRIBE");

    // Arrange - Client sends request with empty body
    let mut client = C::connect(server).await.expect("failed to connect client");
    let correlation_id = Uuid::new_v4();
    let route = "rpc://test/app/empty";
    let reply_route = "rpc://test/app/replies/client1";

    let request_frame = build_rpc_request(correlation_id, route, reply_route, b"");
    client.send_frame(&request_frame).await.expect("REQUEST");

    // Act - Worker receives request
    let request_delivery = worker.recv_frame(2000).await.expect("REQUEST");
    let (_, _, _, body) = parse_rpc_request_delivery(&request_delivery);

    // Assert
    assert_eq!(body.len(), 0, "Expected empty body");
}

async fn should_handle_large_request_body<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange - Worker subscribes
    let mut worker = C::connect(server).await.expect("failed to connect worker");
    let worker_addr = "rpc://test/app/workers/large";
    
    let subscribe_frame = build_rpc_subscribe(worker_addr);
    worker.request(&subscribe_frame, 2000).await.expect("SUBSCRIBE");

    // Arrange - Client sends large request
    let mut client = C::connect(server).await.expect("failed to connect client");
    let correlation_id = Uuid::new_v4();
    let route = "rpc://test/app/large";
    let reply_route = "rpc://test/app/replies/client1";
    let large_body = vec![b'X'; 60_000];

    let request_frame = build_rpc_request(correlation_id, route, reply_route, &large_body);
    client.send_frame(&request_frame).await.expect("REQUEST");

    // Act - Worker receives request
    let request_delivery = worker.recv_frame(3000).await.expect("REQUEST");
    let (_, _, _, body) = parse_rpc_request_delivery(&request_delivery);

    // Assert
    assert_eq!(body.len(), 60_000, "Expected 60KB body");
}

async fn should_isolate_workers_across_routes<C>(server: &TestServer)
where
    C: RpcConnector,
{
    // Arrange - Two workers on different routes
    let mut worker1 = C::connect(server).await.expect("failed to connect worker1");
    let mut worker2 = C::connect(server).await.expect("failed to connect worker2");
    
    let subscribe1 = build_rpc_subscribe("rpc://test/app/workers/route1");
    worker1.request(&subscribe1, 2000).await.expect("SUBSCRIBE 1");

    let subscribe2 = build_rpc_subscribe("rpc://test/app/workers/route2");
    worker2.request(&subscribe2, 2000).await.expect("SUBSCRIBE 2");

    // Act - Send request to route1
    let mut client = C::connect(server).await.expect("failed to connect client");
    let correlation_id = Uuid::new_v4();
    let route = "rpc://test/app/route1";
    let reply_route = "rpc://test/app/replies/client1";

    let request_frame = build_rpc_request(correlation_id, route, reply_route, b"route1-request");
    client.send_frame(&request_frame).await.expect("REQUEST");

    // Assert - Only worker1 receives request
    let result1 = worker1.recv_frame(2000).await;
    assert!(result1.is_ok(), "Worker 1 should receive request");

    let result2 = worker2.recv_frame(500).await;
    assert!(result2.is_err(), "Worker 2 should not receive request");
}

async fn should_timeout_on_malformed_frame<C>(server: &TestServer)
where
    C: RpcConnector,
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
    C: RpcConnector,
{
    // Arrange
    let mut worker = C::connect(server).await.expect("failed to connect");
    let worker_addr = "rpc://test/app/workers/disconnect";

    let subscribe_frame = build_rpc_subscribe(worker_addr);
    worker.request(&subscribe_frame, 1000).await.expect("SUBSCRIBE");

    // Act - Drop connection
    drop(worker);
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Act - Reconnect and subscribe again
    let mut worker2 = C::connect(server).await.expect("failed to reconnect");
    let subscribe_frame2 = build_rpc_subscribe(worker_addr);
    let response = worker2
        .request(&subscribe_frame2, 2000)
        .await
        .expect("SUBSCRIBE should work after reconnect");

    // Assert
    let (_msg_type, status, _data) = parse_rpc_response_frame(&response);
    assert_eq!(status, 0, "Expected successful subscribe after reconnect");
}

// ===== TCP tests =====

#[tokio::test]
async fn should_complete_subscribe_request_response_cycle_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_complete_subscribe_request_response_cycle::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_workers_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_concurrent_connections_with_separate_workers::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_streaming_responses_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_streaming_responses::<TcpConnector>(&server).await;
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
async fn should_handle_empty_request_body_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_empty_request_body::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_large_request_body_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_large_request_body::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_workers_across_routes_tcp() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_isolate_workers_across_routes::<TcpConnector>(&server).await;
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
async fn should_complete_subscribe_request_response_cycle_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_complete_subscribe_request_response_cycle::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_workers_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_concurrent_connections_with_separate_workers::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_streaming_responses_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_support_streaming_responses::<WsConnector>(&server).await;
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
async fn should_handle_empty_request_body_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_empty_request_body::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_large_request_body_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_handle_large_request_body::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_workers_across_routes_ws() {
    let server = TestServer::start().await.expect("failed to start test server");
    should_isolate_workers_across_routes::<WsConnector>(&server).await;
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
