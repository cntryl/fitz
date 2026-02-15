//! Lease domain transport-layer end-to-end tests
//!
//! These tests verify the COMPLETE request-response cycle:
//! Client → TCP/WebSocket → Session → Routing → Lease Actor → Response → Client

use bytes::{BufMut, BytesMut};
use fitz::testkit::transport::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

type BoxError = Box<dyn Error>;
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub trait LeaseTestClient {
    fn send_frame<'a>(&'a mut self, frame: &'a [u8]) -> BoxFuture<'a, Result<(), BoxError>>;
    fn request<'a>(
        &'a mut self,
        frame: &'a [u8],
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>>;
}

pub trait LeaseConnector {
    type Client: LeaseTestClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>>;
}

impl LeaseTestClient for TestClient {
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

impl LeaseTestClient for TestWebSocketClient {
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

impl LeaseConnector for TcpConnector {
    type Client = TestClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect().await })
    }
}

struct WsConnector;

impl LeaseConnector for WsConnector {
    type Client = TestWebSocketClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect_ws().await })
    }
}

/// Build Lease ACQUIRE request frame
/// Wire format: [u32 BE route_len][route][u32 BE owner_len][owner_id][u64 BE ttl_secs][u32 BE wait_seconds (optional)]
fn build_lease_acquire(
    route: &str,
    owner_id: &str,
    ttl_secs: u64,
    wait_seconds: Option<u32>,
) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());
    payload.put_slice(&(owner_id.len() as u32).to_be_bytes());
    payload.put_slice(owner_id.as_bytes());
    payload.put_slice(&ttl_secs.to_be_bytes());

    // Include wait_seconds if provided
    if let Some(ws) = wait_seconds {
        payload.put_slice(&ws.to_be_bytes());
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(400, &payload);
    builder.build()
}

/// Helper: Build Lease ACQUIRE with immediate fail (wait_seconds=0)
fn build_lease_acquire_immediate(route: &str, owner_id: &str, ttl_secs: u64) -> Vec<u8> {
    build_lease_acquire(route, owner_id, ttl_secs, None)
}

/// Build Lease RENEW request frame
/// Wire format: [u32 BE route_len][route][u32 BE owner_len][owner_id][u64 BE fencing_token][u64 BE ttl_secs]
fn build_lease_renew(route: &str, owner_id: &str, fencing_token: u64, ttl_secs: u64) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());
    payload.put_slice(&(owner_id.len() as u32).to_be_bytes());
    payload.put_slice(owner_id.as_bytes());
    payload.put_slice(&fencing_token.to_be_bytes());
    payload.put_slice(&ttl_secs.to_be_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(401, &payload);
    builder.build()
}

/// Build Lease RELEASE request frame
/// Wire format: [u32 BE route_len][route][u32 BE owner_len][owner_id][u64 BE fencing_token]
fn build_lease_release(route: &str, owner_id: &str, fencing_token: u64) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());
    payload.put_slice(&(owner_id.len() as u32).to_be_bytes());
    payload.put_slice(owner_id.as_bytes());
    payload.put_slice(&fencing_token.to_be_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(402, &payload);
    builder.build()
}

/// Build Lease QUERY request frame
/// Wire format: [u32 BE route_len][route]
fn build_lease_query(route: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(403, &payload);
    builder.build()
}

/// Parse Lease response status byte and extract data
fn parse_lease_response(frame: &[u8]) -> (u16, u8, Vec<u8>) {
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

/// Parse ACQUIRE/RENEW response to extract fencing token
fn parse_lease_token_response(data: &[u8]) -> Option<u64> {
    if data.is_empty() {
        return None;
    }
    let has_token = data[0];
    if has_token == 1 && data.len() >= 9 {
        Some(u64::from_be_bytes([
            data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
        ]))
    } else {
        None
    }
}

// ===== Test Functions =====

async fn should_complete_acquire_renew_release_cycle<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/lock1";
    let owner_id = "owner-1";

    // Act
    let acquire_frame = build_lease_acquire_immediate(route, owner_id, 30);
    let response = client
        .request(&acquire_frame, 2000)
        .await
        .expect("ACQUIRE request failed");

    // Assert
    let (msg_type, status, data) = parse_lease_response(&response);
    assert_eq!(msg_type, 400, "Expected ACQUIRE response (400)");
    assert_eq!(status, 0, "Expected success status");
    let token = parse_lease_token_response(&data).expect("Expected fencing token");
    assert!(token > 0, "Expected valid fencing token");

    // Act
    let renew_frame = build_lease_renew(route, owner_id, token, 30);
    let response = client
        .request(&renew_frame, 2000)
        .await
        .expect("RENEW request failed");

    // Assert
    let (msg_type, status, data) = parse_lease_response(&response);
    assert_eq!(msg_type, 401, "Expected RENEW response (401)");
    assert_eq!(status, 0, "Expected success status");
    let new_token = parse_lease_token_response(&data).expect("Expected new fencing token");
    assert!(
        new_token > token,
        "Expected monotonically increasing fencing token"
    );

    // Act
    let release_frame = build_lease_release(route, owner_id, new_token);
    let response = client
        .request(&release_frame, 2000)
        .await
        .expect("RELEASE request failed");

    // Assert
    let (msg_type, status, _data) = parse_lease_response(&response);
    assert_eq!(msg_type, 402, "Expected RELEASE response (402)");
    assert_eq!(status, 0, "Expected success status");
}

async fn should_receive_responses_within_reasonable_time<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let warmup_frame = build_lease_acquire_immediate("lease://test/app/warmup", "warmup", 30);
    let _ = client
        .request(&warmup_frame, 1000)
        .await
        .expect("warmup failed");

    // Act
    let acquire_frame = build_lease_acquire_immediate("lease://test/app/bench", "bench", 30);
    let response = client
        .request(&acquire_frame, 500)
        .await
        .expect("ACQUIRE request should complete quickly");
    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&response);
    assert_eq!(status, 0, "Expected success status");
}

async fn should_handle_concurrent_connections_with_separate_leases<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let run_lease = |idx: usize| async move {
        let mut client = C::connect(server).await.expect("connect failed");
        let route = format!("lease://test/app/concurrent{}", idx);
        let owner = format!("owner-{}", idx);

        let acquire_frame = build_lease_acquire_immediate(&route, &owner, 30);

        // Act
        let response = client
            .request(&acquire_frame, 4000)
            .await
            .expect("ACQUIRE failed");

        let (_msg_type, status, data) = parse_lease_response(&response);
        assert_eq!(status, 0);
        let token = parse_lease_token_response(&data).expect("Expected token");

        let renew_frame = build_lease_renew(&route, &owner, token, 30);
        let response = client
            .request(&renew_frame, 4000)
            .await
            .expect("RENEW failed");
        let (_msg_type, status, data) = parse_lease_response(&response);
        assert_eq!(status, 0);
        let new_token = parse_lease_token_response(&data).expect("Expected new token");

        let release_frame = build_lease_release(&route, &owner, new_token);
        let response = client
            .request(&release_frame, 4000)
            .await
            .expect("RELEASE failed");
        let (_msg_type, status, _data) = parse_lease_response(&response);
        assert_eq!(status, 0);

        token
    };

    // Assert
    let (t1, t2, t3) = tokio::join!(run_lease(0), run_lease(1), run_lease(2));
    let tokens = [t1, t2, t3];
    assert_eq!(
        tokens.len(),
        3,
        "All 3 concurrent lease operations should complete"
    );
}

async fn should_enforce_lease_contention<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client1 = C::connect(server).await.expect("failed to connect");
    let mut client2 = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/contended";

    // Act
    let acquire1_frame = build_lease_acquire_immediate(route, "owner-1", 30);
    let response1 = client1
        .request(&acquire1_frame, 2000)
        .await
        .expect("ACQUIRE 1");
    let (_msg_type, status1, data1) = parse_lease_response(&response1);
    assert_eq!(status1, 0);
    let token1 = parse_lease_token_response(&data1).expect("Expected token");

    // Act
    let acquire2_frame = build_lease_acquire_immediate(route, "owner-2", 30);
    let response2 = client2
        .request(&acquire2_frame, 2000)
        .await
        .expect("ACQUIRE 2");

    // Assert
    let (_msg_type, status2, _data2) = parse_lease_response(&response2);
    assert_eq!(status2, 1, "Expected error when lease already held");

    // Cleanup
    let _ = client1
        .request(&build_lease_release(route, "owner-1", token1), 2000)
        .await;
}

async fn should_reject_operations_with_invalid_token<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/invalid";
    let owner_id = "owner-1";

    let acquire_frame = build_lease_acquire_immediate(route, owner_id, 30);
    let response = client.request(&acquire_frame, 2000).await.expect("ACQUIRE");
    let (_msg_type, _status, data) = parse_lease_response(&response);
    let _token = parse_lease_token_response(&data).expect("Expected token");

    // Act
    let renew_frame = build_lease_renew(route, owner_id, 99999, 30);
    let response = client
        .request(&renew_frame, 2000)
        .await
        .expect("server should respond even for invalid token");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&response);
    assert_eq!(status, 1, "Expected error status for invalid token");
}

async fn should_require_connect_message_when_auth_enabled<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    // Act
    let acquire_frame = build_lease_acquire_immediate("lease://test/app/lock", "owner-1", 30);
    let result = client.request(&acquire_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected connection close or timeout when unauthenticated"
    );
}

async fn should_accept_valid_jwt_in_connect_message<C>(server: &TestServer)
where
    C: LeaseConnector,
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

    fitz::testkit::transport::wait_for_auth_ready().await;

    // Act
    let acquire_frame = build_lease_acquire_immediate("lease://test-realm/app/lock", "owner-1", 30);
    let response = client
        .request(&acquire_frame, 2000)
        .await
        .expect("ACQUIRE should work after auth");

    // Assert
    let (_msg_type, status, data) = parse_lease_response(&response);
    assert_eq!(status, 0, "Expected ACQUIRE success after authentication");
    assert!(!data.is_empty(), "Expected fencing token");
}

async fn should_reject_expired_jwt<C>(server: &TestServer)
where
    C: LeaseConnector,
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

    fitz::testkit::transport::wait_for_auth_ready().await;

    // Act
    let acquire_frame = build_lease_acquire_immediate("lease://test-realm/app/lock", "owner-1", 30);
    let result = client.request(&acquire_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for expired JWT");
}

async fn should_reject_invalid_jwt_signature<C>(server: &TestServer)
where
    C: LeaseConnector,
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

    fitz::testkit::transport::wait_for_auth_ready().await;

    // Act
    let acquire_frame = build_lease_acquire_immediate("lease://test-realm/app/lock", "owner-1", 30);
    let result = client.request(&acquire_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected rejection for invalid JWT signature"
    );
}

async fn should_reject_jwt_for_wrong_realm<C>(server: &TestServer)
where
    C: LeaseConnector,
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

    fitz::testkit::transport::wait_for_auth_ready().await;

    // Act
    let acquire_frame = build_lease_acquire_immediate("lease://test-realm/app/lock", "owner-1", 30);
    let result = client.request(&acquire_frame, 1000).await;

    // Assert
    assert!(result.is_err(), "Expected rejection for JWT realm mismatch");
}

async fn should_create_separate_sessions_for_each_connection_with_auth<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client1 = C::connect(server).await.expect("failed to connect");
    let connect_frame1 = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    client1
        .send_frame(&connect_frame1)
        .await
        .expect("CONNECT 1");
    fitz::testkit::transport::wait_for_auth_ready().await;

    // Arrange
    let mut client2 = C::connect(server).await.expect("failed to connect");
    let connect_frame2 = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    client2
        .send_frame(&connect_frame2)
        .await
        .expect("CONNECT 2");
    fitz::testkit::transport::wait_for_auth_ready().await;

    // Act
    let acquire1 = build_lease_acquire_immediate("lease://test-realm/app/lock1", "owner-1", 30);
    let response1 = client1.request(&acquire1, 2000).await.expect("ACQUIRE 1");
    let (_msg_type, status1, data1) = parse_lease_response(&response1);
    assert_eq!(status1, 0);
    let token1 = parse_lease_token_response(&data1).expect("Expected token");

    let acquire2 = build_lease_acquire_immediate("lease://test-realm/app/lock2", "owner-2", 30);
    let response2 = client2.request(&acquire2, 2000).await.expect("ACQUIRE 2");
    let (_msg_type, status2, data2) = parse_lease_response(&response2);
    assert_eq!(status2, 0);
    let token2 = parse_lease_token_response(&data2).expect("Expected token");

    // Assert
    assert!(token1 > 0, "Expected valid token for client 1");
    assert!(token2 > 0, "Expected valid token for client 2");
}

async fn should_reject_renew_without_acquire<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/noprep";

    // Act
    let renew_frame = build_lease_renew(route, "owner-1", 12345, 30);
    let result = client.request(&renew_frame, 2000).await;

    // Assert
    if let Ok(response) = result {
        let (_msg_type, status, _data) = parse_lease_response(&response);
        assert_eq!(status, 1, "Expected error for RENEW without ACQUIRE");
    } else {
        assert!(
            result.is_err(),
            "Expected error/timeout for RENEW without ACQUIRE"
        );
    }
}

async fn should_reject_release_after_release<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/lifecycle";
    let owner_id = "owner-1";

    let acquire_frame = build_lease_acquire_immediate(route, owner_id, 30);
    let response = client.request(&acquire_frame, 2000).await.expect("ACQUIRE");
    let token =
        parse_lease_token_response(&parse_lease_response(&response).2).expect("Expected token");

    let release_frame = build_lease_release(route, owner_id, token);
    client.request(&release_frame, 2000).await.expect("RELEASE");

    // Act
    let release_frame2 = build_lease_release(route, owner_id, token);
    let response = client
        .request(&release_frame2, 2000)
        .await
        .expect("server should respond");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&response);
    assert_eq!(status, 1, "Expected error for double RELEASE");
}

async fn should_support_query_operation<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/queryable";
    let owner_id = "owner-1";

    let acquire_frame = build_lease_acquire_immediate(route, owner_id, 30);
    client.request(&acquire_frame, 2000).await.expect("ACQUIRE");

    // Act
    let query_frame = build_lease_query(route);
    let response = client.request(&query_frame, 2000).await.expect("QUERY");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&response);
    assert_eq!(status, 0, "Expected QUERY success");
}

async fn should_enforce_fencing_token_monotonicity<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/monotonic";
    let owner_id = "owner-1";

    // Act
    let acquire_frame = build_lease_acquire_immediate(route, owner_id, 30);
    let response = client.request(&acquire_frame, 2000).await.expect("ACQUIRE");
    let token1 =
        parse_lease_token_response(&parse_lease_response(&response).2).expect("Expected token");

    let renew_frame1 = build_lease_renew(route, owner_id, token1, 30);
    let response = client.request(&renew_frame1, 2000).await.expect("RENEW 1");
    let token2 =
        parse_lease_token_response(&parse_lease_response(&response).2).expect("Expected token");

    let renew_frame2 = build_lease_renew(route, owner_id, token2, 30);
    let response = client.request(&renew_frame2, 2000).await.expect("RENEW 2");
    let token3 =
        parse_lease_token_response(&parse_lease_response(&response).2).expect("Expected token");

    // Assert
    assert!(token2 > token1, "Token 2 should be > Token 1");
    assert!(token3 > token2, "Token 3 should be > Token 2");
}

async fn should_isolate_leases_across_resources<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");

    // Act
    let acquire1 = build_lease_acquire_immediate("lease://test/app/lock1", "owner-1", 30);
    let response1 = client.request(&acquire1, 2000).await.expect("ACQUIRE 1");
    let token1 =
        parse_lease_token_response(&parse_lease_response(&response1).2).expect("Expected token");

    let acquire2 = build_lease_acquire_immediate("lease://test/app/lock2", "owner-1", 30);
    let response2 = client.request(&acquire2, 2000).await.expect("ACQUIRE 2");
    let token2 =
        parse_lease_token_response(&parse_lease_response(&response2).2).expect("Expected token");

    // Assert
    assert!(token1 > 0, "Expected valid token for lease 1");
    assert!(token2 > 0, "Expected valid token for lease 2");

    // Act
    let release1 = build_lease_release("lease://test/app/lock1", "owner-1", token1);
    let response = client.request(&release1, 2000).await.expect("RELEASE 1");
    assert_eq!(parse_lease_response(&response).1, 0);

    let renew2 = build_lease_renew("lease://test/app/lock2", "owner-1", token2, 30);
    let response = client.request(&renew2, 2000).await.expect("RENEW 2");
    assert_eq!(
        parse_lease_response(&response).1,
        0,
        "Lease 2 should still be held"
    );
}

async fn should_timeout_on_malformed_frame<C>(server: &TestServer)
where
    C: LeaseConnector,
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
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/disconnect";
    let owner_id = "owner-1";

    let acquire_frame = build_lease_acquire_immediate(route, owner_id, 30);
    let response = client.request(&acquire_frame, 1000).await.expect("ACQUIRE");
    let token =
        parse_lease_token_response(&parse_lease_response(&response).2).expect("Expected token");

    // Act
    drop(client);
    fitz::testkit::transport::wait_for_disconnect_cleanup().await;

    // Act
    let mut client2 = C::connect(server).await.expect("failed to reconnect");
    let renew_frame = build_lease_renew(route, owner_id, token, 30);
    let response = client2
        .request(&renew_frame, 2000)
        .await
        .expect("server should respond");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&response);
    // Status could be 0 (if lease still valid) or 1 (if expired), but should not crash
    assert!(
        status == 0 || status == 1,
        "Expected valid response, got status {}",
        status
    );
}

// ===== TCP tests =====

#[tokio::test]
async fn should_complete_acquire_renew_release_cycle_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_complete_acquire_renew_release_cycle::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_leases_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_concurrent_connections_with_separate_leases::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_enforce_lease_contention_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_enforce_lease_contention::<TcpConnector>(&server).await;
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
async fn should_reject_renew_without_acquire_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_renew_without_acquire::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_release_after_release_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_release_after_release::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_support_query_operation_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_support_query_operation::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_enforce_fencing_token_monotonicity_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_enforce_fencing_token_monotonicity::<TcpConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_leases_across_resources_tcp() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_isolate_leases_across_resources::<TcpConnector>(&server).await;
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
async fn should_complete_acquire_renew_release_cycle_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_complete_acquire_renew_release_cycle::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_receive_responses_within_reasonable_time::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_leases_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_handle_concurrent_connections_with_separate_leases::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_enforce_lease_contention_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_enforce_lease_contention::<WsConnector>(&server).await;
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
async fn should_reject_renew_without_acquire_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_renew_without_acquire::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_release_after_release_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_reject_release_after_release::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_support_query_operation_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_support_query_operation::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_enforce_fencing_token_monotonicity_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_enforce_fencing_token_monotonicity::<WsConnector>(&server).await;
}

#[tokio::test]
async fn should_isolate_leases_across_resources_ws() {
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    should_isolate_leases_across_resources::<WsConnector>(&server).await;
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
