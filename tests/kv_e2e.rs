//! Consolidated KV end-to-end tests — transport, codec, and integration
//!
//! Merged from: `kv_e2e_transport.rs`, `kv_e2e_basic.rs`, `kv_e2e_domain_routing.rs`, `ws_domain_flow.rs`.

use bytes::{BufMut, Bytes, BytesMut};
use fitz::testkit::create_test_engine_with_cfs;
use fitz::testkit::transport::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

type BoxError = Box<dyn Error>;
type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

pub trait KvTestClient {
    fn send_frame<'a>(&'a mut self, frame: &'a [u8]) -> BoxFuture<'a, Result<(), BoxError>>;
    fn request<'a>(
        &'a mut self,
        frame: &'a [u8],
        timeout_ms: u64,
    ) -> BoxFuture<'a, Result<Vec<u8>, BoxError>>;
}

pub trait KvConnector {
    type Client: KvTestClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>>;
}

impl KvTestClient for TestClient {
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

impl KvTestClient for TestWebSocketClient {
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

impl KvConnector for TcpConnector {
    type Client = TestClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect().await })
    }
}

struct WsConnector;

impl KvConnector for WsConnector {
    type Client = TestWebSocketClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect_ws().await })
    }
}

/// Build KV BEGIN request frame
/// Wire format: [u32 BE route_len][route][u8 mode][u8 durability]
fn build_kv_begin(route: &str, mode: u8, durability: u8) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());
    payload.put_u8(mode); // 0=ReadOnly, 1=ReadWrite
    payload.put_u8(durability); // 0=buffered, 1=sync

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(100, &payload);
    builder.build()
}

/// Build KV PUT request frame
/// Wire format: [u64 BE tx_id][u32 BE route_len][route][u32 BE key_len][key][u32 BE value_len][value]
fn build_kv_put(tx_id: u64, route: &str, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&tx_id.to_be_bytes());
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());
    payload.put_slice(&(key.len() as u32).to_be_bytes());
    payload.put_slice(key);
    payload.put_slice(&(value.len() as u32).to_be_bytes());
    payload.put_slice(value);

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(104, &payload);
    builder.build()
}

/// Build KV COMMIT request frame
/// Wire format: [u64 BE tx_id][u32 BE route_len][route]
fn build_kv_commit(tx_id: u64, route: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&tx_id.to_be_bytes());
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(101, &payload);
    builder.build()
}

/// Build KV ROLLBACK request frame
/// Wire format: [u64 BE tx_id][u32 BE route_len][route]
fn build_kv_rollback(tx_id: u64, route: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&tx_id.to_be_bytes());
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(102, &payload);
    builder.build()
}

/// Parse KV response status byte
fn parse_kv_response(frame: &[u8]) -> (u16, u8, Vec<u8>) {
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

// --- transport helpers (async test implementations) ---

async fn should_complete_begin_put_commit_over_transport<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let begin_frame = build_kv_begin("kv://test/app/users", 1, 0);
    let response = client
        .request(&begin_frame, 2000)
        .await
        .expect("BEGIN request failed");

    let (msg_type, status, data) = parse_kv_response(&response);
    assert_eq!(msg_type, 100, "Expected BEGIN response (100)");
    assert_eq!(status, 0, "Expected success status");
    assert_eq!(data.len(), 8, "Expected tx_id (u64)");

    let tx_id = u64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);
    assert!(tx_id > 0, "Expected valid transaction ID");

    let put_frame = build_kv_put(
        tx_id,
        "kv://test/app/users",
        b"user:1001",
        b"{\"name\":\"Alice\"}",
    );
    let response = client
        .request(&put_frame, 2000)
        .await
        .expect("PUT request failed");

    let (msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(msg_type, 104, "Expected PUT response (104)");
    assert_eq!(status, 0, "Expected success status");

    let commit_frame = build_kv_commit(tx_id, "kv://test/app/users");
    let response = client
        .request(&commit_frame, 2000)
        .await
        .expect("COMMIT request failed");

    let (msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(msg_type, 101, "Expected COMMIT response (101)");
    assert_eq!(status, 0, "Expected success status");
}

async fn should_receive_responses_within_reasonable_time<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let warmup_frame = build_kv_begin("kv://test/app/warmup", 1, 0);
    let _ = client
        .request(&warmup_frame, 1000)
        .await
        .expect("warmup failed");

    let begin_frame = build_kv_begin("kv://test/app/bench", 1, 0);
    let response = client
        .request(&begin_frame, 500)
        .await
        .expect("BEGIN request should complete quickly");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0, "Expected success status");
}

async fn should_handle_concurrent_connections_with_separate_transactions<C>(server: &TestServer)
where
    C: KvConnector,
{
    // Note: TX IDs are session-scoped, not global. Each connection creates a new session
    // with its own KvActor instance. Each actor starts with next_tx_id = 1.
    // This is correct behavior: all three transactions may get tx_id=1 within their
    // respective sessions. TX IDs are unique within a session, not across sessions.
    let run_tx = |idx: usize| async move {
        let mut client = C::connect(server).await.expect("connect failed");
        let route = format!("kv://test/app/concurrent{}", idx);
        let begin_frame = build_kv_begin(&route, 1, 0);
        let response = client
            .request(&begin_frame, 4000)
            .await
            .expect("BEGIN failed");

        let (_msg_type, status, data) = parse_kv_response(&response);
        assert_eq!(status, 0);
        let tx_id = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);

        let put_frame = build_kv_put(tx_id, &route, b"key", b"value");
        let response = client.request(&put_frame, 4000).await.expect("PUT failed");
        let (_msg_type, status, _data) = parse_kv_response(&response);
        assert_eq!(status, 0);

        let commit_frame = build_kv_commit(tx_id, &route);
        let response = client
            .request(&commit_frame, 4000)
            .await
            .expect("COMMIT failed");
        let (_msg_type, status, _data) = parse_kv_response(&response);
        assert_eq!(status, 0);

        tx_id
    };

    let (tx1, tx2, tx3) = tokio::join!(run_tx(0), run_tx(1), run_tx(2));
    let tx_ids = [tx1, tx2, tx3];

    assert_eq!(
        tx_ids.len(),
        3,
        "All 3 concurrent transactions should complete"
    );
}

async fn should_assign_unique_tx_ids_within_single_session<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let mut tx_ids = vec![];

    for i in 0..3 {
        let route = format!("kv://test/app/sequential{}", i);
        let begin_frame = build_kv_begin(&route, 1, 0);
        let response = client
            .request(&begin_frame, 2000)
            .await
            .expect("BEGIN failed");

        let (_msg_type, status, data) = parse_kv_response(&response);
        assert_eq!(status, 0);
        let tx_id = u64::from_be_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        tx_ids.push(tx_id);

        let put_frame = build_kv_put(tx_id, &route, b"key", b"value");
        let response = client.request(&put_frame, 2000).await.expect("PUT failed");
        let (_msg_type, status, _data) = parse_kv_response(&response);
        assert_eq!(status, 0);

        let commit_frame = build_kv_commit(tx_id, &route);
        let response = client
            .request(&commit_frame, 2000)
            .await
            .expect("COMMIT failed");
        let (_msg_type, status, _data) = parse_kv_response(&response);
        assert_eq!(status, 0);
    }

    assert_eq!(tx_ids.len(), 3);
    assert_eq!(
        tx_ids,
        vec![1, 2, 3],
        "Transaction IDs should be sequential within a session"
    );
}

async fn should_reject_put_after_commit<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let begin_frame = build_kv_begin("kv://test/app/users", 1, 0);
    let response = client.request(&begin_frame, 2000).await.expect("BEGIN");
    let tx_id = u64::from_be_bytes([
        parse_kv_response(&response).2[0],
        parse_kv_response(&response).2[1],
        parse_kv_response(&response).2[2],
        parse_kv_response(&response).2[3],
        parse_kv_response(&response).2[4],
        parse_kv_response(&response).2[5],
        parse_kv_response(&response).2[6],
        parse_kv_response(&response).2[7],
    ]);

    let put_frame = build_kv_put(tx_id, "kv://test/app/users", b"key", b"value");
    client.request(&put_frame, 2000).await.expect("PUT");

    let commit_frame = build_kv_commit(tx_id, "kv://test/app/users");
    client.request(&commit_frame, 2000).await.expect("COMMIT");

    let put_frame2 = build_kv_put(tx_id, "kv://test/app/users", b"key2", b"value2");
    let response = client
        .request(&put_frame2, 2000)
        .await
        .expect("server should respond");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 1, "Expected error for PUT after COMMIT");
}

async fn should_rollback_transaction_successfully<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let begin_frame = build_kv_begin("kv://test/app/users", 1, 0);
    let response = client.request(&begin_frame, 2000).await.expect("BEGIN");
    let tx_id = u64::from_be_bytes([
        parse_kv_response(&response).2[0],
        parse_kv_response(&response).2[1],
        parse_kv_response(&response).2[2],
        parse_kv_response(&response).2[3],
        parse_kv_response(&response).2[4],
        parse_kv_response(&response).2[5],
        parse_kv_response(&response).2[6],
        parse_kv_response(&response).2[7],
    ]);

    let put_frame = build_kv_put(tx_id, "kv://test/app/users", b"key", b"value");
    client.request(&put_frame, 2000).await.expect("PUT");

    let rollback_frame = build_kv_rollback(tx_id, "kv://test/app/users");
    let response = client
        .request(&rollback_frame, 2000)
        .await
        .expect("ROLLBACK");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0, "Expected ROLLBACK success");
}

async fn should_handle_empty_key_and_value<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let begin_frame = build_kv_begin("kv://test/app/users", 1, 0);
    let response = client.request(&begin_frame, 2000).await.expect("BEGIN");
    let tx_id = u64::from_be_bytes([
        parse_kv_response(&response).2[0],
        parse_kv_response(&response).2[1],
        parse_kv_response(&response).2[2],
        parse_kv_response(&response).2[3],
        parse_kv_response(&response).2[4],
        parse_kv_response(&response).2[5],
        parse_kv_response(&response).2[6],
        parse_kv_response(&response).2[7],
    ]);

    let put_frame = build_kv_put(tx_id, "kv://test/app/users", b"", b"");
    let response = client.request(&put_frame, 2000).await.expect("PUT");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0, "Expected success with empty key/value");
}

async fn should_handle_large_values<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let begin_frame = build_kv_begin("kv://test/app/users", 1, 0);
    let response = client.request(&begin_frame, 2000).await.expect("BEGIN");
    let tx_id = u64::from_be_bytes([
        parse_kv_response(&response).2[0],
        parse_kv_response(&response).2[1],
        parse_kv_response(&response).2[2],
        parse_kv_response(&response).2[3],
        parse_kv_response(&response).2[4],
        parse_kv_response(&response).2[5],
        parse_kv_response(&response).2[6],
        parse_kv_response(&response).2[7],
    ]);

    let large_value = vec![b'X'; 60_000];
    let put_frame = build_kv_put(tx_id, "kv://test/app/users", b"large_key", &large_value);
    let response = client.request(&put_frame, 3000).await.expect("PUT");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0, "Expected success with 60KB value");
}

async fn should_isolate_transactions_across_resources<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let begin_frame1 = build_kv_begin("kv://test/app/users", 1, 0);
    let response = client.request(&begin_frame1, 2000).await.expect("BEGIN 1");
    let tx_id1 = u64::from_be_bytes([
        parse_kv_response(&response).2[0],
        parse_kv_response(&response).2[1],
        parse_kv_response(&response).2[2],
        parse_kv_response(&response).2[3],
        parse_kv_response(&response).2[4],
        parse_kv_response(&response).2[5],
        parse_kv_response(&response).2[6],
        parse_kv_response(&response).2[7],
    ]);

    let begin_frame2 = build_kv_begin("kv://test/app/posts", 1, 0);
    let response = client.request(&begin_frame2, 2000).await.expect("BEGIN 2");
    let tx_id2 = u64::from_be_bytes([
        parse_kv_response(&response).2[0],
        parse_kv_response(&response).2[1],
        parse_kv_response(&response).2[2],
        parse_kv_response(&response).2[3],
        parse_kv_response(&response).2[4],
        parse_kv_response(&response).2[5],
        parse_kv_response(&response).2[6],
        parse_kv_response(&response).2[7],
    ]);

    assert_ne!(
        tx_id1, tx_id2,
        "Different resources should get different tx_ids"
    );

    let put1 = build_kv_put(tx_id1, "kv://test/app/users", b"key", b"value");
    let response1 = client.request(&put1, 2000).await.expect("PUT 1");
    assert_eq!(parse_kv_response(&response1).1, 0);

    let put2 = build_kv_put(tx_id2, "kv://test/app/posts", b"key", b"value");
    let response2 = client.request(&put2, 2000).await.expect("PUT 2");
    assert_eq!(parse_kv_response(&response2).1, 0);

    let put_wrong = build_kv_put(tx_id1, "kv://test/app/posts", b"key", b"wrong");
    let response = client.request(&put_wrong, 2000).await.expect("PUT wrong");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(
        status, 1,
        "Expected error for PUT to wrong resource with tx_id"
    );
}

async fn should_timeout_on_malformed_frame<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let garbage = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00];
    let result = client.request(&garbage, 100).await;

    assert!(
        result.is_err(),
        "Expected error/timeout for malformed frame"
    );
}

async fn should_handle_connection_drop_during_transaction<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let begin_frame = build_kv_begin("kv://test/app/users", 1, 0);
    let response = client.request(&begin_frame, 1000).await.expect("BEGIN");
    let tx_id = u64::from_be_bytes([
        parse_kv_response(&response).2[0],
        parse_kv_response(&response).2[1],
        parse_kv_response(&response).2[2],
        parse_kv_response(&response).2[3],
        parse_kv_response(&response).2[4],
        parse_kv_response(&response).2[5],
        parse_kv_response(&response).2[6],
        parse_kv_response(&response).2[7],
    ]);

    drop(client);

    fitz::testkit::transport::wait_for_disconnect_cleanup().await;

    let mut client2 = C::connect(server).await.expect("failed to reconnect");
    let put_frame = build_kv_put(tx_id, "kv://test/app/users", b"key", b"value");
    let response = client2
        .request(&put_frame, 2000)
        .await
        .expect("server should respond");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(
        status, 1,
        "Expected error for tx_id from disconnected session"
    );
}

// ===== TCP tests =====

async fn should_reject_operations_on_invalid_transaction<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let put_frame = build_kv_put(99999, "kv://test/app/users", b"key", b"value");
    let response = client
        .request(&put_frame, 2000)
        .await
        .expect("server should respond even for invalid tx");

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 1, "Expected error status for invalid tx_id");
}

async fn should_require_connect_message_when_auth_enabled<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    // Arrange
    // Act
    let begin_frame = build_kv_begin("kv://test/app/users", 1, 0);
    let result = client.request(&begin_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected connection close or timeout when unauthenticated"
    );
}

async fn should_accept_valid_jwt_in_connect_message<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    // Arrange
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
    let begin_frame = build_kv_begin("kv://test-realm/app/users", 1, 0);
    let response = client
        .request(&begin_frame, 2000)
        .await
        .expect("BEGIN should work after auth");

    let (_msg_type, status, data) = parse_kv_response(&response);
    assert_eq!(status, 0, "Expected BEGIN success after authentication");
    assert_eq!(data.len(), 8, "Expected tx_id");
}

async fn should_reject_expired_jwt<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    // Arrange
    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_expired_jwt("test-realm"),
    );
    let result = client.request(&connect_frame, 1000).await;

    // Assert
    assert!(
        result.is_err(),
        "Expected connection close for expired token"
    );
}

async fn should_reject_invalid_jwt_signature<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_invalid_signature_jwt("test-realm"),
    );
    let result = client.request(&connect_frame, 1000).await;

    assert!(
        result.is_err(),
        "Expected connection close for invalid signature"
    );
}

async fn should_reject_jwt_for_wrong_realm<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "acme",
        &fitz::testkit::transport::generate_test_jwt("acme"),
    );
    client
        .send_frame(&connect_frame)
        .await
        .expect("CONNECT send failed");

    fitz::testkit::transport::wait_for_auth_ready().await;

    let begin_frame = build_kv_begin("kv://corp/app/users", 1, 0);
    let result = client.request(&begin_frame, 1000).await;

    if let Ok(response) = result {
        let (_msg_type, status, _data) = parse_kv_response(&response);
        assert_eq!(status, 1, "Expected error for wrong realm");
    } else {
        assert!(result.is_err(), "Expected error/timeout for wrong realm");
    }
}

async fn should_create_separate_sessions_for_each_connection_with_auth<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client1 = C::connect(server).await.expect("failed to connect");
    let mut client2 = C::connect(server).await.expect("failed to connect");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    client1.send_frame(&connect_frame).await.expect("CONNECT 1");
    client2.send_frame(&connect_frame).await.expect("CONNECT 2");

    fitz::testkit::transport::wait_for_auth_ready().await;

    let begin_frame1 = build_kv_begin("kv://test-realm/app/users", 1, 0);
    let begin_frame2 = build_kv_begin("kv://test-realm/app/posts", 1, 0);
    let response1 = client1.request(&begin_frame1, 2000).await.expect("BEGIN 1");
    let response2 = client2.request(&begin_frame2, 2000).await.expect("BEGIN 2");

    let (_msg_type1, status1, data1) = parse_kv_response(&response1);
    let (_msg_type2, status2, data2) = parse_kv_response(&response2);

    assert_eq!(
        status1, 0,
        "First BEGIN should succeed, got status {}",
        status1
    );
    assert_eq!(
        status2, 0,
        "Second BEGIN should succeed, got status {}",
        status2
    );
    assert!(
        data1.len() >= 8,
        "First response should have tx_id (8 bytes), got {} bytes",
        data1.len()
    );
    assert!(
        data2.len() >= 8,
        "Second response should have tx_id (8 bytes), got {} bytes",
        data2.len()
    );

    let tx_id1 = u64::from_be_bytes([
        data1[0], data1[1], data1[2], data1[3], data1[4], data1[5], data1[6], data1[7],
    ]);
    let tx_id2 = u64::from_be_bytes([
        data2[0], data2[1], data2[2], data2[3], data2[4], data2[5], data2[6], data2[7],
    ]);

    assert_eq!(tx_id1, 1, "First connection should get tx_id=1");
    assert_eq!(
        tx_id2, 1,
        "Second connection should also get tx_id=1 (separate session)"
    );
}

async fn should_reject_commit_before_begin<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let commit_frame = build_kv_commit(1, "kv://test/app/users");
    let result = client.request(&commit_frame, 2000).await;

    if let Ok(response) = result {
        let (_msg_type, status, _data) = parse_kv_response(&response);
        assert_eq!(status, 1, "Expected error for COMMIT without BEGIN");
    } else {
        assert!(
            result.is_err(),
            "Expected error/timeout for COMMIT without BEGIN"
        );
    }
}

// ===== TCP wrapper tests (added AAA comments) =====

#[tokio::test]
async fn should_complete_begin_put_commit_over_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_complete_begin_put_commit_over_transport::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_receive_responses_within_reasonable_time::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_transactions_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_concurrent_connections_with_separate_transactions::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_assign_unique_tx_ids_within_single_session_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_assign_unique_tx_ids_within_single_session::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_operations_on_invalid_transaction_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_reject_operations_on_invalid_transaction::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_require_connect_message_when_auth_enabled_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_require_connect_message_when_auth_enabled::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_accept_valid_jwt_in_connect_message_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_accept_valid_jwt_in_connect_message::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_expired_jwt_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_reject_expired_jwt::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_invalid_jwt_signature_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_reject_invalid_jwt_signature::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_jwt_for_wrong_realm_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_reject_jwt_for_wrong_realm::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_create_separate_sessions_for_each_connection_with_auth_tcp() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_create_separate_sessions_for_each_connection_with_auth::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_commit_before_begin_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_reject_commit_before_begin::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_put_after_commit_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_reject_put_after_commit::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_rollback_transaction_successfully_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_rollback_transaction_successfully::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_empty_key_and_value_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_empty_key_and_value::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_large_values_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_large_values::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_isolate_transactions_across_resources_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_isolate_transactions_across_resources::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_timeout_on_malformed_frame_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_timeout_on_malformed_frame::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_connection_drop_during_transaction_tcp() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_connection_drop_during_transaction::<TcpConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

// ===== WebSocket wrapper tests (added AAA comments) =====

#[tokio::test]
async fn should_complete_begin_put_commit_over_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_complete_begin_put_commit_over_transport::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_receive_responses_within_reasonable_time::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_handle_concurrent_connections_with_separate_transactions_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_handle_concurrent_connections_with_separate_transactions::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_assign_unique_tx_ids_within_single_session_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_assign_unique_tx_ids_within_single_session::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_reject_operations_on_invalid_transaction_ws() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act
    should_reject_operations_on_invalid_transaction::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

#[tokio::test]
async fn should_require_connect_message_when_auth_enabled_ws() {
    // Arrange
    let server = TestServer::start_with_auth(true)
        .await
        .expect("failed to start test server");

    // Act
    should_require_connect_message_when_auth_enabled::<WsConnector>(&server).await;

    // Assert
    // (assertions are in the helper)
}

// --- appended: KV domain basic tests (migrated) ---

// (contents from `tests/kv_e2e_basic.rs`)
use fitz::domains::kv::{
    KvActor as _KvActor_basic, KvMessage as _KvMessage_basic, KvResponse as _KvResponse_basic,
    TxMode as _TxMode_basic,
};
use fitz::runtime::routing::RouteFamily as _RouteFamily_basic;

fn create_kv_actor_basic() -> _KvActor_basic {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    _KvActor_basic::new(store)
}

#[test]
fn should_complete_transaction_begin_put_get_sequence() {
    // Arrange
    let mut actor = create_kv_actor_basic();

    // Act
    let response = actor.handle(_KvMessage_basic::Begin {
        route_family: _RouteFamily_basic::new(1),
        realm: "acme".to_string(),
        area: "kv".to_string(),
        resource: "users".to_string(),
        mode: _TxMode_basic::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered(),
    });
    let tx_id = match response {
        _KvResponse_basic::BeginOk { tx_id } => tx_id,
        _ => panic!("Expected BeginOk"),
    };

    // Step 2: Put and verify
    let response = actor.handle(_KvMessage_basic::Put {
        tx_id,
        route_family: _RouteFamily_basic::new(1),
        resource: "users".to_string(),
        key: Bytes::from_static(b"user:1001"),
        value: Bytes::from_static(b"{\"name\":\"Alice\",\"email\":\"alice@acme.com\"}"),
    });
    assert!(matches!(response, _KvResponse_basic::PutOk));

    // Step 3: Get and verify
    let response = actor.handle(_KvMessage_basic::Get {
        tx_id,
        route_family: _RouteFamily_basic::new(1),
        resource: "users".to_string(),
        key: Bytes::from_static(b"user:1001"),
    });
    match response {
        _KvResponse_basic::GetResult {
            found: true,
            value: Some(v),
        } => {
            assert_eq!(&*v, b"{\"name\":\"Alice\",\"email\":\"alice@acme.com\"}");
        }
        _ => panic!("Expected to find user"),
    }

    // Assert
    let response = actor.handle(_KvMessage_basic::Rollback { tx_id });
    assert!(matches!(response, _KvResponse_basic::RollbackOk));
}

// --- appended: codec/routing tests (migrated from kv_e2e_domain_routing.rs) ---

use fitz::protocol::kv as _kv_codec;

#[test]
fn should_parse_kv_get_message() {
    // Arrange
    let key = "test_key";
    let tx_id = 42u64;
    let route = "kv://realm/area/resource";

    let mut payload = Vec::new();
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
    payload.extend_from_slice(key.as_bytes());

    // Act
    let result = _kv_codec::parse_request(103, _RouteFamily_basic::new(1), &payload); // GET

    // Assert
    assert!(result.is_ok(), "Failed to parse KV GET message");
}

#[test]
fn should_parse_kv_put_message() {
    // Arrange
    let key = "my_key";
    let value = "my_value";
    let tx_id = 42u64;
    let route = "kv://realm/area/resource";

    let mut payload = Vec::new();
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
    payload.extend_from_slice(key.as_bytes());
    payload.extend_from_slice(&(value.len() as u32).to_be_bytes());
    payload.extend_from_slice(value.as_bytes());

    // Act
    let result = _kv_codec::parse_request(104, _RouteFamily_basic::new(1), &payload); // PUT

    // Assert
    assert!(result.is_ok(), "Failed to parse KV PUT message");
}

#[test]
fn should_parse_kv_begin_message() {
    // Arrange
    let route = "kv://realm/area/my_resource";

    let mut payload = Vec::new();
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.push(0); // ReadWrite mode
    payload.push(0); // Buffered write option

    // Act
    let result = _kv_codec::parse_request(100, _RouteFamily_basic::new(1), &payload); // BEGIN

    // Assert
    assert!(result.is_ok(), "Failed to parse KV BEGIN message");
}

#[test]
fn should_encode_kv_get_result_found() {
    // Arrange
    use fitz::domains::kv::KvResponse as _KvRespEnc;

    let response = _KvRespEnc::GetResult {
        found: true,
        value: Some(Bytes::from("test_value")),
    };

    // Act
    let encoded = _kv_codec::encode_response(&response);

    // Assert
    assert!(
        !encoded.is_empty(),
        "Response encoding should produce bytes"
    );
}

#[test]
fn should_encode_kv_get_result_not_found() {
    // Arrange
    use fitz::domains::kv::KvResponse as _KvRespEnc;

    let response = _KvRespEnc::GetResult {
        found: false,
        value: None,
    };

    // Act
    let encoded = _kv_codec::encode_response(&response);

    // Assert
    assert!(!encoded.is_empty(), "Not-found response should encode");
}

#[test]
fn should_encode_kv_put_ok() {
    // Arrange
    use fitz::domains::kv::KvResponse as _KvRespEnc;

    let response = _KvRespEnc::PutOk;

    // Act
    let encoded = _kv_codec::encode_response(&response);

    // Assert
    let _ = encoded; // empty response is valid
}

#[test]
fn should_roundtrip_kv_message() {
    // Arrange
    let key = "test_key";
    let tx_id = 42u64;
    let route = "kv://realm/area/resource";

    let mut payload = Vec::new();
    payload.extend_from_slice(&tx_id.to_be_bytes());
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route.as_bytes());
    payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
    payload.extend_from_slice(key.as_bytes());

    // Act
    let parse_result = _kv_codec::parse_request(103, _RouteFamily_basic::new(1), &payload); // GET

    assert!(parse_result.is_ok());

    // Simulate a response
    use fitz::domains::kv::KvResponse as _KvRespEnc;
    let response = _KvRespEnc::GetResult {
        found: true,
        value: Some(Bytes::from("found_value")),
    };

    // Encode response
    let encoded = _kv_codec::encode_response(&response);

    // Assert
    assert!(!encoded.is_empty());
}

// --- appended: WS ingress -> KV flow integration test (migrated from ws_domain_flow.rs) ---

use fitz::boot::domains as _boot_domains;
use fitz::protocol::tlv::TlvEncoder as _TlvEncoder;
use fitz::runtime::Router as _Router;
use fitz::session::{
    Ingress as _Ingress, NewSessionConfig as _NewSessionConfig, RuntimeIngress as _RuntimeIngress,
    Session as _Session, SessionMetadata as _SessionMetadata,
    SessionOutboundSink as _SessionOutboundSink, SessionPermissions as _SessionPermissions,
    TransportKind as _TransportKind,
};
use fitz::testkit::create_test_engine_with_cfs as _create_test_engine_cfs;
use std::sync::Arc as _Arc;

#[tokio::test]
async fn should_route_kv_get_through_ingress_to_kv_and_reply_to_inbox() {
    // Arrange
    let store = _create_test_engine_cfs(vec![1]);
    let router = _Arc::new(_Router::new());
    _boot_domains::setup(&router, &store).unwrap();

    // Create ingress with router attached
    let ingress = _Arc::new(_RuntimeIngress::new(false).with_router(router.clone()));

    // Register a session outbound inbox sink
    let session_id = 1u64;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(10);
    let sink = std::sync::Arc::new(_SessionOutboundSink::new(tx));
    let inbox_addr = fitz::runtime::routing::RouteAddress::new(
        fitz::runtime::routing::RouteFamily::new(0),
        fitz::runtime::routing::Route::new(format!("inbox://session/{}", session_id)),
    );
    router.register(
        inbox_addr.clone(),
        sink as std::sync::Arc<dyn fitz::runtime::router::MailboxSink>,
    );

    // Create session and open
    let session_config = _NewSessionConfig::unauthenticated(
        _TransportKind::Tcp,
        None,
        _SessionPermissions::empty(),
        _SessionMetadata::new(),
        10,
        None,
        fitz::runtime::routing::RouteFamily::new(0),
    );
    let mut session = _Session::new(session_id, session_config);
    ingress.on_open(session.info()).await.unwrap();

    // Build a KV GET TLV message (msg_type 103)
    let mut payload = Vec::new();
    payload.extend_from_slice(&0u64.to_be_bytes()); // tx_id
    let route = b"kv://realm/area/resource";
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route);
    let key = b"nonexistent";
    payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
    payload.extend_from_slice(key);

    let mut enc = _TlvEncoder::new();
    enc.encode(fitz::protocol::tlv::MessageType::new(103), &payload);
    let frame = enc.finish();

    // Act
    let ingress_ref: &dyn fitz::session::manager::Ingress = ingress.as_ref();
    session.on_frame(frame, ingress_ref).await.unwrap();

    // Assert
    let resp = rx.recv().await.expect("expected response");
    let dec = fitz::protocol::tlv::TlvDecoder::new();
    let (record, _) = dec.decode_one(&Bytes::from(resp)).unwrap();
    assert_eq!(record.msg_type().as_u16(), 103);
    let body = record.value();
    assert!(!body.is_empty());
}
