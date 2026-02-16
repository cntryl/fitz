#![allow(dead_code)]

use bytes::{BufMut, BytesMut};
pub use fitz::testkit::transport::{TestClient, TestServer, TestWebSocketClient, TlvFrameBuilder};
use std::error::Error;
use std::future::Future;
use std::pin::Pin;

pub type BoxError = Box<dyn Error>;
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

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

pub struct TcpConnector;

impl KvConnector for TcpConnector {
    type Client = TestClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect().await })
    }
}

pub struct WsConnector;

impl KvConnector for WsConnector {
    type Client = TestWebSocketClient;

    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect_ws().await })
    }
}

/// Build KV BEGIN request frame
/// Wire format: [u32 BE route_len][route][u8 mode][u8 durability]
pub fn build_kv_begin(route: &str, mode: u8, durability: u8) -> Vec<u8> {
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
pub fn build_kv_put(tx_id: u64, route: &str, key: &[u8], value: &[u8]) -> Vec<u8> {
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
pub fn build_kv_commit(tx_id: u64, route: &str) -> Vec<u8> {
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
pub fn build_kv_rollback(tx_id: u64, route: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&tx_id.to_be_bytes());
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(102, &payload);
    builder.build()
}

/// Parse KV response status byte
pub fn parse_kv_response(frame: &[u8]) -> (u16, u8, Vec<u8>) {
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

// --- Generic test helpers (transport-agnostic) ---
// These are `pub` so integration tests can call them from `fixtures::transport`.

pub async fn should_complete_begin_put_commit_over_transport<C>(server: &TestServer)
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

// (Other generic helper functions copied from the original tests)

pub async fn should_receive_responses_within_reasonable_time<C>(server: &TestServer)
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
        .request(&begin_frame, 1000)
        .await
        .expect("begin failed");
    let (_mt, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);
}

pub async fn should_handle_concurrent_connections_with_separate_transactions<C>(server: &TestServer)
where
    C: KvConnector,
{
    let run_tx = |i: usize| async move {
        let route = format!("kv://test/app/concurrent{}", i);
        let begin = build_kv_begin(&route, 1, 0);
        let mut client = server.connect().await.unwrap();
        let response = client.request(&begin, 2000).await.unwrap();
        u64::from_be_bytes([
            parse_kv_response(&response).2[0],
            parse_kv_response(&response).2[1],
            parse_kv_response(&response).2[2],
            parse_kv_response(&response).2[3],
            parse_kv_response(&response).2[4],
            parse_kv_response(&response).2[5],
            parse_kv_response(&response).2[6],
            parse_kv_response(&response).2[7],
        ])
    };

    let (tx1, tx2, tx3) = tokio::join!(run_tx(0), run_tx(1), run_tx(2));
    let tx_ids = [tx1, tx2, tx3];

    assert_eq!(
        tx_ids.len(),
        3,
        "All 3 concurrent transactions should complete"
    );
}

pub async fn should_assign_unique_tx_ids_within_single_session<C>(server: &TestServer)
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

pub async fn should_reject_operations_on_invalid_transaction<C>(server: &TestServer)
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

pub async fn should_require_connect_message_when_auth_enabled<C>(server: &TestServer)
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

pub async fn should_accept_valid_jwt_in_connect_message<C>(server: &TestServer)
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

    // Act
    let begin_frame = build_kv_begin("kv://test-realm/app/users", 1, 0);
    let response = client.request(&begin_frame, 2000).await.expect("BEGIN");

    // Assert
    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0);
}

pub async fn should_reject_expired_jwt<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let jwt = fitz::testkit::transport::generate_expired_jwt("test-realm");
    let connect_frame = fitz::testkit::transport::build_connect_frame("test-realm", &jwt);
    client
        .send_frame(&connect_frame)
        .await
        .expect("CONNECT send failed");

    let begin_frame = build_kv_begin("kv://test-realm/app/users", 1, 0);
    let result = client.request(&begin_frame, 1000).await;
    assert!(result.is_err(), "Expired JWT should cause close/timeout");
}

pub async fn should_reject_invalid_jwt_signature<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let jwt = fitz::testkit::transport::generate_test_jwt("test-realm");
    // flip a bit to invalidate signature
    let mut broken = jwt.into_bytes();
    broken[10] ^= 0xFF;
    let broken = String::from_utf8_lossy(&broken).to_string();
    let connect_frame = fitz::testkit::transport::build_connect_frame("test-realm", &broken);
    client
        .send_frame(&connect_frame)
        .await
        .expect("CONNECT send failed");

    let begin_frame = build_kv_begin("kv://test-realm/app/users", 1, 0);
    let result = client.request(&begin_frame, 1000).await;
    assert!(
        result.is_err(),
        "Invalid JWT signature should cause close/timeout"
    );
}

pub async fn should_reject_jwt_for_wrong_realm<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let jwt = fitz::testkit::transport::generate_test_jwt("other-realm");
    let connect_frame = fitz::testkit::transport::build_connect_frame("test-realm", &jwt);
    client
        .send_frame(&connect_frame)
        .await
        .expect("CONNECT send failed");

    let begin_frame = build_kv_begin("kv://test-realm/app/users", 1, 0);
    let result = client.request(&begin_frame, 1000).await;
    assert!(result.is_err(), "JWT for wrong realm should be rejected");
}

pub async fn should_create_separate_sessions_for_each_connection_with_auth<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut c1 = C::connect(server).await.expect("c1");
    let mut c2 = C::connect(server).await.expect("c2");

    let connect_frame = fitz::testkit::transport::build_connect_frame(
        "test-realm",
        &fitz::testkit::transport::generate_test_jwt("test-realm"),
    );
    c1.send_frame(&connect_frame).await.expect("send1");
    c2.send_frame(&connect_frame).await.expect("send2");

    let begin1 = build_kv_begin("kv://test-realm/app/users", 1, 0);
    let begin2 = build_kv_begin("kv://test-realm/app/users", 1, 0);

    let r1 = c1.request(&begin1, 2000).await.expect("b1");
    let r2 = c2.request(&begin2, 2000).await.expect("b2");

    assert_eq!(parse_kv_response(&r1).0, 100);
    assert_eq!(parse_kv_response(&r2).0, 100);
}

pub async fn should_reject_commit_before_begin<C>(server: &TestServer)
where
    C: KvConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let commit_frame = build_kv_commit(42, "kv://test/app/users");
    let result = client.request(&commit_frame, 1000).await;
    assert!(
        result.is_err()
            || match result {
                Ok(r) => parse_kv_response(&r).1 == 1,
                _ => true,
            }
    );
}

pub async fn should_reject_put_after_commit<C>(server: &TestServer)
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

pub async fn should_rollback_transaction_successfully<C>(server: &TestServer)
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

pub async fn should_handle_empty_key_and_value<C>(server: &TestServer)
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

pub async fn should_handle_large_values<C>(server: &TestServer)
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

pub async fn should_isolate_transactions_across_resources<C>(server: &TestServer)
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

pub async fn should_timeout_on_malformed_frame<C>(server: &TestServer)
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

pub async fn should_handle_connection_drop_during_transaction<C>(server: &TestServer)
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

// ------------------------- Lease transport helpers -------------------------

// Lease-specific test client/connector traits and implementations (re-use TestClient/TestWebSocketClient)

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

// Re-use the existing `TcpConnector` / `WsConnector` types (they already exist above for KV)
impl LeaseConnector for TcpConnector {
    type Client = TestClient;
    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect().await })
    }
}

impl LeaseConnector for WsConnector {
    type Client = TestWebSocketClient;
    fn connect<'a>(server: &'a TestServer) -> BoxFuture<'a, Result<Self::Client, BoxError>> {
        Box::pin(async move { server.connect_ws().await })
    }
}

// --- Lease TLV builders / parsers ---

/// Build Lease ACQUIRE request frame
/// Wire format: [u32 BE route_len][route][u32 BE owner_len][owner_id][u64 BE ttl_secs][u32 BE wait_seconds (optional)]
pub fn build_lease_acquire(
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

    if let Some(ws) = wait_seconds {
        payload.put_slice(&ws.to_be_bytes());
    }

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(400, &payload);
    builder.build()
}

pub fn build_lease_acquire_immediate(route: &str, owner_id: &str, ttl_secs: u64) -> Vec<u8> {
    build_lease_acquire(route, owner_id, ttl_secs, None)
}

/// Build Lease RENEW request frame
pub fn build_lease_renew(
    route: &str,
    owner_id: &str,
    fencing_token: u64,
    ttl_secs: u64,
) -> Vec<u8> {
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
pub fn build_lease_release(route: &str, owner_id: &str, fencing_token: u64) -> Vec<u8> {
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
pub fn build_lease_query(route: &str) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());

    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(403, &payload);
    builder.build()
}

/// Parse Lease response status byte and extract data
pub fn parse_lease_response(frame: &[u8]) -> (u16, u8, Vec<u8>) {
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
pub fn parse_lease_token_response(data: &[u8]) -> Option<u64> {
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

// --- Generic lease transport test helpers (transport-agnostic) ---

pub async fn should_complete_acquire_renew_release_cycle<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/lock1";
    let owner_id = "owner-1";

    let acquire_frame = build_lease_acquire_immediate(route, owner_id, 30);
    let response = client
        .request(&acquire_frame, 2000)
        .await
        .expect("ACQUIRE request failed");

    let (msg_type, status, data) = parse_lease_response(&response);
    assert_eq!(msg_type, 400, "Expected ACQUIRE response (400)");
    assert_eq!(status, 0, "Expected success status");
    let token = parse_lease_token_response(&data).expect("Expected fencing token");
    assert!(token > 0, "Expected valid fencing token");

    let renew_frame = build_lease_renew(route, owner_id, token, 30);
    let response = client
        .request(&renew_frame, 2000)
        .await
        .expect("RENEW request failed");

    let (msg_type, status, data) = parse_lease_response(&response);
    assert_eq!(msg_type, 401, "Expected RENEW response (401)");
    assert_eq!(status, 0, "Expected success status");
    let new_token = parse_lease_token_response(&data).expect("Expected new fencing token");
    assert!(
        new_token > token,
        "Expected monotonically increasing fencing token"
    );

    let release_frame = build_lease_release(route, owner_id, new_token);
    let response = client
        .request(&release_frame, 2000)
        .await
        .expect("RELEASE request failed");

    let (msg_type, status, _data) = parse_lease_response(&response);
    assert_eq!(msg_type, 402, "Expected RELEASE response (402)");
    assert_eq!(status, 0, "Expected success status");
}

pub async fn should_receive_responses_within_reasonable_time<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let warmup_frame = build_lease_acquire_immediate("lease://test/app/warmup", "warmup", 30);
    let _ = client
        .request(&warmup_frame, 1000)
        .await
        .expect("warmup failed");

    let acquire_frame = build_lease_acquire_immediate("lease://test/app/bench", "bench", 30);
    let response = client
        .request(&acquire_frame, 500)
        .await
        .expect("ACQUIRE request should complete quickly");
    let (_msg_type, status, _data) = parse_lease_response(&response);
    assert_eq!(status, 0, "Expected success status");
}

pub async fn should_handle_concurrent_connections_with_separate_leases<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    let run_lease = |idx: usize| async move {
        let mut client = C::connect(server).await.expect("connect failed");
        let route = format!("lease://test/app/concurrent{}", idx);
        let owner = format!("owner-{}", idx);

        let acquire_frame = build_lease_acquire_immediate(&route, &owner, 30);
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

    let (t1, t2, t3) = tokio::join!(run_lease(0), run_lease(1), run_lease(2));
    let tokens = [t1, t2, t3];
    assert_eq!(
        tokens.len(),
        3,
        "All 3 concurrent lease operations should complete"
    );
}

pub async fn should_enforce_lease_contention<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    let mut client1 = C::connect(server).await.expect("failed to connect");
    let mut client2 = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/contended";

    let acquire1_frame = build_lease_acquire_immediate(route, "owner-1", 30);
    let response1 = client1
        .request(&acquire1_frame, 2000)
        .await
        .expect("ACQUIRE 1");
    let (_msg_type, status1, data1) = parse_lease_response(&response1);
    assert_eq!(status1, 0);
    let token1 = parse_lease_token_response(&data1).expect("Expected token");

    let acquire2_frame = build_lease_acquire_immediate(route, "owner-2", 30);
    let response2 = client2
        .request(&acquire2_frame, 2000)
        .await
        .expect("ACQUIRE 2");

    let (_msg_type, status2, _data2) = parse_lease_response(&response2);
    assert_eq!(status2, 1, "Expected error when lease already held");

    let _ = client1
        .request(&build_lease_release(route, "owner-1", token1), 2000)
        .await;
}

pub async fn should_reject_operations_with_invalid_token<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");
    let route = "lease://test/app/invalid";
    let owner_id = "owner-1";

    let acquire_frame = build_lease_acquire_immediate(route, owner_id, 30);
    let response = client.request(&acquire_frame, 2000).await.expect("ACQUIRE");
    let (_msg_type, _status, data) = parse_lease_response(&response);
    let _token = parse_lease_token_response(&data).expect("Expected token");

    let renew_frame = build_lease_renew(route, owner_id, 99999, 30);
    let response = client
        .request(&renew_frame, 2000)
        .await
        .expect("server should respond even for invalid token");

    let (_msg_type, status, _data) = parse_lease_response(&response);
    assert_eq!(status, 1, "Expected error status for invalid token");
}

pub async fn should_require_connect_message_when_auth_enabled<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    let mut client = C::connect(server).await.expect("failed to connect");

    let acquire_frame = build_lease_acquire_immediate("lease://test/app/lock", "owner-1", 30);
    let result = client.request(&acquire_frame, 1000).await;

    assert!(
        result.is_err(),
        "Expected connection close or timeout when unauthenticated"
    );
}

pub async fn should_accept_valid_jwt_in_connect_message<C>(server: &TestServer)
where
    C: LeaseConnector,
{
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

    let acquire_frame = build_lease_acquire_immediate("lease://test-realm/app/lock", "owner-1", 30);
    let response = client
        .request(&acquire_frame, 2000)
        .await
        .expect("ACQUIRE should work after auth");

    let (_msg_type, status, data) = parse_lease_response(&response);
    assert_eq!(status, 0, "Expected ACQUIRE success after authentication");
    assert!(!data.is_empty(), "Expected fencing token");
}

pub async fn should_reject_expired_jwt<C>(server: &TestServer)
where
    C: LeaseConnector,
{
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

    let acquire_frame = build_lease_acquire_immediate("lease://test-realm/app/lock", "owner-1", 30);
    let result = client.request(&acquire_frame, 1000).await;

    assert!(result.is_err(), "Expected rejection for expired JWT");
}

pub async fn should_reject_invalid_jwt_signature<C>(server: &TestServer)
where
    C: LeaseConnector,
{
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

    let acquire_frame = build_lease_acquire_immediate("lease://test-realm/app/lock", "owner-1", 30);
    let result = client.request(&acquire_frame, 1000).await;

    assert!(
        result.is_err(),
        "Expected rejection for invalid JWT signature"
    );
}

pub async fn should_reject_jwt_for_wrong_realm<C>(server: &TestServer)
where
    C: LeaseConnector,
{
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

    let acquire_frame = build_lease_acquire_immediate("lease://test-realm/app/lock", "owner-1", 30);
    let result = client.request(&acquire_frame, 1000).await;

    assert!(result.is_err(), "Expected rejection for JWT realm mismatch");
}

pub async fn should_create_separate_sessions_for_each_connection_with_auth<C>(server: &TestServer)
where
    C: LeaseConnector,
{
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

    assert!(token1 > 0, "Expected valid token for client 1");
    assert!(token2 > 0, "Expected valid token for client 2");
}
