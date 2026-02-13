//! KV domain transport-layer end-to-end tests
//!
//! These tests verify the COMPLETE request-response cycle:
//! TCP Client → Session → Routing → KV Actor → Response Encoding → TCP Client
//!
//! Unlike unit tests that call actor.handle() directly, these tests send
//! actual TLV-encoded bytes over TCP and verify responses are received.

use bytes::{BufMut, BytesMut};
use fitz::testkit::transport::{TestServer, TlvFrameBuilder};

/// Build KV BEGIN request frame
/// Wire format: [u32 BE route_len][route][u8 mode][u8 durability]
fn build_kv_begin(route: &str, mode: u8, durability: u8) -> Vec<u8> {
    let mut payload = BytesMut::new();
    payload.put_slice(&(route.len() as u32).to_be_bytes());
    payload.put_slice(route.as_bytes());
    payload.put_u8(mode);       // 0=ReadOnly, 1=ReadWrite
    payload.put_u8(durability); // 0=buffered, 1=sync

    // Wrap in TLV frame: [msg_type: 100][length][payload]
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

    // Wrap in TLV frame: [msg_type: 104][length][payload]
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

    // Wrap in TLV frame: [msg_type: 101][length][payload]
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(101, &payload);
    builder.build()
}

/// Parse KV response status byte
fn parse_kv_response(frame: &[u8]) -> (u16, u8, Vec<u8>) {
    // TLV format: [msg_type: u8 or ESCAPE+u16][length: u16][value]
    let mut offset = 0;
    
    // Parse msg_type
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
    
    // Parse length (u16 BE)
    let _length = u16::from_be_bytes([frame[offset], frame[offset + 1]]) as usize;
    offset += 2;
    
    let payload = &frame[offset..];

    // Response format: [status: u8][...optional data]
    let status = payload[0];
    let data = payload[1..].to_vec();

    (msg_type, status, data)
}

#[tokio::test]
async fn should_complete_begin_put_commit_over_tcp() {
    // Arrange - Start test server
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    let mut client = server.connect().await.expect("failed to connect");

    // Act - Send KV BEGIN request
    let begin_frame = build_kv_begin("kv://test/app/users", 1, 0);
    let response = client
        .request(&begin_frame, 1000)
        .await
        .expect("BEGIN request failed");

    // Assert - Verify BEGIN_OK response
    let (msg_type, status, data) = parse_kv_response(&response);
    assert_eq!(msg_type, 100, "Expected BEGIN response (100)");
    assert_eq!(status, 0, "Expected success status");
    assert_eq!(data.len(), 8, "Expected tx_id (u64)");

    // Extract tx_id from response
    let tx_id = u64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]);
    assert!(tx_id > 0, "Expected valid transaction ID");

    // Act - Send KV PUT request
    let put_frame = build_kv_put(
        tx_id,
        "kv://test/app/users",
        b"user:1001",
        b"{\"name\":\"Alice\"}",
    );
    let response = client
        .request(&put_frame, 1000)
        .await
        .expect("PUT request failed");

    // Assert - Verify PUT_OK response
    let (msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(msg_type, 104, "Expected PUT response (104)");
    assert_eq!(status, 0, "Expected success status");

    // Act - Send KV COMMIT request
    let commit_frame = build_kv_commit(tx_id, "kv://test/app/users");
    let response = client
        .request(&commit_frame, 1000)
        .await
        .expect("COMMIT request failed");

    // Assert - Verify COMMIT_OK response
    let (msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(msg_type, 101, "Expected COMMIT response (101)");
    assert_eq!(status, 0, "Expected success status");
}

#[tokio::test]
async fn should_receive_responses_within_reasonable_time() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    let mut client = server.connect().await.expect("failed to connect");

    // Act - Measure BEGIN latency
    let begin_frame = build_kv_begin("kv://test/app/bench", 1, 0);
    let start = std::time::Instant::now();
    let response = client
        .request(&begin_frame, 100) // 100ms timeout
        .await
        .expect("BEGIN request should complete quickly");
    let latency = start.elapsed();

    // Assert - Response should arrive in <10ms for in-memory operations
    assert!(
        latency.as_millis() < 10,
        "Expected sub-10ms latency, got {:?}",
        latency
    );

    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 0, "Expected success status");
}

#[tokio::test]
async fn should_handle_multiple_concurrent_transactions() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");

    // Act - Create 3 concurrent clients with 3 transactions
    let mut handles = vec![];
    for i in 0..3 {
        let addr = server.addr;
        let handle = tokio::spawn(async move {
            let mut client = fitz::testkit::transport::TestClient::new(addr)
                .await
                .expect("connect failed");

            let route = format!("kv://test/app/concurrent{}", i);
            let begin_frame = build_kv_begin(&route, 1, 0);
            let response = client
                .request(&begin_frame, 1000)
                .await
                .expect("BEGIN failed");

            let (_msg_type, status, data) = parse_kv_response(&response);
            assert_eq!(status, 0);
            let tx_id = u64::from_be_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);

            // PUT
            let put_frame = build_kv_put(tx_id, &route, b"key", b"value");
            let response = client.request(&put_frame, 1000).await.expect("PUT failed");
            let (_msg_type, status, _data) = parse_kv_response(&response);
            assert_eq!(status, 0);

            // COMMIT
            let commit_frame = build_kv_commit(tx_id, &route);
            let response = client
                .request(&commit_frame, 1000)
                .await
                .expect("COMMIT failed");
            let (_msg_type, status, _data) = parse_kv_response(&response);
            assert_eq!(status, 0);

            tx_id
        });
        handles.push(handle);
    }

    // Assert - All transactions complete successfully
    let mut tx_ids = vec![];
    for handle in handles {
        let tx_id = handle.await.expect("task failed");
        tx_ids.push(tx_id);
    }

    // Verify all transactions got unique IDs
    assert_eq!(tx_ids.len(), 3);
    tx_ids.sort();
    tx_ids.dedup();
    assert_eq!(tx_ids.len(), 3, "Transaction IDs should be unique");
}

#[tokio::test]
async fn should_reject_operations_on_invalid_transaction() {
    // Arrange
    let server = TestServer::start()
        .await
        .expect("failed to start test server");
    let mut client = server.connect().await.expect("failed to connect");

    // Act - Try to PUT with non-existent tx_id
    let put_frame = build_kv_put(99999, "kv://test/app/users", b"key", b"value");
    let response = client
        .request(&put_frame, 1000)
        .await
        .expect("server should respond even for invalid tx");

    // Assert - Should get error response
    let (_msg_type, status, _data) = parse_kv_response(&response);
    assert_eq!(status, 1, "Expected error status for invalid tx_id");
}
