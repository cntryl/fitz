//! End-to-end tests for Fitz broker running on ports 4090 (HTTP/WS) and 4091 (TCP)
//!
//! These tests verify the broker is operational and can handle basic protocol interactions.
//! The broker must be running before these tests execute.

use bytes::Bytes;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Helper to construct a length-prefixed frame
fn encode_tcp_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Helper to read a length-prefixed frame
async fn read_tcp_frame(stream: &mut TcpStream) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

#[tokio::test]
#[ignore = "Requires broker running on 4091"]
async fn should_connect_via_tcp() {
    // Arrange - Connect to TCP endpoint
    let mut stream = TcpStream::connect("127.0.0.1:4091")
        .await
        .expect("Failed to connect to TCP endpoint on 4091");

    // Act - Send a test frame
    let test_payload = b"hello fitz";
    stream
        .write_all(&encode_tcp_frame(test_payload))
        .await
        .expect("Failed to send frame");

    // Note: We're not expecting a response yet (broker doesn't route frames)
    // This test just verifies TCP connectivity and framing works

    // Assert - Connection remains open
    assert!(stream.peek(&mut [0u8; 1]).await.is_ok());
}

#[tokio::test]
#[ignore = "Requires broker running on 4090"]
async fn should_upgrade_to_websocket() {
    // Arrange - Connect to HTTP endpoint
    let stream = TcpStream::connect("127.0.0.1:4090")
        .await
        .expect("Failed to connect to HTTP endpoint on 4090");

    // Act - Send WebSocket upgrade request
    let upgrade_request = b"GET / HTTP/1.1\r\n\
                           Host: localhost:4090\r\n\
                           Upgrade: websocket\r\n\
                           Connection: Upgrade\r\n\
                           Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                           Sec-WebSocket-Version: 13\r\n\r\n";

    let mut stream = stream;
    stream
        .write_all(upgrade_request)
        .await
        .expect("Failed to send upgrade request");

    // Assert - We should get an HTTP response (101 Switching Protocols or similar)
    let mut buf = vec![0u8; 1024];
    let n = timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("Read timed out")
        .expect("Failed to read response");

    let response = String::from_utf8_lossy(&buf[..n]);
    // We just check that we got some HTTP response, not empty
    assert!(!response.is_empty());
}

#[tokio::test]
#[ignore = "Requires broker running on 4091"]
async fn should_handle_multiple_tcp_connections() {
    // Arrange - Create two concurrent TCP connections
    let stream1 = TcpStream::connect("127.0.0.1:4091")
        .await
        .expect("Failed to connect stream 1");
    let stream2 = TcpStream::connect("127.0.0.1:4091")
        .await
        .expect("Failed to connect stream 2");

    // Assert - Both connections are open
    assert!(stream1.peer_addr().is_ok());
    assert!(stream2.peer_addr().is_ok());
}

// ============================================================================
// Integration with broker message routing (future tests)
// ============================================================================

// Once the broker fully wires up domain actors, add tests like:
//
// #[tokio::test]
// async fn should_enqueue_message_via_tcp() {
//     // Test: Send a queue/enqueue request via TCP
//     // Expected: Message stored in queue domain
// }
//
// #[tokio::test]
// async fn should_subscribe_via_websocket() {
//     // Test: Subscribe to notice route via WebSocket
//     // Expected: Receive published messages
// }
//
// #[tokio::test]
// async fn should_acquire_lease_via_tcp() {
//     // Test: Send a lease/acquire request via TCP
//     // Expected: Receive fencing token
// }
