//! End-to-end tests for Fitz broker with in-memory instances
//!
//! These tests spin up a Fitz broker instance on dynamic ports
//! and verify basic protocol interactions (TCP and WebSocket).

use fitz::boot::{BootConfig, BootResult};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::time::timeout;

/// Helper to construct a length-prefixed frame
fn encode_tcp_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Helper to read a length-prefixed frame
#[allow(dead_code)]
async fn read_tcp_frame(stream: &mut TcpStream) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

/// Spawn a Fitz broker instance in the background
///
/// Returns (http_port, tcp_port, shutdown_handle)
async fn spawn_broker() -> (u16, u16, tokio::task::JoinHandle<()>) {
    use rand::Rng;

    // Pick random ports to avoid conflicts between parallel tests
    let mut rng = rand::thread_rng();
    let http_port = rng.gen_range(20000..30000);
    let tcp_port = rng.gen_range(30000..40000);

    let config = BootConfig {
        bind_addr: "127.0.0.1".to_string(),
        http_port,
        tcp_port,
        storage_mode: fitz::boot::runtime::StorageMode::Memory,
        auth_required: false,
        max_connections: 100,
        max_frame_size: 1024 * 1024,
        channel_capacity: 100,
    };

    // Barrier to wait for broker to be ready
    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = barrier.clone();

    let handle = tokio::spawn(async move {
        // Initialize broker (without blocking on shutdown signal)
        if let Err(e) = boot_without_signal(config, barrier_clone).await {
            eprintln!("Broker error: {:?}", e);
        }
    });

    // Wait for broker to be ready
    barrier.wait().await;

    // Give broker a moment to fully start listeners
    tokio::time::sleep(Duration::from_millis(100)).await;

    (http_port, tcp_port, handle)
}

/// Boot broker without waiting for Ctrl+C (for testing)
async fn boot_without_signal(config: BootConfig, ready_barrier: Arc<Barrier>) -> BootResult<()> {
    use fitz::boot::{domains, handlers, runtime, storage};

    // Initialize storage
    let store = storage::init(&config).await?;

    // Create runtime infrastructure
    let (router, ingress, ingress_config, _scheduler, runtime_stats) = runtime::init(&store)?;
    runtime_stats.mark_storage_ready();

    // Register domain actors
    domains::setup(&router, &store)?;
    runtime_stats.mark_domains_ready();

    // Start transport listeners
    handlers::spawn_tcp_listener(&config, ingress.clone(), ingress_config.clone()).await?;
    handlers::spawn_http_listener(
        &config,
        ingress.clone(),
        ingress_config.clone(),
        runtime_stats.clone(),
    )
    .await?;

    runtime_stats.mark_startup_complete();

    // Signal that broker is ready
    ready_barrier.wait().await;

    // Keep broker alive (would normally wait for Ctrl+C here)
    tokio::time::sleep(Duration::from_secs(3600)).await;

    Ok(())
}

#[tokio::test]
async fn should_connect_via_tcp() {
    // Arrange - Spawn broker
    let (_, tcp_port, _handle) = spawn_broker().await;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .expect("Failed to connect to TCP endpoint");

    // Act - Send a test frame
    let test_payload = b"hello fitz";
    stream
        .write_all(&encode_tcp_frame(test_payload))
        .await
        .expect("Failed to send frame");

    // Note: We're not expecting a response yet (broker doesn't route frames)
    // This test just verifies TCP connectivity and framing works

    // Assert - Connection is established and frame was sent successfully
    assert!(stream.peer_addr().is_ok());
}

#[tokio::test]
async fn should_upgrade_to_websocket() {
    // Arrange - Spawn broker
    let (http_port, _, _handle) = spawn_broker().await;

    let stream = TcpStream::connect(format!("127.0.0.1:{}", http_port))
        .await
        .expect("Failed to connect to HTTP endpoint");

    // Act - Send WebSocket upgrade request
    let upgrade_request = format!(
        "GET / HTTP/1.1\r\n\
         Host: localhost:{}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n",
        http_port
    );

    let mut stream = stream;
    stream
        .write_all(upgrade_request.as_bytes())
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
async fn should_handle_multiple_tcp_connections() {
    // Arrange - Spawn broker
    let (_, tcp_port, _handle) = spawn_broker().await;

    // Create two concurrent TCP connections
    let stream1 = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
        .await
        .expect("Failed to connect stream 1");
    let stream2 = TcpStream::connect(format!("127.0.0.1:{}", tcp_port))
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
