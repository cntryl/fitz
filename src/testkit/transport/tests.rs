use super::*;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

async fn websocket_upgrade_status_for_addr(
    addr: SocketAddr,
) -> Result<u16, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    let request = format!(
        "GET / HTTP/1.1\r\n\
             Host: {addr}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut response = [0_u8; 1024];
    let bytes_read = stream.read(&mut response).await?;
    let status_line = std::str::from_utf8(&response[..bytes_read])?
        .lines()
        .next()
        .ok_or_else(|| std::io::Error::other("missing HTTP status line"))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| std::io::Error::other("missing HTTP status code"))?
        .parse::<u16>()?;
    Ok(status)
}

#[test]
fn should_encode_tlv_frame() {
    // Arrange
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(100, b"test_value");

    // Act
    let frame = builder.build();

    // Assert
    assert!(frame.len() >= 3 + b"test_value".len());
    assert_eq!(frame[0], 100); // msg_type
    assert_eq!(frame[1], 0);
    assert_eq!(
        frame[2],
        u8::try_from(b"test_value".len()).expect("test value length fits in u8")
    );
    assert_eq!(&frame[3..], b"test_value");
}

#[test]
fn should_decode_tlv_frame() {
    // Arrange
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(100, b"test_value");
    builder.encode_field(200, b"another_value");
    let frame = builder.build();

    // Act
    let mut parser = TlvFrameParser::new(&frame);
    let fields = parser.parse_all();

    // Assert
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].0, 100);
    assert_eq!(fields[0].1, b"test_value");
    assert_eq!(fields[1].0, 200);
    assert_eq!(fields[1].1, b"another_value");
}

#[test]
fn should_build_connect_frame() {
    // Arrange
    let realm = "test-realm";
    let jwt = "fake-jwt-token";

    // Act
    let frame = build_connect_frame(realm, jwt);

    // Assert
    assert!(!frame.is_empty());
    assert_eq!(frame[0], 1); // msg_type 1 (CONNECT)
}

#[test]
fn should_generate_valid_jwt() {
    // Arrange
    let realm = "test-realm";

    // Act
    let jwt = generate_test_jwt(realm);

    // Assert
    let parts: Vec<&str> = jwt.split('.').collect();
    assert_eq!(
        parts.len(),
        3,
        "JWT should have header.payload.signature format"
    );
}

#[tokio::test]
async fn should_shutdown_with_active_tcp_and_websocket_sessions() {
    // Arrange
    let server = TestServer::start().await.expect("start test server");
    let _tcp = server.connect().await.expect("connect tcp client");
    let _websocket = server.connect_ws().await.expect("connect websocket client");
    server
        .wait_for_session_count(2)
        .await
        .expect("wait for active sessions");

    // Act
    let result = server.shutdown().await;

    // Assert
    match result {
        Ok(()) => {}
        Err(error) => panic!("shutdown failed: {error}"),
    }
}

#[tokio::test]
async fn should_accept_websocket_upgrade_given_allowed_origin() {
    // Arrange
    let server = TestServer::start_with_ws_allowed_origins(&["https://app.example.com"])
        .await
        .expect("start test server");

    // Act
    let _websocket = server
        .connect_ws_with_origin("https://app.example.com")
        .await
        .expect("connect websocket");

    // Assert
    server
        .wait_for_session_count(1)
        .await
        .expect("wait for websocket session");
}

#[tokio::test]
async fn should_reject_websocket_upgrade_given_disallowed_origin() {
    // Arrange
    let server = TestServer::start_with_ws_allowed_origins(&["https://app.example.com"])
        .await
        .expect("start test server");

    // Act
    let status = server
        .websocket_upgrade_status(Some("https://evil.example.com"))
        .await
        .expect("read websocket status");

    // Assert
    assert_eq!(status, 403);
    assert_eq!(server.runtime.session_count(), 0);
}

#[tokio::test]
async fn should_allow_websocket_upgrade_given_missing_origin_when_origins_configured() {
    // Arrange
    let server = TestServer::start_with_ws_allowed_origins(&["https://app.example.com"])
        .await
        .expect("start test server");

    // Act
    let status = server
        .websocket_upgrade_status(None)
        .await
        .expect("read websocket status");

    // Assert
    assert_eq!(status, 101);
}

#[tokio::test]
async fn should_reject_websocket_upgrade_given_duplicate_origin_headers() {
    // Arrange
    let server = TestServer::start_with_ws_allowed_origins(&["https://app.example.com"])
        .await
        .expect("start test server");

    // Act
    let status = server
        .websocket_upgrade_status_with_origin_headers(&[
            "https://app.example.com",
            "https://app.example.com",
        ])
        .await
        .expect("read websocket status");

    // Assert
    assert_eq!(status, 403);
    assert_eq!(server.runtime.session_count(), 0);
}

#[tokio::test]
async fn should_allow_loopback_websocket_upgrade_without_origin_config() {
    // Arrange
    let server = TestServer::start().await.expect("start test server");

    // Act
    let status = server
        .websocket_upgrade_status(None)
        .await
        .expect("read websocket status");

    // Assert
    assert_eq!(status, 101);
}

#[tokio::test]
async fn should_reject_websocket_upgrade_before_data_plane_readiness() {
    // Arrange
    let ws_socket = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind websocket listener");
    let ws_addr = ws_socket.local_addr().expect("websocket listener addr");
    let boot_config = crate::boot::BootConfig::with_memory_storage()
        .with_auth_config(crate::auth::AuthConfig::Disabled)
        .with_bind_addr("127.0.0.1".to_string())
        .with_http_port(ws_addr.port());
    let (_, ingress, ingress_config, runtime) =
        crate::boot::runtime::init(&boot_config).expect("initialize runtime");
    runtime.mark_auth_config_ready();
    let handle = crate::api::handlers::spawn_http_listener_with_bound_socket(
        ws_socket,
        ingress,
        &ingress_config,
        runtime,
        boot_config.ws_allowed_origins.clone(),
    )
    .expect("spawn websocket listener");
    handle.ready.await.expect("websocket listener ready");

    // Act
    let status = websocket_upgrade_status_for_addr(ws_addr)
        .await
        .expect("read websocket status");

    // Assert
    assert_eq!(status, 503);
    let _ = handle.shutdown.send(());
    handle.join.await.expect("websocket listener shutdown");
}

#[tokio::test]
async fn should_reject_tcp_session_before_data_plane_readiness() {
    // Arrange
    let tcp_socket = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tcp listener");
    let tcp_addr = tcp_socket.local_addr().expect("tcp listener addr");
    let boot_config = crate::boot::BootConfig::with_memory_storage()
        .with_auth_config(crate::auth::AuthConfig::Disabled)
        .with_bind_addr("127.0.0.1".to_string())
        .with_tcp_port(tcp_addr.port());
    let (_, ingress, ingress_config, runtime) =
        crate::boot::runtime::init(&boot_config).expect("initialize runtime");
    runtime.mark_auth_config_ready();
    let handle = crate::api::handlers::spawn_tcp_listener_with_bound_socket(
        tcp_socket,
        ingress,
        &ingress_config,
        runtime.clone(),
    )
    .expect("spawn tcp listener");
    handle.ready.await.expect("tcp listener ready");

    // Act
    let _stream = TcpStream::connect(tcp_addr)
        .await
        .expect("connect tcp socket");
    tokio::task::yield_now().await;

    // Assert
    assert_eq!(runtime.session_count(), 0);
    let _ = handle.shutdown.send(());
    handle.join.await.expect("tcp listener shutdown");
}

#[tokio::test]
async fn should_reject_websocket_upgrade_when_broker_is_draining() {
    // Arrange
    let server = TestServer::start_with_ws_allowed_origins(&["https://app.example.com"])
        .await
        .expect("start test server");
    server.runtime.begin_drain();

    // Act
    let status = server
        .websocket_upgrade_status(Some("https://app.example.com"))
        .await
        .expect("read websocket status");

    // Assert
    assert_eq!(status, 503);
    assert_eq!(server.runtime.session_count(), 0);
}

#[tokio::test]
async fn should_reject_tcp_session_when_broker_is_draining() {
    // Arrange
    let server = TestServer::start().await.expect("start test server");
    server.runtime.begin_drain();

    // Act
    let _stream = TcpStream::connect(server.tcp_addr)
        .await
        .expect("connect tcp socket");
    tokio::task::yield_now().await;

    // Assert
    assert_eq!(server.runtime.session_count(), 0);
}
