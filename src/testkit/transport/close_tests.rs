use super::*;
use crate::runtime::routing::{session_inbox_address, RouteFamily};
use crate::runtime::{Envelope, SessionCloseRequest};

fn request_close(server: &TestServer) {
    let session = server
        .runtime
        .admin_read_model()
        .sessions()
        .into_iter()
        .next()
        .unwrap();
    let session_id = session.session_id.parse().unwrap();
    let destination = session_inbox_address(
        RouteFamily::new(u32::try_from(session.route_family).unwrap()),
        session_id,
    );
    server
        .runtime
        .router
        .route(Envelope::new(
            destination,
            SessionCloseRequest {
                reason: "RPC cancellation grace expired",
            },
        ))
        .unwrap();
}

#[tokio::test]
async fn should_close_tcp_through_transport_lifecycle_given_internal_close_request() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = server.connect().await.unwrap();
    client
        .send_frame(&build_connect_frame(
            "test-realm",
            &generate_test_jwt("test-realm"),
        ))
        .await
        .unwrap();
    server.wait_for_authenticated_sessions(1).await.unwrap();

    // Act
    request_close(&server);
    server.wait_for_session_count(0).await.unwrap();
    let closed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while client.recv_frame_bytes_without_timeout().await.is_ok() {}
    })
    .await;

    // Assert
    assert!(
        closed.is_ok(),
        "TCP socket must close, not just its registry entry"
    );
    assert!(server.runtime.admin_read_model().sessions().is_empty());
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn should_close_websocket_through_transport_lifecycle_given_internal_close_request() {
    // Arrange
    let server = TestServer::start().await.unwrap();
    let mut client = server.connect_ws().await.unwrap();
    client
        .send_frame(&build_connect_frame(
            "test-realm",
            &generate_test_jwt("test-realm"),
        ))
        .await
        .unwrap();
    server.wait_for_authenticated_sessions(1).await.unwrap();

    // Act
    request_close(&server);
    server.wait_for_session_count(0).await.unwrap();
    let closed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while client.recv_frame_bytes_without_timeout().await.is_ok() {}
    })
    .await;

    // Assert
    assert!(
        closed.is_ok(),
        "WebSocket must close, not just its registry entry"
    );
    assert!(server.runtime.admin_read_model().sessions().is_empty());
    server.shutdown().await.unwrap();
}
