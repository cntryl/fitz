//! Lease end-to-end transport tests
//!
//! Tests full lease domain functionality across TCP and WebSocket transports.

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::transport::*;

// ===== GENERIC TEST IMPLEMENTATIONS =====

async fn should_acquire_lease_immediately<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act
    let frame = build_lease_acquire_immediate("lease://test/locks/db", "owner1", 30);
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&response);
    assert_eq!(status, 0, "Expected success for acquire");
}

async fn should_reject_renew_of_unowned_lease<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act
    let renew_frame = build_lease_renew("lease://test/locks/db", "owner1", 999_999_999, 30);
    let response = client
        .send_and_receive(&renew_frame, 2000)
        .await
        .expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&response);
    assert_ne!(status, 0, "Should reject renew of unowned lease");
}

// ===== TCP TESTS =====

#[tokio::test]
async fn should_acquire_lease_immediately_tcp() {
    let server = TestServer::start().await.expect("start");
    should_acquire_lease_immediately::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_renew_of_unowned_lease_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_renew_of_unowned_lease::<TcpLeaseConnector>(&server).await;
}

// ===== WEBSOCKET TESTS =====

#[tokio::test]
async fn should_acquire_lease_immediately_ws() {
    let server = TestServer::start().await.expect("start");
    should_acquire_lease_immediately::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_renew_of_unowned_lease_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_renew_of_unowned_lease::<WsLeaseConnector>(&server).await;
}
