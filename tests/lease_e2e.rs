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

async fn should_acquire_and_renew_lease_before_expiry<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Acquire lease
    let acquire_frame = build_lease_acquire_immediate("lease://test/locks/resource", "owner1", 10);
    let acquire_response = client
        .send_and_receive(&acquire_frame, 2000)
        .await
        .expect("acquire");

    let (_msg_type, status, data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0, "Expected acquire success");

    let token = parse_lease_token_response(&data).expect("parse token");

    // Act - Renew lease before expiry
    let renew_frame = build_lease_renew("lease://test/locks/resource", "owner1", token, 20);
    let renew_response = client
        .send_and_receive(&renew_frame, 2000)
        .await
        .expect("renew");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&renew_response);
    assert_eq!(status, 0, "Expected renew success");
}

async fn should_release_lease_and_allow_reacquisition<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Acquire lease
    let acquire_frame = build_lease_acquire_immediate("lease://test/locks/app", "owner1", 30);
    let acquire_response = client
        .send_and_receive(&acquire_frame, 2000)
        .await
        .expect("acquire");

    let (_msg_type, status, data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0);

    let token = parse_lease_token_response(&data).expect("parse token");

    // Act - Release lease
    let release_frame = build_lease_release("lease://test/locks/app", "owner1", token);
    let release_response = client
        .send_and_receive(&release_frame, 2000)
        .await
        .expect("release");

    let (_msg_type, status, _data) = parse_lease_response(&release_response);
    assert_eq!(status, 0, "Expected release success");

    // Act - Try to reacquire (should succeed now)
    let reacquire_frame = build_lease_acquire_immediate("lease://test/locks/app", "owner2", 30);
    let reacquire_response = client
        .send_and_receive(&reacquire_frame, 2000)
        .await
        .expect("reacquire");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&reacquire_response);
    assert_eq!(status, 0, "Expected reacquisition after release");
}

async fn should_return_valid_lease_token_on_acquire<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act
    let acquire_frame = build_lease_acquire_immediate("lease://test/tokens/holder", "owner1", 15);
    let acquire_response = client
        .send_and_receive(&acquire_frame, 2000)
        .await
        .expect("acquire");

    // Assert
    let (_msg_type, status, data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0, "Expected acquire success");

    let token = parse_lease_token_response(&data);
    assert!(token.is_ok(), "Expected valid token in response");
    assert!(token.unwrap() > 0, "Token should be non-zero");
}

async fn should_prevent_renew_with_invalid_token<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Acquire lease
    let acquire_frame = build_lease_acquire_immediate("lease://test/locks/valid", "owner1", 20);
    let acquire_response = client
        .send_and_receive(&acquire_frame, 2000)
        .await
        .expect("acquire");

    let (_msg_type, status, _data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0);

    // Act - Try to renew with wrong token
    let renew_frame = build_lease_renew("lease://test/locks/valid", "owner1", 0, 30);
    let renew_response = client
        .send_and_receive(&renew_frame, 2000)
        .await
        .expect("renew");

    // Assert
    let (_msg_type, status, _data) = parse_lease_response(&renew_response);
    assert_ne!(status, 0, "Should reject renew with invalid token");
}

async fn should_handle_multiple_sequential_leases<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Acquire first lease
    let lease1_frame = build_lease_acquire_immediate("lease://test/locks/l1", "owner1", 10);
    let lease1_response = client
        .send_and_receive(&lease1_frame, 2000)
        .await
        .expect("lease1");

    let (_msg_type, status1, _data) = parse_lease_response(&lease1_response);
    assert_eq!(status1, 0);

    // Act - Acquire second lease (different resource)
    let lease2_frame = build_lease_acquire_immediate("lease://test/locks/l2", "owner1", 10);
    let lease2_response = client
        .send_and_receive(&lease2_frame, 2000)
        .await
        .expect("lease2");

    let (_msg_type, status2, _data) = parse_lease_response(&lease2_response);
    assert_eq!(status2, 0);

    // Act - Acquire third lease
    let lease3_frame = build_lease_acquire_immediate("lease://test/locks/l3", "owner1", 10);
    let lease3_response = client
        .send_and_receive(&lease3_frame, 2000)
        .await
        .expect("lease3");

    // Assert
    let (_msg_type, status3, _data) = parse_lease_response(&lease3_response);
    assert_eq!(status3, 0, "Should allow multiple sequential leases");
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

#[tokio::test]
async fn should_acquire_and_renew_lease_before_expiry_tcp() {
    let server = TestServer::start().await.expect("start");
    should_acquire_and_renew_lease_before_expiry::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_release_lease_and_allow_reacquisition_tcp() {
    let server = TestServer::start().await.expect("start");
    should_release_lease_and_allow_reacquisition::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_return_valid_lease_token_on_acquire_tcp() {
    let server = TestServer::start().await.expect("start");
    should_return_valid_lease_token_on_acquire::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_prevent_renew_with_invalid_token_tcp() {
    let server = TestServer::start().await.expect("start");
    should_prevent_renew_with_invalid_token::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_multiple_sequential_leases_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_multiple_sequential_leases::<TcpLeaseConnector>(&server).await;
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

#[tokio::test]
async fn should_acquire_and_renew_lease_before_expiry_ws() {
    let server = TestServer::start().await.expect("start");
    should_acquire_and_renew_lease_before_expiry::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_release_lease_and_allow_reacquisition_ws() {
    let server = TestServer::start().await.expect("start");
    should_release_lease_and_allow_reacquisition::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_return_valid_lease_token_on_acquire_ws() {
    let server = TestServer::start().await.expect("start");
    should_return_valid_lease_token_on_acquire::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_prevent_renew_with_invalid_token_ws() {
    let server = TestServer::start().await.expect("start");
    should_prevent_renew_with_invalid_token::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_multiple_sequential_leases_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_multiple_sequential_leases::<WsLeaseConnector>(&server).await;
}
