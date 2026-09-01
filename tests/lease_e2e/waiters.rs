//! FIFO wait-queue behavior: granted-on-release, timeout, and pending-waiter
//! visibility via QUERY.

use super::common::*;

async fn should_grant_waiting_acquire_when_holder_releases<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let route = "lease://test/locks/wait-grant";
    let mut holder = C::connect(server).await.expect("holder connect");
    let mut waiter = C::connect(server).await.expect("waiter connect");

    let acquire_frame = build_lease_acquire_immediate(route, "owner1", 30);
    let acquire_response = holder
        .send_and_receive(&acquire_frame, 2000)
        .await
        .expect("holder acquire");
    let (_msg_type, status, data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0, "Expected holder acquire success");
    let token = parse_lease_token_response(&data).expect("holder token");

    // Act
    let queued_frame = build_lease_acquire_with_wait(route, "owner2", 30, 3);
    let queued_response = waiter
        .send_and_receive(&queued_frame, 2000)
        .await
        .expect("waiter queue response");
    let (_msg_type, queued_status, queued_data) = parse_lease_response(&queued_response);
    assert_eq!(queued_status, 0, "Expected queued acquire acknowledgement");
    assert_eq!(
        parse_lease_acquire_response_type(&queued_data).expect("queued response type"),
        fitz::protocol::lease_codec::acquire_response_type::QUEUED,
        "Expected acquire to be queued"
    );

    let release_frame = build_lease_release(route, "owner1", token);
    let release_response = holder
        .send_and_receive(&release_frame, 2000)
        .await
        .expect("holder release");
    let (_msg_type, release_status, _data) = parse_lease_response(&release_response);
    assert_eq!(release_status, 0, "Expected release success");

    let deferred_response = waiter.recv_frame(3000).await.expect("deferred acquire");

    // Assert
    let (_msg_type, deferred_status, deferred_data) = parse_lease_response(&deferred_response);
    assert_eq!(deferred_status, 0, "Expected deferred acquire success");
    assert_eq!(
        parse_lease_acquire_response_type(&deferred_data).expect("deferred response type"),
        fitz::protocol::lease_codec::acquire_response_type::ACQUIRED,
        "Expected waiter to receive acquired response"
    );
    assert!(
        parse_lease_token_response(&deferred_data).expect("deferred token") > 0,
        "Expected deferred acquire to include a token"
    );
}

async fn should_time_out_waiting_acquire_when_holder_remains<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let route = "lease://test/locks/wait-timeout";
    let mut holder = C::connect(server).await.expect("holder connect");
    let mut waiter = C::connect(server).await.expect("waiter connect");

    let acquire_frame = build_lease_acquire_immediate(route, "owner1", 30);
    let acquire_response = holder
        .send_and_receive(&acquire_frame, 2000)
        .await
        .expect("holder acquire");
    let (_msg_type, status, _data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0, "Expected holder acquire success");

    // Act
    let queued_frame = build_lease_acquire_with_wait(route, "owner2", 30, 1);
    let queued_response = waiter
        .send_and_receive(&queued_frame, 2000)
        .await
        .expect("waiter queue response");
    let (_msg_type, queued_status, queued_data) = parse_lease_response(&queued_response);
    assert_eq!(queued_status, 0, "Expected queued acquire acknowledgement");
    assert_eq!(
        parse_lease_acquire_response_type(&queued_data).expect("queued response type"),
        fitz::protocol::lease_codec::acquire_response_type::QUEUED,
        "Expected acquire to be queued"
    );

    let timeout_response = waiter.recv_frame(3000).await.expect("timeout response");

    // Assert
    let (_msg_type, timeout_status, timeout_data) = parse_lease_response(&timeout_response);
    assert_ne!(timeout_status, 0, "Expected timeout error response");
    assert_eq!(
        parse_lease_error_message(&timeout_data).expect("timeout message"),
        "Timeout",
        "Expected queued acquire to time out"
    );
}

async fn should_report_pending_waiters_while_acquire_is_queued<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let route = "lease://test/locks/wait-query";
    let mut holder = C::connect(server).await.expect("holder connect");
    let mut waiter = C::connect(server).await.expect("waiter connect");
    let mut observer = C::connect(server).await.expect("observer connect");

    let acquire_frame = build_lease_acquire_immediate(route, "owner1", 30);
    let acquire_response = holder
        .send_and_receive(&acquire_frame, 2000)
        .await
        .expect("holder acquire");
    let (_msg_type, status, _data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0, "Expected holder acquire success");

    let queued_frame = build_lease_acquire_with_wait(route, "owner2", 30, 3);
    let queued_response = waiter
        .send_and_receive(&queued_frame, 2000)
        .await
        .expect("waiter queue response");
    let (_msg_type, queued_status, queued_data) = parse_lease_response(&queued_response);
    assert_eq!(queued_status, 0, "Expected queued acquire acknowledgement");
    assert_eq!(
        parse_lease_acquire_response_type(&queued_data).expect("queued response type"),
        fitz::protocol::lease_codec::acquire_response_type::QUEUED,
        "Expected acquire to be queued"
    );

    // Act
    let query_frame = build_lease_query(route);
    let query_response = observer
        .send_and_receive(&query_frame, 2000)
        .await
        .expect("query response");

    // Assert
    let (_msg_type, query_status, query_data) = parse_lease_response(&query_response);
    assert_eq!(query_status, 0, "Expected query success");
    let status_payload = parse_lease_status_payload(&query_data).expect("status payload");
    assert!(status_payload.has_holder, "Expected held lease status");
    assert!(
        status_payload.owner_id.as_deref().is_some_and(
            |owner_id| owner_id.starts_with("session:") && owner_id.ends_with(":owner1")
        ),
        "Expected query to report the current session-scoped owner"
    );
    assert_eq!(
        status_payload.pending_waiters, 1,
        "Expected query to report one pending waiter"
    );
}

// ===== TCP TESTS =====

#[tokio::test]
async fn should_grant_waiting_acquire_when_holder_releases_tcp() {
    let server = TestServer::start().await.expect("start");
    should_grant_waiting_acquire_when_holder_releases::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_time_out_waiting_acquire_when_holder_remains_tcp() {
    let server = TestServer::start().await.expect("start");
    should_time_out_waiting_acquire_when_holder_remains::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_report_pending_waiters_while_acquire_is_queued_tcp() {
    let server = TestServer::start().await.expect("start");
    should_report_pending_waiters_while_acquire_is_queued::<TcpLeaseConnector>(&server).await;
}

// ===== WEBSOCKET TESTS =====

#[tokio::test]
async fn should_grant_waiting_acquire_when_holder_releases_ws() {
    let server = TestServer::start().await.expect("start");
    should_grant_waiting_acquire_when_holder_releases::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_time_out_waiting_acquire_when_holder_remains_ws() {
    let server = TestServer::start().await.expect("start");
    should_time_out_waiting_acquire_when_holder_remains::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_report_pending_waiters_while_acquire_is_queued_ws() {
    let server = TestServer::start().await.expect("start");
    should_report_pending_waiters_while_acquire_is_queued::<WsLeaseConnector>(&server).await;
}
