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

async fn should_remove_session_owned_leases_when_client_disconnects<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let route = "lease://test/locks/disconnect";
    let mut client = C::connect(server).await.expect("connect");

    let acquire_frame = build_lease_acquire_immediate(route, "owner1", 30);
    let acquire_response = client
        .send_and_receive(&acquire_frame, 2000)
        .await
        .expect("acquire before disconnect");
    let (_msg_type, status, _data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0, "Expected acquire success before disconnect");

    // Act
    drop(client);
    server
        .wait_for_session_count(0)
        .await
        .expect("disconnect cleanup");
    server
        .wait_for_lease_count(0)
        .await
        .expect("lease disconnect cleanup");

    // Assert
    assert!(
        server.runtime.lease_list_leases(None).is_empty(),
        "Lease admin snapshot should reflect disconnect cleanup"
    );

    let mut reconnect = C::connect(server).await.expect("reconnect");
    let reacquire_frame = build_lease_acquire_immediate(route, "owner2", 30);
    let reacquire_response = reconnect
        .send_and_receive(&reacquire_frame, 2000)
        .await
        .expect("reacquire after disconnect");
    let (_msg_type, reacquire_status, _data) = parse_lease_response(&reacquire_response);
    assert_eq!(
        reacquire_status, 0,
        "Expected reacquire success after disconnect"
    );
}

async fn should_lose_all_leases_on_broker_restart<C>()
where
    C: LeaseConnector,
{
    // Arrange
    let route = "lease://test/locks/restart";

    {
        let server = TestServer::start().await.expect("start first server");
        let mut client = C::connect(&server).await.expect("connect before restart");

        let acquire_frame = build_lease_acquire_immediate(route, "owner1", 30);
        let acquire_response = client
            .send_and_receive(&acquire_frame, 2000)
            .await
            .expect("acquire before restart");
        let (_msg_type, status, data) = parse_lease_response(&acquire_response);
        assert_eq!(status, 0, "Expected acquire success before restart");
        assert!(parse_lease_token_response(&data).expect("token before restart") > 0);
    }

    let restarted_server = TestServer::start().await.expect("start restarted server");
    assert!(
        restarted_server.runtime.lease_list_leases(None).is_empty(),
        "Lease admin snapshot should be empty after broker restart"
    );

    // Act
    let mut challenger = C::connect(&restarted_server)
        .await
        .expect("connect after restart");
    let reacquire_frame = build_lease_acquire_immediate(route, "owner2", 30);
    let reacquire_response = challenger
        .send_and_receive(&reacquire_frame, 2000)
        .await
        .expect("reacquire after restart");

    // Assert
    let (_msg_type, status, data) = parse_lease_response(&reacquire_response);
    assert_eq!(status, 0, "Expected reacquire success after restart");
    assert!(parse_lease_token_response(&data).expect("token after restart") > 0);
}

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

async fn should_deliver_lease_watch_notification_after_release<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let route = "lease://test/locks/watch-release";
    let mut watcher = C::connect(server).await.expect("watcher connect");
    let mut holder = C::connect(server).await.expect("holder connect");

    let subscribe_response = watcher
        .send_and_receive(&build_lease_subscribe(route), 2000)
        .await
        .expect("subscribe lease watch");
    let (_msg_type, status, data) = parse_lease_response(&subscribe_response);
    assert_eq!(status, 0, "Expected subscribe success");
    let subscription_id = extract_lease_subscription_id(&data).expect("subscription id");

    let acquire_response = holder
        .send_and_receive(&build_lease_acquire_immediate(route, "owner1", 30), 2000)
        .await
        .expect("acquire lease");
    let (_msg_type, status, data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0, "Expected acquire success");
    let token = parse_lease_token_response(&data).expect("lease token");

    // Act
    let release_response = holder
        .send_and_receive(&build_lease_release(route, "owner1", token), 2000)
        .await
        .expect("release lease");
    let (_msg_type, status, _data) = parse_lease_response(&release_response);
    assert_eq!(status, 0, "Expected release success");

    let notify_frame = watcher.recv_frame(2000).await.expect("lease notify frame");

    // Assert
    let delivery = parse_lease_watch_delivery(&notify_frame).expect("lease watch delivery");
    assert_eq!(delivery.msg_type, 409);
    assert_eq!(delivery.subscription_id, subscription_id);
    assert_eq!(delivery.route, route);
    assert!(
        delivery.payload.is_empty(),
        "Expected empty lease notify payload"
    );
}

async fn should_not_deliver_lease_watch_notification_after_unsubscribe<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let route = "lease://test/locks/watch-unsubscribe";
    let mut watcher = C::connect(server).await.expect("watcher connect");
    let mut holder = C::connect(server).await.expect("holder connect");

    let subscribe_response = watcher
        .send_and_receive(&build_lease_subscribe(route), 2000)
        .await
        .expect("subscribe lease watch");
    let (_msg_type, status, _data) = parse_lease_response(&subscribe_response);
    assert_eq!(status, 0, "Expected subscribe success");

    let unsubscribe_response = watcher
        .send_and_receive(&build_lease_unsubscribe(route), 2000)
        .await
        .expect("unsubscribe lease watch");
    let (_msg_type, status, _data) = parse_lease_response(&unsubscribe_response);
    assert_eq!(status, 0, "Expected unsubscribe success");

    let acquire_response = holder
        .send_and_receive(&build_lease_acquire_immediate(route, "owner1", 30), 2000)
        .await
        .expect("acquire lease");
    let (_msg_type, status, data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0, "Expected acquire success");
    let token = parse_lease_token_response(&data).expect("lease token");

    // Act
    let release_response = holder
        .send_and_receive(&build_lease_release(route, "owner1", token), 2000)
        .await
        .expect("release lease");
    let (_msg_type, status, _data) = parse_lease_response(&release_response);
    assert_eq!(status, 0, "Expected release success");

    // Assert
    let notify_result = watcher.recv_frame(750).await;
    assert!(
        notify_result.is_err(),
        "Expected no lease notify after unsubscribe"
    );
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

#[tokio::test]
async fn should_remove_session_owned_leases_when_client_disconnects_tcp() {
    let server = TestServer::start().await.expect("start");
    should_remove_session_owned_leases_when_client_disconnects::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_lose_all_leases_on_broker_restart_tcp() {
    should_lose_all_leases_on_broker_restart::<TcpLeaseConnector>().await;
}

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

#[tokio::test]
async fn should_deliver_lease_watch_notification_after_release_tcp() {
    let server = TestServer::start().await.expect("start");
    should_deliver_lease_watch_notification_after_release::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_not_deliver_lease_watch_notification_after_unsubscribe_tcp() {
    let server = TestServer::start().await.expect("start");
    should_not_deliver_lease_watch_notification_after_unsubscribe::<TcpLeaseConnector>(&server)
        .await;
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

#[tokio::test]
async fn should_remove_session_owned_leases_when_client_disconnects_ws() {
    let server = TestServer::start().await.expect("start");
    should_remove_session_owned_leases_when_client_disconnects::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_lose_all_leases_on_broker_restart_ws() {
    should_lose_all_leases_on_broker_restart::<WsLeaseConnector>().await;
}

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

#[tokio::test]
async fn should_deliver_lease_watch_notification_after_release_ws() {
    let server = TestServer::start().await.expect("start");
    should_deliver_lease_watch_notification_after_release::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_not_deliver_lease_watch_notification_after_unsubscribe_ws() {
    let server = TestServer::start().await.expect("start");
    should_not_deliver_lease_watch_notification_after_unsubscribe::<WsLeaseConnector>(&server)
        .await;
}
