//! Lease SUBSCRIBE/UNSUBSCRIBE: exact and wildcard watch notifications,
//! selector grammar validation, and idempotency.

use super::common::*;

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

async fn should_reject_malformed_lease_subscription_routes<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let invalid_routes = [
        "lease://test/locks/lock*",
        "notice://test/locks/primary",
        "lease://test/locks",
        "lease://test/locks/primary/extra",
        "lease://test//primary",
    ];

    // Act / Assert
    for route in invalid_routes {
        for frame in [build_lease_subscribe(route), build_lease_unsubscribe(route)] {
            let response = client
                .send_and_receive(&frame, 2000)
                .await
                .expect("invalid Lease subscription response");
            let (_message_type, status, data) = parse_lease_response(&response);
            assert_eq!(status, 1, "malformed route must fail: {route}");
            let (code, _message) = fitz::protocol::error_codes::decode_error_body(&data)
                .expect("Lease subscription error envelope");
            assert_eq!(
                code,
                fitz::protocol::error_codes::lease::ERR_INVALID_SUBSCRIPTION_ROUTE
            );
        }
    }
}

async fn should_accept_wildcard_lease_subscription_routes<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let valid_routes = [
        "lease://acme/renderers/document-123",
        "lease://acme/renderers/*",
        "lease://acme/*/document-123",
        "lease://acme/*/*",
        "lease://*/renderers/*",
        "lease://*/*/*",
        "lease://acme/**",
        "lease://**",
    ];

    // Act / Assert
    for route in valid_routes {
        for frame in [build_lease_subscribe(route), build_lease_unsubscribe(route)] {
            let response = client
                .send_and_receive(&frame, 2000)
                .await
                .expect("wildcard Lease subscription response");
            let (_message_type, status, _data) = parse_lease_response(&response);
            assert_eq!(status, 0, "wildcard selector must be accepted: {route}");
        }
    }
}

async fn should_deliver_lease_watch_notification_to_matching_wildcard_subscription<C>(
    server: &TestServer,
) where
    C: LeaseConnector,
{
    // Arrange
    let selector = "lease://acme/renderers/*";
    let route = "lease://acme/renderers/document-123";
    let other_route = "lease://acme/printers/document-456";
    let mut watcher = C::connect(server).await.expect("watcher connect");
    let mut holder = C::connect(server).await.expect("holder connect");

    let subscribe_response = watcher
        .send_and_receive(&build_lease_subscribe(selector), 2000)
        .await
        .expect("subscribe wildcard lease watch");
    let (_msg_type, status, data) = parse_lease_response(&subscribe_response);
    assert_eq!(status, 0, "Expected wildcard subscribe success");
    let subscription_id = extract_lease_subscription_id(&data).expect("subscription id");

    // Act: acquire a matching route and a non-matching route.
    let acquire_response = holder
        .send_and_receive(
            &build_lease_acquire_immediate(other_route, "owner1", 30),
            2000,
        )
        .await
        .expect("acquire non-matching lease");
    let (_msg_type, status, _data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0, "Expected non-matching acquire success");

    let acquire_response = holder
        .send_and_receive(&build_lease_acquire_immediate(route, "owner1", 30), 2000)
        .await
        .expect("acquire matching lease");
    let (_msg_type, status, _data) = parse_lease_response(&acquire_response);
    assert_eq!(status, 0, "Expected matching acquire success");

    let notify_frame = watcher
        .recv_frame(2000)
        .await
        .expect("wildcard lease notify frame");

    // Assert: exactly one notification, for the matching concrete route.
    let delivery = parse_lease_watch_delivery(&notify_frame).expect("lease watch delivery");
    assert_eq!(delivery.subscription_id, subscription_id);
    assert_eq!(delivery.route, route);

    let unexpected = watcher.recv_frame(500).await;
    assert!(
        unexpected.is_err(),
        "Expected no second notification for the non-matching route"
    );
}

async fn should_keep_exact_lease_subscription_idempotent<C>(server: &TestServer)
where
    C: LeaseConnector,
{
    // Arrange
    let route = "lease://test/locks/idempotent";
    let mut client = C::connect(server).await.expect("connect");

    // Act
    let first = client
        .send_and_receive(&build_lease_subscribe(route), 2000)
        .await
        .expect("first exact Lease subscription");
    let second = client
        .send_and_receive(&build_lease_subscribe(route), 2000)
        .await
        .expect("duplicate exact Lease subscription");

    // Assert
    let (_, first_status, first_data) = parse_lease_response(&first);
    let (_, second_status, second_data) = parse_lease_response(&second);
    assert_eq!(first_status, 0);
    assert_eq!(second_status, 0);
    assert_eq!(
        extract_lease_subscription_id(&first_data).expect("first Lease subscription id"),
        extract_lease_subscription_id(&second_data).expect("duplicate Lease subscription id")
    );
}

// ===== TCP TESTS =====

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

#[tokio::test]
async fn should_reject_malformed_lease_subscription_routes_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_malformed_lease_subscription_routes::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_accept_wildcard_lease_subscription_routes_tcp() {
    let server = TestServer::start().await.expect("start");
    should_accept_wildcard_lease_subscription_routes::<TcpLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_deliver_lease_watch_notification_to_matching_wildcard_subscription_tcp() {
    let server = TestServer::start().await.expect("start");
    should_deliver_lease_watch_notification_to_matching_wildcard_subscription::<TcpLeaseConnector>(
        &server,
    )
    .await;
}

#[tokio::test]
async fn should_keep_exact_lease_subscription_idempotent_tcp() {
    let server = TestServer::start().await.expect("start");
    should_keep_exact_lease_subscription_idempotent::<TcpLeaseConnector>(&server).await;
}

// ===== WEBSOCKET TESTS =====

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

#[tokio::test]
async fn should_reject_malformed_lease_subscription_routes_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_malformed_lease_subscription_routes::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_accept_wildcard_lease_subscription_routes_ws() {
    let server = TestServer::start().await.expect("start");
    should_accept_wildcard_lease_subscription_routes::<WsLeaseConnector>(&server).await;
}

#[tokio::test]
async fn should_deliver_lease_watch_notification_to_matching_wildcard_subscription_ws() {
    let server = TestServer::start().await.expect("start");
    should_deliver_lease_watch_notification_to_matching_wildcard_subscription::<WsLeaseConnector>(
        &server,
    )
    .await;
}

#[tokio::test]
async fn should_keep_exact_lease_subscription_idempotent_ws() {
    let server = TestServer::start().await.expect("start");
    should_keep_exact_lease_subscription_idempotent::<WsLeaseConnector>(&server).await;
}
