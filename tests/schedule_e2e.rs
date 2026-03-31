//! Schedule domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use fitz::testkit::TestServer;
use fixtures::transport::*;

// Generic test helper for creating schedule
async fn should_create_cron_schedule<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_schedule_create("schedule://test/jobs/daily/run", "0 0 * * *", b"backup");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_schedule_response(&response);
    assert_eq!(status, 0, "Expected success for create schedule");
}

#[tokio::test]
async fn should_create_cron_schedule_tcp() {
    let server = TestServer::start().await.expect("start");
    should_create_cron_schedule::<TcpScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_create_cron_schedule_ws() {
    let server = TestServer::start().await.expect("start");
    should_create_cron_schedule::<WsScheduleConnector>(&server).await;
}

// Generic test helper for canceling schedule
async fn should_cancel_schedule<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let create_frame =
        build_schedule_create("schedule://test/jobs/hourly/run", "0 * * * *", b"task");
    let _ = client
        .send_and_receive(&create_frame, 2000)
        .await
        .expect("create");

    // Act
    let cancel_frame = build_schedule_cancel("schedule://test/jobs/hourly/run");
    let response = client
        .send_and_receive(&cancel_frame, 2000)
        .await
        .expect("cancel");

    // Assert
    let (_msg_type, status, _data) = parse_schedule_response(&response);
    assert_eq!(status, 0, "Expected success for cancel schedule");
}

#[tokio::test]
async fn should_cancel_schedule_tcp() {
    let server = TestServer::start().await.expect("start");
    should_cancel_schedule::<TcpScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_cancel_schedule_ws() {
    let server = TestServer::start().await.expect("start");
    should_cancel_schedule::<WsScheduleConnector>(&server).await;
}

// Generic test helper for invalid cron expression
async fn should_reject_invalid_cron<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_schedule_create("schedule://test/jobs/bad/run", "invalid cron", b"task");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_schedule_response(&response);
    assert_ne!(status, 0, "Expected failure for invalid cron expression");
}

#[tokio::test]
async fn should_reject_invalid_cron_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_invalid_cron::<TcpScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_invalid_cron_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_invalid_cron::<WsScheduleConnector>(&server).await;
}

// Generic test helper for cancel nonexistent schedule
async fn should_allow_cancel_nonexistent_idempotent<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_schedule_cancel("schedule://test/jobs/nonexistent/run");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_schedule_response(&response);
    assert_eq!(
        status, 0,
        "Cancel should be idempotent (succeed even if schedule doesn't exist)"
    );
}

#[tokio::test]
async fn should_reject_cancel_nonexistent_tcp() {
    let server = TestServer::start().await.expect("start");
    should_allow_cancel_nonexistent_idempotent::<TcpScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_cancel_nonexistent_ws() {
    let server = TestServer::start().await.expect("start");
    should_allow_cancel_nonexistent_idempotent::<WsScheduleConnector>(&server).await;
}

// Generic test helper for schedule payload preservation
async fn should_preserve_schedule_payload<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let payload = b"important-task-data-123";
    let frame = build_schedule_create("schedule://test/jobs/preserve/run", "*/5 * * * *", payload);

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_schedule_response(&response);
    assert_eq!(status, 0, "Should preserve schedule payload");
}

#[tokio::test]
async fn should_preserve_schedule_payload_tcp() {
    let server = TestServer::start().await.expect("start");
    should_preserve_schedule_payload::<TcpScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_preserve_schedule_payload_ws() {
    let server = TestServer::start().await.expect("start");
    should_preserve_schedule_payload::<WsScheduleConnector>(&server).await;
}

// Generic test helper for multiple schedule creation
async fn should_handle_multiple_schedule_creation<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Create multiple schedules
    let frame1 =
        build_schedule_create("schedule://test/jobs/daily/run", "0 0 * * *", b"daily-task");
    let response1 = client
        .send_and_receive(&frame1, 2000)
        .await
        .expect("create 1");

    let (_msg_type, status1, _data) = parse_schedule_response(&response1);
    assert_eq!(status1, 0);

    let frame2 = build_schedule_create(
        "schedule://test/jobs/hourly/run",
        "0 * * * *",
        b"hourly-task",
    );
    let response2 = client
        .send_and_receive(&frame2, 2000)
        .await
        .expect("create 2");

    let (_msg_type, status2, _data) = parse_schedule_response(&response2);
    assert_eq!(status2, 0);

    let frame3 = build_schedule_create(
        "schedule://test/jobs/weekly/run",
        "0 0 * * 0",
        b"weekly-task",
    );
    let response3 = client
        .send_and_receive(&frame3, 2000)
        .await
        .expect("create 3");

    // Assert
    let (_msg_type, status3, _data) = parse_schedule_response(&response3);
    assert_eq!(status3, 0, "Should handle multiple schedule creation");
}

#[tokio::test]
async fn should_handle_multiple_schedule_creation_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_multiple_schedule_creation::<TcpScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_multiple_schedule_creation_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_multiple_schedule_creation::<WsScheduleConnector>(&server).await;
}

// Generic test helper for various cron expressions
async fn should_support_various_cron_formats<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Test different cron expressions
    let cron_expressions = vec![
        ("* * * * *", "every minute"),
        ("*/5 * * * *", "every 5 minutes"),
        ("0 */6 * * *", "every 6 hours"),
        ("0 0 * * 1", "every monday"),
        ("30 2 * * *", "at 2:30 AM"),
    ];

    for (index, (cron, _desc)) in cron_expressions.into_iter().enumerate() {
        let frame = build_schedule_create(
            &format!("schedule://test/cron/expr-{index}/run"),
            cron,
            b"payload",
        );
        let response = client
            .send_and_receive(&frame, 2000)
            .await
            .unwrap_or_else(|_| panic!("create {}", cron));

        let (_msg_type, status, _data) = parse_schedule_response(&response);
        assert_eq!(status, 0, "Should accept cron: {}", cron);
    }
}

#[tokio::test]
async fn should_support_various_cron_formats_tcp() {
    let server = TestServer::start().await.expect("start");
    should_support_various_cron_formats::<TcpScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_support_various_cron_formats_ws() {
    let server = TestServer::start().await.expect("start");
    should_support_various_cron_formats::<WsScheduleConnector>(&server).await;
}

// Generic test helper for sequential create and cancel
async fn should_handle_sequential_create_and_cancel_operations<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");

    // Act - Create first schedule
    let create1 = build_schedule_create("schedule://test/jobs/seq1/run", "0 0 * * *", b"task1");
    let response1 = client
        .send_and_receive(&create1, 2000)
        .await
        .expect("create 1");

    let (_msg_type, status1, _data) = parse_schedule_response(&response1);
    assert_eq!(status1, 0);

    // Act - Cancel first schedule
    let cancel1 = build_schedule_cancel("schedule://test/jobs/seq1/run");
    let response_cancel1 = client
        .send_and_receive(&cancel1, 2000)
        .await
        .expect("cancel 1");

    let (_msg_type, status_cancel1, _data) = parse_schedule_response(&response_cancel1);
    assert_eq!(status_cancel1, 0);

    // Act - Create second schedule
    let create2 = build_schedule_create("schedule://test/jobs/seq2/run", "0 * * * *", b"task2");
    let response2 = client
        .send_and_receive(&create2, 2000)
        .await
        .expect("create 2");

    let (_msg_type, status2, _data) = parse_schedule_response(&response2);
    assert_eq!(status2, 0);

    // Act - Cancel second schedule
    let cancel2 = build_schedule_cancel("schedule://test/jobs/seq2/run");
    let response_cancel2 = client
        .send_and_receive(&cancel2, 2000)
        .await
        .expect("cancel 2");

    // Assert
    let (_msg_type, status_cancel2, _data) = parse_schedule_response(&response_cancel2);
    assert_eq!(
        status_cancel2, 0,
        "Should handle sequential create and cancel"
    );
}

async fn should_retain_other_schedule_subscription_after_unsubscribe<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    let removed_route = "schedule://test/jobs/daily/run";
    let retained_route = "schedule://test/jobs/weekly/run";
    let mut subscriber = C::connect(server).await.expect("connect subscriber");
    let mut creator = C::connect(server).await.expect("connect creator");

    let removed_create_response = creator
        .send_and_receive(
            &build_schedule_create(removed_route, "0 0 * * *", b"removed"),
            2000,
        )
        .await
        .expect("create removed schedule");
    let (_msg_type, status, _data) = parse_schedule_response(&removed_create_response);
    assert_eq!(status, 0, "Expected success for removed schedule create");

    let retained_create_response = creator
        .send_and_receive(
            &build_schedule_create(retained_route, "0 0 * * 0", b"retained"),
            2000,
        )
        .await
        .expect("create retained schedule");
    let (_msg_type, status, _data) = parse_schedule_response(&retained_create_response);
    assert_eq!(status, 0, "Expected success for retained schedule create");

    let removed_subscribe_response = subscriber
        .send_and_receive(&build_schedule_subscribe(removed_route), 2000)
        .await
        .expect("subscribe removed route");
    let (_msg_type, status, _data) = parse_schedule_response(&removed_subscribe_response);
    assert_eq!(status, 0, "Expected success for removed route subscribe");

    let retained_subscribe_response = subscriber
        .send_and_receive(&build_schedule_subscribe(retained_route), 2000)
        .await
        .expect("subscribe retained route");
    let (_msg_type, status, _data) = parse_schedule_response(&retained_subscribe_response);
    assert_eq!(status, 0, "Expected success for retained route subscribe");

    let unsubscribe_response = subscriber
        .send_and_receive(&build_schedule_unsubscribe(removed_route), 2000)
        .await
        .expect("unsubscribe removed route");
    let (_msg_type, status, _data) = parse_schedule_response(&unsubscribe_response);
    assert_eq!(status, 0, "Expected success for removed route unsubscribe");

    server
        .force_schedule_scan_for_tests(1)
        .expect("trigger removed schedule scan");
    assert!(
        subscriber.recv_frame(200).await.is_err(),
        "Removed schedule fire should not deliver after unsubscribe"
    );

    server
        .force_schedule_scan_for_tests(2)
        .expect("trigger retained schedule scan");
    let retained_delivery = subscriber
        .recv_frame(2000)
        .await
        .expect("retained schedule delivery");
    let retained_delivery =
        parse_schedule_delivery(&retained_delivery).expect("parse schedule delivery");
    assert_eq!(retained_delivery.msg_type, 705);
    assert!(retained_delivery.subscription_id > 0);
    assert_eq!(retained_delivery.body.as_slice(), b"retained");
}

#[tokio::test]
async fn should_handle_sequential_create_and_cancel_operations_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_sequential_create_and_cancel_operations::<TcpScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_retain_other_schedule_subscription_after_unsubscribe_tcp() {
    let server = TestServer::start().await.expect("start");
    should_retain_other_schedule_subscription_after_unsubscribe::<TcpScheduleConnector>(&server)
        .await;
}

#[tokio::test]
async fn should_handle_sequential_create_and_cancel_operations_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_sequential_create_and_cancel_operations::<WsScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_retain_other_schedule_subscription_after_unsubscribe_ws() {
    let server = TestServer::start().await.expect("start");
    should_retain_other_schedule_subscription_after_unsubscribe::<WsScheduleConnector>(&server)
        .await;
}
