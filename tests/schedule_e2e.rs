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
    let frame = build_schedule_create("schedule://test/jobs/daily", "0 0 * * *", b"backup");

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
    let create_frame = build_schedule_create("schedule://test/jobs/hourly", "0 * * * *", b"task");
    let _ = client
        .send_and_receive(&create_frame, 2000)
        .await
        .expect("create");

    // Act
    let cancel_frame = build_schedule_cancel("schedule://test/jobs/hourly");
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
    let frame = build_schedule_create("schedule://test/bad", "invalid cron", b"task");

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
async fn should_reject_cancel_nonexistent<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let frame = build_schedule_cancel("schedule://test/nonexistent");

    // Act
    let response = client.send_and_receive(&frame, 2000).await.expect("send");

    // Assert
    let (_msg_type, status, _data) = parse_schedule_response(&response);
    assert_ne!(
        status, 0,
        "Expected failure for cancel nonexistent schedule"
    );
}

#[tokio::test]
async fn should_reject_cancel_nonexistent_tcp() {
    let server = TestServer::start().await.expect("start");
    should_reject_cancel_nonexistent::<TcpScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_reject_cancel_nonexistent_ws() {
    let server = TestServer::start().await.expect("start");
    should_reject_cancel_nonexistent::<WsScheduleConnector>(&server).await;
}

// Generic test helper for schedule payload preservation
async fn should_preserve_schedule_payload<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let payload = b"important-task-data-123";
    let frame = build_schedule_create("schedule://test/preserve", "*/5 * * * *", payload);

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
    let frame1 = build_schedule_create("schedule://test/daily", "0 0 * * *", b"daily-task");
    let response1 = client
        .send_and_receive(&frame1, 2000)
        .await
        .expect("create 1");

    let (_msg_type, status1, _data) = parse_schedule_response(&response1);
    assert_eq!(status1, 0);

    let frame2 = build_schedule_create("schedule://test/hourly", "0 * * * *", b"hourly-task");
    let response2 = client
        .send_and_receive(&frame2, 2000)
        .await
        .expect("create 2");

    let (_msg_type, status2, _data) = parse_schedule_response(&response2);
    assert_eq!(status2, 0);

    let frame3 = build_schedule_create("schedule://test/weekly", "0 0 * * 0", b"weekly-task");
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

    for (cron, _desc) in cron_expressions {
        let frame = build_schedule_create(
            &format!("schedule://test/cron-{}", cron.replace(" ", "-")),
            cron,
            b"payload",
        );
        let response = client
            .send_and_receive(&frame, 2000)
            .await
            .expect(&format!("create {}", cron));

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
    let create1 = build_schedule_create("schedule://test/seq1", "0 0 * * *", b"task1");
    let response1 = client
        .send_and_receive(&create1, 2000)
        .await
        .expect("create 1");

    let (_msg_type, status1, _data) = parse_schedule_response(&response1);
    assert_eq!(status1, 0);

    // Act - Cancel first schedule
    let cancel1 = build_schedule_cancel("schedule://test/seq1");
    let response_cancel1 = client
        .send_and_receive(&cancel1, 2000)
        .await
        .expect("cancel 1");

    let (_msg_type, status_cancel1, _data) = parse_schedule_response(&response_cancel1);
    assert_eq!(status_cancel1, 0);

    // Act - Create second schedule
    let create2 = build_schedule_create("schedule://test/seq2", "0 * * * *", b"task2");
    let response2 = client
        .send_and_receive(&create2, 2000)
        .await
        .expect("create 2");

    let (_msg_type, status2, _data) = parse_schedule_response(&response2);
    assert_eq!(status2, 0);

    // Act - Cancel second schedule
    let cancel2 = build_schedule_cancel("schedule://test/seq2");
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

#[tokio::test]
async fn should_handle_sequential_create_and_cancel_operations_tcp() {
    let server = TestServer::start().await.expect("start");
    should_handle_sequential_create_and_cancel_operations::<TcpScheduleConnector>(&server).await;
}

#[tokio::test]
async fn should_handle_sequential_create_and_cancel_operations_ws() {
    let server = TestServer::start().await.expect("start");
    should_handle_sequential_create_and_cancel_operations::<WsScheduleConnector>(&server).await;
}
