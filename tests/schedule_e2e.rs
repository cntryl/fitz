//! Schedule domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use fixtures::transport::*;
use fitz::testkit::TestServer;

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
    assert_ne!(status, 0, "Expected failure for cancel nonexistent schedule");
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
