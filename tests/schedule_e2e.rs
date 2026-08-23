//! Schedule domain end-to-end tests
//! Tests both TCP and WebSocket transports

mod fixtures;
use bytes::Bytes;
use fitz::protocol::payload_codec::PayloadDecoder;
use fitz::testkit::TestServer;
use fixtures::define_transport_tests;
use fixtures::transport::*;
use std::time::Duration;

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

// Generic test helper for invalid cron expression
async fn should_reject_invalid_cron<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let invalid_expressions = [
        "invalid cron",
        "60 0 * * *",
        "0 9-2 * * *",
        "*/0 * * * *",
        "0 0 31 2 *",
    ];

    // Act
    let mut responses = Vec::new();
    for expression in invalid_expressions {
        let frame = build_schedule_create("schedule://test/jobs/bad/run", expression, b"task");
        responses.push(
            client
                .send_and_receive(&frame, 2000)
                .await
                .expect("send invalid cron"),
        );
    }

    // Assert
    for response in responses {
        let (_msg_type, status, data) = parse_schedule_response(&response);
        assert_ne!(status, 0, "Expected failure for invalid cron expression");
        assert!(data.len() >= 5);
        let length =
            u32::from_be_bytes(data[1..5].try_into().expect("schedule error length")) as usize;
        assert_eq!(data.len(), 5 + length);
        let message = String::from_utf8(data[5..].to_vec()).expect("schedule error UTF-8");
        assert!(!message.is_empty());
    }
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
    let create_response = client.send_and_receive(&frame, 2000).await.expect("send");
    let list_response = client
        .send_and_receive(&build_schedule_list(), 2000)
        .await
        .expect("list schedules");

    // Assert
    let (_msg_type, create_status, _data) = parse_schedule_response(&create_response);
    assert_eq!(create_status, 0, "Expected success for create schedule");

    let (_msg_type, list_status, list_payload) = parse_schedule_response(&list_response);
    assert_eq!(list_status, 0, "Expected success for schedule list");

    // Decode the ListDefs entry and confirm the exact payload bytes round-tripped
    // over the wire, not just that the create call reported success.
    let mut dec = PayloadDecoder::new(&list_payload[1..]);
    let _total_count = dec.get_u64().expect("total count");
    let has_entry = dec.get_u8().expect("has_entry flag");
    assert_eq!(has_entry, 1, "expected the created schedule in the list");
    let route = dec.get_string().expect("route");
    assert_eq!(route, "schedule://test/jobs/preserve/run");
    let _cron = dec.get_string().expect("cron");
    let _delivery_mode = dec.get_u8().expect("delivery mode");
    let stored_payload = dec.get_bytes().expect("payload");
    assert_eq!(
        stored_payload.as_ref(),
        payload,
        "schedule payload must round-trip byte-for-byte over the wire"
    );
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

async fn should_create_schedule_batch<C>(server: &TestServer)
where
    C: ScheduleConnector,
{
    // Arrange
    let mut client = C::connect(server).await.expect("connect");
    let entries = [
        (
            "schedule://test/jobs/batch-1/run",
            "0 0 * * *",
            &b"backup"[..],
        ),
        (
            "schedule://test/jobs/batch-2/run",
            "15 6 * * *",
            &b"report"[..],
        ),
        (
            "schedule://test/jobs/batch-3/run",
            "0 * * * *",
            &b"sync"[..],
        ),
    ];
    let create_frame = build_schedule_create_batch(&entries);

    // Act
    let create_response = client
        .send_and_receive(&create_frame, 2000)
        .await
        .expect("batch create");
    let list_response = client
        .send_and_receive(&build_schedule_list(), 2000)
        .await
        .expect("list schedules");

    // Assert
    let (_msg_type, create_status, _data) = parse_schedule_response(&create_response);
    assert_eq!(
        create_status, 0,
        "Expected success for schedule batch create"
    );

    let (_msg_type, list_status, list_payload) = parse_schedule_response(&list_response);
    assert_eq!(list_status, 0, "Expected success for schedule list");

    let mut dec = PayloadDecoder::new(&list_payload[1..]);
    let total_count = dec.get_u64().expect("total count");
    assert_eq!(total_count, entries.len() as u64);
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
            .unwrap_or_else(|_| panic!("create {cron}"));

        let (_msg_type, status, _data) = parse_schedule_response(&response);
        assert_eq!(status, 0, "Should accept cron: {cron}");
    }
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
    assert_eq!(retained_delivery.route, retained_route);
    assert_eq!(retained_delivery.body.as_slice(), b"retained");
}

#[tokio::test]
async fn should_recover_schedule_definitions_before_accepting_schedule_traffic() {
    let tempdir = tempfile::TempDir::new().expect("tempdir");
    let db_path = tempdir.path().join("fitz-schedule-restart");
    let db_path = db_path.to_string_lossy().to_string();
    let route = "schedule://test/jobs/restart/run";

    let boot_config = fitz::boot::runtime::BootConfig::with_local_storage(db_path.clone());
    let store = fitz::boot::storage::init(&boot_config)
        .await
        .expect("init local storage");
    let mut actor = fitz::domains::schedule::ScheduleActor::new(
        fitz::runtime::routing::RouteFamily::new(1),
        store.clone(),
        cntryl_midge::WriteOptions::buffered(),
    );
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"persisted"),
        )
        .expect("persist schedule through actor");
    drop(actor);
    drop(store);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let server = TestServer::start_with_local_storage(db_path)
        .await
        .expect("start server from persisted schedule state");

    let schedules = server.runtime.schedule_list_schedules(None);
    let schedule = schedules
        .iter()
        .find(|item| {
            item.realm == "test"
                && item.area == "jobs"
                && item.resource == "restart"
                && item.operation == "run"
        })
        .expect("schedule should be visible before any schedule-domain traffic");
    assert_eq!(schedule.cron, "* * * * *");
    assert!(schedule.enabled);

    let mut subscriber = TcpScheduleConnector::connect(&server)
        .await
        .expect("connect subscriber");
    let subscribe_response = subscriber
        .send_and_receive(&build_schedule_subscribe(route), 2000)
        .await
        .expect("subscribe schedule");
    let (_msg_type, status, _data) = parse_schedule_response(&subscribe_response);
    assert_eq!(status, 0, "Expected success for subscribe after restart");

    server
        .force_schedule_scan_for_tests(1)
        .expect("force schedule scan after restart");

    let delivery = subscriber
        .recv_frame(2000)
        .await
        .expect("receive restarted schedule delivery");
    let delivery = parse_schedule_delivery(&delivery).expect("parse schedule delivery");
    assert_eq!(delivery.msg_type, 705);
    assert_eq!(delivery.route, route);
    assert_eq!(delivery.body.as_slice(), b"persisted");

    let refreshed = server.runtime.schedule_list_schedules(None);
    assert!(
        refreshed.iter().any(|item| {
            item.realm == "test"
                && item.area == "jobs"
                && item.resource == "restart"
                && item.operation == "run"
        }),
        "schedule should remain present in admin after post-restart fire"
    );
}

define_transport_tests!(
    TcpScheduleConnector,
    WsScheduleConnector;
    should_create_cron_schedule_tcp / should_create_cron_schedule_ws => should_create_cron_schedule,
    should_create_schedule_batch_tcp / should_create_schedule_batch_ws => should_create_schedule_batch,
    should_cancel_schedule_tcp / should_cancel_schedule_ws => should_cancel_schedule,
    should_reject_invalid_cron_tcp / should_reject_invalid_cron_ws => should_reject_invalid_cron,
    should_reject_cancel_nonexistent_tcp / should_reject_cancel_nonexistent_ws => should_allow_cancel_nonexistent_idempotent,
    should_preserve_schedule_payload_tcp / should_preserve_schedule_payload_ws => should_preserve_schedule_payload,
    should_handle_multiple_schedule_creation_tcp / should_handle_multiple_schedule_creation_ws => should_handle_multiple_schedule_creation,
    should_support_various_cron_formats_tcp / should_support_various_cron_formats_ws => should_support_various_cron_formats,
    should_handle_sequential_create_and_cancel_operations_tcp / should_handle_sequential_create_and_cancel_operations_ws => should_handle_sequential_create_and_cancel_operations,
    should_retain_other_schedule_subscription_after_unsubscribe_tcp / should_retain_other_schedule_subscription_after_unsubscribe_ws => should_retain_other_schedule_subscription_after_unsubscribe,
);
