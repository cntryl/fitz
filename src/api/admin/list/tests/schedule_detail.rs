use super::*;

fn schedule(
    operation: &str,
    cron: &str,
    next_run: &str,
    executions_total: u64,
    enabled: bool,
) -> ScheduleInfo {
    ScheduleInfo {
        route_family: 1,
        realm: "acme".to_string(),
        area: "billing".to_string(),
        resource: "invoices".to_string(),
        operation: operation.to_string(),
        cron: cron.to_string(),
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
        next_run: next_run.to_string(),
        last_run: None,
        executions_total,
        enabled,
    }
}

#[test]
fn should_never_expose_cron_on_a_single_schedule_resource_detail() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime.admin_read_model().replace_schedules(vec![schedule(
        "send",
        "*/5 * * * *",
        "2026-03-31T02:00:00Z",
        4,
        true,
    )]);
    let path = ResourcePath {
        realm: "acme",
        area: "billing",
        resource: "invoices",
    };

    // Act
    let detail = schedule_detail(&runtime, &path, Some(1));
    let json = serde_json::to_value(&detail).expect("serialize detail");

    // Assert
    assert!(
        json.get("cron").is_none(),
        "resource detail must not carry a schedule cron"
    );
    assert_eq!(detail.next_run.as_deref(), Some("2026-03-31T02:00:00Z"));
    assert_eq!(detail.executions_total, 4);
}

#[test]
fn should_aggregate_schedule_detail_given_multiple_schedules() {
    // Arrange
    let path = ResourcePath {
        realm: "acme",
        area: "billing",
        resource: "invoices",
    };
    let schedules = vec![
        ScheduleInfo {
            route_family: 1,
            realm: "acme".to_string(),
            area: "billing".to_string(),
            resource: "invoices".to_string(),
            operation: "send".to_string(),
            cron: "0 * * * *".to_string(),
            delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            next_run: "2026-03-31T02:00:00Z".to_string(),
            last_run: None,
            executions_total: 2,
            enabled: false,
        },
        ScheduleInfo {
            route_family: 1,
            realm: "acme".to_string(),
            area: "billing".to_string(),
            resource: "invoices".to_string(),
            operation: "retry".to_string(),
            cron: "*/5 * * * *".to_string(),
            delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            next_run: "2026-03-31T01:00:00Z".to_string(),
            last_run: None,
            executions_total: 3,
            enabled: true,
        },
    ];

    // Act
    let detail = ScheduleResourceDetail::aggregate(&path, &schedules);

    // Assert
    assert!(detail.enabled);
    assert_eq!(detail.next_run.as_deref(), Some("2026-03-31T01:00:00Z"));
    assert_eq!(detail.executions_total, 5);
}
