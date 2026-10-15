use super::*;

#[test]
fn should_preserve_schedule_definition_after_invalid_batch_domain_invariant() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/existing/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"original"),
        )
        .expect("create");
    let original_due = actor.schedules[route].next_fire_ms;
    let entry = |route: &str, cron: &str| ScheduleCreateEntry {
        route: route.to_string(),
        cron: cron.to_string(),
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::from_static(b"replacement"),
    };

    // Act
    let rejected = actor.create_schedules(vec![
        entry(route, "0 2 * * *"),
        entry("schedule://acme/jobs/new/run", "* * * * *"),
        entry("schedule://acme/jobs/invalid/run", "invalid"),
    ]);
    let persisted = actor
        .store
        .load_all(1, crate::domains::WritePolicy::Buffered)
        .expect("read durable definitions");

    // Assert
    assert!(rejected.is_err());
    assert_eq!(actor.schedules.len(), 1);
    assert_eq!(actor.schedules[route].cron, "* * * * *");
    assert_eq!(actor.schedules[route].payload.as_ref(), b"original");
    assert_eq!(actor.schedules[route].next_fire_ms, original_due);
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].route, route);
    assert_eq!(persisted[0].payload.as_ref(), b"original");
    assert_eq!(persisted[0].next_fire_ms, original_due);
}
