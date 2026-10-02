use super::*;

fn expression(index: usize) -> String {
    format!("{} {} * * *", index % 60, index / 60)
}

#[test]
fn should_bound_cron_cache_after_create_delete_churn() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/churn/run";

    // Act
    for index in 0..256 {
        actor
            .create_schedule(route.to_string(), expression(index), Bytes::new())
            .expect("create");
        actor.delete_schedule(route).expect("delete");
    }

    // Assert
    assert!(actor.schedules.is_empty());
    assert!(
        actor.cron_cache.len() <= 128,
        "retained {} expressions",
        actor.cron_cache.len()
    );
}

#[test]
fn should_bound_cron_cache_after_rejected_batch() {
    // Arrange
    let mut actor = make_actor();
    let mut entries: Vec<_> = (0..256)
        .map(|index| ScheduleCreateEntry {
            route: format!("schedule://acme/jobs/batch-{index}/run"),
            cron: expression(index),
            delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            payload: Bytes::new(),
        })
        .collect();
    entries.push(ScheduleCreateEntry {
        route: "schedule://acme/jobs/invalid/run".to_string(),
        cron: "invalid".to_string(),
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::new(),
    });

    // Act
    let result = actor.create_schedules(entries);

    // Assert
    assert!(result.is_err());
    assert!(actor.schedules.is_empty());
    assert!(
        actor.cron_cache.len() <= 128,
        "retained {} expressions",
        actor.cron_cache.len()
    );
}

#[test]
#[serial]
fn should_bound_cron_cache_after_failed_persistence_churn() {
    // Arrange
    let mut actor = make_actor();

    // Act
    for index in 0..256 {
        actor.store.fail_next_commit_for_tests();
        assert!(actor
            .create_schedule(
                "schedule://acme/jobs/failure/run".to_string(),
                expression(index),
                Bytes::new()
            )
            .is_err());
    }

    // Assert
    assert!(actor.schedules.is_empty());
    assert!(actor.cron_cache.len() <= 128);
}

#[test]
fn should_bound_preloaded_cron_cache_without_losing_live_definitions() {
    // Arrange
    let mut actor = make_actor();
    let entries = (0..256)
        .map(|index| ScheduleCreateEntry {
            route: format!("schedule://acme/jobs/live-{index}/run"),
            cron: expression(index),
            delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            payload: Bytes::new(),
        })
        .collect();
    actor
        .create_schedules(entries)
        .expect("persist definitions");
    let first_due = actor.schedules["schedule://acme/jobs/live-0/run"].next_fire_ms;

    // Act
    let recovered = ScheduleActor::try_new(actor.family, actor.store.clone(), actor.write_policy)
        .expect("recover");

    // Assert
    assert_eq!(recovered.schedules.len(), 256);
    assert!(recovered.cron_cache.len() <= 128);
    assert_eq!(
        recovered.schedules["schedule://acme/jobs/live-0/run"].next_fire_ms,
        first_due
    );
}
