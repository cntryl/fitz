use super::*;

#[test]
fn should_classify_run_now_enqueue_failure_when_actor_is_stopped() {
    // Arrange
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let sink = ScheduleDomain::new(
        crate::domains::schedule::ScheduleStore::new(store),
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    sink.stop();

    // Act
    let result = sink.run_now(
        RouteFamily::new(1),
        "schedule://acme/jobs/nightly/run".to_string(),
        Duration::from_secs(1),
    );

    // Assert
    assert!(matches!(result, Err(ScheduleRunNowError::Enqueue(_))));
}

#[test]
fn should_classify_disconnected_run_now_reply_channel() {
    // Arrange
    let timeout = Duration::from_secs(1);

    // Act
    let result = ScheduleRunNowError::from_reply_wait(
        crossbeam_channel::RecvTimeoutError::Disconnected,
        timeout,
    );

    // Assert
    assert!(matches!(result, ScheduleRunNowError::ReplyDisconnected));
}
