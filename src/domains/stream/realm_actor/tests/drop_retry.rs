use super::*;

fn take_next_timer(context: &mut Context<RealmActor>) -> crate::runtime::context::TimerId {
    let deadline = context
        .timer_manager()
        .next_deadline()
        .expect("timer scheduled");
    let fired = context.timer_manager().fired_timers_at(deadline);
    assert_eq!(fired.len(), 1, "one timer should fire");
    fired[0]
}

#[test]
fn should_rearm_realm_watermark_retry_after_drop_cutoff_read_failure() {
    // Arrange
    let (mut actor, mut context) = make_test_actor();
    StreamStore::fail_next_realm_watermark_persist_for_tests();
    actor.handle_batch_committed(0, 3, 3, &mut context);
    let timer = take_next_timer(&mut context);
    StreamStore::fail_next_drop_cutoff_read_for_tests();

    // Act
    actor.on_timer(timer, &mut context);

    // Assert
    assert_eq!(actor.realm_watermark, None);
    assert!(
        context.timer_manager().next_deadline().is_some(),
        "a transient cutoff read failure must retain a runnable retry"
    );
}

#[test]
fn should_publish_realm_notification_after_transient_drop_cutoff_read_failure() {
    // Arrange
    let (mut actor, mut context, mailbox) = make_test_actor_with_stream_mailbox();
    actor.handle_batch_committed(0, 3, 3, &mut context);
    let notification_timer = take_next_timer(&mut context);
    StreamStore::fail_next_drop_cutoff_read_for_tests();
    actor.on_timer(notification_timer, &mut context);

    // Act
    for _ in 0..3 {
        if context.timer_manager().next_deadline().is_none() {
            break;
        }
        let timer = take_next_timer(&mut context);
        actor.on_timer(timer, &mut context);
    }

    // Assert
    let published = mailbox
        .receiver()
        .try_recv()
        .expect("notification should recover without another commit");
    let event = published
        .payload::<crate::runtime::DomainPublishEvent>()
        .expect("watermark event");
    assert_eq!(event.family_id, actor.family_id);
    assert!(actor.is_idle());
}

#[test]
fn should_preserve_a_persisted_realm_watermark_after_initial_cutoff_read_failure() {
    // Arrange
    let (old_actor, mut context) = make_test_actor_with_watermark(Some(5));
    StreamStore::fail_next_drop_cutoff_read_for_tests();
    let mut actor = RealmActor::new(
        old_actor.family_id,
        old_actor.realm.clone(),
        old_actor.store.clone(),
        old_actor.durable_metrics.clone(),
    );

    // Act
    actor.handle_batch_committed(6, 6, 6, &mut context);

    // Assert
    assert_eq!(actor.realm_watermark, Some(6));
    assert_eq!(
        actor
            .store
            .get_persisted_realm_watermark(actor.family_id.as_u64(), &actor.realm)
            .unwrap(),
        Some(6)
    );
    assert!(actor.committed_ranges.is_empty());
}
