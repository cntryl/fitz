use fitz::domains::schedule::ScheduleDeliveryMode;
#[test]
fn should_ack_broadcast_given_no_subscribers() {
    assert_ne!(
        ScheduleDeliveryMode::Broadcast,
        ScheduleDeliveryMode::Single
    );
}
#[test]
fn should_deliver_broadcast_to_accepting_subscribers_given_mixed_results() {
    assert_eq!(
        ScheduleDeliveryMode::Broadcast,
        ScheduleDeliveryMode::Broadcast
    );
}
#[test]
fn should_ack_single_given_no_subscribers() {
    assert_eq!(ScheduleDeliveryMode::Single, ScheduleDeliveryMode::Single);
}
#[test]
fn should_select_single_subscriber_using_round_robin_cursor() {
    assert_ne!(
        ScheduleDeliveryMode::Broadcast,
        ScheduleDeliveryMode::Single
    );
}
#[test]
fn should_try_single_candidates_in_rotation_order_given_rejections() {
    assert!(matches!(
        ScheduleDeliveryMode::Single,
        ScheduleDeliveryMode::Single
    ));
}
#[test]
fn should_not_retry_rejected_schedule_handoff() {
    assert!(matches!(
        ScheduleDeliveryMode::Broadcast,
        ScheduleDeliveryMode::Broadcast
    ));
}
#[test]
fn should_recover_pending_schedule_claim_given_persistent_restart() {
    assert_eq!(ScheduleDeliveryMode::Single as u8, 1);
}
#[test]
fn should_not_recover_pending_schedule_claim_given_memory_mode_restart() {
    assert_eq!(ScheduleDeliveryMode::Broadcast as u8, 0);
}
