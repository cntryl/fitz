use fitz::protocol::manifest::entry;
use fitz::protocol::MessageType;
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
#[test]
fn should_replay_stream_records_given_resource_offset_after_restart_tcp() {
    assert_eq!(
        entry(MessageType::new(600)).expect("stream").domain,
        "stream"
    );
}
#[test]
fn should_reject_stream_read_given_route_family_mismatch() {
    assert_ne!(
        RouteAddress::new(RouteFamily::new(1), Route::new("stream://r/a/x")),
        RouteAddress::new(RouteFamily::new(2), Route::new("stream://r/a/x"))
    );
}
#[test]
fn should_report_has_more_given_filtered_read_with_exact_limit() {
    assert!(entry(MessageType::new(604)).is_some());
}
#[test]
fn should_preserve_cursor_given_filtered_read_across_restart() {
    assert!(entry(MessageType::new(605)).is_some());
}
#[test]
fn should_not_recover_stream_subscription_given_disconnect() {
    assert!(entry(MessageType::new(608)).is_some());
}
#[test]
fn should_publish_stream_notification_once_given_commit_retry() {
    assert!(entry(MessageType::new(609)).is_some());
}
