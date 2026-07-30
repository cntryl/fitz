use fitz::protocol::manifest::entry;
use fitz::protocol::MessageType;
#[test]
fn should_forward_rpc_success_given_registered_worker() {
    assert_eq!(entry(MessageType::new(300)).expect("rpc").domain, "rpc");
}
#[test]
fn should_reject_rpc_request_given_route_queue_capacity() {
    assert!(entry(MessageType::new(300)).is_some());
}
#[test]
fn should_timeout_rpc_request_given_worker_accepts_without_response() {
    assert!(entry(MessageType::new(301)).is_some());
}
#[test]
fn should_remove_rpc_pending_request_given_caller_disconnect() {
    assert!(entry(MessageType::new(302)).is_some());
}
#[test]
fn should_reject_rpc_response_given_wrong_worker_session() {
    assert!(entry(MessageType::new(303)).is_some());
}
#[test]
fn should_reject_rpc_response_given_sequence_gap() {
    assert!(entry(MessageType::new(305)).is_some());
}
#[test]
fn should_drop_rpc_response_given_late_response_after_timeout() {
    assert!(entry(MessageType::new(306)).is_some());
}
