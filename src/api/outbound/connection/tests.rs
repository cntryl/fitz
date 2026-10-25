use super::*;
use crate::domains::lease::LeaseResponse;
use crate::protocol::frame::ChannelId;
use crate::protocol::message::MessageType;
use crate::protocol::wire2::header::{encode_message, HeaderFlags};
use bytes::Bytes;

#[test]
fn should_require_connect_before_a_domain_request() {
    // Arrange
    let state = ConnectionState::default();
    // Act
    let result = state.validate_request(&request(400, 1, &[]));
    // Assert
    assert_eq!(result, Err("CONNECT must be the first message"));
}

#[test]
fn should_require_nonzero_correlation_for_a_response_bearing_request() {
    // Arrange
    let state = ConnectionState::default();
    state.validate_request(&request(1, 0, &[])).unwrap();
    // Act
    let result = state.validate_request(&request(400, 0, &[]));
    // Assert
    assert_eq!(result, Err("request correlation must be non-zero"));
}

#[test]
fn should_allow_zero_correlation_for_unacknowledged_notice_publish() {
    // Arrange
    let state = ConnectionState::default();
    state.validate_request(&request(1, 0, &[])).unwrap();
    // Act
    let result = state.validate_request(&request(500, 0, &[]));
    // Assert
    assert_eq!(result, Ok(()));
}

#[test]
fn should_require_correlation_when_notice_publish_requests_an_acknowledgement() {
    // Arrange
    let state = ConnectionState::default();
    state.validate_request(&request(1, 0, &[])).unwrap();
    let message = request(500, 0, &[]).with_flags(HeaderFlags::ack_requested());
    // Act
    let result = state.validate_request(&message);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_allow_zero_correlation_for_uuid_addressed_rpc_worker_acknowledgements() {
    // Arrange
    let state = ConnectionState::default();
    state.validate_request(&request(1, 0, &[])).unwrap();
    // Act
    let result = state.validate_request(&request(304, 0, &[3]));
    // Assert
    assert_eq!(result, Ok(()));
}

fn request(kind: u16, id: u64, payload: &[u8]) -> ChannelMessage {
    ChannelMessage::new(
        ChannelId::Lease,
        MessageType::new(kind),
        Bytes::copy_from_slice(payload),
    )
    .with_correlation(NonZeroU64::new(id))
}

fn fill_leases(state: &ConnectionState, count: usize) {
    for id in 1..=count {
        assert!(state.admit(&request(400, u64::try_from(id).unwrap(), &[])));
    }
}

fn lease_answer(state: &ConnectionState, id: u64, response: &LeaseResponse) {
    state.sent(&encode_message(
        400,
        HeaderFlags::none(),
        NonZeroU64::new(id),
        &crate::protocol::lease_codec::encode_domain_response(response),
    ));
}

#[test]
fn should_hold_a_queued_acquire_slot_until_its_grant() {
    // Arrange
    let state = ConnectionState::default();
    fill_leases(&state, MAX_IN_FLIGHT);
    // Act
    lease_answer(&state, 1, &LeaseResponse::Queued { fencing_token: 7 });
    let while_queued = state.admit(&request(400, 65, &[]));
    lease_answer(&state, 1, &LeaseResponse::Acquired { fencing_token: 7 });
    let after_grant = state.admit(&request(400, 65, &[]));
    // Assert
    assert!(!while_queued);
    assert!(after_grant);
}

#[test]
fn should_not_release_capacity_for_an_unmatched_response() {
    // Arrange
    let state = ConnectionState::default();
    fill_leases(&state, MAX_IN_FLIGHT);
    // Act
    lease_answer(&state, 99, &LeaseResponse::Acquired { fencing_token: 7 });
    let admitted = state.admit(&request(400, 65, &[]));
    // Assert
    assert!(!admitted);
}

#[test]
fn should_release_a_slot_when_a_deferred_acquire_times_out() {
    // Arrange
    let state = ConnectionState::default();
    fill_leases(&state, MAX_IN_FLIGHT);
    // Act
    lease_answer(&state, 1, &LeaseResponse::Timeout);
    let admitted = state.admit(&request(400, 65, &[]));
    // Assert
    assert!(admitted);
}

#[test]
fn should_hold_an_rpc_slot_until_the_terminal_chunk() {
    // Arrange
    let state = ConnectionState::default();
    fill_leases(&state, MAX_IN_FLIGHT - 1);
    let id = uuid::Uuid::new_v4();
    assert!(state.admit(&request(302, 64, id.as_bytes())));
    let mut response = crate::domains::rpc::protocol::RpcResponse::single(id, Bytes::new());
    response.stream_end = false;
    // Act
    state.sent(&encode_message(
        303,
        HeaderFlags::none(),
        None,
        &crate::protocol::rpc_codec::encode_response_message_payload(&response),
    ));
    let while_streaming = state.admit(&request(400, 65, &[]));
    response.stream_end = true;
    state.sent(&encode_message(
        303,
        HeaderFlags::none(),
        None,
        &crate::protocol::rpc_codec::encode_response_message_payload(&response),
    ));
    let after_terminal = state.admit(&request(400, 65, &[]));
    // Assert
    assert!(!while_streaming);
    assert!(after_terminal);
}

#[test]
fn should_admit_worker_responses_at_the_request_limit() {
    // Arrange
    let state = ConnectionState::default();
    fill_leases(&state, MAX_IN_FLIGHT);
    let message = request(303, 1, &[]);
    // Act
    let admitted = state.admit(&message);
    // Assert
    assert!(admitted);
}

#[test]
fn should_admit_worker_cleanup_at_the_request_limit() {
    // Arrange
    let state = ConnectionState::default();
    fill_leases(&state, MAX_IN_FLIGHT);
    let message = request(304, 1, &[3]);
    // Act
    let admitted = state.admit(&message);
    // Assert
    assert!(admitted);
}

#[test]
fn should_admit_unacknowledged_publish_at_the_request_limit() {
    // Arrange
    let state = ConnectionState::default();
    fill_leases(&state, MAX_IN_FLIGHT);
    let message = request(500, 1, &[]);
    // Act
    let admitted = state.admit(&message);
    // Assert
    assert!(admitted);
}

#[test]
fn should_refuse_acknowledged_publish_at_the_request_limit() {
    // Arrange
    let state = ConnectionState::default();
    fill_leases(&state, MAX_IN_FLIGHT);
    let message = request(500, 1, &[]).with_flags(HeaderFlags::ack_requested());
    // Act
    let admitted = state.admit(&message);
    // Assert
    assert!(!admitted);
}

#[test]
fn should_release_only_the_cancel_slot_for_a_cancellation_result() {
    // Arrange
    let state = ConnectionState::default();
    fill_leases(&state, MAX_IN_FLIGHT - 2);
    let id = uuid::Uuid::new_v4();
    assert!(state.admit(&request(302, 63, id.as_bytes())));
    let mut cancel = vec![1];
    cancel.extend_from_slice(id.as_bytes());
    cancel.push(1);
    assert!(state.admit(&request(304, 64, &cancel)));
    // Act
    state.sent(&crate::protocol::rpc_codec::encode_cancel_result_frame(
        &id,
        crate::protocol::rpc_codec::RpcCancellationResult::Forwarded,
    ));
    let first = state.admit(&request(400, 65, &[]));
    let second = state.admit(&request(400, 66, &[]));
    // Assert
    assert!(first);
    assert!(
        !second,
        "the canceled call retains capacity until terminal completion"
    );
}

#[test]
fn should_release_the_rpc_slot_when_queued_cancellation_removes_the_call() {
    // Arrange
    let state = ConnectionState::default();
    fill_leases(&state, MAX_IN_FLIGHT - 2);
    let id = uuid::Uuid::new_v4();
    assert!(state.admit(&request(302, 63, id.as_bytes())));
    let mut cancel = vec![1];
    cancel.extend_from_slice(id.as_bytes());
    cancel.push(1);
    assert!(state.admit(&request(304, 64, &cancel)));

    // Act
    state.sent(&crate::protocol::rpc_codec::encode_cancel_result_frame(
        &id,
        crate::protocol::rpc_codec::RpcCancellationResult::QueuedRemoved,
    ));
    let first = state.admit(&request(400, 65, &[]));
    let second = state.admit(&request(400, 66, &[]));

    // Assert
    assert!(first);
    assert!(
        second,
        "both cancellation and original RPC slots are retired"
    );
}

#[test]
fn should_keep_connection_admission_independent() {
    // Arrange
    let full = ConnectionState::default();
    let other = ConnectionState::default();
    fill_leases(&full, MAX_IN_FLIGHT);
    // Act
    let admitted = other.admit(&request(400, 1, &[]));
    // Assert
    assert!(admitted);
}

#[tokio::test]
async fn should_keep_admission_capacity_when_a_refusal_reuses_a_pending_correlation() {
    // Arrange
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let sink = crate::api::outbound::SessionOutboundSink::new(tx);
    fill_leases(&sink.connection, MAX_IN_FLIGHT);
    let body = crate::protocol::error_codes::encode_error_body(5, "connection busy");
    // Act
    sink.send_unadmitted_response(1, 400, NonZeroU64::new(1), &body)
        .unwrap();
    let response = rx.recv().await.unwrap();
    let admitted = sink.connection.admit(&request(400, 65, &[]));
    // Assert
    assert_eq!(decode_message(&response, u32::MAX).unwrap().1, body);
    assert!(!admitted);
}
