//! Shared RPC E2E imports and assertions.

pub(crate) use crate::fixtures::transport::*;
pub(crate) use fitz::testkit::TestServer;
pub(crate) use serial_test::serial;
pub(crate) use std::time::Duration;

pub(crate) fn assert_worker_disconnect_error_frame(
    frame: &[u8],
    expected_correlation_id: uuid::Uuid,
) {
    let response = parse_rpc_response_delivery(frame).expect("parse rpc response delivery");
    assert_eq!(response.correlation_id, expected_correlation_id);
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);

    let (code, message) =
        fitz::protocol::rpc_codec::decode_error_body(&response.body).expect("parse rpc error body");
    assert_eq!(code, fitz::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND);
    assert_eq!(message, "Worker disconnected or unregistered");
}

pub(crate) fn assert_rpc_timeout_error_frame(frame: &[u8], expected_correlation_id: uuid::Uuid) {
    let response = parse_rpc_response_delivery(frame).expect("parse rpc response delivery");
    assert_eq!(response.correlation_id, expected_correlation_id);
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);

    let (code, message) =
        fitz::protocol::rpc_codec::decode_error_body(&response.body).expect("parse rpc error body");
    assert_eq!(code, fitz::protocol::error_codes::rpc::ERR_RPC_TIMEOUT);
    assert_eq!(message, "Worker did not reply within timeout period");
}

pub(crate) fn assert_rpc_correlation_not_found_error_frame(
    frame: &[u8],
    expected_correlation_id: uuid::Uuid,
) {
    let response = parse_rpc_response_delivery(frame).expect("parse rpc response delivery");
    assert_eq!(response.correlation_id, expected_correlation_id);
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);

    let (code, message) =
        fitz::protocol::rpc_codec::decode_error_body(&response.body).expect("parse rpc error body");
    assert_eq!(
        code,
        fitz::protocol::error_codes::rpc::ERR_CORRELATION_NOT_FOUND
    );
    assert_eq!(message, "Correlation ID not found (orphaned response)");
}

pub(crate) fn assert_rpc_invalid_sequence_error_frame(
    frame: &[u8],
    expected_correlation_id: uuid::Uuid,
) {
    let response = parse_rpc_response_delivery(frame).expect("parse rpc response delivery");
    assert_eq!(response.correlation_id, expected_correlation_id);
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);

    let (code, message) =
        fitz::protocol::rpc_codec::decode_error_body(&response.body).expect("parse rpc error body");
    assert_eq!(
        code,
        fitz::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE
    );
    assert_eq!(
        message,
        "RPC response sequence must start at seq=0 and advance contiguously"
    );
}

pub(crate) fn assert_rpc_route_not_registered_error_response(frame: &[u8]) {
    let response =
        parse_rpc_response_delivery(frame).expect("parse route-not-registered terminal response");
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);

    let (code, message) =
        fitz::protocol::rpc_codec::decode_error_body(&response.body).expect("parse rpc error body");
    assert_eq!(
        code,
        fitz::protocol::error_codes::rpc::ERR_ROUTE_NOT_REGISTERED
    );
    assert_eq!(message, "No workers registered for route");
}

pub(crate) fn assert_rpc_terminal_validation_error_response(
    frame: &[u8],
    expected_code: u16,
    expected_message: &str,
) {
    let response = parse_rpc_response_delivery(frame).expect("parse RPC terminal validation error");
    assert_eq!(response.msg_type, 303);
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);
    let (code, message) =
        fitz::protocol::rpc_codec::decode_error_body(&response.body).expect("decode RPC error");
    assert_eq!(code, expected_code);
    assert_eq!(message, expected_message);
}

pub(crate) fn assert_rpc_control_validation_error_response(
    frame: &[u8],
    expected_message_type: u16,
    expected_code: u16,
    expected_message: &str,
) {
    let mut parser = TlvFrameParser::new(frame);
    let (message_type, payload) = parser
        .next_field()
        .expect("RPC control validation response");
    assert_eq!(message_type, expected_message_type);
    assert!(parser.next_field().is_none());
    let (code, message) =
        fitz::protocol::rpc_codec::decode_error_body(&payload).expect("decode RPC control error");
    assert_eq!(code, expected_code);
    assert_eq!(message, expected_message);
}

pub(crate) fn assert_forwarded_rpc_response_frame(
    frame: &[u8],
    expected_correlation_id: uuid::Uuid,
    expected_body: &[u8],
) {
    let response = parse_rpc_response_delivery(frame).expect("parse rpc response delivery");
    assert_eq!(response.correlation_id, expected_correlation_id);
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);
    assert_eq!(response.body.as_slice(), expected_body);
}
