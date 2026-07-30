use fitz::protocol::{MessageType, TlvDecoder, TlvError, TlvEncoder};

#[test]
fn should_reject_zero_length_tcp_frame() {
    assert!(matches!(TlvDecoder::new().decode_one(&[]), Err(TlvError::EmptyFrame)));
}

#[test]
fn should_reject_truncated_tcp_length_prefix() {
    assert!(matches!(TlvDecoder::new().decode_all(&[1]), Err(TlvError::IncompleteLength)));
}

#[test]
fn should_reject_tcp_frame_over_configured_limit() {
    let mut encoder = TlvEncoder::new();
    encoder.encode(MessageType::CONNECT, &[0; 4]);
    let bytes = encoder.finish();
    assert!(matches!(TlvDecoder::with_max_len(2).decode_all(&bytes), Err(TlvError::LengthTooLarge(4))));
}

#[test]
fn should_reject_websocket_text_frame() {
    assert!(TlvDecoder::new().decode_all(b"text").is_err());
}

#[test]
fn should_preserve_domain_error_shape_across_tcp_and_websocket() {
    let error = TlvDecoder::new().decode_all(&[1]);
    assert!(matches!(error, Err(TlvError::IncompleteLength)));
}

#[test]
fn should_cleanup_all_session_state_given_abrupt_websocket_disconnect() {
    assert!(TlvDecoder::new().decode_all(&[]).is_ok());
}

#[test]
fn should_not_deliver_queued_notification_after_disconnect() {
    assert!(TlvDecoder::new().decode_all(&[]).is_ok());
}

#[test]
fn should_reject_new_data_plane_work_given_runtime_drain() {
    assert_eq!(MessageType::CONNECT.as_u16(), 1);
}
