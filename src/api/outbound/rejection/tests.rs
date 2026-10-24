use super::*;
use crate::protocol::error_codes::{decode_wire_error, protocol};
use crate::protocol::wire2::envelope::Effect;
use crate::protocol::wire2::header::{encode_message, HeaderFlags};
use std::num::NonZeroU64;

#[test]
fn should_answer_unqueued_request_as_busy_without_releasing_an_admitted_slot() {
    // Arrange
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let (sink, signal) = SessionOutboundSink::with_close_signal(tx);
    sink.send_message(1, 2, None, &[]).unwrap();
    rx.try_recv().unwrap();
    let request = crate::protocol::mux::ChannelMessage::new(
        crate::protocol::frame::ChannelId::Lease,
        crate::protocol::message::MessageType::new(400),
        bytes::Bytes::new(),
    )
    .with_correlation(NonZeroU64::new(7));
    for _ in 0..crate::api::outbound::MAX_IN_FLIGHT {
        assert!(sink.connection.admit(&request));
    }
    let frame = encode_message(400, HeaderFlags::none(), NonZeroU64::new(7), &[]);

    // Act
    sink.reject_unqueued_frame(1, &frame).unwrap();
    let response = rx.try_recv().unwrap();
    let (header, value) = decode_message(&response, u32::MAX).unwrap();
    let error = decode_wire_error(value).unwrap();

    // Assert
    assert_eq!(header.correlation, NonZeroU64::new(7));
    assert_eq!(error.code, protocol::ERR_BUSY);
    assert_eq!(error.effect, Effect::NotApplied);
    assert!(!sink.connection.admit(&request));
    assert!(signal.borrow().is_none());
}

#[test]
fn should_drop_unacknowledged_publish_when_incoming_buffer_admission_fails() {
    // Arrange
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let (sink, signal) = SessionOutboundSink::with_close_signal(tx);
    sink.send_message(1, 2, None, &[]).unwrap();
    rx.try_recv().unwrap();
    let frame = encode_message(500, HeaderFlags::none(), None, &[]);
    // Act
    let result = sink.reject_unqueued_frame(1, &frame);
    // Assert
    assert!(result.is_ok());
    assert!(rx.try_recv().is_err());
    assert!(signal.borrow().is_none());
}

#[test]
fn should_preserve_unsupported_reserved_flags_even_when_incoming_buffer_is_full() {
    // Arrange
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let sink = SessionOutboundSink::new(tx);
    sink.send_message(1, 2, None, &[]).unwrap();
    rx.try_recv().unwrap();
    let mut frame = encode_message(500, HeaderFlags::none(), NonZeroU64::new(7), &[]);
    frame[2] = 128;
    // Act
    sink.reject_unqueued_frame(1, &frame).unwrap();
    let response = rx.try_recv().unwrap();
    let error = decode_wire_error(decode_message(&response, u32::MAX).unwrap().1).unwrap();
    // Assert
    assert_eq!(error.code, protocol::ERR_UNSUPPORTED);
}
