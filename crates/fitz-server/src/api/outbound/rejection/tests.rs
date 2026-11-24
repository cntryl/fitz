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

#[test]
fn should_preserve_rpc_uuid_when_rejecting_an_extended_request() {
    // Arrange
    use crate::protocol::wire3::extensions::{encode_payload, ExtensionDirection, ExtensionField};
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let sink = SessionOutboundSink::new(tx);
    sink.send_message(1, 2, None, &[]).unwrap();
    rx.try_recv().unwrap();
    let id = uuid::Uuid::from_bytes([0xAB; 16]);
    let mut core = crate::protocol::payload_codec::PayloadEncoder::new();
    core.put_raw(id.as_bytes());
    core.put_string("rpc://acme/app/echo");
    core.put_bytes(b"body");
    core.put_u8(0);
    let core = core.finish();
    let payload = encode_payload(
        &core,
        302,
        ExtensionDirection::ClientToBroker,
        &[ExtensionField {
            field_id: 4096,
            value: b"future",
        }],
    )
    .unwrap();
    let frame = encode_message(
        302,
        HeaderFlags::extensions_present(),
        NonZeroU64::new(7),
        &payload.value,
    );

    // Act
    sink.reject_unqueued_frame(1, &frame).unwrap();
    let response = rx.try_recv().unwrap();
    let (header, value) = decode_message(&response, u32::MAX).unwrap();

    // Assert
    assert_eq!(header.message_type, 303);
    assert_eq!(header.correlation, None);
    assert_eq!(&value[..16], id.as_bytes());
    let mut response = crate::protocol::payload_codec::PayloadDecoder::new(value);
    response.get_u64().unwrap();
    response.get_u64().unwrap();
    response.get_u64().unwrap();
    assert_eq!(response.get_u8().unwrap(), 1);
    let body = response.get_bytes().unwrap();
    let error = decode_wire_error(&body).unwrap();
    assert_eq!(error.code, protocol::ERR_BUSY);
    assert_eq!(error.effect, Effect::NotApplied);
}

#[test]
fn should_reject_malformed_extensions_when_transport_buffer_is_full() {
    // Arrange
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let sink = SessionOutboundSink::new(tx);
    sink.send_message(1, 2, None, &[]).unwrap();
    rx.try_recv().unwrap();
    let malformed = [0, 0, 0, 0, 0, 8, 0, 0, 0, 4, b'x'];
    let frame = encode_message(
        100,
        HeaderFlags::extensions_present(),
        NonZeroU64::new(7),
        &malformed,
    );

    // Act
    let result = sink.reject_unqueued_frame(1, &frame);

    // Assert
    assert!(
        result.is_err(),
        "malformed extension must close, got {result:?}"
    );
    assert!(rx.try_recv().is_err());
}

#[test]
fn should_answer_extended_rpc_cancellation_as_busy_when_transport_buffer_is_full() {
    // Arrange
    use crate::protocol::wire3::extensions::{encode_payload, ExtensionDirection, ExtensionField};
    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let sink = SessionOutboundSink::new(tx);
    sink.send_message(1, 2, None, &[]).unwrap();
    rx.try_recv().unwrap();
    let mut core = vec![1];
    core.extend_from_slice(&[0xAB; 16]);
    core.push(1);
    let payload = encode_payload(
        &core,
        304,
        ExtensionDirection::ClientToBroker,
        &[ExtensionField {
            field_id: 4096,
            value: b"future",
        }],
    )
    .unwrap();
    let frame = encode_message(
        304,
        HeaderFlags::extensions_present(),
        NonZeroU64::new(7),
        &payload.value,
    );

    // Act
    let result = sink.reject_unqueued_frame(1, &frame);
    let response = rx.try_recv();

    // Assert
    assert!(
        result.is_ok(),
        "a response-bearing cancellation should receive BUSY: {result:?}"
    );
    let response = response.unwrap();
    let (header, body) = decode_message(&response, u32::MAX).unwrap();
    assert_eq!(header.message_type, 304);
    assert_eq!(header.correlation, NonZeroU64::new(7));
    assert!(header.flags.is_response_final());
    let error = decode_wire_error(body).unwrap();
    assert_eq!(error.code, protocol::ERR_BUSY);
    assert_eq!(error.effect, Effect::NotApplied);
}
