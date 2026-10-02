use super::*;
use crate::runtime::routing::{session_inbox_address, RouteFamily};
use crate::runtime::SessionCloseRequest;

fn close_envelope(reason: &'static str) -> Envelope {
    Envelope::new(
        session_inbox_address(RouteFamily::new(1), 1),
        SessionCloseRequest { reason },
    )
}

#[test]
fn should_signal_session_close_even_when_outbound_data_is_full() {
    // Arrange
    let (tx, mut data) = mpsc::channel(1);
    tx.try_send(Bytes::from_static(b"pending")).unwrap();
    let (sink, signal) = SessionOutboundSink::with_close_signal(tx);

    // Act
    let result = sink.deliver_high_priority(close_envelope("RPC cancellation grace expired"));

    // Assert
    assert_eq!(result, Ok(()));
    assert_eq!(*signal.borrow(), Some("RPC cancellation grace expired"));
    assert_eq!(data.try_recv().unwrap(), Bytes::from_static(b"pending"));
    assert!(data.try_recv().is_err());
}

#[test]
fn should_coalesce_duplicate_close_requests_and_preserve_first_reason() {
    // Arrange
    let (tx, _data) = mpsc::channel(1);
    let (sink, signal) = SessionOutboundSink::with_close_signal(tx);
    sink.deliver(close_envelope("first cause")).unwrap();

    // Act
    for _ in 0..1024 {
        sink.deliver(close_envelope("later cause")).unwrap();
    }

    // Assert
    assert_eq!(*signal.borrow(), Some("first cause"));
}

#[test]
fn should_reject_close_request_after_transport_receiver_exits() {
    // Arrange
    let (tx, _data) = mpsc::channel(1);
    let (sink, signal) = SessionOutboundSink::with_close_signal(tx);
    drop(signal);

    // Act
    let result = sink.deliver(close_envelope("closed"));

    // Assert
    assert_eq!(result, Err(DeliveryError::ActorStopped));
}

#[test]
fn should_reject_control_close_for_sink_without_transport_lifecycle() {
    // Arrange
    let (tx, _data) = mpsc::channel(1);
    let sink = SessionOutboundSink::new(tx);

    // Act
    let result = sink.deliver(close_envelope("closed"));

    // Assert
    assert_eq!(result, Err(DeliveryError::UnsupportedPayload));
}
