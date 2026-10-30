use super::*;
use crate::runtime::routing::{session_inbox_address, RouteFamily};
use crate::runtime::SessionCloseRequest;
use std::sync::Arc;

#[test]
fn should_share_outbound_byte_limit_until_the_transport_releases_the_frame() {
    // Arrange
    let shared = Arc::new(tokio::sync::Semaphore::new(16));
    let (first_tx, mut first_rx) = mpsc::channel(8);
    let (second_tx, mut second_rx) = mpsc::channel(8);
    let first = SessionOutboundSink::new(first_tx).with_buffer_budget(
        crate::api::buffer_budget::BufferBudget::new(shared.clone(), 16),
    );
    let second = SessionOutboundSink::new(second_tx)
        .with_buffer_budget(crate::api::buffer_budget::BufferBudget::new(shared, 16));
    first.send_message(1, 504, None, &[0]).unwrap();

    // Act
    let refused = second.send_message(2, 504, None, &[0]);
    let retained = first_rx.try_recv().unwrap();
    let still_refused = second.send_message(2, 504, None, &[0]);
    drop(retained);
    let recovered = second.send_message(2, 504, None, &[0]);

    // Assert
    assert!(refused.is_err());
    assert!(still_refused.is_err());
    assert!(recovered.is_ok());
    assert_eq!(second_rx.try_recv().unwrap().len(), 16);
}

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
fn should_preserve_first_close_reason_under_duplicate_requests() {
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

#[test]
fn should_report_frame_in_progress_as_frame_loop_busy_time() {
    // Arrange
    let (tx, _data) = mpsc::channel(1);
    let (sink, _signal) = SessionOutboundSink::with_close_signal(tx);
    let probe = Instant::now() + Duration::from_secs(60);
    let idle = MailboxSink::frame_loop_busy_time(&sink, probe);

    // Act
    let _busy = sink.begin_frame();
    let busy = MailboxSink::frame_loop_busy_time(&sink, probe);

    // Assert
    assert_eq!(idle, Some(Duration::ZERO));
    assert!(busy.is_some_and(|busy| busy >= Duration::from_secs(59)));
}

#[test]
fn should_close_session_when_a_required_reply_cannot_be_queued() {
    // Arrange
    let (tx, _data) = mpsc::channel(1);
    tx.try_send(Bytes::from_static(b"pending")).unwrap();
    let (sink, signal) = SessionOutboundSink::with_close_signal(tx);

    // Act
    let result = sink.send_message(1, 200, std::num::NonZeroU64::new(7), &[0]);

    // Assert
    assert!(matches!(result, Err(DeliveryError::MailboxFull { .. })));
    assert!(signal.borrow().is_some());
}

#[test]
fn should_close_session_when_a_required_reply_exceeds_the_message_budget() {
    // Arrange
    let (tx, _data) = mpsc::channel(8);
    let (sink, signal) = SessionOutboundSink::with_close_signal(tx);
    let sink = sink.with_max_message_bytes(16);
    // Act
    let result = sink.send_message(1, 200, std::num::NonZeroU64::new(7), &[0; 2]);
    // Assert
    assert!(matches!(result, Err(DeliveryError::InvalidPayload { .. })));
    assert!(signal.borrow().is_some());
}

#[test]
fn should_keep_session_open_when_a_best_effort_notification_is_dropped() {
    // Arrange
    let (tx, _data) = mpsc::channel(1);
    tx.try_send(Bytes::from_static(b"pending")).unwrap();
    let (sink, signal) = SessionOutboundSink::with_close_signal(tx);

    // Act
    let result = sink.send_message(1, 504, None, &[0]);

    // Assert
    assert!(matches!(result, Err(DeliveryError::MailboxFull { .. })));
    assert!(signal.borrow().is_none());
}
