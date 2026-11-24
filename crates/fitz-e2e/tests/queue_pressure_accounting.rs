#[path = "../benches/stress_support/queue_pressure/ledger.rs"]
mod ledger;
use ledger::Ledger;

#[test]
fn should_reconcile_delivery_before_enqueue_response() {
    // Arrange
    let mut ledger = Ledger::default();
    let sequence = ledger.dispatch();
    // Act
    ledger.acknowledged(sequence, 42).unwrap();
    ledger.accepted(sequence, 42).unwrap();
    // Assert
    assert!(ledger.verify_drained().is_ok());
}

#[test]
fn should_reject_changed_message_identity() {
    // Arrange
    let mut ledger = Ledger::default();
    let sequence = ledger.dispatch();
    ledger.accepted(sequence, 42).unwrap();
    // Act
    let result = ledger.acknowledged(sequence, 43);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_duplicate_acknowledgement() {
    // Arrange
    let mut ledger = Ledger::default();
    let sequence = ledger.dispatch();
    ledger.accepted(sequence, 42).unwrap();
    ledger.acknowledged(sequence, 42).unwrap();
    // Act
    let result = ledger.acknowledged(sequence, 42);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_delivery_of_rejected_work() {
    // Arrange
    let mut ledger = Ledger::default();
    let sequence = ledger.dispatch();
    ledger.acknowledged(sequence, 42).unwrap();
    // Act
    let result = ledger.rejected(sequence);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_fail_drain_for_missing_or_indeterminate_work() {
    // Arrange
    let mut ledger = Ledger::default();
    let sequence = ledger.dispatch();
    // Act
    let indeterminate = ledger.verify_drained();
    ledger.accepted(sequence, 42).unwrap();
    let missing = ledger.verify_drained();
    // Assert
    assert!(indeterminate.is_err());
    assert!(missing.is_err());
}

#[test]
fn should_reject_reused_enqueue_id_across_messages() {
    // Arrange
    let mut ledger = Ledger::default();
    let first = ledger.dispatch();
    let second = ledger.dispatch();
    ledger.accepted(first, 42).unwrap();
    // Act
    let result = ledger.accepted(second, 42);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_delivery_without_a_sent_request() {
    // Arrange
    let mut ledger = Ledger::default();
    // Act
    let result = ledger.acknowledged(0, 42);
    // Assert
    assert!(result.is_err());
}

#[test]
fn should_count_known_backlog_despite_out_of_order_replies() {
    // Arrange
    let mut ledger = Ledger::default();
    let first = ledger.dispatch();
    let second = ledger.dispatch();
    ledger.accepted(first, 42).unwrap();
    // Act
    ledger.acknowledged(second, 43).unwrap();
    // Assert
    assert_eq!(ledger.counts.accepted_unacknowledged, 1);
}
