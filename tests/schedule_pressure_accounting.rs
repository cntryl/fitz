#[path = "../benches/stress_support/schedule_pressure/ledger.rs"]
mod ledger;
use ledger::Ledger;

#[test]
fn should_count_broadcast_only_after_both_receivers() {
    // Arrange
    let mut ledger = Ledger::new(1, true);
    // Act
    let first = ledger.receive(0, 0).unwrap();
    let second = ledger.receive(0, 1).unwrap();
    // Assert
    assert!(!first);
    assert!(second);
    assert_eq!(ledger.completed, 1);
    assert!(ledger.verify().is_ok());
}

#[test]
fn should_reject_single_delivery_to_two_receivers() {
    // Arrange
    let mut ledger = Ledger::new(1, false);
    // Act
    ledger.receive(0, 0).unwrap();
    let duplicate = ledger.receive(0, 1);
    // Assert
    assert!(duplicate.is_err());
    assert_eq!(ledger.completed, 1);
}

#[test]
fn should_reject_missing_broadcast_recipient() {
    // Arrange
    let mut ledger = Ledger::new(1, true);
    // Act
    ledger.receive(0, 0).unwrap();
    // Assert
    assert!(ledger.verify().is_err());
    assert_eq!(ledger.completed, 0);
}

#[test]
fn should_reject_unknown_occurrence_and_receiver() {
    // Arrange
    let mut ledger = Ledger::new(1, false);
    // Act
    let occurrence = ledger.receive(1, 0);
    let receiver = ledger.receive(0, 2);
    // Assert
    assert!(occurrence.is_err());
    assert!(receiver.is_err());
    assert_eq!(ledger.completed, 0);
}

#[test]
fn should_reject_duplicate_broadcast_recipient() {
    // Arrange
    let mut ledger = Ledger::new(1, true);
    // Act
    ledger.receive(0, 1).unwrap();
    let duplicate = ledger.receive(0, 1);
    // Assert
    assert!(duplicate.is_err());
    assert_eq!(ledger.received, [0, 1]);
}
