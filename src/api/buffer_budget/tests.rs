use super::*;

#[test]
fn should_retain_byte_reservation_until_last_frame_slice_is_dropped() {
    // Arrange
    let shared = Arc::new(Semaphore::new(8));
    let budget = BufferBudget::new(shared.clone(), 8);
    let frame = budget.wrap(Bytes::from_static(b"12345678")).unwrap();
    let slice = frame.slice(1..2);

    // Act
    drop(frame);
    let while_retained = budget.wrap(Bytes::from_static(b"x"));
    drop(slice);
    let after_release = budget.wrap(Bytes::from_static(b"12345678"));

    // Assert
    assert!(while_retained.is_err());
    assert!(after_release.is_ok());
    assert_eq!(shared.available_permits(), 0);
}

#[test]
fn should_share_byte_admission_across_connections() {
    // Arrange
    let shared = Arc::new(Semaphore::new(8));
    let first = BufferBudget::new(shared.clone(), 8);
    let second = BufferBudget::new(shared, 8);
    let frame = first.wrap(Bytes::from_static(b"12345678")).unwrap();

    // Act
    let refused = second.wrap(Bytes::from_static(b"x"));
    drop(frame);
    let recovered = second.wrap(Bytes::from_static(b"12345678"));

    // Assert
    assert!(refused.is_err());
    assert!(recovered.is_ok());
}

#[test]
fn should_release_session_reservation_when_shared_admission_refuses_frame() {
    // Arrange
    let shared = Arc::new(Semaphore::new(8));
    let first = BufferBudget::new(shared.clone(), 8);
    let second = BufferBudget::new(shared, 8);
    let frame = first.wrap(Bytes::from_static(b"12345678")).unwrap();
    assert!(second.wrap(Bytes::from_static(b"12345678")).is_err());

    // Act
    drop(frame);
    let recovered = second.wrap(Bytes::from_static(b"12345678"));

    // Assert
    assert!(recovered.is_ok());
}
