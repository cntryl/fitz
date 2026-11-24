use super::*;

#[test]
fn should_default_cancellation_grace_to_five_seconds() {
    // Arrange
    let setting = None;

    // Act
    let grace = cancellation_grace_period_from_setting(setting);

    // Assert
    assert_eq!(grace, Some(Duration::from_secs(5)));
}

#[test]
fn should_accept_cancellation_grace_at_configured_boundaries() {
    // Arrange
    let settings = ["0", "5000", "86400000"];

    // Act
    let actual = settings.map(|value| cancellation_grace_period_from_setting(Some(value)));

    // Assert
    assert_eq!(
        actual,
        [
            Some(Duration::ZERO),
            Some(Duration::from_secs(5)),
            Some(Duration::from_hours(24))
        ]
    );
}

#[test]
fn should_reject_invalid_cancellation_grace_settings() {
    // Arrange
    let settings = ["", "-1", "86400001", "4294967296", "5000ms"];

    // Act
    let actual = settings.map(|value| cancellation_grace_period_from_setting(Some(value)));

    // Assert
    assert_eq!(actual, [None; 5]);
}
