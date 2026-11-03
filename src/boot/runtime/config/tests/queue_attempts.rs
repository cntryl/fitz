use super::*;

fn assert_invalid_limit(value: &str) {
    with_auth_env(&[("FITZ_QUEUE_MAX_ATTEMPTS", value)], || {
        // Arrange
        let config = auth_ready_config();

        // Act
        let result = StorageConfig::from_boot_config(&config).validate();

        // Assert
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("FITZ_QUEUE_MAX_ATTEMPTS"));
    });
}

#[test]
#[serial]
fn should_reject_zero_queue_max_attempts() {
    assert_invalid_limit("0");
}

#[test]
#[serial]
fn should_reject_negative_queue_max_attempts() {
    assert_invalid_limit("-1");
}

#[test]
#[serial]
fn should_reject_overflowing_queue_max_attempts() {
    assert_invalid_limit("4294967296");
}

#[test]
#[serial]
fn should_reject_malformed_queue_max_attempts() {
    assert_invalid_limit("three");
}

#[test]
#[serial]
fn should_reject_empty_queue_max_attempts() {
    assert_invalid_limit("");
}

#[test]
#[serial]
fn should_default_queue_max_attempts_to_unlimited() {
    with_auth_env(&[], || {
        // Arrange

        // Act
        let config = BootConfig::new();

        // Assert
        assert_eq!(config.queue_max_attempts, None);
    });
}

#[test]
#[serial]
fn should_accept_one_queue_attempt() {
    with_auth_env(&[("FITZ_QUEUE_MAX_ATTEMPTS", "1")], || {
        // Arrange

        // Act
        let config = BootConfig::new();

        // Assert
        assert_eq!(
            config.queue_max_attempts.map(std::num::NonZeroU32::get),
            Some(1)
        );
        assert!(StorageConfig::from_boot_config(&config).validate().is_ok());
    });
}

#[test]
#[serial]
fn should_accept_maximum_queue_attempts() {
    with_auth_env(&[("FITZ_QUEUE_MAX_ATTEMPTS", "4294967295")], || {
        // Arrange

        // Act
        let config = BootConfig::new();

        // Assert
        assert_eq!(
            config.queue_max_attempts.map(std::num::NonZeroU32::get),
            Some(u32::MAX)
        );
        assert!(StorageConfig::from_boot_config(&config).validate().is_ok());
    });
}
