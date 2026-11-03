use super::*;

#[test]
#[serial]
fn should_convert_queue_message_ttl_days_to_seconds() {
    with_auth_env(&[("FITZ_QUEUE_MESSAGE_TTL", "2")], || {
        // Arrange
        // Act
        let config = BootConfig::new();
        // Assert
        assert_eq!(config.queue_retention.message_seconds, Some(172_800));
    });
}

#[test]
#[serial]
fn should_convert_queue_dlq_ttl_days_to_seconds() {
    with_auth_env(&[("FITZ_QUEUE_DLQ_TTL", "3")], || {
        // Arrange
        // Act
        let config = BootConfig::new();
        // Assert
        assert_eq!(config.queue_retention.dlq_seconds, Some(259_200));
    });
}

#[test]
#[serial]
fn should_default_queue_retention_to_no_expiry() {
    with_auth_env(&[], || {
        // Arrange
        // Act
        let config = BootConfig::new();
        // Assert
        assert_eq!(config.queue_retention.message_seconds, None);
        assert_eq!(config.queue_retention.dlq_seconds, None);
    });
}

fn rejects_retention(key: &str, value: &str) {
    with_auth_env(&[(key, value)], || {
        // Arrange
        let config = auth_ready_config();
        // Act
        let result = StorageConfig::from_boot_config(&config).validate();
        // Assert
        assert!(result.unwrap_err().to_string().contains(key));
    });
}

#[test]
#[serial]
fn should_reject_zero_queue_message_ttl() {
    rejects_retention("FITZ_QUEUE_MESSAGE_TTL", "0");
}

#[test]
#[serial]
fn should_reject_zero_queue_dlq_ttl() {
    rejects_retention("FITZ_QUEUE_DLQ_TTL", "0");
}

#[test]
#[serial]
fn should_reject_empty_queue_message_ttl() {
    rejects_retention("FITZ_QUEUE_MESSAGE_TTL", "");
}

#[test]
#[serial]
fn should_reject_negative_queue_dlq_ttl() {
    rejects_retention("FITZ_QUEUE_DLQ_TTL", "-1");
}

#[test]
#[serial]
fn should_reject_overflowing_queue_message_ttl() {
    rejects_retention("FITZ_QUEUE_MESSAGE_TTL", "4294967296");
}
