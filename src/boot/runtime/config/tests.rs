use super::*;

mod base_auth_and_network;
use base_auth_and_network::*;
mod cloud_storage;
mod storage_local_and_stream;

#[test]
fn should_validate_transport_independently_of_invalid_drain() {
    // Arrange
    let mut config = auth_ready_config();
    config.drain_grace_seconds = 0;
    config.bind_addr = "127.0.0.1".to_string();

    // Act
    let result = TransportConfig::from_boot_config(&config).validate(false);

    // Assert
    assert!(result.is_ok(), "transport validation failed: {result:?}");
}

#[test]
fn should_validate_storage_independently_of_invalid_transport() {
    // Arrange
    let mut config = auth_ready_config();
    config.metrics_bind_addr.clear();

    // Act
    let result = StorageConfig::from_boot_config(&config).validate();

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_validate_drain_independently_of_invalid_storage() {
    // Arrange
    let mut config = auth_ready_config();
    config.storage_mode = StorageMode::Invalid {
        reason: "invalid storage".to_string(),
    };

    // Act
    let result = DrainConfig::from_boot_config(&config).validate();

    // Assert
    assert!(result.is_ok());
}
