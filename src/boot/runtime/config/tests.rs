use super::*;

mod base_auth_and_network;
use base_auth_and_network::*;
mod cloud_storage;
mod storage_local_and_stream;

#[test]
#[serial]
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
#[serial]
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
#[serial]
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

#[test]
#[serial]
fn should_reject_configured_http_ws_origin_even_when_env_origins_are_valid() {
    with_auth_env(
        &[("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com")],
        || {
            // Arrange
            let origin = crate::api::origin::parse_exact_origin("http://app.example.com")
                .expect("parse origin");
            let config = auth_ready_config()
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(true)
                .with_ws_allowed_origins(vec![origin]);

            // Act
            let result = config.validate();

            // Assert
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_WS_ALLOWED_ORIGINS entries must use https"));
        },
    );
}

#[test]
#[serial]
fn should_reject_configured_empty_ws_origins_even_when_env_origins_are_valid() {
    with_auth_env(
        &[("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com")],
        || {
            // Arrange
            let config = auth_ready_config()
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(true)
                .with_ws_allowed_origins(Vec::new());

            // Act
            let result = config.validate();

            // Assert
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_WS_ALLOWED_ORIGINS is required"));
        },
    );
}
