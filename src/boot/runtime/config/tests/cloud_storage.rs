use super::*;

#[test]
#[serial]
fn should_reject_missing_required_cloud_fields_given_real_provider() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "aws-s3"),
            ("FITZ_STORAGE_REGION", "us-east-1"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let result = config.validate();

            // Assert
            assert!(result.is_err());
        },
    );
}

#[test]
#[serial]
fn should_reject_old_cloud_mode_aliases_given_vendor_in_storage_mode() {
    with_storage_env(&[("FITZ_STORAGE_MODE", "s3")], || {
        // Arrange

        // Act
        let config = BootConfig::new();
        let result = config.validate();

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("FITZ_STORAGE_MODE=s3 is no longer supported"));
    });
}

#[test]
#[serial]
fn should_ignore_storage_path_given_cloud_cache_path() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "peas-s3"),
            ("FITZ_STORAGE_BUCKET", "   "),
            ("FITZ_STORAGE_PATH", "/legacy/path"),
            ("FITZ_STORAGE_CACHE_PATH", "/cache/path"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert_eq!(cloud.local_cache_path, "/cache/path");
            assert_eq!(
                cloud.provider_config.bucket_or_container(),
                DEFAULT_PEAS_BUCKET
            );
        },
    );
}

#[test]
#[serial]
fn should_map_cloud_durability_to_write_options() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "peas-s3"),
            ("FITZ_STORAGE_CLOUD_DURABILITY", "strict"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();

            // Assert
            assert!(config.server_write_options().is_cloud_strict());
            assert!(config.request_sync_write_options().is_cloud_strict());
        },
    );
}

#[test]
#[serial]
fn should_accept_background_cloud_durability() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "peas-s3"),
            ("FITZ_STORAGE_CLOUD_DURABILITY", "background"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_ok());
            assert_eq!(config.cloud_durability, CloudDurabilityMode::Background);
            assert!(!config.server_write_options().is_cloud_strict());
        },
    );
}

#[test]
#[serial]
fn should_reject_invalid_cloud_durability() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "peas-s3"),
            ("FITZ_STORAGE_CLOUD_DURABILITY", "stict"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("unsupported FITZ_STORAGE_CLOUD_DURABILITY='stict'"));
        },
    );
}

#[test]
#[serial]
fn should_map_queue_strict_write_policy_to_local_sync() {
    with_storage_env(&[("FITZ_QUEUE_WRITE_POLICY", "strict")], || {
        // Arrange

        // Act
        let config = BootConfig::new();
        let write_options = config.queue_write_options();

        // Assert
        assert_eq!(config.queue_write_policy, QueueWritePolicy::Strict);
        assert!(write_options.is_sync());
        assert!(!write_options.is_cloud_strict());
    });
}

#[test]
#[serial]
fn should_map_queue_strict_write_policy_to_cloud_strict() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "peas-s3"),
            ("FITZ_QUEUE_WRITE_POLICY", "strict"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();

            // Assert
            assert_eq!(config.queue_write_policy, QueueWritePolicy::Strict);
            assert!(config.queue_write_options().is_cloud_strict());
        },
    );
}

#[test]
#[serial]
fn should_reject_invalid_queue_write_policy() {
    with_storage_env(&[("FITZ_QUEUE_WRITE_POLICY", "speedy")], || {
        // Arrange

        // Act
        let config = BootConfig::new();
        let result = config.validate();

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unsupported FITZ_QUEUE_WRITE_POLICY='speedy'"));
    });
}

#[test]
#[serial]
fn should_reject_invalid_queue_loss_window() {
    with_storage_env(&[("FITZ_QUEUE_LOSS_WINDOW_MS", "0")], || {
        // Arrange

        // Act
        let config = BootConfig::new();
        let result = config.validate();

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("FITZ_QUEUE_LOSS_WINDOW_MS must be greater than 0"));
    });
}

#[test]
fn should_keep_non_cloud_sync_write_options_local() {
    // Arrange
    let config = BootConfig::with_local_storage("/data/fitz");

    // Act
    let write_options = config.request_sync_write_options();

    // Assert
    assert!(write_options.is_sync());
    assert!(!write_options.is_cloud_strict());
}
