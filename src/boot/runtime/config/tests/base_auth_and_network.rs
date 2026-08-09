use super::*;
pub(super) use crate::prelude;
pub(super) use serial_test::serial;

pub(super) fn with_env_var<T>(key: &str, value: &str, test: impl FnOnce() -> T) -> T {
    let previous = std::env::var(key).ok();
    unsafe {
        std::env::set_var(key, value);
    }

    let result = test();

    match previous {
        Some(previous) => unsafe {
            std::env::set_var(key, previous);
        },
        None => unsafe {
            std::env::remove_var(key);
        },
    }

    result
}

pub(super) fn with_auth_env<T>(values: &[(&str, &str)], test: impl FnOnce() -> T) -> T {
    let keys = [
        "FITZ_AUTH_REQUIRED",
        "FITZ_JWT_AUDIENCE",
        "FITZ_JWT_AUDIENCES",
        "FITZ_JWT_JWKS_MAP",
        "FITZ_JWT_HMAC_SECRET",
        "FITZ_JWT_ALLOW_INSECURE_HTTP",
        "FITZ_ROUTE_FAMILIES",
        "FITZ_ROUTE_FAMILY_MAP",
        "FITZ_ROUTE_FAMILY_CLAIM",
        "FITZ_AUTH_CUSTOM_CLAIM",
        "FITZ_AUTH_ROLE_CLAIM",
        "FITZ_AUTH_ALLOW_JWT_ROUTE_FAMILY",
        "FITZ_ASSUME_EXTERNAL_TLS",
        "FITZ_ASSUME_LOCAL_LOOPBACK_EDGE",
        "FITZ_TCP_ENABLED",
        "FITZ_WS_ALLOWED_ORIGINS",
        "FITZ_ADMIN_AUTH_MODE",
        "FITZ_ROOT_PASSWORD",
        "FITZ_ADMIN_COOKIE_SECURE",
        "FITZ_ADMIN_PUBLIC_ORIGIN",
        ENV_QUEUE_WRITE_POLICY,
        ENV_QUEUE_LOSS_WINDOW_MS,
        ENV_KV_IDLE_TRANSACTION_TTL_SECS,
        ENV_DRAIN_GRACE_SECONDS,
        ENV_DRAIN_CLOSE_REASON,
    ];
    let previous = keys
        .iter()
        .map(|key| (*key, std::env::var(key).ok()))
        .collect::<Vec<_>>();

    unsafe {
        for key in keys {
            std::env::remove_var(key);
        }
        for (key, value) in values {
            std::env::set_var(key, value);
        }
    }

    let result = test();

    unsafe {
        for (key, value) in previous {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }

    result
}

pub(super) fn with_storage_env<T>(values: &[(&str, &str)], test: impl FnOnce() -> T) -> T {
    let keys = [
        "FITZ_STORAGE_MODE",
        "FITZ_STORAGE_PROVIDER",
        "FITZ_STORAGE_BUCKET",
        "FITZ_STORAGE_CONTAINER",
        "FITZ_STORAGE_PREFIX",
        "FITZ_STORAGE_CACHE_PATH",
        "FITZ_STORAGE_PATH",
        "FITZ_STORAGE_ENDPOINT",
        "FITZ_STORAGE_REGION",
        "FITZ_STORAGE_FORCE_PATH_STYLE",
        "FITZ_STORAGE_NAMESPACE",
        "FITZ_STORAGE_ACCOUNT",
        "FITZ_STORAGE_CLOUD_DURABILITY",
        "FITZ_STORAGE_MEMTABLE_BYTES",
        ENV_QUEUE_WRITE_POLICY,
        ENV_QUEUE_LOSS_WINDOW_MS,
        ENV_KV_IDLE_TRANSACTION_TTL_SECS,
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AZURE_STORAGE_ACCOUNT_NAME",
        "AZURE_STORAGE_ACCOUNT_KEY",
        "AZURE_STORAGE_CONNECTION_STRING",
        "AZURE_STORAGE_SAS_TOKEN",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "GOOGLE_CLOUD_PROJECT",
        "GCS_HMAC_ACCESS_ID",
        "GCS_HMAC_SECRET",
        "FITZ_ADMIN_AUTH_MODE",
        "FITZ_ROOT_PASSWORD",
        ENV_DRAIN_GRACE_SECONDS,
        ENV_DRAIN_CLOSE_REASON,
    ];
    let previous = keys
        .iter()
        .map(|key| (*key, std::env::var(key).ok()))
        .collect::<Vec<_>>();

    unsafe {
        for key in keys {
            std::env::remove_var(key);
        }
        for (key, value) in values {
            std::env::set_var(key, value);
        }
    }

    let result = test();

    unsafe {
        for (key, value) in previous {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }

    result
}

pub(super) fn with_clean_config_env<T>(test: impl FnOnce() -> T) -> T {
    with_storage_env(&[], || with_auth_env(&[], test))
}

pub(super) fn cloud_config(config: &BootConfig) -> &CloudStorageConfig {
    match &config.storage_mode {
        StorageMode::CloudBacked(cloud) => cloud.as_ref(),
        other => panic!("expected cloud storage mode, got {other:?}"),
    }
}

#[test]
#[serial]
pub(super) fn should_create_default_boot_config() {
    // Arrange

    // Act
    let config = BootConfig::default();

    // Assert
    assert_eq!(config.tcp_port, prelude::DEFAULT_TCP_PORT);
    assert_eq!(config.http_port, prelude::DEFAULT_HTTP_PORT);
    assert!(config.tcp_enabled);
    assert_eq!(config.bind_addr, "0.0.0.0");
    assert_eq!(config.max_connections, 10_000);
    assert_eq!(config.cloud_durability, CloudDurabilityMode::Background);
    assert_eq!(config.storage_memtable, StorageMemtableConfig::Auto);
    assert_eq!(config.queue_write_policy, QueueWritePolicy::Fast);
    assert_eq!(
        config.queue_write_policy_source,
        QueueWritePolicySource::Defaulted
    );
    assert!(config.queue_write_policy_defaulted_fast());
    assert_eq!(config.queue_loss_window_ms, DEFAULT_QUEUE_LOSS_WINDOW_MS);
    assert!(config.queue_write_options().is_best_effort());
    assert_eq!(config.drain_grace_seconds, DEFAULT_DRAIN_GRACE_SECONDS);
    assert_eq!(config.drain_close_reason, DEFAULT_DRAIN_CLOSE_REASON);
    assert!(!config.assume_external_tls());
    assert!(!config.assume_local_loopback_edge());
    assert_eq!(config.route_families, vec![1]);
    assert_eq!(
        config.auth_claims_config.identity_claim,
        crate::auth::DEFAULT_ROUTE_FAMILY_CLAIM
    );
    assert!(config.route_family_resolver.mappings.is_empty());
    assert_eq!(
        config.stream_storage_layout,
        StreamStorageLayout::PromotionFrontier
    );
}

#[test]
#[serial]
fn should_distinguish_explicit_queue_fast_policy_from_defaulted_fast() {
    with_storage_env(&[(ENV_QUEUE_WRITE_POLICY, "fast")], || {
        // Arrange

        // Act
        let config = BootConfig::default();

        // Assert
        assert_eq!(config.queue_write_policy, QueueWritePolicy::Fast);
        assert_eq!(
            config.queue_write_policy_source,
            QueueWritePolicySource::Explicit
        );
        assert!(!config.queue_write_policy_defaulted_fast());
    });
}

#[test]
#[serial]
pub(super) fn should_use_implicit_local_ws_allowed_origins_by_default() {
    with_auth_env(&[("FITZ_AUTH_REQUIRED", "false")], || {
        // Arrange

        // Act
        let config = BootConfig::default();

        // Assert
        let origins = config
            .ws_allowed_origins
            .iter()
            .map(crate::api::origin::ExactOrigin::as_str)
            .collect::<Vec<_>>();
        assert_eq!(
            origins,
            vec![
                "http://localhost:3000",
                "http://127.0.0.1:3000",
                "http://localhost:4090",
                "http://127.0.0.1:4090",
            ]
        );
    });
}

#[test]
pub(super) fn should_customize_boot_config() {
    // Arrange

    // Act
    let config = BootConfig::new()
        .with_tcp_port(5091)
        .with_tcp_enabled(false)
        .with_http_port(5090)
        .with_bind_addr("127.0.0.1".to_string());

    // Assert
    assert_eq!(config.tcp_port, 5091);
    assert!(!config.tcp_enabled);
    assert_eq!(config.http_port, 5090);
    assert_eq!(config.bind_addr, "127.0.0.1");
}

#[test]
#[serial]
pub(super) fn should_read_drain_config_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            (ENV_DRAIN_GRACE_SECONDS, "45"),
            (ENV_DRAIN_CLOSE_REASON, "planned deploy"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default();

            // Assert
            assert_eq!(config.drain_grace_seconds, 45);
            assert_eq!(config.drain_close_reason, "planned deploy");
            assert!(config.validate().is_ok());
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_invalid_drain_grace_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            (ENV_DRAIN_GRACE_SECONDS, "nope"),
        ],
        || {
            // Arrange
            let config = BootConfig::default();

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains(ENV_DRAIN_GRACE_SECONDS));
        },
    );
}

pub(super) fn auth_ready_config() -> BootConfig {
    BootConfig::new()
        .with_auth_config(crate::auth::AuthConfig::hmac("test-secret-key", "fitz"))
        .with_route_family_resolver(crate::auth::RouteFamilyResolverConfig::from_mappings(
            "tid",
            [("acme", 1)],
        ))
}

#[test]
#[serial]
pub(super) fn should_reject_public_bind_without_external_tls_ack_when_auth_required() {
    with_auth_env(&[], || {
        // Arrange
        let config = auth_ready_config()
            .with_bind_addr("0.0.0.0".to_string())
            .with_assume_external_tls(false);

        // Act
        let result = config.validate();

        // Assert
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("FITZ_ASSUME_EXTERNAL_TLS=true is required"));
        assert!(!config.assume_external_tls());
    });
}

#[test]
#[serial]
pub(super) fn should_allow_public_bind_with_external_tls_ack_when_auth_required() {
    with_auth_env(
        &[("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com")],
        || {
            // Arrange
            let config = auth_ready_config()
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(true);

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_ok());
        },
    );
}

#[test]
#[serial]
pub(super) fn should_allow_public_bind_with_local_loopback_edge_ack_when_auth_required() {
    with_auth_env(
        &[
            ("FITZ_ASSUME_LOCAL_LOOPBACK_EDGE", "true"),
            ("FITZ_WS_ALLOWED_ORIGINS", "http://127.0.0.1:4090"),
        ],
        || {
            // Arrange
            let config = auth_ready_config().with_bind_addr("0.0.0.0".to_string());

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_ok());
            assert!(!config.assume_external_tls());
            assert!(config.assume_local_loopback_edge());
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_non_loopback_origin_with_local_loopback_edge_ack() {
    with_auth_env(
        &[
            ("FITZ_ASSUME_LOCAL_LOOPBACK_EDGE", "true"),
            ("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com"),
        ],
        || {
            // Arrange
            let config = auth_ready_config().with_bind_addr("0.0.0.0".to_string());

            // Act
            let result = config.validate();

            // Assert
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_ASSUME_LOCAL_LOOPBACK_EDGE requires loopback"));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_external_tls_and_local_loopback_edge_acks_together() {
    with_auth_env(
        &[
            ("FITZ_ASSUME_EXTERNAL_TLS", "true"),
            ("FITZ_ASSUME_LOCAL_LOOPBACK_EDGE", "true"),
            ("FITZ_WS_ALLOWED_ORIGINS", "http://127.0.0.1:4090"),
        ],
        || {
            // Arrange
            let config = auth_ready_config().with_bind_addr("0.0.0.0".to_string());

            // Act
            let result = config.validate();

            // Assert
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_ASSUME_EXTERNAL_TLS and FITZ_ASSUME_LOCAL_LOOPBACK_EDGE are mutually exclusive"));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_public_admin_origin_with_local_loopback_edge_ack() {
    with_auth_env(
        &[
            ("FITZ_ADMIN_AUTH_MODE", "open"),
            ("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com"),
            ("FITZ_ASSUME_LOCAL_LOOPBACK_EDGE", "true"),
            ("FITZ_WS_ALLOWED_ORIGINS", "http://127.0.0.1:4090"),
        ],
        || {
            // Arrange
            let config = BootConfig::new()
                .with_auth_config(crate::auth::AuthConfig::Disabled)
                .with_bind_addr("0.0.0.0".to_string());

            // Act
            let result = config.validate();

            // Assert
            assert!(result.unwrap_err().to_string().contains(
                "FITZ_ASSUME_LOCAL_LOOPBACK_EDGE requires a loopback FITZ_ADMIN_PUBLIC_ORIGIN"
            ));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_public_auth_bind_with_empty_ws_allowed_origins() {
    with_auth_env(&[], || {
        // Arrange
        let config = auth_ready_config()
            .with_bind_addr("0.0.0.0".to_string())
            .with_assume_external_tls(true)
            .with_ws_allowed_origins(Vec::new());

        // Act
        let result = config.validate();

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("FITZ_WS_ALLOWED_ORIGINS is required"));
    });
}

#[test]
#[serial]
pub(super) fn should_allow_public_auth_bind_with_ws_allowed_origins() {
    with_auth_env(
        &[("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com")],
        || {
            // Arrange
            let config = auth_ready_config()
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(true);

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_ok());
        },
    );
}

#[test]
#[serial]
pub(super) fn should_allow_loopback_bind_without_external_tls_when_auth_required() {
    // Arrange
    let config = auth_ready_config()
        .with_bind_addr("127.0.0.1".to_string())
        .with_assume_external_tls(false);

    // Act
    let result = config.validate();

    // Assert
    assert!(result.is_ok());
}

#[test]
#[serial]
pub(super) fn should_reject_public_bind_without_external_tls_ack_when_protected_admin_configured() {
    with_auth_env(
        &[
            ("FITZ_ROOT_PASSWORD", "pwd123"),
            ("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com"),
        ],
        || {
            // Arrange
            let config = BootConfig::new()
                .with_auth_config(crate::auth::AuthConfig::Disabled)
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(false);

            // Act
            let result = config.validate();

            // Assert
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_ASSUME_EXTERNAL_TLS=true is required"));
            assert!(!config.assume_external_tls());
        },
    );
}

#[test]
#[serial]
pub(super) fn should_allow_loopback_bind_without_external_tls_when_protected_admin_configured() {
    with_auth_env(&[("FITZ_ROOT_PASSWORD", "pwd123")], || {
        // Arrange
        let config = BootConfig::new()
            .with_auth_config(crate::auth::AuthConfig::Disabled)
            .with_bind_addr("127.0.0.1".to_string())
            .with_assume_external_tls(false);

        // Act
        let result = config.validate();

        // Assert
        assert!(result.is_ok());
    });
}

#[test]
#[serial]
pub(super) fn should_allow_public_bind_with_external_tls_ack_when_protected_admin_configured() {
    with_auth_env(
        &[
            ("FITZ_ROOT_PASSWORD", "pwd123"),
            ("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com"),
        ],
        || {
            // Arrange
            let config = BootConfig::new()
                .with_auth_config(crate::auth::AuthConfig::Disabled)
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(true);

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_ok());
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_public_protected_admin_without_public_origin() {
    with_auth_env(&[("FITZ_ROOT_PASSWORD", "pwd123")], || {
        // Arrange
        let config = BootConfig::new()
            .with_auth_config(crate::auth::AuthConfig::Disabled)
            .with_bind_addr("0.0.0.0".to_string())
            .with_assume_external_tls(true);

        // Act
        let result = config.validate();

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("FITZ_ADMIN_PUBLIC_ORIGIN is required"));
    });
}

#[test]
#[serial]
pub(super) fn should_reject_insecure_public_admin_origin() {
    with_auth_env(
        &[
            ("FITZ_ROOT_PASSWORD", "pwd123"),
            ("FITZ_ADMIN_PUBLIC_ORIGIN", "http://admin.example.com"),
        ],
        || {
            // Arrange
            let config = BootConfig::new()
                .with_auth_config(crate::auth::AuthConfig::Disabled)
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(true);

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_ADMIN_PUBLIC_ORIGIN must use https"));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_insecure_admin_cookie_on_public_bind() {
    with_auth_env(
        &[
            ("FITZ_ROOT_PASSWORD", "pwd123"),
            ("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com"),
            ("FITZ_ADMIN_COOKIE_SECURE", "false"),
        ],
        || {
            // Arrange
            let config = BootConfig::new()
                .with_auth_config(crate::auth::AuthConfig::Disabled)
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(true);

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_ADMIN_COOKIE_SECURE=false is only allowed on loopback"));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_allow_public_bind_without_external_tls_when_admin_open_mode_configured() {
    with_auth_env(
        &[
            ("FITZ_ADMIN_AUTH_MODE", "open"),
            ("FITZ_ROOT_PASSWORD", "pwd123"),
        ],
        || {
            // Arrange
            let config = BootConfig::new()
                .with_auth_config(crate::auth::AuthConfig::Disabled)
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(false);

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_ok());
        },
    );
}

#[test]
#[serial]
pub(super) fn should_read_listener_ports_from_environment() {
    // Arrange

    // Act
    let config = with_clean_config_env(|| {
        with_env_var("FITZ_HTTP_PORT", "6080", || {
            with_env_var("FITZ_TCP_PORT", "6081", BootConfig::default)
        })
    });

    // Assert
    assert_eq!(config.http_port, 6080);
    assert_eq!(config.tcp_port, 6081);
}

#[test]
#[serial]
pub(super) fn should_read_tcp_enabled_from_environment() {
    // Arrange

    // Act
    let config = with_env_var("FITZ_TCP_ENABLED", "false", BootConfig::default);

    // Assert
    assert!(!config.tcp_enabled);
}

#[test]
#[serial]
pub(super) fn should_reject_invalid_tcp_enabled_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_TCP_ENABLED", "maybe"),
        ],
        || {
            // Arrange
            let config = BootConfig::default();

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_TCP_ENABLED must be true or false"));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_read_ws_allowed_origins_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            (
                "FITZ_WS_ALLOWED_ORIGINS",
                "https://app.example.com,http://localhost:3000",
            ),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default();

            // Assert
            assert_eq!(config.ws_allowed_origins.len(), 2);
            assert_eq!(
                config.ws_allowed_origins[0].as_str(),
                "https://app.example.com"
            );
            assert_eq!(
                config.ws_allowed_origins[1].as_str(),
                "http://localhost:3000"
            );
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_invalid_ws_allowed_origins_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com/path"),
        ],
        || {
            // Arrange
            let config = BootConfig::default();

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_WS_ALLOWED_ORIGINS"));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_preserve_invalid_ws_allowed_origins_error_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com/path"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default();

            // Assert
            assert!(config.ws_allowed_origins_error.is_some());
            assert!(config
                .ws_allowed_origins_error
                .as_deref()
                .unwrap()
                .contains("FITZ_WS_ALLOWED_ORIGINS"));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_ws_allowed_origin_with_trailing_slash_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_WS_ALLOWED_ORIGINS", "https://app.example.com/"),
        ],
        || {
            // Arrange
            let config = BootConfig::default();

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_WS_ALLOWED_ORIGINS"));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_admin_public_origin_with_trailing_slash_from_environment() {
    with_auth_env(
        &[
            ("FITZ_ROOT_PASSWORD", "pwd123"),
            ("FITZ_ADMIN_PUBLIC_ORIGIN", "https://admin.example.com/"),
        ],
        || {
            // Arrange
            let config = BootConfig::new()
                .with_auth_config(crate::auth::AuthConfig::Disabled)
                .with_bind_addr("0.0.0.0".to_string())
                .with_assume_external_tls(true);

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_ADMIN_PUBLIC_ORIGIN"));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_allow_localhost_http_ws_allowed_origin() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_WS_ALLOWED_ORIGINS", "http://localhost:3000"),
        ],
        || {
            // Arrange
            let config = BootConfig::default();

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_ok());
        },
    );
}

#[test]
#[serial]
pub(super) fn should_reject_public_http_ws_allowed_origin() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_WS_ALLOWED_ORIGINS", "http://app.example.com"),
        ],
        || {
            // Arrange
            let config = BootConfig::default();

            // Act
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_WS_ALLOWED_ORIGINS entries must use https"));
        },
    );
}

#[test]
#[serial]
pub(super) fn should_read_storage_memtable_bytes_from_environment() {
    with_storage_env(&[("FITZ_STORAGE_MEMTABLE_BYTES", "8388608")], || {
        // Arrange

        // Act
        let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);

        // Assert
        assert_eq!(
            config.storage_memtable,
            StorageMemtableConfig::Bytes(8 * 1024 * 1024)
        );
        assert_eq!(config.storage_memtable_bytes(), Some(8 * 1024 * 1024));
        assert!(config.validate().is_ok());
    });
}
