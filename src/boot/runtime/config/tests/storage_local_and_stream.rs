use super::*;

#[test]
#[serial]
fn should_reject_zero_storage_memtable_bytes() {
    with_storage_env(&[("FITZ_STORAGE_MEMTABLE_BYTES", "0")], || {
        // Arrange

        // Act
        let config = BootConfig::default();
        let result = config.validate();

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("FITZ_STORAGE_MEMTABLE_BYTES must be greater than 0"));
    });
}

#[test]
#[serial]
fn should_reject_invalid_storage_memtable_bytes() {
    with_storage_env(&[("FITZ_STORAGE_MEMTABLE_BYTES", "small")], || {
        // Arrange

        // Act
        let config = BootConfig::default();
        let result = config.validate();

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("FITZ_STORAGE_MEMTABLE_BYTES must be an unsigned integer byte count"));
    });
}

#[test]
fn should_customize_stream_storage_layout() {
    // Arrange

    // Act
    let config =
        BootConfig::new().with_stream_storage_layout(StreamStorageLayout::PromotionFrontier);

    // Assert
    assert_eq!(
        config.stream_storage_layout,
        StreamStorageLayout::PromotionFrontier
    );
}

#[test]
fn should_use_local_sync_for_schedule_durability() {
    // Arrange
    let config = BootConfig::new().with_storage_mode(StorageMode::LocalDisk {
        db_path: "./target/test-schedule-durability".to_string(),
    });

    // Act
    let write_options = config.schedule_write_options();

    // Assert
    assert!(write_options.is_sync());
    assert!(!write_options.is_cloud_strict());
}

#[test]
fn should_keep_memory_schedule_mode_best_effort() {
    // Arrange
    let config = BootConfig::new().with_storage_mode(StorageMode::Memory);

    // Act
    let write_options = config.schedule_write_options();

    // Assert
    assert!(!write_options.is_sync());
    assert!(!write_options.is_cloud_strict());
}

#[test]
fn should_normalize_legacy_stream_storage_layout_given_explicit_boot_config() {
    // Arrange

    // Act
    let config = BootConfig::new().with_stream_storage_layout(StreamStorageLayout::LegacyCovering);

    // Assert
    assert_eq!(
        config.stream_storage_layout,
        StreamStorageLayout::PromotionFrontier
    );
}

#[test]
#[serial]
fn should_accept_contiguous_route_family_allowlist() {
    with_clean_config_env(|| {
        // Arrange
        let config = BootConfig::new()
            .with_auth_config(crate::auth::AuthConfig::Disabled)
            .with_route_families(vec![1, 2, 3]);

        // Act
        let result = config.validate();

        // Assert
        assert!(result.is_ok());
    });
}

#[test]
#[serial]
fn should_reject_empty_route_family_allowlist() {
    with_clean_config_env(|| {
        // Arrange
        let config = BootConfig::new()
            .with_auth_config(crate::auth::AuthConfig::Disabled)
            .with_route_families(Vec::new());

        // Act
        let result = config.validate();

        // Assert
        assert!(result.is_err());
    });
}

#[test]
#[serial]
fn should_reject_gapped_route_family_allowlist() {
    with_clean_config_env(|| {
        // Arrange
        let config = BootConfig::new()
            .with_auth_config(crate::auth::AuthConfig::Disabled)
            .with_route_families(vec![1, 3]);

        // Act
        let result = config.validate();

        // Assert
        assert!(result.is_err());
    });
}

#[test]
#[serial]
fn should_read_route_family_identity_map_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_ROUTE_FAMILIES", "1,2,3"),
            ("FITZ_ROUTE_FAMILY_MAP", "abc=1,xyz=2,zzz=3"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);

            // Assert
            assert_eq!(config.route_family_resolver.mappings.get("abc"), Some(&1));
            assert_eq!(config.route_family_resolver.mappings.get("xyz"), Some(&2));
            assert_eq!(config.route_family_resolver.mappings.get("zzz"), Some(&3));
            assert!(config.validate().is_ok());
        },
    );
}

#[test]
#[serial]
fn should_reject_duplicate_route_family_identity_mapping() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_ROUTE_FAMILY_MAP", "abc=1,abc=2"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("duplicate identity"));
        },
    );
}

#[test]
#[serial]
fn should_reject_zero_route_family_identity_mapping() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_ROUTE_FAMILY_MAP", "abc=0"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("route family 0"));
        },
    );
}

#[test]
#[serial]
fn should_reject_invalid_route_family_identity_mapping_integer() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_ROUTE_FAMILY_MAP", "abc=two"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("must be an unsigned integer"));
        },
    );
}

#[test]
#[serial]
fn should_reject_unprovisioned_route_family_identity_mapping() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_ROUTE_FAMILIES", "1"),
            ("FITZ_ROUTE_FAMILY_MAP", "xyz=2"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("unprovisioned"));
        },
    );
}

#[test]
#[serial]
fn should_reject_removed_legacy_route_family_env() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_AUTH_ALLOW_JWT_ROUTE_FAMILY", "true"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_AUTH_ALLOW_JWT_ROUTE_FAMILY has been removed"));
        },
    );
}

#[test]
#[serial]
fn should_reject_removed_custom_claim_alias_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_AUTH_CUSTOM_CLAIM", "fitz"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_AUTH_CUSTOM_CLAIM=fitz is no longer supported"));
        },
    );
}

#[test]
#[serial]
fn should_read_org_claim_override_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_AUTH_ORG_CLAIM", "fitz://org_id"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);

            // Assert
            assert_eq!(
                config.auth_claims_config.org_claim_override.as_deref(),
                Some("fitz://org_id")
            );
            assert_eq!(
                config.route_family_resolver.org_claim_override.as_deref(),
                Some("fitz://org_id")
            );
        },
    );
}

#[test]
#[serial]
fn should_read_permissions_claim_override_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_AUTH_PERMISSIONS_CLAIM", "fitz://permissions"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);

            // Assert
            assert_eq!(
                config
                    .auth_claims_config
                    .permissions_claim_override
                    .as_deref(),
                Some("fitz://permissions")
            );
        },
    );
}

#[test]
#[serial]
fn should_reject_invalid_override_collisions_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_AUTH_CUSTOM_CLAIM", "fitz://permissions"),
            ("FITZ_AUTH_PERMISSIONS_CLAIM", "fitz://permissions"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_AUTH_PERMISSIONS_CLAIM must not match FITZ_AUTH_CUSTOM_CLAIM"));
        },
    );
}

#[test]
#[serial]
fn should_reject_overlapping_role_claim_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_ROUTE_FAMILY_CLAIM", "tid"),
            ("FITZ_AUTH_ROLE_CLAIM", "tid"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FITZ_AUTH_ROLE_CLAIM must not match FITZ_ROUTE_FAMILY_CLAIM"));
        },
    );
}

#[test]
#[serial]
fn should_reject_reserved_role_claim_from_environment() {
    with_auth_env(
        &[
            ("FITZ_AUTH_REQUIRED", "false"),
            ("FITZ_AUTH_ROLE_CLAIM", "scope"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::default().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains(
                "FITZ_AUTH_ROLE_CLAIM must not overlap with top-level permission sources"
            ));
        },
    );
}

#[test]
#[serial]
fn should_reject_cloud_mode_without_provider() {
    with_storage_env(&[("FITZ_STORAGE_MODE", "cloud")], || {
        // Arrange

        // Act
        let config = BootConfig::new();
        let result = config.validate();

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cloud storage requires FITZ_STORAGE_PROVIDER"));
    });
}

#[test]
#[serial]
fn should_reject_blank_provider_given_cloud_mode() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "   "),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("cloud storage requires FITZ_STORAGE_PROVIDER"));
        },
    );
}

#[test]
#[serial]
fn should_parse_sqrzl_s3_defaults_given_explicit_provider() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "sqrzl-s3"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert_eq!(cloud.provider_name, "sqrzl-s3");
            assert_eq!(cloud.local_cache_path, DEFAULT_CLOUD_CACHE_PATH);
            assert_eq!(cloud.prefix, None);
            match &cloud.provider_config {
                cntryl_midge::CloudProviderConfig::S3Compatible {
                    bucket,
                    endpoint,
                    path_style,
                    credentials,
                    ..
                } => {
                    assert_eq!(bucket, DEFAULT_SQRZL_EMULATOR_BUCKET);
                    assert_eq!(endpoint, DEFAULT_SQRZL_EMULATOR_ENDPOINT);
                    assert!(*path_style);
                    assert!(matches!(
                        credentials,
                        cntryl_midge::S3CredentialSource::Static { access_key, secret_key, .. }
                            if access_key == DEFAULT_SQRZL_EMULATOR_ACCESS_KEY
                                && secret_key == DEFAULT_SQRZL_EMULATOR_SECRET_KEY
                    ));
                }
                other => panic!("expected Sqrzl emulator S3-compatible config, got {other:?}"),
            }
        },
    );
}

#[test]
#[serial]
fn should_parse_sqrzl_azure_defaults_given_explicit_provider() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "sqrzl-azure"),
            ("FITZ_STORAGE_BUCKET", "ignored-by-azure"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert_eq!(cloud.provider_name, "sqrzl-azure");
            assert!(matches!(
                &cloud.provider_config,
                cntryl_midge::CloudProviderConfig::AzureBlob {
                    account,
                    container,
                    credential: cntryl_midge::AzureCredentialSource::SharedKey { account_key },
                    ..
                } if account == DEFAULT_SQRZL_EMULATOR_ACCESS_KEY
                    && container == DEFAULT_SQRZL_EMULATOR_BUCKET
                    && account_key == DEFAULT_SQRZL_EMULATOR_SECRET_KEY
            ));
        },
    );
}

#[test]
#[serial]
fn should_parse_sqrzl_gcs_defaults_given_explicit_provider() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "sqrzl-gcs"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert_eq!(cloud.provider_name, "sqrzl-gcs");
            assert!(matches!(
                &cloud.provider_config,
                cntryl_midge::CloudProviderConfig::Gcs {
                    bucket,
                    project_id,
                    api: cntryl_midge::GcsApiStyle::Xml,
                    credential: cntryl_midge::GcsCredentialSource::HmacKey { access_id, secret, },
                    ..
                } if bucket == DEFAULT_SQRZL_EMULATOR_BUCKET
                    && project_id == "sqrzl"
                    && access_id == DEFAULT_SQRZL_EMULATOR_ACCESS_KEY
                    && secret == DEFAULT_SQRZL_EMULATOR_SECRET_KEY
            ));
        },
    );
}

#[test]
#[serial]
fn should_parse_aws_s3_provider_given_cloud_env() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "aws-s3"),
            ("FITZ_STORAGE_BUCKET", "fitz-prod"),
            ("FITZ_STORAGE_REGION", "us-west-2"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert_eq!(cloud.provider_name, "aws-s3");
            assert!(matches!(
                &cloud.provider_config,
                cntryl_midge::CloudProviderConfig::AwsS3 {
                    bucket,
                    region,
                    credentials: cntryl_midge::S3CredentialSource::AwsDefaultChain,
                } if bucket == "fitz-prod" && region == "us-west-2"
            ));
        },
    );
}

#[test]
#[serial]
fn should_parse_s3_compatible_provider_given_cloud_env() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "s3-compatible"),
            ("FITZ_STORAGE_BUCKET", "fitz-dev"),
            ("FITZ_STORAGE_ENDPOINT", "http://objects:9000"),
            ("FITZ_STORAGE_REGION", "us-east-2"),
            ("FITZ_STORAGE_FORCE_PATH_STYLE", "false"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert!(matches!(
                &cloud.provider_config,
                cntryl_midge::CloudProviderConfig::S3Compatible {
                    bucket,
                    region,
                    endpoint,
                    path_style: false,
                    credentials: cntryl_midge::S3CredentialSource::Environment,
                } if bucket == "fitz-dev"
                    && region == "us-east-2"
                    && endpoint == "http://objects:9000"
            ));
        },
    );
}

#[test]
#[serial]
fn should_parse_s3_family_vendor_providers_given_cloud_env() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "minio"),
            ("FITZ_STORAGE_BUCKET", "fitz-minio"),
            ("FITZ_STORAGE_ENDPOINT", "http://minio:9000"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert!(matches!(
                &cloud.provider_config,
                cntryl_midge::CloudProviderConfig::S3Compatible {
                    bucket,
                    region,
                    endpoint,
                    path_style: true,
                    credentials: cntryl_midge::S3CredentialSource::Environment,
                } if bucket == "fitz-minio"
                    && region == "us-east-1"
                    && endpoint == "http://minio:9000"
            ));
        },
    );

    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "wasabi"),
            ("FITZ_STORAGE_BUCKET", "fitz-wasabi"),
            ("FITZ_STORAGE_REGION", "us-east-1"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert!(matches!(
                &cloud.provider_config,
                cntryl_midge::CloudProviderConfig::S3Compatible {
                    bucket,
                    region,
                    endpoint,
                    path_style: true,
                    credentials: cntryl_midge::S3CredentialSource::Environment,
                } if bucket == "fitz-wasabi"
                    && region == "us-east-1"
                    && endpoint == "https://s3.us-east-1.wasabisys.com"
            ));
        },
    );

    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "oci-s3"),
            ("FITZ_STORAGE_NAMESPACE", "fitzns"),
            ("FITZ_STORAGE_BUCKET", "fitz-oci"),
            ("FITZ_STORAGE_REGION", "us-phoenix-1"),
            ("FITZ_STORAGE_FORCE_PATH_STYLE", "true"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert!(matches!(
                &cloud.provider_config,
                cntryl_midge::CloudProviderConfig::S3Compatible {
                    bucket,
                    region,
                    endpoint,
                    path_style: true,
                    credentials: cntryl_midge::S3CredentialSource::Environment,
                } if bucket == "fitz-oci"
                    && region == "us-phoenix-1"
                    && endpoint
                        == "https://fitzns.compat.objectstorage.us-phoenix-1.oraclecloud.com"
            ));
        },
    );
}

#[test]
#[serial]
fn should_parse_azure_blob_provider_given_cloud_env() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "azure-blob"),
            ("FITZ_STORAGE_CONTAINER", "fitz-container"),
            ("AZURE_STORAGE_ACCOUNT_NAME", "fitzaccount"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "account-key"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert!(matches!(
                &cloud.provider_config,
                cntryl_midge::CloudProviderConfig::AzureBlob {
                    account,
                    container,
                    credential: cntryl_midge::AzureCredentialSource::SharedKey { account_key },
                    ..
                } if account == "fitzaccount"
                    && container == "fitz-container"
                    && account_key == "account-key"
            ));
        },
    );
}

#[test]
#[serial]
fn should_reject_azure_blob_bucket_alias_given_missing_container() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "azure-blob"),
            ("FITZ_STORAGE_BUCKET", "fitz-bucket"),
            ("AZURE_STORAGE_ACCOUNT_NAME", "fitzaccount"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "account-key"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new().with_auth_config(crate::auth::AuthConfig::Disabled);
            let result = config.validate();

            // Assert
            assert!(result.is_err());
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("azure-blob storage requires FITZ_STORAGE_CONTAINER"));
        },
    );
}

#[test]
#[serial]
fn should_reject_azure_blob_account_alias_given_missing_azure_account_name() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "azure-blob"),
            ("FITZ_STORAGE_CONTAINER", "fitz-container"),
            ("FITZ_STORAGE_ACCOUNT", "fitzaccount"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "account-key"),
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
                .contains("AZURE_STORAGE_ACCOUNT_NAME"));
        },
    );
}

#[test]
#[serial]
fn should_parse_gcs_provider_given_cloud_env() {
    with_storage_env(
        &[
            ("FITZ_STORAGE_MODE", "cloud"),
            ("FITZ_STORAGE_PROVIDER", "gcs"),
            ("FITZ_STORAGE_BUCKET", "fitz-gcs"),
            ("GOOGLE_APPLICATION_CREDENTIALS", "/var/run/gcp.json"),
            ("GOOGLE_CLOUD_PROJECT", "fitz-project"),
        ],
        || {
            // Arrange

            // Act
            let config = BootConfig::new();
            let cloud = cloud_config(&config);

            // Assert
            assert!(matches!(
                &cloud.provider_config,
                cntryl_midge::CloudProviderConfig::Gcs {
                    bucket,
                    project_id,
                    credential: cntryl_midge::GcsCredentialSource::ServiceAccountJsonFile { path },
                    ..
                } if bucket == "fitz-gcs"
                    && project_id == "fitz-project"
                    && path == std::path::Path::new("/var/run/gcp.json")
            ));
        },
    );
}
