use super::env::{env_bool, env_non_empty, required_env, required_region};
use super::{
    DEFAULT_SQRZL_EMULATOR_ACCESS_KEY, DEFAULT_SQRZL_EMULATOR_BUCKET,
    DEFAULT_SQRZL_EMULATOR_ENDPOINT, DEFAULT_SQRZL_EMULATOR_SECRET_KEY,
};

pub(super) fn build_cloud_provider_config(
    provider: &str,
) -> Result<cntryl_midge::CloudProviderConfig, String> {
    match provider {
        "sqrzl-s3" => Ok(cntryl_midge::CloudProviderConfig::s3_compatible_static(
            env_non_empty("FITZ_STORAGE_BUCKET")
                .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_BUCKET.to_string()),
            env_non_empty("FITZ_STORAGE_ENDPOINT")
                .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_ENDPOINT.to_string()),
            DEFAULT_SQRZL_EMULATOR_ACCESS_KEY,
            DEFAULT_SQRZL_EMULATOR_SECRET_KEY,
        )),
        "sqrzl-azure" => Ok(cntryl_midge::CloudProviderConfig::AzureBlob {
            account: DEFAULT_SQRZL_EMULATOR_ACCESS_KEY.to_string(),
            container: env_non_empty("FITZ_STORAGE_CONTAINER")
                .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_BUCKET.to_string()),
            endpoint: Some(
                env_non_empty("FITZ_STORAGE_ENDPOINT")
                    .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_ENDPOINT.to_string()),
            ),
            credential: cntryl_midge::AzureCredentialSource::shared_key(
                DEFAULT_SQRZL_EMULATOR_SECRET_KEY,
            ),
        }),
        "sqrzl-gcs" => Ok(cntryl_midge::CloudProviderConfig::Gcs {
            bucket: env_non_empty("FITZ_STORAGE_BUCKET")
                .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_BUCKET.to_string()),
            project_id: "sqrzl".to_string(),
            endpoint: Some(
                env_non_empty("FITZ_STORAGE_ENDPOINT")
                    .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_ENDPOINT.to_string()),
            ),
            api: cntryl_midge::GcsApiStyle::Xml,
            credential: cntryl_midge::GcsCredentialSource::hmac_key(
                DEFAULT_SQRZL_EMULATOR_ACCESS_KEY,
                DEFAULT_SQRZL_EMULATOR_SECRET_KEY,
            ),
        }),
        "aws-s3" => Ok(cntryl_midge::CloudProviderConfig::aws_s3(
            required_env("FITZ_STORAGE_BUCKET")?,
            required_region()?,
        )),
        "s3-compatible" => Ok(cntryl_midge::CloudProviderConfig::S3Compatible {
            bucket: required_env("FITZ_STORAGE_BUCKET")?,
            region: env_non_empty("FITZ_STORAGE_REGION")
                .unwrap_or_else(|| "us-east-1".to_string()),
            endpoint: required_env("FITZ_STORAGE_ENDPOINT")?,
            path_style: env_bool("FITZ_STORAGE_FORCE_PATH_STYLE", true)?,
            credentials: cntryl_midge::S3CredentialSource::environment(),
        }),
        "minio" => Ok(cntryl_midge::CloudProviderConfig::s3_compatible_env(
            required_env("FITZ_STORAGE_BUCKET")?,
            required_env("FITZ_STORAGE_ENDPOINT")?,
        )),
        "wasabi" => {
            let bucket = required_env("FITZ_STORAGE_BUCKET")?;
            let region = required_env("FITZ_STORAGE_REGION")?;
            let endpoint = env_non_empty("FITZ_STORAGE_ENDPOINT")
                .unwrap_or_else(|| format!("https://s3.{region}.wasabisys.com"));

            Ok(cntryl_midge::CloudProviderConfig::S3Compatible {
                bucket,
                region,
                endpoint,
                path_style: true,
                credentials: cntryl_midge::S3CredentialSource::environment(),
            })
        }
        "oci-s3" => {
            let bucket = required_env("FITZ_STORAGE_BUCKET")?;
            let namespace = required_env("FITZ_STORAGE_NAMESPACE")?;
            let region = required_env("FITZ_STORAGE_REGION")?;
            let endpoint = env_non_empty("FITZ_STORAGE_ENDPOINT").unwrap_or_else(|| {
                format!("https://{namespace}.compat.objectstorage.{region}.oraclecloud.com")
            });

            Ok(cntryl_midge::CloudProviderConfig::S3Compatible {
                bucket,
                region,
                endpoint,
                path_style: env_bool("FITZ_STORAGE_FORCE_PATH_STYLE", false)?,
                credentials: cntryl_midge::S3CredentialSource::environment(),
            })
        }
        "azure-blob" => build_azure_blob_provider(),
        "gcs" => build_gcs_provider(),
        other => Err(format!(
            "unsupported FITZ_STORAGE_PROVIDER='{other}'; expected sqrzl-s3, sqrzl-azure, sqrzl-gcs, aws-s3, s3-compatible, minio, wasabi, oci-s3, azure-blob, or gcs"
        )),
    }
}

fn build_azure_blob_provider() -> Result<cntryl_midge::CloudProviderConfig, String> {
    let endpoint = env_non_empty("FITZ_STORAGE_ENDPOINT");
    let container = required_env("FITZ_STORAGE_CONTAINER")
        .map_err(|_| "azure-blob storage requires FITZ_STORAGE_CONTAINER".to_string())?;

    let mut provider = if let Some(connection_string) =
        env_non_empty("AZURE_STORAGE_CONNECTION_STRING")
    {
        cntryl_midge::CloudProviderConfig::azure_blob_connection_string(
            container,
            connection_string,
        )
    } else {
        let account = env_non_empty("AZURE_STORAGE_ACCOUNT_NAME").ok_or_else(|| {
            "azure-blob storage requires AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_CONNECTION_STRING"
                .to_string()
        })?;
        if let Some(account_key) = env_non_empty("AZURE_STORAGE_ACCOUNT_KEY") {
            cntryl_midge::CloudProviderConfig::azure_blob_shared_key(
                account,
                container,
                account_key,
            )
        } else if let Some(sas_token) = env_non_empty("AZURE_STORAGE_SAS_TOKEN") {
            cntryl_midge::CloudProviderConfig::azure_blob_sas(account, container, sas_token)
        } else {
            cntryl_midge::CloudProviderConfig::azure_blob(account, container)
        }
    };

    if let Some(endpoint) = endpoint {
        provider = provider
            .with_endpoint(endpoint)
            .map_err(|error| error.to_string())?;
    }

    Ok(provider)
}

fn build_gcs_provider() -> Result<cntryl_midge::CloudProviderConfig, String> {
    let bucket = required_env("FITZ_STORAGE_BUCKET")?;
    let mut provider = match (
        env_non_empty("GCS_HMAC_ACCESS_ID"),
        env_non_empty("GCS_HMAC_SECRET"),
    ) {
        (Some(access_id), Some(secret)) => {
            cntryl_midge::CloudProviderConfig::gcs_hmac(bucket, access_id, secret)
        }
        (Some(_), None) | (None, Some(_)) => {
            return Err(
                "gcs HMAC storage requires both GCS_HMAC_ACCESS_ID and GCS_HMAC_SECRET".to_string(),
            )
        }
        (None, None) => {
            if let Some(path) = env_non_empty("GOOGLE_APPLICATION_CREDENTIALS") {
                cntryl_midge::CloudProviderConfig::gcs_service_account_file(bucket, path)
            } else {
                cntryl_midge::CloudProviderConfig::gcs(bucket)
            }
        }
    };

    if let Some(project_id) = env_non_empty("GOOGLE_CLOUD_PROJECT") {
        provider = provider
            .with_gcs_project_id(project_id)
            .map_err(|error| error.to_string())?;
    }
    if let Some(endpoint) = env_non_empty("FITZ_STORAGE_ENDPOINT") {
        provider = provider
            .with_endpoint(endpoint)
            .map_err(|error| error.to_string())?;
    }

    Ok(provider)
}
