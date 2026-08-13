use super::env::{env_bool, env_non_empty, required_env, required_region};
use super::{
    DEFAULT_SQRZL_EMULATOR_ACCESS_KEY, DEFAULT_SQRZL_EMULATOR_BUCKET,
    DEFAULT_SQRZL_EMULATOR_ENDPOINT, DEFAULT_SQRZL_EMULATOR_SECRET_KEY,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum NamespaceShape {
    Bucket,
    Container,
}

struct ProviderDescriptor {
    name: &'static str,
    namespace: NamespaceShape,
}

const PROVIDER_DESCRIPTORS: &[ProviderDescriptor] = &[
    ProviderDescriptor {
        name: "sqrzl-s3",
        namespace: NamespaceShape::Bucket,
    },
    ProviderDescriptor {
        name: "sqrzl-azure",
        namespace: NamespaceShape::Container,
    },
    ProviderDescriptor {
        name: "sqrzl-gcs",
        namespace: NamespaceShape::Bucket,
    },
    ProviderDescriptor {
        name: "aws-s3",
        namespace: NamespaceShape::Bucket,
    },
    ProviderDescriptor {
        name: "s3-compatible",
        namespace: NamespaceShape::Bucket,
    },
    ProviderDescriptor {
        name: "minio",
        namespace: NamespaceShape::Bucket,
    },
    ProviderDescriptor {
        name: "wasabi",
        namespace: NamespaceShape::Bucket,
    },
    ProviderDescriptor {
        name: "oci-s3",
        namespace: NamespaceShape::Bucket,
    },
    ProviderDescriptor {
        name: "azure-blob",
        namespace: NamespaceShape::Container,
    },
    ProviderDescriptor {
        name: "gcs",
        namespace: NamespaceShape::Bucket,
    },
];

fn descriptor(provider: &str) -> Result<&'static ProviderDescriptor, String> {
    PROVIDER_DESCRIPTORS
        .iter()
        .find(|descriptor| descriptor.name == provider)
        .ok_or_else(|| {
            let names = PROVIDER_DESCRIPTORS
                .iter()
                .map(|descriptor| descriptor.name)
                .collect::<Vec<_>>()
                .join(", ");
            format!("unsupported FITZ_STORAGE_PROVIDER='{provider}'; expected {names}")
        })
}

fn required_namespace(provider: &str) -> Result<String, String> {
    let descriptor = descriptor(provider)?;
    let key = match descriptor.namespace {
        NamespaceShape::Bucket => "FITZ_STORAGE_BUCKET",
        NamespaceShape::Container => "FITZ_STORAGE_CONTAINER",
    };
    required_env(key)
}

pub(super) fn build_cloud_provider_config(
    provider: &str,
) -> Result<cntryl_midge::CloudProviderConfig, String> {
    descriptor(provider)?;
    match provider {
        "sqrzl-s3" => Ok(cntryl_midge::CloudProviderConfig::s3_compatible_static(
            env_non_empty("FITZ_STORAGE_BUCKET")
                .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_BUCKET.to_string()),
            env_non_empty("FITZ_STORAGE_ENDPOINT")
                .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_ENDPOINT.to_string()),
            DEFAULT_SQRZL_EMULATOR_ACCESS_KEY,
            DEFAULT_SQRZL_EMULATOR_SECRET_KEY,
        )),
        "sqrzl-azure" => cntryl_midge::CloudProviderConfig::azure_blob_shared_key(
            DEFAULT_SQRZL_EMULATOR_ACCESS_KEY,
            env_non_empty("FITZ_STORAGE_CONTAINER")
                .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_BUCKET.to_string()),
            DEFAULT_SQRZL_EMULATOR_SECRET_KEY,
        )
        .with_endpoint(
            env_non_empty("FITZ_STORAGE_ENDPOINT")
                .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_ENDPOINT.to_string()),
        )
        .map_err(|error| error.to_string()),
        "sqrzl-gcs" => cntryl_midge::CloudProviderConfig::gcs_hmac(
            env_non_empty("FITZ_STORAGE_BUCKET")
                .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_BUCKET.to_string()),
            DEFAULT_SQRZL_EMULATOR_ACCESS_KEY,
            DEFAULT_SQRZL_EMULATOR_SECRET_KEY,
        )
        .with_gcs_project_id("sqrzl")
        .and_then(|provider| {
            provider.with_endpoint(
                env_non_empty("FITZ_STORAGE_ENDPOINT")
                    .unwrap_or_else(|| DEFAULT_SQRZL_EMULATOR_ENDPOINT.to_string()),
            )
        })
        .map_err(|error| error.to_string()),
        "aws-s3" => Ok(cntryl_midge::CloudProviderConfig::aws_s3(
            required_namespace(provider)?,
            required_region()?,
        )),
        "s3-compatible" => Ok(s3_compatible_provider(
            required_namespace(provider)?,
            env_non_empty("FITZ_STORAGE_REGION").unwrap_or_else(|| "us-east-1".to_string()),
            required_env("FITZ_STORAGE_ENDPOINT")?,
            env_bool("FITZ_STORAGE_FORCE_PATH_STYLE", true)?,
        )),
        "minio" => Ok(cntryl_midge::CloudProviderConfig::s3_compatible_env(
            required_namespace(provider)?,
            required_env("FITZ_STORAGE_ENDPOINT")?,
        )),
        "wasabi" => {
            let bucket = required_namespace(provider)?;
            let region = required_env("FITZ_STORAGE_REGION")?;
            let endpoint = env_non_empty("FITZ_STORAGE_ENDPOINT")
                .unwrap_or_else(|| format!("https://s3.{region}.wasabisys.com"));

            Ok(s3_compatible_provider(bucket, region, endpoint, true))
        }
        "oci-s3" => {
            let bucket = required_namespace(provider)?;
            let namespace = required_env("FITZ_STORAGE_NAMESPACE")?;
            let region = required_env("FITZ_STORAGE_REGION")?;
            let endpoint = env_non_empty("FITZ_STORAGE_ENDPOINT").unwrap_or_else(|| {
                format!("https://{namespace}.compat.objectstorage.{region}.oraclecloud.com")
            });

            Ok(s3_compatible_provider(
                bucket,
                region,
                endpoint,
                env_bool("FITZ_STORAGE_FORCE_PATH_STYLE", false)?,
            ))
        }
        "azure-blob" => build_azure_blob_provider(provider),
        "gcs" => build_gcs_provider(provider),
        _ => unreachable!("provider descriptor and constructor match must stay aligned"),
    }
}

fn s3_compatible_provider(
    bucket: String,
    region: String,
    endpoint: String,
    path_style: bool,
) -> cntryl_midge::CloudProviderConfig {
    cntryl_midge::S3CompatibleConfig::new(
        bucket,
        region,
        endpoint,
        cntryl_midge::S3CredentialSource::environment(),
    )
    .with_path_style(path_style)
    .into()
}

fn build_azure_blob_provider(
    provider_name: &str,
) -> Result<cntryl_midge::CloudProviderConfig, String> {
    let endpoint = env_non_empty("FITZ_STORAGE_ENDPOINT");
    let container = required_namespace(provider_name)
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

fn build_gcs_provider(provider_name: &str) -> Result<cntryl_midge::CloudProviderConfig, String> {
    let bucket = required_namespace(provider_name)?;
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
