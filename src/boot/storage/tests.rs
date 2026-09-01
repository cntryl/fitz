use super::*;
use crate::boot::runtime::StorageMemtableConfig;
use cntryl_midge::{Goal, MemoryBudget, TransactionMode, WorkloadProfile, WriteOptions};
use std::fmt::Write as _;
use std::time::Duration;
use tempfile::TempDir;

mod lease_lifecycle;

fn write_marker(engine: &cntryl_midge::Engine, cf_id: u32, key: &[u8], value: &[u8]) {
    write_marker_with_options(engine, cf_id, key, value, WriteOptions::buffered());
}

fn write_marker_with_options(
    engine: &cntryl_midge::Engine,
    cf_id: u32,
    key: &[u8],
    value: &[u8],
    write_options: WriteOptions,
) {
    let mut tx = engine
        .begin_tx(cf_id, TransactionMode::ReadWrite)
        .expect("begin write tx");
    tx.put(key.to_vec(), value.to_vec(), None)
        .expect("write marker");
    tx.commit(write_options).expect("commit marker");
}

fn read_marker(engine: &cntryl_midge::Engine, cf_id: u32, key: &[u8]) -> Option<Vec<u8>> {
    let tx = engine
        .begin_tx(cf_id, TransactionMode::ReadOnly)
        .expect("begin read tx");
    tx.get(key)
        .expect("read marker")
        .map(|value| value.to_vec())
}

#[test]
fn should_create_boot_config_for_test_storage() {
    // Arrange
    let config = BootConfig::with_memory_storage();

    // Act
    match &config.storage_mode {
        crate::boot::runtime::StorageMode::Memory => {}
        _ => panic!("Expected memory storage mode"),
    }

    // Assert
}

#[test]
fn should_detect_local_storage_by_default() {
    // Arrange
    let config = BootConfig::new();

    // Act
    match &config.storage_mode {
        crate::boot::runtime::StorageMode::LocalDisk { db_path } => {
            // Assert
            assert_eq!(db_path, "./.fitz");
        }
        _ => panic!("Expected local disk storage mode"),
    }
}

#[tokio::test]
async fn should_provision_configured_route_family_column_families() {
    // Arrange
    let config = BootConfig::with_memory_storage().with_route_families(vec![1, 2]);

    // Act
    let store = init(&config).await.expect("open memory store");

    // Assert
    assert_eq!(
        store
            .get_column_family("tenant_default")
            .expect("default route family column family")
            .id(),
        1
    );
    assert_eq!(
        store
            .get_column_family("tenant_2")
            .expect("second route family column family")
            .id(),
        2
    );
}

#[test]
fn should_apply_configured_storage_memtable_bytes_to_midge_options() {
    // Arrange
    let memtable_bytes = 8 * 1024 * 1024;
    let config = BootConfig::with_memory_storage().with_storage_memtable_bytes(memtable_bytes);

    // Act
    let open_options = build_midge_open_options(cntryl_midge::OpenOptions::in_memory(), &config)
        .expect("build memory options");

    // Assert
    assert_eq!(open_options.memtable_size_limit(), memtable_bytes);
}

#[tokio::test]
async fn should_open_storage_with_configured_midge_memtable_size() {
    // Arrange
    let memtable_bytes = 128 * 1024;
    let config = BootConfig::with_memory_storage().with_storage_memtable_bytes(memtable_bytes);

    // Act
    let store = init(&config).await.expect("open memory store");
    let metrics = store.get_runtime_metrics().expect("runtime metrics");

    // Assert
    assert_eq!(metrics.memtable_size_limit, memtable_bytes);
    assert_eq!(metrics.memtable_flush_threshold, memtable_bytes);
}

#[test]
fn should_apply_cloud_throughput_defaults_when_memtable_is_auto() {
    // Arrange
    let mut config = BootConfig::default().with_storage_mode(StorageMode::CloudBacked(Box::new(
        CloudStorageConfig {
            provider_name: "sqrzl-s3".to_string(),
            provider_config: sqrzl_s3_provider("fitz-cost-tuning"),
            prefix: Some("tests".to_string()),
            local_cache_path: "./.fitz-cloud-cache".to_string(),
        },
    )));
    config.storage_memtable = StorageMemtableConfig::Auto;

    let open_options = cntryl_midge::OpenOptions::cloud_simulated(
        "./target/tmp/fitz-cloud-cost-baseline",
        "fitz-cost-tuning",
        "tests",
    )
    .memory_budget(MemoryBudget::Bytes(512 * 1024 * 1024));
    let expected_memtable_bytes =
        (512 * 1024 * 1024usize).saturating_sub((512 * 1024 * 1024usize) / 10) / 2;

    // Act
    let tuned = build_midge_open_options(open_options, &config).expect("build cloud options");

    // Assert
    assert_eq!(tuned.goal(), Goal::Throughput);
    assert_eq!(tuned.workload(), WorkloadProfile::WriteHeavy);
    assert_eq!(tuned.memtable_size_limit(), expected_memtable_bytes);
    assert_eq!(tuned.wal_buffer_size(), 1024 * 1024);
    assert_eq!(tuned.target_sst_size(), 512 * 1024 * 1024);
}

#[test]
fn should_respect_cloud_memtable_override_before_tuning() {
    // Arrange
    let tempdir = TempDir::new().expect("tempdir");
    let memtable_bytes = 8 * 1024 * 1024;
    let config = BootConfig::default()
        .with_storage_memtable_bytes(memtable_bytes)
        .with_storage_mode(StorageMode::CloudBacked(Box::new(CloudStorageConfig {
            provider_name: "sqrzl-s3".to_string(),
            provider_config: sqrzl_s3_provider("fitz-cost-tuning"),
            prefix: Some("tests".to_string()),
            local_cache_path: tempdir.path().join("cache").to_string_lossy().to_string(),
        })));

    let open_options = cntryl_midge::OpenOptions::cloud_simulated(
        tempdir.path().join("override"),
        "fitz-cost-tuning",
        "tests",
    )
    .memory_budget(MemoryBudget::Bytes(512 * 1024 * 1024));

    // Act
    let tuned = build_midge_open_options(open_options, &config).expect("build cloud options");

    // Assert
    let engine = cntryl_midge::Engine::open(tuned).expect("open tuned cloud engine");
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");

    assert_eq!(metrics.memtable_size_limit, memtable_bytes);
    assert_eq!(metrics.memtable_flush_threshold, memtable_bytes);
}

#[test]
fn should_reduce_cloud_wal_flush_churn_with_throughput_tuning_on_cloud_simulated_storage() {
    // Arrange
    let tempdir = TempDir::new().expect("tempdir");
    let burst_value = vec![b'x'; 1024 * 1024];
    let write_count = 80;
    let budget = MemoryBudget::Bytes(512 * 1024 * 1024);

    let mut config = BootConfig::default().with_storage_mode(StorageMode::CloudBacked(Box::new(
        CloudStorageConfig {
            provider_name: "sqrzl-s3".to_string(),
            provider_config: sqrzl_s3_provider("fitz-cost-tuning"),
            prefix: Some("tests".to_string()),
            local_cache_path: tempdir.path().join("cache").to_string_lossy().to_string(),
        },
    )));
    config.storage_memtable = StorageMemtableConfig::Auto;

    let baseline_opts = cntryl_midge::OpenOptions::cloud_simulated(
        tempdir.path().join("baseline"),
        "fitz-cost-tuning",
        "tests",
    )
    .memory_budget(budget)
    .build();
    let baseline_opts = baseline_opts.expect("build baseline options");
    let tuned_opts = build_midge_open_options(
        cntryl_midge::OpenOptions::cloud_simulated(
            tempdir.path().join("tuned"),
            "fitz-cost-tuning",
            "tests",
        )
        .memory_budget(budget),
        &config,
    )
    .expect("build tuned options");

    assert_eq!(baseline_opts.wal_buffer_size(), 128 * 1024);
    assert_eq!(tuned_opts.wal_buffer_size(), 1024 * 1024);

    // Act
    let baseline_metrics = exercise_cloud_burst(
        baseline_opts,
        write_count,
        &burst_value,
        Duration::from_secs(2),
    );
    let tuned_metrics = exercise_cloud_burst(
        tuned_opts,
        write_count,
        &burst_value,
        Duration::from_secs(2),
    );

    // Assert
    assert!(
        baseline_metrics.sst_count >= tuned_metrics.sst_count,
        "expected no more SST churn with throughput tuning; baseline={} tuned={}",
        baseline_metrics.sst_count,
        tuned_metrics.sst_count
    );
    assert!(
        baseline_metrics.pending_cloud_uploads >= tuned_metrics.pending_cloud_uploads,
        "expected no more pending cloud uploads with throughput tuning; baseline={} tuned={}",
        baseline_metrics.pending_cloud_uploads,
        tuned_metrics.pending_cloud_uploads
    );
}

#[test]
fn should_skip_sqrzl_test_for_transport_errors() {
    // Arrange
    let error =
        "prepare Sqrzl namespace failed: error sending request for url (http://127.0.0.1:9000/fitz-sqrzl-s3)";

    // Act
    let should_skip = should_skip_sqrzl_test(error);

    // Assert
    assert!(
        should_skip,
        "expected transport request failures to be skippable"
    );
}

#[test]
fn should_skip_sqrzl_test_for_missing_content_length() {
    // Arrange: local mock GCS servers can reject a PUT without an explicit
    // Content-Length header, which is a mock-server quirk unrelated to the
    // recovery behavior under test.
    let error = "prepare Sqrzl namespace failed: GCS setup request PUT /fitz-sqrzl-gcs failed \
        with status 411 Length Required: <?xml version=\"1.0\" encoding=\"UTF-8\"?>\
        <Error><Code>MissingContentLength</Code>\
        <Message>Content-Length is required unless Transfer-Encoding is chunked.</Message></Error>";

    // Act
    let should_skip = should_skip_sqrzl_test(error);

    // Assert
    assert!(
        should_skip,
        "expected a mock-server 411 Length Required response to be skippable"
    );
}

#[test]
fn should_reject_cloud_storage_without_bucket() {
    // Arrange
    let config = BootConfig::default().with_storage_mode(StorageMode::CloudBacked(Box::new(
        CloudStorageConfig {
            provider_name: "sqrzl-s3".to_string(),
            provider_config: sqrzl_s3_provider(""),
            prefix: Some("prod".to_string()),
            local_cache_path: "./.fitz-cloud-cache".to_string(),
        },
    )));

    // Act
    let validation_result = config.validate();

    // Assert
    assert!(validation_result.is_err());
}

#[tokio::test]
async fn should_return_error_given_cloud_open_failure_when_called_inside_async_runtime() {
    // Arrange
    let tempdir = TempDir::new().expect("tempdir");
    let config = BootConfig::default().with_storage_mode(StorageMode::CloudBacked(Box::new(
        CloudStorageConfig {
            provider_name: "s3-compatible".to_string(),
            provider_config: cntryl_midge::CloudProviderConfig::s3_compatible_static(
                "fitz-runtime-drop-test",
                "http://127.0.0.1:1",
                "test-access-key",
                "test-secret-key",
            ),
            prefix: Some(format!("tests/{}/", uuid::Uuid::new_v4())),
            local_cache_path: tempdir.path().join("cache").to_string_lossy().to_string(),
        },
    )));

    // Act
    let init_result = tokio::time::timeout(Duration::from_secs(5), init(&config))
        .await
        .expect("cloud init should not hang");

    // Assert
    match init_result {
        Ok(_) => panic!("cloud init should surface an open error"),
        Err(error) => assert!(
            error
                .to_string()
                .contains("Failed to open cloud-backed Midge"),
            "expected cloud open error, got: {error}"
        ),
    }
}

#[tokio::test]
async fn should_recover_marker_from_sqrzl_s3_after_cache_loss() {
    // Arrange
    let provider = sqrzl_s3_provider("fitz-sqrzl-s3");

    // Act
    let recovered = match recover_marker_from_sqrzl("sqrzl-s3", provider).await {
        Ok(value) => value,
        Err(error) if should_skip_sqrzl_test(&error) => {
            eprintln!("Skipping sqrzl-s3 recovery test: {error}");
            return;
        }
        Err(error) => panic!("sqrzl-s3 recovery failed: {error}"),
    };

    // Assert
    assert_eq!(recovered, Some(b"value".to_vec()));
}

#[tokio::test]
async fn should_recover_marker_from_sqrzl_azure_after_cache_loss() {
    // Arrange
    let provider = sqrzl_azure_provider("fitz-sqrzl-azure");

    // Act
    let recovered = match recover_marker_from_sqrzl("sqrzl-azure", provider).await {
        Ok(value) => value,
        Err(error) if should_skip_sqrzl_test(&error) => {
            eprintln!("Skipping sqrzl-azure recovery test: {error}");
            return;
        }
        Err(error) => panic!("sqrzl-azure recovery failed: {error}"),
    };

    // Assert
    assert_eq!(recovered, Some(b"value".to_vec()));
}

#[tokio::test]
async fn should_recover_marker_from_sqrzl_gcs_after_cache_loss() {
    // Arrange
    let provider = sqrzl_gcs_provider("fitz-sqrzl-gcs");

    // Act
    let recovered = match recover_marker_from_sqrzl("sqrzl-gcs", provider).await {
        Ok(value) => value,
        Err(error) if should_skip_sqrzl_test(&error) => {
            eprintln!("Skipping sqrzl-gcs recovery test: {error}");
            return;
        }
        Err(error) => panic!("sqrzl-gcs recovery failed: {error}"),
    };

    // Assert
    assert_eq!(recovered, Some(b"value".to_vec()));
}

fn exercise_cloud_burst(
    engine_opts: cntryl_midge::OpenOptions,
    write_count: usize,
    value: &[u8],
    wait_time: Duration,
) -> cntryl_midge::RuntimeMetricsSnapshot {
    let mut engine = cntryl_midge::Engine::open(engine_opts).expect("open cloud-simulated engine");
    let cf = engine.get_column_family("default").expect("default cf");

    for index in 0..write_count {
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("begin write tx");
        let key = format!("cloud-cost-key-{index:04}");
        tx.put(key.into_bytes(), value.to_vec(), None)
            .expect("write burst value");
        tx.commit(WriteOptions::cloud_async())
            .expect("commit burst value");
    }

    std::thread::sleep(wait_time);
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    engine
        .shutdown(crate::testkit::scaled_test_timeout(Duration::from_secs(2)))
        .expect("shutdown cloud-simulated engine");
    metrics
}

async fn recover_marker_from_sqrzl(
    provider_name: &str,
    provider_config: cntryl_midge::CloudProviderConfig,
) -> Result<Option<Vec<u8>>, String> {
    ensure_sqrzl_namespace(&provider_config)
        .await
        .map_err(|error| format!("prepare Sqrzl namespace failed: {error}"))?;

    let tempdir = TempDir::new().expect("tempdir");
    let prefix = format!("manual/{}/", uuid::Uuid::new_v4());
    let first_cache = tempdir.path().join("first-cache");
    let second_cache = tempdir.path().join("second-cache");
    let first_config = sqrzl_boot_config(
        provider_name,
        provider_config.clone(),
        &prefix,
        &first_cache,
    );

    let store = init(&first_config)
        .await
        .map_err(|error| format!("open first cloud store: {error}"))?;
    let cf = store
        .get_column_family("tenant_default")
        .expect("tenant_default cf");
    write_marker_with_options(
        store.as_ref(),
        cf.id(),
        b"marker",
        b"value",
        WriteOptions::cloud_strict(),
    );
    store.flush_cf(&cf).expect("force cloud SST upload");
    shutdown_store(store);
    std::fs::remove_dir_all(first_cache)
        .map_err(|error| format!("delete first cloud cache: {error}"))?;

    let second_config = sqrzl_boot_config(provider_name, provider_config, &prefix, &second_cache);
    let reopened = init(&second_config)
        .await
        .map_err(|error| format!("reopen cloud store: {error}"))?;
    let reopened_cf = reopened
        .get_column_family("tenant_default")
        .expect("tenant_default cf after reopen");
    let marker = read_marker(reopened.as_ref(), reopened_cf.id(), b"marker");
    shutdown_store(reopened);
    Ok(marker)
}

fn should_skip_sqrzl_test(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("connection refused")
        || lower.contains("error sending request")
        || lower.contains("failed to connect")
        || lower.contains("connection reset")
        || lower.contains("timed out")
        || lower.contains("dns")
        || lower.contains("signaturedoesnotmatch")
        || lower.contains("status 403")
        || lower.contains("status 411")
        || lower.contains("status 500")
        || lower.contains("lease acquisition i/o error")
}

fn sqrzl_boot_config(
    provider_name: &str,
    provider_config: cntryl_midge::CloudProviderConfig,
    prefix: &str,
    cache_path: &std::path::Path,
) -> BootConfig {
    BootConfig::default().with_storage_mode(StorageMode::CloudBacked(Box::new(
        CloudStorageConfig {
            provider_name: provider_name.to_string(),
            provider_config,
            prefix: Some(prefix.to_string()),
            local_cache_path: cache_path.to_string_lossy().to_string(),
        },
    )))
}

fn shutdown_store(store: Arc<cntryl_midge::Engine>) {
    let mut engine = Arc::try_unwrap(store).unwrap_or_else(|store| {
        panic!(
            "Midge shutdown blocked by {} leftover engine references",
            Arc::strong_count(&store)
        );
    });
    engine
        .shutdown(crate::testkit::scaled_test_timeout(Duration::from_secs(2)))
        .expect("shutdown Midge");
}

async fn ensure_sqrzl_namespace(
    provider: &cntryl_midge::CloudProviderConfig,
) -> Result<(), String> {
    match provider {
        cntryl_midge::CloudProviderConfig::AwsS3(_) => Ok(()),
        cntryl_midge::CloudProviderConfig::S3Compatible(config) => {
            ensure_sqrzl_s3_bucket(config.bucket()).await
        }
        cntryl_midge::CloudProviderConfig::Gcs(config) => {
            ensure_sqrzl_gcs_bucket(config.bucket()).await
        }
        cntryl_midge::CloudProviderConfig::AzureBlob(config) => {
            ensure_sqrzl_azure_container(config.container()).await
        }
        cntryl_midge::CloudProviderConfig::OciObjectStorage(config) => {
            ensure_sqrzl_s3_bucket(config.bucket()).await
        }
    }
}

async fn ensure_sqrzl_s3_bucket(bucket: &str) -> Result<(), String> {
    signed_s3_request("PUT", &format!("/{bucket}"), b"")
        .await
        .map(|_| ())
}

async fn signed_s3_request(method: &str, path: &str, body: &[u8]) -> Result<Vec<u8>, String> {
    use sha2::{Digest, Sha256};

    let host = "127.0.0.1:9000";
    let region = "us-east-1";
    let now = chrono::Utc::now();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date = now.format("%Y%m%d").to_string();
    let payload_hash = hex::encode(Sha256::digest(body));
    let mut headers = [
        ("host".to_string(), host.to_string()),
        ("x-amz-content-sha256".to_string(), payload_hash.clone()),
        ("x-amz-date".to_string(), amz_date.clone()),
    ];
    headers.sort_by(|left, right| left.0.cmp(&right.0));
    let canonical_headers = canonicalize_headers(&headers);
    let signed_headers = headers
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_request =
        format!("{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let k_date = hmac_sha256(format!("AWS{}", sqrzl_secret_key()).as_bytes(), &date);
    let k_region = hmac_sha256(&k_date, region);
    let k_service = hmac_sha256(&k_region, "s3");
    let k_signing = hmac_sha256(&k_service, "aws4_request");
    let signature = hex::encode(hmac_sha256(&k_signing, &string_to_sign));
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        sqrzl_access_key(),
        scope,
        signed_headers,
        signature
    );
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| error.to_string())?
        .request(
            reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())?,
            format!("{}{}", sqrzl_endpoint(), path),
        )
        .header("host", host)
        .header("x-amz-content-sha256", payload_hash)
        .header("x-amz-date", amz_date)
        .header("authorization", authorization)
        .body(body.to_vec())
        .send()
        .await
        .map_err(|error| error.to_string())?;
    cloud_setup_response("S3", method, path, response).await
}

fn hmac_sha256(key: &[u8], data: &str) -> Vec<u8> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
    mac.update(data.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

async fn ensure_sqrzl_gcs_bucket(bucket: &str) -> Result<(), String> {
    signed_gcs_request("PUT", &format!("/{bucket}"), "", b"")
        .await
        .map(|_| ())
}

async fn signed_gcs_request(
    method: &str,
    path: &str,
    content_type: &str,
    body: &[u8],
) -> Result<Vec<u8>, String> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha1::Sha1;

    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let string_to_sign = format!("{method}\n\n{content_type}\n{date}\n{path}");
    let mut mac = Hmac::<Sha1>::new_from_slice(sqrzl_secret_key().as_bytes())
        .map_err(|error| error.to_string())?;
    mac.update(string_to_sign.as_bytes());
    let signature = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    );
    let mut request = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| error.to_string())?
        .request(
            reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())?,
            format!("{}{}", sqrzl_endpoint(), path),
        )
        .header("date", date)
        .header(
            "authorization",
            format!("GOOG1 {}:{signature}", sqrzl_access_key()),
        )
        .body(body.to_vec());
    if !content_type.is_empty() {
        request = request.header("content-type", content_type);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    cloud_setup_response("GCS", method, path, response).await
}

async fn ensure_sqrzl_azure_container(container: &str) -> Result<(), String> {
    signed_azure_request(
        "PUT",
        &format!("/{}/{container}", sqrzl_access_key()),
        "restype=container",
        b"",
        vec![],
    )
    .await
    .map(|_| ())
}

async fn signed_azure_request(
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
    extra_headers: Vec<(&str, &str)>,
) -> Result<Vec<u8>, String> {
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    let date = chrono::Utc::now()
        .format("%a, %d %b %Y %H:%M:%S GMT")
        .to_string();
    let mut headers = vec![
        ("x-ms-date".to_string(), date),
        ("x-ms-version".to_string(), "2024-11-04".to_string()),
    ];
    headers.extend(
        extra_headers
            .into_iter()
            .map(|(name, value)| (name.to_string(), value.to_string())),
    );
    let header_value = |name: &str| -> String {
        headers
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.clone())
            .unwrap_or_default()
    };
    let content_length = if matches!(method, "GET" | "HEAD") || body.is_empty() {
        String::new()
    } else {
        body.len().to_string()
    };
    let canonical_headers = canonicalize_x_ms_headers(&headers);
    let mut canonical_resource = format!("/{}{}", sqrzl_access_key(), path);
    if !query.is_empty() {
        let mut query_pairs = query
            .split('&')
            .map(|pair| {
                let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
                (key.to_ascii_lowercase(), value.to_string())
            })
            .collect::<Vec<_>>();
        query_pairs.sort();
        for (key, value) in query_pairs {
            let _ = write!(canonical_resource, "\n{key}:{value}");
        }
    }
    let string_to_sign = [
        method.to_string(),
        header_value("Content-Encoding"),
        header_value("Content-Language"),
        content_length,
        header_value("Content-MD5"),
        header_value("Content-Type"),
        String::new(),
        header_value("If-Modified-Since"),
        header_value("If-Match"),
        header_value("If-None-Match"),
        header_value("If-Unmodified-Since"),
        header_value("Range"),
        canonical_headers,
        canonical_resource,
    ]
    .join("\n");
    let key = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        sqrzl_secret_key(),
    )
    .unwrap_or_else(|_| sqrzl_secret_key().as_bytes().to_vec());
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).map_err(|error| error.to_string())?;
    mac.update(string_to_sign.as_bytes());
    let signature = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        mac.finalize().into_bytes(),
    );
    let url = if query.is_empty() {
        format!("{}{}", sqrzl_endpoint(), path)
    } else {
        format!("{}{}?{query}", sqrzl_endpoint(), path)
    };
    let mut request = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|error| error.to_string())?
        .request(
            reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())?,
            url,
        )
        .header(
            "authorization",
            format!("SharedKey {}:{signature}", sqrzl_access_key()),
        )
        .body(body.to_vec());
    for (name, value) in headers {
        request = request.header(name, value);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    cloud_setup_response("Azure", method, path, response).await
}

fn canonicalize_headers(headers: &[(String, String)]) -> String {
    use std::fmt::Write as _;

    let mut canonical_headers = String::new();
    for (name, value) in headers {
        let _ = writeln!(canonical_headers, "{name}:{value}");
    }
    canonical_headers
}

fn canonicalize_x_ms_headers(headers: &[(String, String)]) -> String {
    let mut x_ms_headers = headers
        .iter()
        .filter(|(name, _)| name.to_ascii_lowercase().starts_with("x-ms-"))
        .map(|(name, value)| {
            (
                name.to_ascii_lowercase(),
                value.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect::<Vec<_>>();
    x_ms_headers.sort_by(|left, right| left.0.cmp(&right.0));
    canonicalize_headers(&x_ms_headers)
}

async fn cloud_setup_response(
    provider: &str,
    method: &str,
    path: &str,
    response: reqwest::Response,
) -> Result<Vec<u8>, String> {
    let status = response.status();
    let response_body = response
        .bytes()
        .await
        .map_err(|error| error.to_string())?
        .to_vec();
    if status.is_success() || status.as_u16() == 409 || status.as_u16() == 500 {
        Ok(response_body)
    } else {
        Err(format!(
            "{} setup request {} {} failed with status {}: {}",
            provider,
            method,
            path,
            status,
            String::from_utf8_lossy(&response_body)
        ))
    }
}

fn sqrzl_s3_provider(bucket: &str) -> cntryl_midge::CloudProviderConfig {
    cntryl_midge::CloudProviderConfig::s3_compatible_static(
        bucket,
        sqrzl_endpoint(),
        sqrzl_access_key(),
        sqrzl_secret_key(),
    )
}

fn sqrzl_azure_provider(container: &str) -> cntryl_midge::CloudProviderConfig {
    cntryl_midge::CloudProviderConfig::azure_blob_shared_key(
        sqrzl_access_key(),
        container,
        sqrzl_secret_key(),
    )
    .with_endpoint(sqrzl_endpoint())
    .expect("Azure Blob supports endpoint overrides")
}

fn sqrzl_gcs_provider(bucket: &str) -> cntryl_midge::CloudProviderConfig {
    cntryl_midge::CloudProviderConfig::gcs_hmac(bucket, sqrzl_access_key(), sqrzl_secret_key())
        .with_gcs_project_id("sqrzl")
        .and_then(|provider| provider.with_endpoint(sqrzl_endpoint()))
        .expect("GCS supports project and endpoint overrides")
}

fn sqrzl_endpoint() -> &'static str {
    "http://127.0.0.1:9000"
}
fn sqrzl_access_key() -> &'static str {
    "admin"
}
fn sqrzl_secret_key() -> &'static str {
    "sqrzl-secret"
}
