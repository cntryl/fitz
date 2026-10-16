//! Representative logical migration; not a general exporter or cutover tool.
#[path = "migration_stream.rs"]
mod stream;
use super::*;
use fitz::testkit::cloud_process::{Broker, Campaign};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::process::Command;

const OLD_IMAGE: &str =
    "ghcr.io/cntryl/fitz@sha256:0686055f39f349aeec5e992e2ac19c4b02b585edd8dd7d5f7482eaba8c7e4804";
const ROUTE: &str = "kv://migration/reference/state";
type Records = BTreeMap<Vec<u8>, Vec<u8>>;

fn begin(legacy: bool, mode: u8) -> Vec<u8> {
    if !legacy {
        return build_kv_begin(ROUTE, mode);
    }
    let mut payload = Vec::new();
    payload.extend_from_slice(&u32::try_from(ROUTE.len()).unwrap().to_be_bytes());
    payload.extend_from_slice(ROUTE.as_bytes());
    payload.extend_from_slice(&[mode, 1]);
    let mut builder = TlvFrameBuilder::new();
    builder.encode_field(100, &payload);
    builder.build()
}

async fn import(broker: &Broker, records: &Records, legacy: bool) {
    let mut client = connect(broker.address()).await;
    let started = request(&mut client, &begin(legacy, 1), 100).await;
    let tx = parse_kv_tx_id(&started).unwrap();
    for (key, value) in records {
        request(&mut client, &build_kv_put(tx, ROUTE, key, value), 104).await;
    }
    let commit = if legacy {
        let mut payload = tx.to_be_bytes().to_vec();
        payload.extend_from_slice(&u32::try_from(ROUTE.len()).unwrap().to_be_bytes());
        payload.extend_from_slice(ROUTE.as_bytes());
        let mut builder = TlvFrameBuilder::new();
        builder.encode_field(101, &payload);
        builder.build()
    } else {
        build_kv_commit(tx, ROUTE, 1)
    };
    request(&mut client, &commit, 101).await;
    client.close().await.unwrap();
}

async fn export(broker: &Broker, keys: &Records, legacy: bool) -> Records {
    let mut client = connect(broker.address()).await;
    let started = request(&mut client, &begin(legacy, 0), 100).await;
    let tx = parse_kv_tx_id(&started).unwrap();
    let mut records = Records::new();
    for key in keys.keys() {
        let read = request(&mut client, &build_kv_get(tx, ROUTE, key), 103).await;
        records.insert(key.clone(), extract_kv_value(&read).unwrap());
    }
    request(&mut client, &build_kv_rollback(tx, ROUTE), 102).await;
    client.close().await.unwrap();
    records
}

fn preserved_objects(campaign: &Campaign, phase: &str) -> BTreeMap<String, String> {
    let endpoint = std::env::var("FITZ_S3_TEST_ENDPOINT").unwrap();
    assert!(endpoint.starts_with("http://127.0.0.1:"));
    let output = Command::new("aws")
        .env("AWS_ACCESS_KEY_ID", "admin")
        .env("AWS_SECRET_ACCESS_KEY", "easy-peasy")
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .args([
            "--endpoint-url",
            &endpoint,
            "--region",
            "us-east-1",
            "--no-cli-pager",
            "s3api",
            "list-objects-v2",
            "--no-paginate",
            "--bucket",
            campaign.bucket(),
            "--prefix",
            campaign.prefix(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let listing: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        listing["IsTruncated"], false,
        "rollback copy must be complete"
    );
    let backup = campaign.directory.join(phase);
    std::fs::create_dir_all(&backup).unwrap();
    let mut hashes = BTreeMap::new();
    for object in listing["Contents"].as_array().expect("nonempty old store") {
        let key = object["Key"].as_str().unwrap();
        let relative = key.strip_prefix(campaign.prefix()).unwrap();
        let bytes = campaign.download_object(relative);
        let file = backup.join(relative);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, &bytes).unwrap();
        hashes.insert(relative.to_owned(), hex::encode(Sha256::digest(&bytes)));
    }
    std::fs::write(
        backup.join("object-hashes.json"),
        serde_json::to_vec_pretty(&hashes).unwrap(),
    )
    .unwrap();
    hashes
}

async fn rehearse() -> Value {
    let old = Campaign::new("migration-old");
    let mut original = Broker::start_with_image(&old, OLD_IMAGE);
    original.ready().await;
    let expected: Records = (0_u32..16)
        .map(|key| {
            (
                key.to_be_bytes().to_vec(),
                vec![u8::try_from(key).unwrap(); 16 * 1024],
            )
        })
        .collect();
    import(&original, &expected, true).await;
    let expected_history = stream::fixture();
    stream::replay(&original, &expected_history).await;
    let exported = export(&original, &expected, true).await;
    assert_eq!(exported, expected);
    let history = stream::export(&original).await;
    assert_eq!(history, expected_history);
    std::fs::write(
        old.directory.join("stream-export.json"),
        serde_json::to_vec_pretty(&history).unwrap(),
    )
    .unwrap();
    let logical_export: Vec<Value> = exported
        .iter()
        .map(|(key, value)| json!({"key_hex":hex::encode(key),"value_hex":hex::encode(value)}))
        .collect();
    std::fs::write(
        old.directory.join("logical-export.json"),
        serde_json::to_vec_pretty(&logical_export).unwrap(),
    )
    .unwrap();
    original.checkpoint();
    let preserved = preserved_objects(&old, "rollback-copy");

    let fresh = Campaign::new("migration-new");
    assert_ne!(old.bucket(), fresh.bucket());
    let mut replacement = Broker::start(&fresh);
    replacement.ready().await;
    import(&replacement, &exported, false).await;
    stream::replay(&replacement, &history).await;
    replacement.checkpoint();
    replacement.restart();
    let orderly_ready = replacement.ready().await;
    assert_eq!(export(&replacement, &expected, false).await, exported);
    assert_eq!(stream::export(&replacement).await, history);
    let crash = std::time::Instant::now();
    replacement.crash();
    replacement.remove();
    replacement = Broker::start(&fresh);
    let cold_ready = replacement.ready_since(crash).await;
    assert_eq!(export(&replacement, &expected, false).await, exported);
    assert_eq!(stream::export(&replacement).await, history);
    replacement.checkpoint();
    assert_eq!(
        preserved_objects(&old, "after-import-copy"),
        preserved,
        "migration must not mutate the original store"
    );
    original.restart();
    let rollback_ready = original.ready().await;
    assert_eq!(export(&original, &expected, true).await, exported);
    assert_eq!(stream::export(&original).await, history);
    original.checkpoint();
    let driver = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    assert!(driver.status.success());
    let driver_head = String::from_utf8(driver.stdout).unwrap();
    let report = json!({"status":"passed", "scope":"16 known non-expiring KV records and 8 Stream event bodies/metadata in order; replay creates new timestamps; no Stream TTL or application reconciliation claim", "driver_head":driver_head.trim(), "old_image":OLD_IMAGE, "old_container":stopped_container(&original.name), "new_container":stopped_container(&replacement.name), "exported_records":exported.len(), "exported_stream_events":history.len(), "preserved_objects":preserved, "orderly_ready_seconds":orderly_ready,"cold_ready_seconds":cold_ready,"rollback_ready_seconds":rollback_ready});
    fresh.report(&report);
    original.remove();
    replacement.remove();
    report
}

fn stopped_container(name: &str) -> Value {
    let output = Command::new("docker")
        .args(["inspect", name])
        .output()
        .unwrap();
    assert!(output.status.success());
    let containers: Vec<Value> = serde_json::from_slice(&output.stdout).unwrap();
    let container = containers.into_iter().next().unwrap();
    assert_eq!(container["State"]["Running"], false);
    assert_eq!(container["State"]["ExitCode"], 0);
    assert_eq!(container["State"]["OOMKilled"], false);
    container
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires immutable Fitz 0.1.0 and candidate images, Docker and disposable Sqrzl"]
async fn should_preserve_rollback_store_during_logical_migration() {
    // Arrange
    let deadline = Duration::from_mins(10);

    // Act
    let report = tokio::time::timeout(deadline, rehearse()).await.unwrap();

    // Assert
    assert_eq!(report["status"], "passed");
    assert_eq!(report["exported_records"], 16);
    assert_eq!(report["exported_stream_events"], 8);
}
