//! An offline, catalog-authorized fixture using the pinned engine's codecs.
//! Construct outside the broker cap; recover using the ordinary runtime image.
use super::{
    support::{Broker, Campaign},
    workload, CAMPAIGN_LIMIT, MIB,
};
use crate::fixtures::transport::{
    build_kv_begin, build_kv_get, build_kv_rollback, extract_kv_value, parse_kv_tx_id,
};
use cntryl_midge::__internal::wal::{
    cloud_segment_object_key, encoding, frame, WalOpKind, WalRecord,
};
use cntryl_midge::{Engine, MemoryBudget, OpenOptions, TransactionMode, WriteOptions};
use fitz::utils::storage_key::{self, DomainKeyspace};
use serde_json::json;
use std::time::Duration;

const RECORDS: u32 = 40_960;
const ROUTE: &str = "kv://test/s3/large";

fn scoped_key(index: u32) -> Vec<u8> {
    let mut encoder = storage_key::domain_marker_encoder("test", DomainKeyspace::Kv, 1, 16);
    storage_key::encode_bytes_segment_into(&mut encoder, b"s3");
    storage_key::encode_bytes_segment_into(&mut encoder, b"large");
    let mut key = encoder.into_vec();
    key.extend_from_slice(&index.to_be_bytes());
    key
}

pub(super) fn publish(campaign: &Campaign) -> u64 {
    let cache = tempfile::tempdir().unwrap();
    let options = OpenOptions::cloud(cache.path(), campaign.location())
        .memory_budget(MemoryBudget::Bytes(256 * 1024 * 1024))
        .lease_ttl(Duration::from_secs(59))
        .build()
        .unwrap();
    let mut engine = Engine::open(options).unwrap();
    let cf = engine.create_column_family("tenant_default").unwrap();
    assert_eq!(cf.id(), 1);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(scoped_key(u32::MAX), b"seed".to_vec(), None)
        .unwrap();
    tx.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(&cf).unwrap();
    let mut sequence = engine
        .metrics()
        .get_storage_layout()
        .unwrap()
        .manifest_last_persisted_sequence
        + 1;
    let mut segment_id = engine
        .metrics()
        .get_runtime_metrics()
        .unwrap()
        .wal_current_segment_id
        + 1;
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);

    // No writer runs while this immutable synthetic fixture is published.
    let mut catalog: serde_json::Value =
        serde_json::from_slice(&campaign.download_object("wal/publication-catalog.v1.json"))
            .unwrap();
    let epoch = catalog["fencing_epoch"].as_u64().unwrap();
    let segments = catalog["segments"].as_object_mut().unwrap();
    segment_id = segment_id.max(
        segments
            .keys()
            .map(|id| id.parse::<u64>().unwrap())
            .max()
            .unwrap_or(0)
            + 1,
    );
    let mut bytes = Vec::new();
    let mut actual_bytes = 0;
    for index in 0..RECORDS {
        let mut record = WalRecord::new(
            WalOpKind::Put,
            scoped_key(index).into(),
            Some(workload::body(index).into()),
            sequence,
            epoch,
        );
        record.cf_id = cf.id();
        frame::append_frame(&mut bytes, &encoding::encode(&record).unwrap()).unwrap();
        sequence += 1;
        if bytes.len() >= 16 * 1024 * 1024 || index + 1 == RECORDS {
            let object_key = cloud_segment_object_key(segment_id, epoch);
            campaign.upload_object(&object_key, &bytes);
            segments.insert(segment_id.to_string(), json!({
                "segment_id": segment_id, "writer_epoch": epoch, "max_sequence": sequence - 1,
                "size_bytes": bytes.len(), "content_crc32c": crc32c::crc32c(&bytes), "object_key": object_key,
            }));
            actual_bytes += u64::try_from(bytes.len()).unwrap();
            bytes.clear();
            segment_id += 1;
        }
    }
    let bytes = serde_json::to_vec(&catalog).unwrap();
    campaign.upload_object("wal/publication-catalog.v1.json", &bytes);
    campaign.upload_object("wal/publication-catalog.v1.mirror.json", &bytes);
    actual_bytes
}

pub(super) async fn verify(broker: &Broker) {
    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..16 {
        let address = broker.address();
        workers.spawn(async move {
            let mut client = super::super::connect(address).await;
            let started =
                super::super::request(&mut client, &build_kv_begin(ROUTE, 0, 1), 100).await;
            let id = parse_kv_tx_id(&started).unwrap();
            let mut verified = 0;
            for index in (worker..RECORDS).step_by(16) {
                let value = super::super::request(
                    &mut client,
                    &build_kv_get(id, ROUTE, &index.to_be_bytes()),
                    103,
                )
                .await;
                assert_eq!(extract_kv_value(&value).unwrap(), workload::body(index));
                verified += 1;
            }
            super::super::request(&mut client, &build_kv_rollback(id, ROUTE), 102).await;
            verified
        });
    }
    let mut verified = 0;
    while let Some(result) = workers.join_next().await {
        verified += result.unwrap();
    }
    assert_eq!(verified, RECORDS);
}

async fn recover() -> serde_json::Value {
    let campaign = Campaign::new("large");
    let wal_bytes = publish(&campaign);
    let backlog = campaign.snapshot("large-backlog");
    assert!(wal_bytes > 512 * MIB);
    assert!(backlog.catalog_bytes >= wal_bytes);
    let mut broker = Broker::start(&campaign);
    let ready_seconds = broker.ready().await;
    verify(&broker).await;
    super::super::smoke(broker.address(), &format!("large-{}", campaign.id)).await;
    let after = super::super::inspect_container(&broker.name);
    let report = json!({ "synthetic_offline_wal": true, "wal_bytes": wal_bytes,
        "exact_kv_values_verified": RECORDS, "ready_seconds": ready_seconds,
        "backlog": backlog, "after": after });
    campaign.report(&report);
    broker.remove();
    report
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker, AWS CLI and pinned Sqrzl; CI runs explicitly"]
async fn should_recover_s3_wal_larger_than_container_memory_from_empty_cache() {
    // Arrange
    let deadline = CAMPAIGN_LIMIT;

    // Act
    let report = tokio::time::timeout(deadline, recover()).await.unwrap();

    // Assert
    assert_eq!(report["exact_kv_values_verified"], RECORDS);
}
