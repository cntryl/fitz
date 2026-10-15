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
    catalog::{CatalogEntry, WalPublicationCatalog},
    segment::SegmentBuilder,
    WalOpKind, WalRecord,
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

pub(super) fn publish(campaign: &Campaign, records: u32) -> u64 {
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
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);

    // No writer runs while this immutable synthetic fixture is published.
    let mut catalog =
        WalPublicationCatalog::decode(&campaign.download_object("wal/publication-catalog.v1.json"))
            .unwrap();
    let epoch = catalog.fencing_epoch();
    let mut segment_id = catalog.next_segment_id();
    let mut sequence = catalog.seq_floor().max(catalog.head().last_seq) + 1;
    let mut builder = SegmentBuilder::new(segment_id, epoch, catalog.head()).first_seq(sequence);
    let mut actual_bytes = 0;
    for index in 0..records {
        let mut record = WalRecord::new(
            WalOpKind::Put,
            scoped_key(index).into(),
            Some(workload::body(index).into()),
            sequence,
        );
        record.cf_id = cf.id();
        builder = builder.record(&record);
        sequence += 1;
        if builder.body_len() >= 16 * 1024 * 1024 || index + 1 == records {
            let (bytes, link) = builder.seal();
            let entry = CatalogEntry::from_sealed_bytes(&bytes).unwrap();
            campaign.upload_object(&entry.object_key(), &bytes);
            catalog.append(epoch, &[entry]).unwrap();
            actual_bytes += u64::try_from(bytes.len()).unwrap();
            segment_id += 1;
            builder = SegmentBuilder::new(segment_id, epoch, link).first_seq(sequence);
        }
    }
    let bytes = catalog.encode().unwrap();
    campaign.upload_object("wal/publication-catalog.v1.json", &bytes);
    campaign.upload_object("wal/publication-catalog.v1.mirror.json", &bytes);
    actual_bytes
}

#[test]
#[ignore = "requires disposable Sqrzl and AWS CLI; see constrained-runtime.md"]
fn should_recover_catalog_authorized_fixture_with_current_midge_format() {
    // Arrange
    let campaign = Campaign::new("fixture-format");
    let published_bytes = publish(&campaign, 32);
    let snapshot = campaign.snapshot("published");
    let cache = tempfile::tempdir().unwrap();

    // Act
    let mut engine = Engine::open(
        OpenOptions::cloud(cache.path(), campaign.location())
            .build()
            .unwrap(),
    )
    .unwrap();
    let tx = engine.begin_tx(1, TransactionMode::ReadOnly).unwrap();
    let values: Vec<_> = (0..32)
        .map(|index| tx.get(&scoped_key(index)).unwrap().unwrap())
        .collect();
    drop(tx);
    engine.shutdown(Duration::from_secs(30)).unwrap();

    // Assert
    assert!(snapshot.catalog_bytes >= published_bytes);
    assert_eq!(values.len(), 32);
    for (index, value) in (0..32).zip(values) {
        assert_eq!(value.as_ref(), workload::body(index));
    }
}

pub(super) async fn verify(broker: &Broker, records: u32) {
    let mut workers = tokio::task::JoinSet::new();
    for worker in 0..16 {
        let address = broker.address();
        workers.spawn(async move {
            let mut client = super::super::connect(address).await;
            let started = super::super::request(&mut client, &build_kv_begin(ROUTE, 0), 100).await;
            let id = parse_kv_tx_id(&started).unwrap();
            let mut verified = 0;
            for index in (worker..records).step_by(16) {
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
    assert_eq!(verified, records);
}

async fn recover() -> serde_json::Value {
    let campaign = Campaign::new("large");
    let wal_bytes = publish(&campaign, RECORDS);
    let backlog = campaign.snapshot("large-backlog");
    assert!(wal_bytes > 512 * MIB);
    assert!(backlog.catalog_bytes >= wal_bytes);
    let mut broker = Broker::start(&campaign);
    let ready_seconds = broker.ready().await;
    verify(&broker, RECORDS).await;
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
