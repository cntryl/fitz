//! Supported-format history with many small segments and a bounded logical state.
use cntryl_midge::__internal::wal::{
    catalog::{CatalogEntry, WalPublicationCatalog},
    segment::SegmentBuilder,
    WalOpKind, WalRecord,
};
use cntryl_midge::{Engine, MemoryBudget, OpenOptions, TransactionMode, WriteOptions};
use fitz::testkit::cloud_process::Campaign;
use fitz::utils::storage_key::{self, DomainKeyspace};
use serde::Serialize;
use std::time::Duration;

pub(super) const ROUTE: &str = "kv://test/catalog/state";
pub(super) const KEYS: u32 = 32;
// The logged decode request was 139,236,128 = 16 * encoded catalog + 4,096.
pub(super) const INCIDENT_CATALOG_BYTES: u64 = 8_702_002;

pub(super) fn key(slot: u32) -> Vec<u8> {
    let mut encoder = storage_key::domain_marker_encoder("test", DomainKeyspace::Kv, 1, 16);
    storage_key::encode_bytes_segment_into(&mut encoder, b"catalog");
    storage_key::encode_bytes_segment_into(&mut encoder, b"state");
    let mut key = encoder.into_vec();
    key.extend_from_slice(&slot.to_be_bytes());
    key
}

pub(super) fn value(sequence: u32) -> Vec<u8> {
    sequence.to_be_bytes().repeat(32)
}

pub(super) fn expected(records: u32, slot: u32) -> Vec<u8> {
    let last = records - 1;
    value(last - (last - slot) % KEYS)
}

#[derive(Serialize)]
pub(super) struct Fixture {
    pub segments: u32,
    pub serialized_catalog_bytes: u64,
    pub wal_bytes: u64,
    pub logical_keys: u32,
}

pub(super) fn publish(campaign: &Campaign, segments: u32) -> Fixture {
    assert!(segments >= KEYS && segments.is_multiple_of(KEYS));
    let cache = tempfile::tempdir().unwrap();
    let mut engine = Engine::open(
        OpenOptions::cloud(cache.path(), campaign.location())
            .memory_budget(MemoryBudget::Bytes(256 * 1024 * 1024))
            .lease_ttl(Duration::from_secs(59))
            .build()
            .unwrap(),
    )
    .unwrap();
    let cf = engine.create_column_family("tenant_default").unwrap();
    assert_eq!(cf.id(), 1);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    tx.put(key(u32::MAX), b"seed".to_vec(), None).unwrap();
    tx.commit(WriteOptions::cloud_strict()).unwrap();
    engine.flush_cf(&cf).unwrap();
    engine.shutdown(Duration::from_secs(30)).unwrap();
    drop(engine);

    // Fixture publication happens offline, with no active writer. It changes
    // neither engine limits nor the broker's normal rotation/flush settings.
    let mut catalog =
        WalPublicationCatalog::decode(&campaign.download_object("wal/publication-catalog.v1.json"))
            .unwrap();
    let epoch = catalog.fencing_epoch();
    let first_id = catalog.next_segment_id();
    let first_sequence = catalog.seq_floor().max(catalog.head().last_seq) + 1;
    let mut previous = catalog.head();
    let files = tempfile::tempdir().unwrap();
    let mut entries = Vec::with_capacity(usize::try_from(segments).unwrap());
    let mut wal_bytes = 0;
    for index in 0..segments {
        let sequence = first_sequence + u64::from(index);
        let mut record = WalRecord::new(
            WalOpKind::Put,
            key(index % KEYS).into(),
            Some(value(index).into()),
            sequence,
        );
        record.cf_id = cf.id();
        let (bytes, link) = SegmentBuilder::new(first_id + u64::from(index), epoch, previous)
            .first_seq(sequence)
            .record(&record)
            .seal();
        let entry = CatalogEntry::from_sealed_bytes(&bytes).unwrap();
        let path = files.path().join(entry.object_key());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, &bytes).unwrap();
        wal_bytes += u64::try_from(bytes.len()).unwrap();
        entries.push(entry);
        previous = link;
    }
    catalog.append(epoch, &entries).unwrap();
    let encoded = catalog.encode().unwrap();
    // Upload every immutable segment before installing either authority copy.
    campaign.upload_directory(files.path());
    campaign.upload_object("wal/publication-catalog.v1.json", &encoded);
    campaign.upload_object("wal/publication-catalog.v1.mirror.json", &encoded);
    Fixture {
        segments,
        serialized_catalog_bytes: u64::try_from(encoded.len()).unwrap(),
        wal_bytes,
        logical_keys: KEYS,
    }
}
