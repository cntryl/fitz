use super::super::read_support::{load_global_locator_record, GlobalFragmentCache};
use super::*;

fn command_history(compact: bool) -> StreamStore {
    let store = StreamStore::new(create_test_engine_with_cfs(vec![1]));
    for offset in 0..64_u64 {
        let events = [EventPayload {
            body: Bytes::copy_from_slice(&offset.to_le_bytes()),
            metadata: None,
            discriminator: None,
        }];
        store
            .commit_records(CommitRecordsParams {
                family: 1,
                realm: "north",
                area: "orders",
                resource: "created",
                expected_resource_next_offset: offset,
                events: &events,
                ingest_metadata: None,
                mode: StreamWriteMode::Sync,
            })
            .expect("commit individual command");
    }
    if compact {
        drain_maintenance(&store, 1);
    }
    store
}

#[test]
fn should_reuse_compacted_global_fragment_buffers_for_stale_locator_hints() {
    // Arrange
    let store = command_history(true);
    let txn = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    assert!(txn
        .get(&encode_compact_global_page_key(31))
        .unwrap()
        .is_none());
    assert!(txn
        .get(&encode_compact_global_page_key(32))
        .unwrap()
        .is_none());
    let mut cache = GlobalFragmentCache::new();

    // Act
    let first = load_global_locator_record(&txn, 31, 31, &mut cache).unwrap();
    let second = load_global_locator_record(&txn, 32, 32, &mut cache).unwrap();

    // Assert
    assert_eq!(first.body.as_ref(), 31_u64.to_le_bytes());
    assert_eq!(second.body.as_ref(), 32_u64.to_le_bytes());
    assert_eq!(first.resource_offset, 31);
    assert_eq!(second.resource_offset, 32);
    assert_eq!(
        cache.get(&31).unwrap().1.records[0].body.as_ptr(),
        cache.get(&32).unwrap().1.records[0].body.as_ptr(),
        "stale hints must share the decoded compacted fragment"
    );
}

#[test]
fn should_reject_corrupt_direct_parent_even_when_compacted_bucket_is_cached() {
    // Arrange
    let store = command_history(true);
    let mut writer = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadWrite)
        .unwrap();
    writer
        .put(
            encode_compact_global_page_key(32),
            b"invalid fragment".to_vec(),
            None,
        )
        .unwrap();
    writer.commit(cntryl_midge::WriteOptions::sync()).unwrap();
    let txn = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let mut cache = GlobalFragmentCache::new();
    load_global_locator_record(&txn, 31, 31, &mut cache).expect("warm compacted parent");

    // Act
    let result = load_global_locator_record(&txn, 32, 32, &mut cache);

    // Assert
    assert!(
        result.is_err(),
        "cached compacted data must not bypass a present corrupt parent"
    );
}

#[test]
fn should_keep_uncompacted_global_parent_fragments_distinct() {
    // Arrange
    let store = command_history(false);
    let txn = store
        .db
        .begin_tx(1, cntryl_midge::TransactionMode::ReadOnly)
        .unwrap();
    let mut cache = GlobalFragmentCache::new();

    // Act
    let first = load_global_locator_record(&txn, 31, 31, &mut cache).unwrap();
    let second = load_global_locator_record(&txn, 32, 32, &mut cache).unwrap();

    // Assert
    assert_eq!(first.body.as_ref(), 31_u64.to_le_bytes());
    assert_eq!(second.body.as_ref(), 32_u64.to_le_bytes());
    assert_eq!(cache.get(&31).unwrap().0, 31);
    assert_eq!(cache.get(&32).unwrap().0, 32);
    assert_ne!(
        cache.get(&31).unwrap().1.records[0].body.as_ptr(),
        cache.get(&32).unwrap().1.records[0].body.as_ptr()
    );
}
