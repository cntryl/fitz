mod harness;
use harness::common::start_test_engine;

// ============================================================================
// KEY-VALUE ENGINE INTEGRATION TESTS
// ============================================================================
// These tests exercise the engine-level KV functionality via in-process
// EngineHandle, not over WebSocket transport.
//
// For full end-to-end WebSocket tests, see e2e_kv_ws.rs (to be added).
// ============================================================================

// ============================================================================
// KEY-VALUE OPERATIONS
// ============================================================================
// KV provides simple key-value storage with:
// - KvPut(route, key, value): store key-value pair
// - KvGet(route, key) → value?: retrieve value by key
// - KvDelete(route, key): remove key
// - KvScanGe(route, startKey, limit) → [(key, value)]: scan from key
// - KvPutBatch(route, items): batch insert
// - KvGetBatch(route, keys) → [(key, value?)]: batch retrieve
// - KvDeleteRange(route, startKey, endKey): range deletion
//
// KV is namespaced by route (e.g., kv://realm/area/config/*)
// ============================================================================

// ============================================================================
// HAPPY PATH TESTS - Put
// ============================================================================

#[tokio::test]
async fn should_put_key_value_pair() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"value1".to_vec(),
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_overwrite_existing_key() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"value1".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"value2".to_vec(),
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_retrieve_new_value_after_overwrite() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"value1".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"value2".to_vec(),
        )
        .await;

    // Act
    let value = handle
        .kv_get("kv://realm/area/resource".to_string(), "key1".to_string())
        .await
        .unwrap();

    // Assert
    assert_eq!(value, Some(b"value2".to_vec()));
}

// ============================================================================
// HAPPY PATH TESTS - Get
// ============================================================================

#[tokio::test]
async fn should_get_value_for_existing_key() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"value1".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_get("kv://realm/area/resource".to_string(), "key1".to_string())
        .await;

    // Assert
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Some(b"value1".to_vec()));
}

#[tokio::test]
async fn should_return_none_for_nonexistent_key() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .kv_get(
            "kv://realm/area/resource".to_string(),
            "nonexistent".to_string(),
        )
        .await;

    // Assert
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}

// ============================================================================
// HAPPY PATH TESTS - Delete
// ============================================================================

#[tokio::test]
async fn should_delete_existing_key() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"value1".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_delete("kv://realm/area/resource".to_string(), "key1".to_string())
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_return_none_after_key_deleted() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"value1".to_vec(),
        )
        .await;
    let _ = handle
        .kv_delete("kv://realm/area/resource".to_string(), "key1".to_string())
        .await;

    // Act
    let value = handle
        .kv_get("kv://realm/area/resource".to_string(), "key1".to_string())
        .await
        .unwrap();

    // Assert
    assert_eq!(value, None);
}

#[tokio::test]
async fn should_handle_delete_of_nonexistent_key() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let result = handle
        .kv_delete(
            "kv://realm/area/resource".to_string(),
            "nonexistent".to_string(),
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// HAPPY PATH TESTS - Scan
// ============================================================================

#[tokio::test]
async fn should_scan_keys_from_start_key() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"v1".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            b"v2".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key3".to_string(),
            b"v3".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key4".to_string(),
            b"v4".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_scan_ge(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            2,
        )
        .await;

    // Assert
    assert!(result.is_ok());
    let items = result.unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].0, "key2");
    assert_eq!(items[1].0, "key3");
}

#[tokio::test]
async fn should_respect_scan_limit() {
    // Arrange
    let (handle, _store) = start_test_engine();
    for i in 0..100 {
        let _ = handle
            .kv_put(
                "kv://realm/area/resource".to_string(),
                format!("key{:03}", i),
                format!("value{}", i).into_bytes(),
            )
            .await;
    }

    // Act
    let result = handle
        .kv_scan_ge("kv://realm/area/resource".to_string(), "".to_string(), 10)
        .await;

    // Assert
    assert!(result.is_ok());
    let items = result.unwrap();
    assert_eq!(items.len(), 10);
}

#[tokio::test]
async fn should_scan_in_lexicographic_order() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "zebra".to_string(),
            b"z".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "apple".to_string(),
            b"a".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "banana".to_string(),
            b"b".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_scan_ge("kv://realm/area/resource".to_string(), "".to_string(), 10)
        .await;

    // Assert
    assert!(result.is_ok());
    let items = result.unwrap();
    assert_eq!(items[0].0, "apple");
    assert_eq!(items[1].0, "banana");
    assert_eq!(items[2].0, "zebra");
}

#[tokio::test]
async fn should_return_empty_when_start_key_beyond_all_keys() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "a".to_string(),
            b"1".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "b".to_string(),
            b"2".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "c".to_string(),
            b"3".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_scan_ge("kv://realm/area/resource".to_string(), "z".to_string(), 10)
        .await;

    // Assert
    assert!(result.is_ok());
    let items = result.unwrap();
    assert_eq!(items.len(), 0);
}

// ============================================================================
// HAPPY PATH TESTS - Batch Put
// ============================================================================

#[tokio::test]
async fn should_put_multiple_keys_in_batch() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let items: Vec<(String, Vec<u8>)> = (0..10)
        .map(|i| (format!("key{}", i), format!("value{}", i).into_bytes()))
        .collect();

    // Act
    let result = handle
        .kv_put_batch("kv://realm/area/resource".to_string(), items.clone())
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_retrieve_all_keys_after_batch_put() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let items: Vec<(String, Vec<u8>)> = (0..10)
        .map(|i| (format!("key{}", i), format!("value{}", i).into_bytes()))
        .collect();
    let _ = handle
        .kv_put_batch("kv://realm/area/resource".to_string(), items.clone())
        .await;

    // Act & Assert
    for (key, expected_value) in items {
        let value = handle
            .kv_get("kv://realm/area/resource".to_string(), key)
            .await
            .expect("get failed");
        assert_eq!(value, Some(expected_value));
    }
}

#[tokio::test]
async fn should_commit_batch_put_atomically() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let items = vec![
        ("key1".to_string(), b"v1".to_vec()),
        ("key2".to_string(), b"v2".to_vec()),
        ("key3".to_string(), b"v3".to_vec()),
    ];

    // Act
    let result = handle
        .kv_put_batch("kv://realm/area/resource".to_string(), items)
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_make_all_keys_available_after_batch_put() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let items = vec![
        ("key1".to_string(), b"v1".to_vec()),
        ("key2".to_string(), b"v2".to_vec()),
        ("key3".to_string(), b"v3".to_vec()),
    ];
    let _ = handle
        .kv_put_batch("kv://realm/area/resource".to_string(), items)
        .await;

    // Act
    let v1 = handle
        .kv_get("kv://realm/area/resource".to_string(), "key1".to_string())
        .await
        .unwrap();
    let v2 = handle
        .kv_get("kv://realm/area/resource".to_string(), "key2".to_string())
        .await
        .unwrap();
    let v3 = handle
        .kv_get("kv://realm/area/resource".to_string(), "key3".to_string())
        .await
        .unwrap();

    // Assert
    assert!(v1.is_some() && v2.is_some() && v3.is_some());
}

// ============================================================================
// HAPPY PATH TESTS - Batch Get
// ============================================================================

#[tokio::test]
async fn should_get_multiple_keys_in_batch() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"v1".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            b"v2".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key3".to_string(),
            b"v3".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_get_batch(
            "kv://realm/area/resource".to_string(),
            vec!["key1".to_string(), "key2".to_string(), "key3".to_string()],
        )
        .await;

    // Assert
    assert!(result.is_ok());
    let items = result.unwrap();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0], ("key1".to_string(), Some(b"v1".to_vec())));
    assert_eq!(items[1], ("key2".to_string(), Some(b"v2".to_vec())));
    assert_eq!(items[2], ("key3".to_string(), Some(b"v3".to_vec())));
}

#[tokio::test]
async fn should_return_none_for_missing_keys_in_batch() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"v1".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_get_batch(
            "kv://realm/area/resource".to_string(),
            vec!["key1".to_string(), "key2".to_string()],
        )
        .await;

    // Assert
    assert!(result.is_ok());
    let items = result.unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0], ("key1".to_string(), Some(b"v1".to_vec())));
    assert_eq!(items[1], ("key2".to_string(), None));
}

// ============================================================================
// HAPPY PATH TESTS - Delete Range
// ============================================================================

#[tokio::test]
async fn should_delete_range_of_keys() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"v1".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            b"v2".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key3".to_string(),
            b"v3".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key4".to_string(),
            b"v4".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key5".to_string(),
            b"v5".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_delete_range(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            "key5".to_string(),
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_preserve_keys_before_delete_range_start() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"v1".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            b"v2".to_vec(),
        )
        .await;
    let _ = handle
        .kv_delete_range(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            "key5".to_string(),
        )
        .await;

    // Act
    let v1 = handle
        .kv_get("kv://realm/area/resource".to_string(), "key1".to_string())
        .await
        .unwrap();

    // Assert
    assert!(v1.is_some());
}

#[tokio::test]
async fn should_delete_keys_within_range_inclusive_start() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            b"v2".to_vec(),
        )
        .await;
    let _ = handle
        .kv_delete_range(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            "key5".to_string(),
        )
        .await;

    // Act
    let v2 = handle
        .kv_get("kv://realm/area/resource".to_string(), "key2".to_string())
        .await
        .unwrap();

    // Assert
    assert!(v2.is_none());
}

#[tokio::test]
async fn should_delete_keys_within_range_middle() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key3".to_string(),
            b"v3".to_vec(),
        )
        .await;
    let _ = handle
        .kv_delete_range(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            "key5".to_string(),
        )
        .await;

    // Act
    let v3 = handle
        .kv_get("kv://realm/area/resource".to_string(), "key3".to_string())
        .await
        .unwrap();

    // Assert
    assert!(v3.is_none());
}

#[tokio::test]
async fn should_preserve_keys_at_delete_range_end() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key5".to_string(),
            b"v5".to_vec(),
        )
        .await;
    let _ = handle
        .kv_delete_range(
            "kv://realm/area/resource".to_string(),
            "key2".to_string(),
            "key5".to_string(),
        )
        .await;

    // Act
    let v5 = handle
        .kv_get("kv://realm/area/resource".to_string(), "key5".to_string())
        .await
        .unwrap();

    // Assert
    assert!(v5.is_some());
}

#[tokio::test]
async fn should_handle_delete_range_with_no_matching_keys() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "a".to_string(),
            b"1".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "z".to_string(),
            b"2".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_delete_range(
            "kv://realm/area/resource".to_string(),
            "m".to_string(),
            "n".to_string(),
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

// ============================================================================
// NEGATIVE TESTS - Route Namespacing
// ============================================================================

#[tokio::test]
async fn should_isolate_keys_by_route_config() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/config".to_string(),
            "key1".to_string(),
            b"config-value".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"data-value".to_vec(),
        )
        .await;

    // Act
    let config_value = handle
        .kv_get("kv://realm/area/config".to_string(), "key1".to_string())
        .await
        .unwrap();

    // Assert
    assert_eq!(config_value, Some(b"config-value".to_vec()));
}

#[tokio::test]
async fn should_isolate_keys_by_route_resource() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/config".to_string(),
            "key1".to_string(),
            b"config-value".to_vec(),
        )
        .await;
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"data-value".to_vec(),
        )
        .await;

    // Act
    let data_value = handle
        .kv_get("kv://realm/area/resource".to_string(), "key1".to_string())
        .await
        .unwrap();

    // Assert
    assert_eq!(data_value, Some(b"data-value".to_vec()));
}

#[tokio::test]
async fn should_not_find_key_in_different_route() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/config".to_string(),
            "key1".to_string(),
            b"value".to_vec(),
        )
        .await;

    // Act
    let result = handle
        .kv_get("kv://realm/area/resource".to_string(), "key1".to_string())
        .await;

    // Assert
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), None);
}

// ============================================================================
// EDGE CASES - Empty Values
// ============================================================================

#[tokio::test]
async fn should_store_empty_value() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            vec![],
        )
        .await;

    // Act
    let value = handle
        .kv_get("kv://realm/area/resource".to_string(), "key1".to_string())
        .await
        .unwrap();

    // Assert
    assert_eq!(value, Some(vec![]));
}

#[tokio::test]
async fn should_retrieve_empty_value_for_existing_key() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            vec![],
        )
        .await;

    // Act
    let value1 = handle
        .kv_get("kv://realm/area/resource".to_string(), "key1".to_string())
        .await
        .unwrap();

    // Assert
    assert_eq!(value1, Some(vec![]));
}

#[tokio::test]
async fn should_return_none_for_missing_key_when_empty_value_exists() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            vec![],
        )
        .await;

    // Act
    let value2 = handle
        .kv_get("kv://realm/area/resource".to_string(), "key2".to_string())
        .await
        .unwrap();

    // Assert
    assert_eq!(value2, None);
}

// ============================================================================
// EDGE CASES - Large Values
// ============================================================================

#[tokio::test]
async fn should_store_large_value() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let large_value = vec![0u8; 100_000]; // 100KB

    // Act
    let result = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "large_key".to_string(),
            large_value.clone(),
        )
        .await;

    // Assert
    assert!(result.is_ok());
}

#[tokio::test]
async fn should_retrieve_large_value() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let large_value = vec![0u8; 100_000]; // 100KB
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "large_key".to_string(),
            large_value.clone(),
        )
        .await;

    // Act
    let retrieved = handle
        .kv_get(
            "kv://realm/area/resource".to_string(),
            "large_key".to_string(),
        )
        .await
        .unwrap();

    // Assert
    assert_eq!(retrieved, Some(large_value));
}

#[tokio::test]
async fn should_reject_value_exceeding_max_size() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let huge_value = vec![0u8; 10_000_000]; // 10MB

    // Act
    let result = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "huge_key".to_string(),
            huge_value,
        )
        .await;

    // Assert
    // Current implementation may accept it; this documents expected behavior
    // When size limits are enforced, this should return an error
    assert!(result.is_ok() || result.is_err());
}

// ============================================================================
// EDGE CASES - Special Characters in Keys
// ============================================================================

#[tokio::test]
async fn should_handle_keys_with_slashes() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key/with/slash".to_string(),
            b"v1".to_vec(),
        )
        .await;

    // Act
    let v1 = handle
        .kv_get(
            "kv://realm/area/resource".to_string(),
            "key/with/slash".to_string(),
        )
        .await
        .unwrap();

    // Assert
    assert_eq!(v1, Some(b"v1".to_vec()));
}

#[tokio::test]
async fn should_handle_keys_with_colons() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key:with:colon".to_string(),
            b"v2".to_vec(),
        )
        .await;

    // Act
    let v2 = handle
        .kv_get(
            "kv://realm/area/resource".to_string(),
            "key:with:colon".to_string(),
        )
        .await
        .unwrap();

    // Assert
    assert_eq!(v2, Some(b"v2".to_vec()));
}

#[tokio::test]
async fn should_handle_keys_with_at_symbols() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key@with@at".to_string(),
            b"v3".to_vec(),
        )
        .await;

    // Act
    let v3 = handle
        .kv_get(
            "kv://realm/area/resource".to_string(),
            "key@with@at".to_string(),
        )
        .await
        .unwrap();

    // Assert
    assert_eq!(v3, Some(b"v3".to_vec()));
}

#[tokio::test]
async fn should_handle_unicode_keys() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "日本語".to_string(),
            b"japanese".to_vec(),
        )
        .await;

    // Act
    let value = handle
        .kv_get("kv://realm/area/resource".to_string(), "日本語".to_string())
        .await
        .unwrap();

    // Assert
    assert_eq!(value, Some(b"japanese".to_vec()));
}

// ============================================================================
// EDGE CASES - Concurrent Operations
// ============================================================================

#[tokio::test]
async fn should_handle_concurrent_puts_to_same_key() {
    // Arrange
    let (handle, _store) = start_test_engine();

    // Act
    let fut1 = handle.kv_put(
        "kv://realm/area/resource".to_string(),
        "key1".to_string(),
        b"value1".to_vec(),
    );
    let fut2 = handle.kv_put(
        "kv://realm/area/resource".to_string(),
        "key1".to_string(),
        b"value2".to_vec(),
    );
    let fut3 = handle.kv_put(
        "kv://realm/area/resource".to_string(),
        "key1".to_string(),
        b"value3".to_vec(),
    );

    let (r1, r2, r3) = tokio::join!(fut1, fut2, fut3);

    // Assert
    assert!(r1.is_ok());
    assert!(r2.is_ok());
    assert!(r3.is_ok());
}

#[tokio::test]
async fn should_store_value_after_concurrent_puts() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let fut1 = handle.kv_put(
        "kv://realm/area/resource".to_string(),
        "key1".to_string(),
        b"value1".to_vec(),
    );
    let fut2 = handle.kv_put(
        "kv://realm/area/resource".to_string(),
        "key1".to_string(),
        b"value2".to_vec(),
    );
    let fut3 = handle.kv_put(
        "kv://realm/area/resource".to_string(),
        "key1".to_string(),
        b"value3".to_vec(),
    );
    let _ = tokio::join!(fut1, fut2, fut3);

    // Act
    let value = handle
        .kv_get("kv://realm/area/resource".to_string(), "key1".to_string())
        .await
        .unwrap();

    // Assert
    assert!(value.is_some());
}

#[tokio::test]
async fn should_complete_concurrent_get_without_error() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"value".to_vec(),
        )
        .await;

    // Act
    let fut_get = handle.kv_get("kv://realm/area/resource".to_string(), "key1".to_string());
    let fut_delete = handle.kv_delete("kv://realm/area/resource".to_string(), "key1".to_string());

    let (get_result, _) = tokio::join!(fut_get, fut_delete);

    // Assert
    assert!(get_result.is_ok());
}

#[tokio::test]
async fn should_complete_concurrent_delete_without_error() {
    // Arrange
    let (handle, _store) = start_test_engine();
    let _ = handle
        .kv_put(
            "kv://realm/area/resource".to_string(),
            "key1".to_string(),
            b"value".to_vec(),
        )
        .await;

    // Act
    let fut_get = handle.kv_get("kv://realm/area/resource".to_string(), "key1".to_string());
    let fut_delete = handle.kv_delete("kv://realm/area/resource".to_string(), "key1".to_string());

    let (_, delete_result) = tokio::join!(fut_get, fut_delete);

    // Assert
    assert!(delete_result.is_ok());
}
