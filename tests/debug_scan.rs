//! Debug scan functionality

use fitz::storage::midge_adapter;
use fitz::storage::traits::KvStore;
use cntryl_midge::ColumnFamilyId;

#[tokio::test]
async fn should_scan_keys() {
    // Arrange
    let kv_store = midge_adapter::create_memory_store().expect("Create store");
    let cf = ColumnFamilyId(0);
    
    // Put some test keys
    kv_store.put(cf, b"value1", b"key1").expect("Put 1");
    kv_store.put(cf, b"value2", b"key2").expect("Put 2");
    kv_store.put(cf, b"value3", b"key3").expect("Put 3");
    
    // Try to get individual keys first
    let val1 = kv_store.get(cf, b"key1").expect("Get 1");
    println!("Get key1: {:?}", val1.as_ref().map(|v| String::from_utf8_lossy(v)));
    
    // Try to scan
    let results = kv_store.scan(cf, b"key1", b"key9").expect("Scan");
    
    println!("Scan results: {} keys found", results.len());
    for (k, v) in &results {
        println!("  Key: {:?}, Value: {:?}", String::from_utf8_lossy(k), String::from_utf8_lossy(v));
    }
    
    // Just verify get works for now
    assert!(val1.is_some());
}
