//! Stream domain integration tests

use fitz::core::stream::{StreamDomain, StreamEvent};
use fitz::storage::midge_adapter;

const TEST_RF: u32 = 0;

#[tokio::test]
async fn should_append_and_read_event() {
    // Arrange
    let kv_store = midge_adapter::create_memory_store().expect("Create store");
    let domain = StreamDomain::new(kv_store);
    let service = domain.get_service();
    
    let event = StreamEvent {
        sequence: 0,
        resource: "test-resource".to_string(),
        area_seq: None,
        body: vec![1, 2, 3],
        metadata: None,
        created_at: 1234567890,
        is_end: false,
    };
    
    // Act - Begin transaction
    let svc = service.read().await;
    let txn = svc.begin_append(TEST_RF, "test-realm", "test-area", "test-resource").await.expect("Begin");
    svc.append_event(txn, TEST_RF, event).await.expect("Append");
    let (first_seq, last_seq, count) = svc.commit_append(txn, TEST_RF).await.expect("Commit");
    drop(svc);

    // Assert
    assert_eq!(first_seq, 0);
    assert_eq!(last_seq, 0);
    assert_eq!(count, 1);
    
    // Act - Read event back
    let svc = service.read().await;
    let read_result = svc.read(TEST_RF, "test-realm", "test-area", "test-resource", 0, 10).await;
    drop(svc);

    // Assert
    assert!(read_result.is_ok());
    let events = read_result.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].body, vec![1, 2, 3]);
}

#[tokio::test]
async fn should_respect_watermark_in_area_read() {
    // Arrange
    let kv_store = midge_adapter::create_memory_store().expect("Create store");
    let domain = StreamDomain::new(kv_store);
    let service = domain.get_service();
    
    let event = StreamEvent {
        sequence: 0,
        resource: "resource1".to_string(),
        area_seq: None,
        body: vec![1, 2, 3],
        metadata: None,
        created_at: 1234567890,
        is_end: false,
    };
    
    // Act - Append and commit event
    let svc = service.read().await;
    let txn = svc.begin_append(TEST_RF, "realm1", "area1", "resource1").await.expect("Begin");
    svc.append_event(txn, TEST_RF, event).await.expect("Append");
    svc.commit_append(txn, TEST_RF).await.expect("Commit");
    
    // Act - Read area
    let read_result = svc.read_area(TEST_RF, "realm1", "area1", 0, 10).await;
    drop(svc);
    
    // Assert
    assert!(read_result.is_ok());
    let events = read_result.unwrap();
    assert_eq!(events.len(), 1);
    
    // Act - Try to read ahead of watermark
    let read_ahead = service.read().await.read_area(TEST_RF, "realm1", "area1", 100, 10).await;
    
    // Assert - Should return empty
    assert!(read_ahead.is_ok());
    let events = read_ahead.unwrap();
    assert_eq!(events.len(), 0);
}

#[tokio::test]
async fn should_append_multiple_events_and_read_in_sequence() {
    // Arrange
    let kv_store = midge_adapter::create_memory_store().expect("Create store");
    let domain = StreamDomain::new(kv_store);
    let service = domain.get_service();
    
    // Act - Append 3 events in single transaction
    let svc = service.read().await;
    let txn = svc.begin_append(TEST_RF, "realm1", "area1", "resource1").await.expect("Begin");
    for i in 0..3 {
        let event = StreamEvent {
            sequence: i,
            resource: "resource1".to_string(),
            area_seq: None,
            body: vec![i as u8],
            metadata: None,
            created_at: 1234567890 + i,
            is_end: false,
        };
        svc.append_event(txn, TEST_RF, event).await.expect("Append");
    }
    svc.commit_append(txn, TEST_RF).await.expect("Commit");
    drop(svc);
    
    // Act - Read all events
    let read_result = service.read().await.read(TEST_RF, "realm1", "area1", "resource1", 0, 10).await;
    
    // Assert
    assert!(read_result.is_ok());
    let events = read_result.unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].body, vec![0]);
    assert_eq!(events[1].body, vec![1]);
    assert_eq!(events[2].body, vec![2]);
}

#[tokio::test]
async fn should_get_correct_watermark() {
    // Arrange
    let kv_store = midge_adapter::create_memory_store().expect("Create store");
    let domain = StreamDomain::new(kv_store);
    let service = domain.get_service();
    
    // Act - Get watermark before any events
    let watermark = service.read().await.get_watermark(TEST_RF, "realm1", "area1").await;
    
    // Assert
    assert!(watermark.is_ok());
    assert_eq!(watermark.unwrap(), 0);
    
    // Act - Append event and commit
    let svc = service.read().await;
    let event = StreamEvent {
        sequence: 0,
        resource: "resource1".to_string(),
        area_seq: None,
        body: vec![1],
        metadata: None,
        created_at: 1234567890,
        is_end: false,
    };
    let txn = svc.begin_append(TEST_RF, "realm1", "area1", "resource1").await.expect("Begin");
    svc.append_event(txn, TEST_RF, event).await.expect("Append");
    svc.commit_append(txn, TEST_RF).await.expect("Commit");
    drop(svc);
    
    // Act - Get watermark after commit
    let watermark = service.read().await.get_watermark(TEST_RF, "realm1", "area1").await;
    
    // Assert
    assert!(watermark.is_ok());
    assert_eq!(watermark.unwrap(), 0);
}
