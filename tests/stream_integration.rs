//! Stream domain integration tests

use fitz::core::stream::{StreamDomain, StreamEvent};
use fitz::storage::midge_adapter;

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
    
    // Act - Append event
    let append_result = service.read().await.append_event(0, "test-area", "test-area", event).await;
    
    // Assert
    assert!(append_result.is_ok());
    let (resource_seq, area_seq) = append_result.unwrap();
    assert_eq!(resource_seq, 0);
    assert_eq!(area_seq, 0);
    
    // Act - Read event back
    let read_result = service.read().await.read(0, "test-area", "test-area", "test-resource", 0, 10).await;
    
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
    
    // Act - Append event
    let _ = service.read().await.append_event(0, "area1", "area1", event).await.expect("Append");
    
    // Act - Read area
    let read_result = service.read().await.read_area(0, "area1", "area1", 0, 10).await;
    
    // Assert
    assert!(read_result.is_ok());
    let events = read_result.unwrap();
    assert_eq!(events.len(), 1);
    
    // Act - Try to read ahead of watermark
    let read_ahead = service.read().await.read_area(0, "area1", "area1", 100, 10).await;
    
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
    
    // Act - Append 3 events
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
        let _ = service.read().await.append_event(0, "area1", "area1", event).await.expect("Append");
    }
    
    // Act - Read all events
    let read_result = service.read().await.read(0, "area1", "area1", "resource1", 0, 10).await;
    
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
    let watermark = service.read().await.get_watermark(0, "area1", "area1").await;
    
    // Assert
    assert!(watermark.is_ok());
    assert_eq!(watermark.unwrap(), 0);
    
    // Act - Append event
    let event = StreamEvent {
        sequence: 0,
        resource: "resource1".to_string(),
        area_seq: None,
        body: vec![1],
        metadata: None,
        created_at: 1234567890,
        is_end: false,
    };
    let _ = service.read().await.append_event(0, "area1", "area1", event).await.expect("Append");
    
    // Act - Get watermark after append
    let watermark = service.read().await.get_watermark(0, "area1", "area1").await;
    
    // Assert
    assert!(watermark.is_ok());
    assert_eq!(watermark.unwrap(), 0);
}
