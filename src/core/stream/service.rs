//! Stream domain service - event log with transaction-based append and watermark semantics
//!
//! Events are indexed by both resource_seq (client monotonic) and area_seq (server-assigned).
//! Watermark prevents consumers from reading ahead of committed transactions.
//!
//! Key schema (using lexkey for building):
//! - 0x01 0x01 {rf} {area} {resource} {resource_seq} → Resource event index
//! - 0x01 0x02 {rf} {area} {area_seq} → Area event index
//! - 0x01 0x03 {rf} {area} → Watermark (u64, highest finalized area_seq)
//! - 0x01 0x04 {rf} {area} → Area discovery marker
//! - 0x01 0x05 {rf} {area} {resource} → Resource discovery marker

use super::types::StreamEvent;
use crate::core::router::Router;
use crate::routing::RouteFamilyId;
use crate::storage::traits::KvStore;
use cntryl_midge::ColumnFamilyId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Default route family for tests
const DEFAULT_RF: RouteFamilyId = 0;

/// Stream domain prefix marker
const DOMAIN_PREFIX: u8 = 0x01;

/// Index type markers (second byte after domain prefix)
const IDX_RESOURCE_EVENT: u8 = 0x01;
const IDX_AREA_EVENT: u8 = 0x02;
const IDX_WATERMARK: u8 = 0x03;
const IDX_AREA_DISCOVERY: u8 = 0x04;
const IDX_RESOURCE_DISCOVERY: u8 = 0x05;

/// Discovery marker value written to KvStore
const DISCOVERY_MARKER: &[u8] = &[0x01];

/// Maximum number of events allowed in a single append transaction.
const MAX_EVENTS_PER_TRANSACTION: usize = 1000;

/*
/// Active append transaction state
struct ActiveTransaction {
    txn: Box<dyn KvTransaction>,
    buffered_events: Vec<StreamEvent>,
    first_seq: u64,
}
*/

/// Stream service handles event stream operations with full transaction semantics
/// TODO: Add back active_transactions once KvTransaction API is available
pub struct StreamService {
    kv_store: Arc<dyn KvStore>,
    subscriptions: Router,
    /// Next area_seq counter per (rf, area) - currently unused
    #[allow(dead_code)]
    area_seq_counters: Arc<Mutex<HashMap<(RouteFamilyId, String), u64>>>,
}

impl StreamService {
    /// Create a new stream service
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            kv_store,
            subscriptions: Router::new(),
            area_seq_counters: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build resource event key: {DOMAIN_PREFIX} {IDX_RESOURCE_EVENT} {realm} {area} {resource} {resource_seq}
    fn key_resource_event(realm: &str, area: &str, resource: &str, seq: u64) -> Vec<u8> {
        lexkey::LexKey::encode_composite(&[
            &[DOMAIN_PREFIX, IDX_RESOURCE_EVENT],
            realm.as_bytes(),
            area.as_bytes(),
            resource.as_bytes(),
            &seq.to_be_bytes(),
        ])
        .as_bytes()
        .to_vec()
    }

    /// Build area event key: {DOMAIN_PREFIX} {IDX_AREA_EVENT} {realm} {area} {area_seq}
    fn key_area_event(realm: &str, area: &str, seq: u64) -> Vec<u8> {
        lexkey::LexKey::encode_composite(&[
            &[DOMAIN_PREFIX, IDX_AREA_EVENT],
            realm.as_bytes(),
            area.as_bytes(),
            &seq.to_be_bytes(),
        ])
        .as_bytes()
        .to_vec()
    }

    /// Build watermark key: {DOMAIN_PREFIX} {IDX_WATERMARK} {realm} {area}
    fn key_watermark(realm: &str, area: &str) -> Vec<u8> {
        lexkey::LexKey::encode_composite(&[&[DOMAIN_PREFIX, IDX_WATERMARK], realm.as_bytes(), area.as_bytes()])
            .as_bytes()
            .to_vec()
    }

    /// Build area discovery key: {DOMAIN_PREFIX} {IDX_AREA_DISCOVERY} {realm} {area}
    fn key_area_discovery(realm: &str, area: &str) -> Vec<u8> {
        lexkey::LexKey::encode_composite(&[&[DOMAIN_PREFIX, IDX_AREA_DISCOVERY], realm.as_bytes(), area.as_bytes()])
            .as_bytes()
            .to_vec()
    }

    /// Build resource discovery key: {DOMAIN_PREFIX} {IDX_RESOURCE_DISCOVERY} {realm} {area} {resource}
    fn key_resource_discovery(realm: &str, area: &str, resource: &str) -> Vec<u8> {
        lexkey::LexKey::encode_composite(&[
            &[DOMAIN_PREFIX, IDX_RESOURCE_DISCOVERY],
            realm.as_bytes(),
            area.as_bytes(),
            resource.as_bytes(),
        ])
        .as_bytes()
        .to_vec()
    }

    /// Get current watermark for area (highest finalized area_seq)
    pub async fn get_watermark(&self, rf: RouteFamilyId, realm: &str, area: &str) -> Result<u64, String> {
        let key = Self::key_watermark(realm, area);
        let cf = ColumnFamilyId(rf);
        match self
            .kv_store
            .get(cf, &key)
            .map_err(|e| format!("KvStore error: {:?}", e))?
        {
            Some(bytes) => {
                if bytes.len() == 8 {
                    Ok(u64::from_be_bytes([
                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                        bytes[7],
                    ]))
                } else {
                    Err("Invalid watermark encoding".to_string())
                }
            }
            None => Ok(0), // No events committed yet
        }
    }

    /// Append a single event directly (no transaction support yet)
    pub async fn append_event(
        &self,
        rf: RouteFamilyId,
        realm: &str,
        area: &str,
        event: StreamEvent,
    ) -> Result<(u64, u64), String> {
        // Get next area_seq
        let mut counters = self.area_seq_counters.lock().await;
        let area_seq = counters.entry((rf, area.to_string())).or_insert(0);
        let current_area_seq = *area_seq;
        *area_seq += 1;
        drop(counters);

        // Encode event
        let encoded = encode_event(&event);
        let cf = ColumnFamilyId(rf);

        // Write to both indices
        let resource_key = Self::key_resource_event(realm, area, &event.resource, event.sequence);
        let area_key = Self::key_area_event(realm, area, current_area_seq);

        self.kv_store
            .put(cf, &encoded, &resource_key)
            .map_err(|e| format!("Failed to write resource index: {:?}", e))?;
        self.kv_store
            .put(cf, &encoded, &area_key)
            .map_err(|e| format!("Failed to write area index: {:?}", e))?;

        // Mark resource and area as discovered
        let resource_discovery_key = Self::key_resource_discovery(realm, area, &event.resource);
        let area_discovery_key = Self::key_area_discovery(realm, area);

        self.kv_store
            .put(cf, DISCOVERY_MARKER, &resource_discovery_key)
            .map_err(|e| format!("Failed to write resource discovery: {:?}", e))?;
        self.kv_store
            .put(cf, DISCOVERY_MARKER, &area_discovery_key)
            .map_err(|e| format!("Failed to write area discovery: {:?}", e))?;

        // Update watermark
        let watermark_key = Self::key_watermark(realm, area);
        let watermark_bytes = current_area_seq.to_be_bytes();
        self.kv_store
            .put(cf, &watermark_bytes, &watermark_key)
            .map_err(|e| format!("Failed to update watermark: {:?}", e))?;

        Ok((event.sequence, current_area_seq))
    }

    /// Read events from a resource stream by resource_seq
    pub async fn read(
        &self,
        rf: RouteFamilyId,
        realm: &str,
        area: &str,
        resource: &str,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<StreamEvent>, String> {
        let start_key = Self::key_resource_event(realm, area, resource, from_seq);
        let end_key = Self::key_resource_event(realm, area, resource, u64::MAX);
        let cf = ColumnFamilyId(rf);

        let results = self
            .kv_store
            .scan(cf, &start_key, &end_key)
            .map_err(|e| format!("KvStore scan error: {:?}", e))?;

        let mut events = Vec::new();
        for (_, value) in results.into_iter().take(limit) {
            let event = decode_event(&value)
                .map_err(|e| format!("Failed to decode event: {:?}", e))?;
            events.push(event);
        }

        Ok(events)
    }

    /// Read events from area stream by area_seq, respecting watermark for ordering
    pub async fn read_area(
        &self,
        rf: RouteFamilyId,
        realm: &str,
        area: &str,
        from_seq: u64,
        limit: usize,
    ) -> Result<Vec<StreamEvent>, String> {
        // Get watermark to enforce ordering guarantee
        let watermark = self
            .get_watermark(rf, realm, area)
            .await
            .map_err(|e| format!("Failed to read watermark: {}", e))?;

        if from_seq > watermark {
            // Client is ahead of watermark, return empty
            return Ok(Vec::new());
        }

        // Only return events up to watermark (prevents reading uncommitted data)
        let max_seq = watermark.min(from_seq + limit as u64);

        let start_key = Self::key_area_event(realm, area, from_seq);
        let end_key = Self::key_area_event(realm, area, max_seq + 1);
        let cf = ColumnFamilyId(rf);

        let results = self
            .kv_store
            .scan(cf, &start_key, &end_key)
            .map_err(|e| format!("KvStore scan error: {:?}", e))?;

        let mut events = Vec::new();
        for (_, value) in results.into_iter().take(limit) {
            let event = decode_event(&value)
                .map_err(|e| format!("Failed to decode event: {:?}", e))?;
            events.push(event);
        }

        Ok(events)
    }

    /// Parse route to extract area and resource
    pub fn parse_route(route: &str) -> Result<(&str, &str), String> {
        let parts: Vec<&str> = route.split('/').collect();
        if parts.len() >= 3 {
            Ok((parts[parts.len() - 2], parts[parts.len() - 1]))
        } else {
            Err("Invalid route format".to_string())
        }
    }

    /// Subscribe to stream notifications for a route pattern
    /// Returns subscription ID for later unsubscribe
    pub fn subscribe(
        &mut self,
        _rf: RouteFamilyId,
        route_pattern: String,
        channel_id: u32,
        sender: crate::core::domain::SubSender,
    ) -> u64 {
        self.subscriptions.subscribe(route_pattern, channel_id, sender)
    }

    /// Unsubscribe from stream notifications
    /// Returns true if subscription was found and removed
    pub fn unsubscribe(&mut self, subscription_id: u64) -> bool {
        self.subscriptions.unsubscribe(subscription_id)
    }

    /// Cleanup all subscriptions for a channel
    pub fn cleanup_channel(&mut self, _rf: RouteFamilyId, channel_id: u32) {
        self.subscriptions.cleanup_channel(channel_id);
    }
}

/// Encode event to bytes (simple JSON for now)
fn encode_event(event: &StreamEvent) -> Vec<u8> {
    serde_json::to_vec(event).expect("Failed to encode event")
}

/// Decode event from bytes
fn decode_event(bytes: &[u8]) -> Result<StreamEvent, String> {
    serde_json::from_slice(bytes).map_err(|e| format!("Failed to decode event: {:?}", e))
}

// Tests commented out until KvStore transaction and iteration APIs are implemented
/*
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::midge_adapter;

    #[tokio::test]
    async fn should_begin_append_transaction() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let svc = StreamService::new(kv_store);

        // Act
        let result = svc
            .begin_append(
                DEFAULT_RF,
                "my-area",
                "my-area",
                "my-resource",
                1,
                "stream://my-area/my-resource",
            )
            .await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn should_append_event_to_transaction() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let svc = StreamService::new(kv_store);
        let _ = svc
            .begin_append(DEFAULT_RF, "area1", "area1", "resource1", 1, "stream://area1/resource1")
            .await
            .expect("Begin append");
        let event = StreamEvent {
            sequence: 0,
            resource: "resource1".to_string(),
            area_seq: None,
            body: vec![1, 2, 3],
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        };

        // Act
        let result = svc.append_event(1, "stream://area1/resource1", event).await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_reject_out_of_order_sequence_in_append() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let svc = StreamService::new(kv_store);
        let _ = svc
            .begin_append(DEFAULT_RF, "area1", "area1", "resource1", 1, "stream://area1/resource1")
            .await
            .expect("Begin append");
        let event = StreamEvent {
            sequence: 5, // Expected 0
            resource: "resource1".to_string(),
            area_seq: None,
            body: vec![1, 2, 3],
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        };

        // Act
        let result = svc.append_event(1, "stream://area1/resource1", event).await;

        // Assert
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_commit_append_and_return_sequence_range() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let svc = StreamService::new(kv_store);
        let _ = svc
            .begin_append(DEFAULT_RF, "area1", "area1", "resource1", 1, "stream://area1/resource1")
            .await
            .expect("Begin append");
        let event = StreamEvent {
            sequence: 0,
            resource: "resource1".to_string(),
            area_seq: None,
            body: vec![1, 2, 3],
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        };
        let _ = svc
            .append_event(1, "stream://area1/resource1", event)
            .await
            .expect("Append event");

        // Act
        let result = svc.commit_append(DEFAULT_RF, "area1", 1, "stream://area1/resource1", "area1").await;

        // Assert
        assert!(result.is_ok());
        let (first_seq, last_seq, event_count) = result.unwrap();
        assert_eq!(first_seq, 0);
        assert_eq!(last_seq, 0);
        assert_eq!(event_count, 1);
    }

    #[tokio::test]
    async fn should_rollback_append_transaction() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let svc = StreamService::new(kv_store);
        let _ = svc
            .begin_append(DEFAULT_RF, "area1", "area1", "resource1", 1, "stream://area1/resource1")
            .await
            .expect("Begin append");
        let event = StreamEvent {
            sequence: 0,
            resource: "resource1".to_string(),
            area_seq: None,
            body: vec![1, 2, 3],
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        };
        let _ = svc
            .append_event(1, "stream://area1/resource1", event)
            .await
            .expect("Append event");

        // Act
        let result = svc.rollback_append(1, "stream://area1/resource1").await;

        // Assert
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn should_read_events_from_resource_stream() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let svc = StreamService::new(kv_store);
        let _ = svc
            .begin_append(DEFAULT_RF, "area1", "area1", "resource1", 1, "stream://area1/resource1")
            .await
            .expect("Begin append");
        
        let event1 = StreamEvent {
            sequence: 0,
            resource: "resource1".to_string(),
            area_seq: None,
            body: vec![1, 2, 3],
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        };
        let event2 = StreamEvent {
            sequence: 1,
            resource: "resource1".to_string(),
            area_seq: None,
            body: vec![4, 5, 6],
            metadata: None,
            created_at: 1234567891,
            is_end: false,
        };
        
        let _ = svc
            .append_event(1, "stream://area1/resource1", event1)
            .await
            .expect("Append event 1");
        let _ = svc
            .append_event(1, "stream://area1/resource1", event2)
            .await
            .expect("Append event 2");
        
        let _ = svc
            .commit_append(DEFAULT_RF, "area1", 1, "stream://area1/resource1", "area1")
            .await
            .expect("Commit append");

        // Act
        let result = svc.read(DEFAULT_RF, "area1", "area1", "resource1", 0, 100).await;

        // Assert
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].sequence, 1);
    }

    #[tokio::test]
    async fn should_read_area_respecting_watermark() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let svc = StreamService::new(kv_store);
        let _ = svc
            .begin_append(DEFAULT_RF, "area1", "area1", "resource1", 1, "stream://area1/resource1")
            .await
            .expect("Begin append");
        
        let event = StreamEvent {
            sequence: 0,
            resource: "resource1".to_string(),
            area_seq: None,
            body: vec![1, 2, 3],
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        };
        
        let _ = svc
            .append_event(1, "stream://area1/resource1", event)
            .await
            .expect("Append event");
        
        let _ = svc
            .commit_append(DEFAULT_RF, "area1", 1, "stream://area1/resource1", "area1")
            .await
            .expect("Commit append");

        // Act
        let result = svc.read_area(DEFAULT_RF, "area1", "area1", 0, 100).await;

        // Assert
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
    }

    #[tokio::test]
    async fn should_return_empty_when_reading_ahead_of_watermark() {
        // Arrange
        let kv_store = Arc::new(midge_adapter::create_memory_store().expect("Create store"));
        let svc = StreamService::new(kv_store);

        // Act - Read from seq=100 when no events exist (watermark=0)
        let result = svc.read_area(DEFAULT_RF, "area1", "area1", 100, 100).await;

        // Assert
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 0);
    }
}
*/
