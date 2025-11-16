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

use super::encoding::{decode_event, encode_event};
use super::types::StreamEvent;
use crate::core::router::Router;
use crate::routing::RouteFamilyId;
use crate::storage::markers::{
    stream as stream_prefixes, STREAM_DISCOVERY_MARKER, STREAM_DOMAIN_PREFIX,
};
use crate::storage::traits::KvStore;
use cntryl_midge::ColumnFamilyId;
use lexkey::{encode_composite, Encodable};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Stream domain prefix marker
const DOMAIN_PREFIX: u8 = STREAM_DOMAIN_PREFIX;

/// Index type markers (second byte after domain prefix)
const IDX_RESOURCE_EVENT: u8 = stream_prefixes::RESOURCE_EVENT;
const IDX_AREA_EVENT: u8 = stream_prefixes::AREA_EVENT;
const IDX_WATERMARK: u8 = stream_prefixes::WATERMARK;
const IDX_AREA_DISCOVERY: u8 = stream_prefixes::AREA_DISCOVERY;
const IDX_RESOURCE_DISCOVERY: u8 = stream_prefixes::RESOURCE_DISCOVERY;

/// Reservation status for area_seq ranges
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReservationStatus {
    Reserved,  // Reserved but not yet committed
    Committed, // Committed and visible
}

/// Area stream state tracking for watermark management
#[derive(Debug, Default)]
struct AreaStreamState {
    /// Next area_seq to allocate
    next_seq: u64,
    /// Low watermark - all sequences < watermark are committed and visible
    watermark: u64,
    /// Reserved ranges tracking (seq -> status)
    reserved_ranges: BTreeMap<u64, ReservationStatus>,
}

/// Active append transaction state - single resource per transaction
/// Events are written immediately to KvStore, transaction tracks metadata only
struct ActiveTransaction {
    realm: String,
    area: String,
    resource: String,
    first_area_seq: u64,
    event_count: usize,
}

/// Stream service handles event stream operations with full transaction semantics
pub struct StreamService {
    kv_store: Arc<dyn KvStore>,
    subscriptions: Router,
    /// Area stream states with watermark and reservation tracking per (rf, area)
    area_states: Arc<Mutex<HashMap<(RouteFamilyId, String), AreaStreamState>>>,
    /// Active transactions per transaction_id
    active_transactions: Arc<Mutex<HashMap<u64, ActiveTransaction>>>,
    /// Next transaction ID
    next_txn_id: Arc<Mutex<u64>>,
}

impl StreamService {
    /// Create a new stream service
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            kv_store,
            subscriptions: Router::new(),
            area_states: Arc::new(Mutex::new(HashMap::new())),
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
            next_txn_id: Arc::new(Mutex::new(1)),
        }
    }

    /// Begin a new append transaction for a specific resource
    /// Single resource per transaction - all events in this transaction must be for this resource
    /// Returns transaction ID to be used for subsequent append operations
    pub async fn begin_append(
        &self,
        rf: RouteFamilyId,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<u64, String> {
        // Get next transaction ID
        let mut txn_id_counter = self.next_txn_id.lock().await;
        let txn_id = *txn_id_counter;
        *txn_id_counter += 1;
        drop(txn_id_counter);

        // Peek at next area_seq (will be reserved on first append)
        let mut area_states = self.area_states.lock().await;
        let area_state = area_states.entry((rf, area.to_string())).or_default();
        let next_area_seq = area_state.next_seq;
        drop(area_states);

        // Create and store transaction state with realm/area/resource tracking
        let txn = ActiveTransaction {
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            first_area_seq: next_area_seq,
            event_count: 0,
        };

        let mut transactions = self.active_transactions.lock().await;
        transactions.insert(txn_id, txn);

        Ok(txn_id)
    }

    /// Append an event to an active transaction
    /// Event is written immediately to KvStore but not yet "finalized" (watermark not updated)
    /// Event must be for the resource specified in begin_append
    /// On first append, reserves area_seq and marks as Reserved
    pub async fn append_event(
        &self,
        txn_id: u64,
        rf: RouteFamilyId,
        event: StreamEvent,
    ) -> Result<(), String> {
        let mut transactions = self.active_transactions.lock().await;
        let txn = transactions
            .get_mut(&txn_id)
            .ok_or("Transaction not found or already committed".to_string())?;

        // Validate client-provided sequence
        let cf = ColumnFamilyId(rf);

        // Check for sequence gaps (sequences must be contiguous)
        let start_key = Self::key_resource_event(&txn.realm, &txn.area, &txn.resource, 0);
        let end_key = Self::key_resource_event(&txn.realm, &txn.area, &txn.resource, u64::MAX);
        let existing_sequences: std::collections::BTreeSet<u64> = self
            .kv_store
            .scan(cf, &start_key, &end_key)
            .map_err(|e| format!("KvStore error checking sequence gaps: {:?}", e))?
            .into_iter()
            .filter_map(|(_, value)| decode_event(&value).ok().map(|e| e.sequence))
            .collect();

        if !existing_sequences.is_empty() && !existing_sequences.contains(&event.sequence) {
            // If there are existing events and this sequence doesn't exist yet,
            // it must be exactly max + 1
            let max_seq = existing_sequences.iter().max().unwrap();
            if event.sequence != max_seq + 1 {
                return Err(format!(
                    "Sequence gap detected: expected {}, got {}",
                    max_seq + 1,
                    event.sequence
                ));
            }
        }
        // If sequence already exists or no existing events, allow it (check conflicts below)

        // Check if stream is already closed (has is_end=true event)
        let start_key = Self::key_resource_event(&txn.realm, &txn.area, &txn.resource, 0);
        let end_key = Self::key_resource_event(&txn.realm, &txn.area, &txn.resource, u64::MAX);
        let existing_events = self
            .kv_store
            .scan(cf, &start_key, &end_key)
            .map_err(|e| format!("KvStore error checking stream closure: {:?}", e))?;

        for (_, value) in existing_events {
            let existing_event = decode_event(&value).map_err(|e| {
                format!("Failed to decode existing event for closure check: {:?}", e)
            })?;
            if existing_event.is_end {
                return Err("Cannot append to closed stream".to_string());
            }
        }

        // Check for sequence conflicts (same sequence, different body)
        let resource_key =
            Self::key_resource_event(&txn.realm, &txn.area, &txn.resource, event.sequence);
        if let Some(existing_data) = self
            .kv_store
            .get(cf, &resource_key)
            .map_err(|e| format!("KvStore error checking for conflicts: {:?}", e))?
        {
            let existing_event = decode_event(&existing_data).map_err(|e| {
                format!(
                    "Failed to decode existing event for conflict check: {:?}",
                    e
                )
            })?;
            if existing_event.body != event.body {
                return Err("Sequence conflict".to_string());
            }
        }
        // Check if stream is already closed (has is_end=true event)
        if event.is_end {
            // Check if any existing event in this resource has is_end=true
            let start_key = Self::key_resource_event(&txn.realm, &txn.area, &txn.resource, 0);
            let end_key = Self::key_resource_event(&txn.realm, &txn.area, &txn.resource, u64::MAX);
            let existing_events = self
                .kv_store
                .scan(cf, &start_key, &end_key)
                .map_err(|e| format!("KvStore error checking stream closure: {:?}", e))?;

            for (_, value) in existing_events {
                let existing_event = decode_event(&value).map_err(|e| {
                    format!("Failed to decode existing event for closure check: {:?}", e)
                })?;
                if existing_event.is_end {
                    return Err("Cannot append to closed stream".to_string());
                }
            }
        }

        let current_area_seq = txn.first_area_seq + txn.event_count as u64;
        let realm = txn.realm.clone();
        let area = txn.area.clone();

        // Reserve area_seq for this event
        drop(transactions);
        let mut area_states = self.area_states.lock().await;
        let area_state = area_states.entry((rf, area.clone())).or_default();
        area_state
            .reserved_ranges
            .insert(current_area_seq, ReservationStatus::Reserved);
        area_state.next_seq = current_area_seq + 1;
        drop(area_states);

        // Write event immediately to both indices
        let encoded = encode_event(&event);

        // Write to area event index (server-assigned sequence)
        let area_key = Self::key_area_event(&realm, &area, current_area_seq);
        self.kv_store
            .put(cf, &area_key, &encoded)
            .map_err(|e| format!("Failed to write area event: {:?}", e))?;

        // Write to resource event index (client-assigned sequence)
        self.kv_store
            .put(cf, &resource_key, &encoded)
            .map_err(|e| format!("Failed to write resource event: {:?}", e))?;

        // Increment event count
        let mut transactions = self.active_transactions.lock().await;
        let txn = transactions
            .get_mut(&txn_id)
            .ok_or("Transaction not found".to_string())?;
        txn.event_count += 1;

        Ok(())
    }

    /// Commit an append transaction - finalizes by updating watermark
    /// Events were already written during append_event calls
    /// Reserves the area_seq range for this transaction
    /// Uses realm/area from transaction state
    pub async fn commit_append(
        &self,
        txn_id: u64,
        rf: RouteFamilyId,
    ) -> Result<(u64, u64, usize), String> {
        let mut transactions = self.active_transactions.lock().await;
        let txn = transactions
            .remove(&txn_id)
            .ok_or("Transaction not found".to_string())?;

        if txn.event_count == 0 {
            return Err("Transaction is empty".to_string());
        }

        let cf = ColumnFamilyId(rf);

        // Mark area as discovered
        let area_discovery_key = Self::key_area_discovery(&txn.realm, &txn.area);
        self.kv_store
            .put(cf, &area_discovery_key, STREAM_DISCOVERY_MARKER)
            .map_err(|e| format!("Failed to write area discovery: {:?}", e))?;

        // Mark all reserved sequences as committed
        let final_area_seq = txn.first_area_seq + (txn.event_count - 1) as u64;
        let realm = txn.realm.clone();
        let area = txn.area.clone();
        let first_seq = txn.first_area_seq;

        drop(transactions);

        let mut area_states = self.area_states.lock().await;
        let area_state = area_states.entry((rf, area.clone())).or_default();

        // Mark all sequences in this transaction as Committed
        for seq in first_seq..=final_area_seq {
            if let Some(status) = area_state.reserved_ranges.get_mut(&seq) {
                *status = ReservationStatus::Committed;
            }
        }

        // Advance watermark to highest contiguous committed sequence
        // Watermark starts at 0 and represents the highest committed area_seq
        // Scan from current watermark, advancing while sequences are Committed
        let mut scan_seq = area_state.watermark;
        let mut highest_committed = area_state.watermark;

        // Special case: if watermark is 0 and we're committing from 0, we need to check seq 0
        if area_state.watermark == 0 && first_seq == 0 {
            scan_seq = 0;
            highest_committed = 0;
        }

        while let Some(status) = area_state.reserved_ranges.get(&scan_seq) {
                if matches!(status, ReservationStatus::Committed) {
                    // This sequence is committed
                    area_state.reserved_ranges.remove(&scan_seq);
                    highest_committed = scan_seq;
                    scan_seq += 1;
                } else {
                    // Hit a Reserved (uncommitted) sequence, stop
                    break;
                }
        }

        // Update in-memory watermark to highest contiguous committed
        area_state.watermark = highest_committed;

        // Persist watermark to KvStore
        let watermark_key = Self::key_watermark(&realm, &area);
        let watermark_bytes = highest_committed.to_be_bytes();
        drop(area_states);

        self.kv_store
            .put(cf, &watermark_key, &watermark_bytes)
            .map_err(|e| format!("Failed to update watermark: {:?}", e))?;

        Ok((txn.first_area_seq, final_area_seq, txn.event_count))
    }

    /// Rollback an append transaction, clearing reservations
    /// Note: Events already written to KvStore remain (orphaned), but watermark won't advance past them
    pub async fn rollback_append(&self, txn_id: u64, rf: RouteFamilyId) -> Result<(), String> {
        let mut transactions = self.active_transactions.lock().await;
        let txn = transactions
            .remove(&txn_id)
            .ok_or("Transaction not found".to_string())?;

        // Clear reserved sequences so watermark can advance past them
        if txn.event_count > 0 {
            let final_area_seq = txn.first_area_seq + (txn.event_count - 1) as u64;
            drop(transactions);

            let mut area_states = self.area_states.lock().await;
            if let Some(area_state) = area_states.get_mut(&(rf, txn.area.clone())) {
                for seq in txn.first_area_seq..=final_area_seq {
                    area_state.reserved_ranges.remove(&seq);
                }
            }
        }

        Ok(())
    }

    /// Build resource event key: {DOMAIN_PREFIX} {IDX_RESOURCE_EVENT} {realm} {area} {resource} {resource_seq}
    fn key_resource_event(realm: &str, area: &str, resource: &str, seq: u64) -> Vec<u8> {
        encode_composite!(
            DOMAIN_PREFIX,
            IDX_RESOURCE_EVENT,
            realm,
            area,
            resource,
            seq
        )
        .as_bytes()
        .to_vec()
    }

    /// Build area event key: {DOMAIN_PREFIX} {IDX_AREA_EVENT} {realm} {area} {area_seq}
    fn key_area_event(realm: &str, area: &str, seq: u64) -> Vec<u8> {
        encode_composite!(DOMAIN_PREFIX, IDX_AREA_EVENT, realm, area, seq)
            .as_bytes()
            .to_vec()
    }

    /// Build watermark key: {DOMAIN_PREFIX} {IDX_WATERMARK} {realm} {area}
    fn key_watermark(realm: &str, area: &str) -> Vec<u8> {
        encode_composite!(DOMAIN_PREFIX, IDX_WATERMARK, realm, area)
            .as_bytes()
            .to_vec()
    }

    /// Build area discovery key: {DOMAIN_PREFIX} {IDX_AREA_DISCOVERY} {realm} {area}
    fn key_area_discovery(realm: &str, area: &str) -> Vec<u8> {
        encode_composite!(DOMAIN_PREFIX, IDX_AREA_DISCOVERY, realm, area)
            .as_bytes()
            .to_vec()
    }

    /// Build resource discovery key: {DOMAIN_PREFIX} {IDX_RESOURCE_DISCOVERY} {realm} {area} {resource}
    fn key_resource_discovery(realm: &str, area: &str, resource: &str) -> Vec<u8> {
        encode_composite!(DOMAIN_PREFIX, IDX_RESOURCE_DISCOVERY, realm, area, resource)
            .as_bytes()
            .to_vec()
    }

    /// Get current watermark for area (highest finalized area_seq)
    pub async fn get_watermark(
        &self,
        rf: RouteFamilyId,
        realm: &str,
        area: &str,
    ) -> Result<u64, String> {
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
            let event =
                decode_event(&value).map_err(|e| format!("Failed to decode event: {:?}", e))?;
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
            let event =
                decode_event(&value).map_err(|e| format!("Failed to decode event: {:?}", e))?;
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
        self.subscriptions
            .subscribe(route_pattern, channel_id, sender)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::midge_adapter;

    const TEST_RF: RouteFamilyId = 0;

    #[tokio::test]
    async fn should_append_single_event_successfully() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let svc = StreamService::new(kv_store);
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
        let txn_id = svc
            .begin_append(TEST_RF, "realm1", "area1", "resource1")
            .await
            .expect("Begin");
        svc.append_event(txn_id, TEST_RF, event)
            .await
            .expect("Append");
        let (first_seq, last_seq, count) =
            svc.commit_append(txn_id, TEST_RF).await.expect("Commit");

        // Assert
        assert_eq!(first_seq, 0);
        assert_eq!(last_seq, 0);
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn should_maintain_monotonic_area_sequences() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let svc = StreamService::new(kv_store);

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
            resource: "resource2".to_string(),
            area_seq: None,
            body: vec![4, 5, 6],
            metadata: None,
            created_at: 1234567891,
            is_end: false,
        };

        // Act - First transaction
        let txn1 = svc
            .begin_append(TEST_RF, "realm1", "area1", "resource1")
            .await
            .expect("Begin 1");
        svc.append_event(txn1, TEST_RF, event1)
            .await
            .expect("Append 1");
        let (first1, last1, _) = svc.commit_append(txn1, TEST_RF).await.expect("Commit 1");

        // Act - Second transaction
        let txn2 = svc
            .begin_append(TEST_RF, "realm1", "area1", "resource2")
            .await
            .expect("Begin 2");
        svc.append_event(txn2, TEST_RF, event2)
            .await
            .expect("Append 2");
        let (first2, last2, _) = svc.commit_append(txn2, TEST_RF).await.expect("Commit 2");

        // Assert
        assert_eq!(first1, 0);
        assert_eq!(last1, 0);
        assert_eq!(first2, 1);
        assert_eq!(last2, 1);
    }

    #[tokio::test]
    async fn should_read_events_from_resource_stream() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let svc = StreamService::new(kv_store);

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

        let txn = svc
            .begin_append(TEST_RF, "realm1", "area1", "resource1")
            .await
            .expect("Begin");
        svc.append_event(txn, TEST_RF, event1)
            .await
            .expect("Append 1");
        svc.append_event(txn, TEST_RF, event2)
            .await
            .expect("Append 2");
        svc.commit_append(txn, TEST_RF).await.expect("Commit");

        // Act
        let result = svc
            .read(TEST_RF, "realm1", "area1", "resource1", 0, 100)
            .await;

        // Assert
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].sequence, 1);
    }

    #[tokio::test]
    async fn should_respect_limit_when_reading_events() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let svc = StreamService::new(kv_store);

        let txn = svc
            .begin_append(TEST_RF, "realm1", "area1", "resource1")
            .await
            .expect("Begin");
        for i in 0u64..5u64 {
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

        // Act
        let result = svc
            .read(TEST_RF, "realm1", "area1", "resource1", 0, 2)
            .await;

        // Assert
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn should_get_watermark_after_commit() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let svc = StreamService::new(kv_store);
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
        let txn = svc
            .begin_append(TEST_RF, "realm1", "area1", "resource1")
            .await
            .expect("Begin");
        svc.append_event(txn, TEST_RF, event).await.expect("Append");
        svc.commit_append(txn, TEST_RF).await.expect("Commit");
        let watermark = svc
            .get_watermark(TEST_RF, "realm1", "area1")
            .await
            .expect("Get watermark");

        // Assert
        assert_eq!(watermark, 0);
    }

    #[tokio::test]
    async fn should_return_zero_watermark_when_no_events() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let svc = StreamService::new(kv_store);

        // Act
        let watermark = svc
            .get_watermark(TEST_RF, "realm1", "area1")
            .await
            .expect("Get watermark");

        // Assert
        assert_eq!(watermark, 0);
    }

    #[tokio::test]
    async fn should_read_all_committed_events_from_area() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let svc = StreamService::new(kv_store);

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
            resource: "resource2".to_string(),
            area_seq: None,
            body: vec![4, 5, 6],
            metadata: None,
            created_at: 1234567891,
            is_end: false,
        };

        // Events must be committed to be readable
        let txn = svc
            .begin_append(TEST_RF, "realm1", "area1", "resource1")
            .await
            .expect("Begin");
        svc.append_event(txn, TEST_RF, event1)
            .await
            .expect("Append 1");
        svc.append_event(txn, TEST_RF, event2)
            .await
            .expect("Append 2");
        svc.commit_append(txn, TEST_RF).await.expect("Commit");

        // Act
        let result = svc.read_area(TEST_RF, "realm1", "area1", 0, 100).await;

        // Assert
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 0);
        assert_eq!(events[1].sequence, 1);
    }

    #[tokio::test]
    async fn should_return_empty_when_reading_ahead_of_watermark() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let svc = StreamService::new(kv_store);

        // Act
        let result = svc.read_area(TEST_RF, "realm1", "area1", 100, 100).await;

        // Assert
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn should_rollback_transaction_discards_events() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let svc = StreamService::new(kv_store);
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
        let txn = svc
            .begin_append(TEST_RF, "realm1", "area1", "resource1")
            .await
            .expect("Begin");
        svc.append_event(txn, TEST_RF, event).await.expect("Append");
        let rollback_result = svc.rollback_append(txn, TEST_RF).await;

        // Assert
        assert!(rollback_result.is_ok());
        let watermark = svc
            .get_watermark(TEST_RF, "realm1", "area1")
            .await
            .expect("Get watermark");
        assert_eq!(watermark, 0); // Watermark not updated, events remain but "uncommitted"
    }

    #[tokio::test]
    async fn should_reject_append_to_unknown_transaction() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let svc = StreamService::new(kv_store);
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
        let result = svc.append_event(999, TEST_RF, event).await;

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_parse_route_correctly() {
        // Arrange
        let route = "stream://my-realm/my-area/my-resource";

        // Act
        let result = StreamService::parse_route(route);

        // Assert
        assert!(result.is_ok());
        let (area, resource) = result.unwrap();
        assert_eq!(area, "my-area");
        assert_eq!(resource, "my-resource");
    }

    #[test]
    fn should_reject_invalid_route_format() {
        // Arrange
        let route = "short";

        // Act
        let result = StreamService::parse_route(route);

        // Assert
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_reject_sequence_gaps() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let service = StreamService::new(kv_store);
        let txn = service
            .begin_append(TEST_RF, "realm", "area", "resource")
            .await
            .unwrap();

        let event1 = StreamEvent {
            sequence: 0,
            resource: "resource".to_string(),
            area_seq: None,
            body: b"first".to_vec(),
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        };
        let event2 = StreamEvent {
            sequence: 2, // Gap! Skips sequence 1
            resource: "resource".to_string(),
            area_seq: None,
            body: b"third".to_vec(),
            metadata: None,
            created_at: 1234567891,
            is_end: false,
        };

        // Act
        service.append_event(txn, TEST_RF, event1).await.unwrap();
        let result = service.append_event(txn, TEST_RF, event2).await;

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Sequence gap"));
    }

    #[tokio::test]
    async fn should_reject_sequence_conflicts() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let service = StreamService::new(kv_store);
        let txn1 = service
            .begin_append(TEST_RF, "realm", "area", "resource")
            .await
            .unwrap();
        let txn2 = service
            .begin_append(TEST_RF, "realm", "area", "resource")
            .await
            .unwrap();

        let event1 = StreamEvent {
            sequence: 0,
            resource: "resource".to_string(),
            area_seq: None,
            body: b"first".to_vec(),
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        };
        let event2 = StreamEvent {
            sequence: 0, // Same sequence
            resource: "resource".to_string(),
            area_seq: None,
            body: b"different".to_vec(), // Different body
            metadata: None,
            created_at: 1234567891,
            is_end: false,
        };

        // Act
        service.append_event(txn1, TEST_RF, event1).await.unwrap();
        service.commit_append(txn1, TEST_RF).await.unwrap(); // Commit first event

        let result = service.append_event(txn2, TEST_RF, event2).await;

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Sequence conflict"));
    }

    #[tokio::test]
    async fn should_allow_sequence_idempotency() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let service = StreamService::new(kv_store);
        let txn1 = service
            .begin_append(TEST_RF, "realm", "area", "resource")
            .await
            .unwrap();
        let txn2 = service
            .begin_append(TEST_RF, "realm", "area", "resource")
            .await
            .unwrap();

        let event1 = StreamEvent {
            sequence: 0,
            resource: "resource".to_string(),
            area_seq: None,
            body: b"same".to_vec(),
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        };
        let event2 = StreamEvent {
            sequence: 0, // Same sequence
            resource: "resource".to_string(),
            area_seq: None,
            body: b"same".to_vec(), // Same body
            metadata: None,
            created_at: 1234567891,
            is_end: false,
        };

        // Act
        service.append_event(txn1, TEST_RF, event1).await.unwrap();
        service.commit_append(txn1, TEST_RF).await.unwrap(); // Commit first event

        let result = service.append_event(txn2, TEST_RF, event2).await;

        // Assert
        assert!(result.is_ok()); // Should allow idempotent append
    }

    #[tokio::test]
    async fn should_reject_appends_to_closed_stream() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let service = StreamService::new(kv_store);
        let txn1 = service
            .begin_append(TEST_RF, "realm", "area", "resource")
            .await
            .unwrap();
        let txn2 = service
            .begin_append(TEST_RF, "realm", "area", "resource")
            .await
            .unwrap();

        let closing_event = StreamEvent {
            sequence: 0,
            resource: "resource".to_string(),
            area_seq: None,
            body: b"closing".to_vec(),
            metadata: None,
            created_at: 1234567890,
            is_end: true, // This closes the stream
        };
        let after_close_event = StreamEvent {
            sequence: 1,
            resource: "resource".to_string(),
            area_seq: None,
            body: b"after".to_vec(),
            metadata: None,
            created_at: 1234567891,
            is_end: false,
        };

        // Act
        service
            .append_event(txn1, TEST_RF, closing_event)
            .await
            .unwrap();
        service.commit_append(txn1, TEST_RF).await.unwrap(); // Commit to make it visible

        let result = service.append_event(txn2, TEST_RF, after_close_event).await;

        // Assert
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Cannot append to closed stream"));
    }

    #[tokio::test]
    async fn should_allow_appending_closing_event_to_open_stream() {
        // Arrange
        let kv_store = midge_adapter::create_memory_store().expect("Create store");
        let service = StreamService::new(kv_store);
        let txn = service
            .begin_append(TEST_RF, "realm", "area", "resource")
            .await
            .unwrap();

        let event1 = StreamEvent {
            sequence: 0,
            resource: "resource".to_string(),
            area_seq: None,
            body: b"first".to_vec(),
            metadata: None,
            created_at: 1234567890,
            is_end: false,
        };
        let closing_event = StreamEvent {
            sequence: 1,
            resource: "resource".to_string(),
            area_seq: None,
            body: b"closing".to_vec(),
            metadata: None,
            created_at: 1234567891,
            is_end: true,
        };

        // Act
        service.append_event(txn, TEST_RF, event1).await.unwrap();
        let result = service.append_event(txn, TEST_RF, closing_event).await;

        // Assert
        assert!(result.is_ok()); // Should allow closing an open stream
    }
}
