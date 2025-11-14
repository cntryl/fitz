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
use super::types::{AppendResult, AreaReadResponse, StreamEvent, StreamOperation};
use crate::routing::{RouteTable, RouteFamilyId, DEFAULT_RF};
use crate::storage::traits::{KvStore, KvTransaction};
use cntryl_lexkey::LexKey;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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

/// Active append transaction state
struct ActiveTransaction {
    txn: Box<dyn KvTransaction>,
    buffered_events: Vec<StreamEvent>,
    first_seq: u64,
}

/// Parameters for stream operations
pub struct StreamOperationParams<'a> {
    pub operation: StreamOperation,
    pub route: &'a str,
    pub channel_id: u32,
    pub body: Option<Vec<u8>>,
    pub metadata: Option<Vec<u8>>,
    pub is_end: bool,
    pub from_seq: Option<u64>,
    pub limit: Option<usize>,
}

/// Stream service handles event stream operations with full transaction semantics
pub struct StreamService {
    kv_store: Arc<dyn KvStore>,
    subscriptions: RouteTable,
    /// Active transactions keyed by (channel_id, route_str)
    active_transactions: Arc<Mutex<HashMap<(u32, String), ActiveTransaction>>>,
    /// Next area_seq counter per (rf, area)
    area_seq_counters: Arc<Mutex<HashMap<(RouteFamilyId, String), u64>>>,
}

impl StreamService {
    /// Create a new stream service
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            kv_store,
            subscriptions: RouteTable::new(),
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
            area_seq_counters: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Build resource event key: {DOMAIN_PREFIX} {IDX_RESOURCE_EVENT} {rf} {area} {resource} {resource_seq}
    fn key_resource_event(rf: RouteFamilyId, area: &str, resource: &str, seq: u64) -> Vec<u8> {
        LexKey::encode_composite(&[
            &[DOMAIN_PREFIX, IDX_RESOURCE_EVENT],
            &rf.to_le_bytes(),
            area.as_bytes(),
            resource.as_bytes(),
            &seq.to_be_bytes(),
        ])
        .as_bytes()
        .to_vec()
    }

    /// Build area event key: {DOMAIN_PREFIX} {IDX_AREA_EVENT} {rf} {area} {area_seq}
    fn key_area_event(rf: RouteFamilyId, area: &str, seq: u64) -> Vec<u8> {
        LexKey::encode_composite(&[
            &[DOMAIN_PREFIX, IDX_AREA_EVENT],
            &rf.to_le_bytes(),
            area.as_bytes(),
            &seq.to_be_bytes(),
        ])
        .as_bytes()
        .to_vec()
    }

    /// Build watermark key: {DOMAIN_PREFIX} {IDX_WATERMARK} {rf} {area}
    fn key_watermark(rf: RouteFamilyId, area: &str) -> Vec<u8> {
        LexKey::encode_composite(&[&[DOMAIN_PREFIX, IDX_WATERMARK], &rf.to_le_bytes(), area.as_bytes()])
            .as_bytes()
            .to_vec()
    }

    /// Build area discovery key: {DOMAIN_PREFIX} {IDX_AREA_DISCOVERY} {rf} {area}
    fn key_area_discovery(rf: RouteFamilyId, area: &str) -> Vec<u8> {
        LexKey::encode_composite(&[&[DOMAIN_PREFIX, IDX_AREA_DISCOVERY], &rf.to_le_bytes(), area.as_bytes()])
            .as_bytes()
            .to_vec()
    }

    /// Build resource discovery key: {DOMAIN_PREFIX} {IDX_RESOURCE_DISCOVERY} {rf} {area} {resource}
    fn key_resource_discovery(rf: RouteFamilyId, area: &str, resource: &str) -> Vec<u8> {
        LexKey::encode_composite(&[
            &[DOMAIN_PREFIX, IDX_RESOURCE_DISCOVERY],
            &rf.to_le_bytes(),
            area.as_bytes(),
            resource.as_bytes(),
        ])
        .as_bytes()
        .to_vec()
    }

    /// Get current watermark for area (highest finalized area_seq)
    async fn get_watermark(&self, rf: RouteFamilyId, area: &str) -> Result<u64, String> {
        let key = Self::key_watermark(rf, area);
        match self
            .kv_store
            .get(&key)
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

    /// Begin a new append transaction
    async fn begin_append(
        &self,
        rf: RouteFamilyId,
        area: &str,
        resource: &str,
        channel_id: u32,
        route: &str,
    ) -> Result<u64, String> {
        let txn = self
            .kv_store
            .begin_transaction()
            .map_err(|e| format!("Failed to begin transaction: {:?}", e))?;

        // Get next resource_seq from discovery or start at 0
        let discovery_key = Self::key_resource_discovery(rf, area, resource);
        let first_seq = match self
            .kv_store
            .get(&discovery_key)
            .map_err(|e| format!("KvStore error: {:?}", e))?
        {
            Some(_) => {
                // Resource exists, need to find highest seq
                // For now, start at 0 (simplified - could scan)
                0
            }
            None => 0,
        };

        let mut txns = self.active_transactions.lock().await;
        txns.insert(
            (channel_id, route.to_string()),
            ActiveTransaction {
                txn: Box::new(txn),
                buffered_events: Vec::new(),
                first_seq,
            },
        );

        Ok(first_seq)
    }

    /// Append an event to an active transaction
    pub async fn append_event(
        &self,
        channel_id: u32,
        route: &str,
        event: StreamEvent,
    ) -> Result<(), String> {
        let tx_key = (channel_id, route.to_string());

        let mut txns = self.active_transactions.lock().await;
        let tx = txns.get_mut(&tx_key).ok_or_else(|| {
            format!(
                "No active transaction for channel {}, route {}",
                channel_id, route
            )
        })?;

        // Validate event sequence is monotonic within this transaction
        let expected_seq = tx.first_seq + tx.buffered_events.len() as u64;
        if event.sequence != expected_seq {
            return Err(format!(
                "Expected sequence {}, got {}",
                expected_seq, event.sequence
            ));
        }

        // Check transaction size limit
        if tx.buffered_events.len() >= MAX_EVENTS_PER_TRANSACTION {
            return Err(format!(
                "Transaction exceeded max events ({})",
                MAX_EVENTS_PER_TRANSACTION
            ));
        }

        tx.buffered_events.push(event);
        Ok(())
    }

    /// Commit an active append transaction
    pub async fn commit_append(
        &self,
        channel_id: u32,
        route: &str,
        area: &str,
    ) -> Result<(u64, u64, usize), String> {
        let tx_key = (channel_id, route.to_string());

        // Extract and remove transaction
        let tx = {
            let mut txns = self.active_transactions.lock().await;
            txns.remove(&tx_key).ok_or_else(|| {
                format!(
                    "No active transaction for channel {}, route {}",
                    channel_id, route
                )
            })?
        };

        let event_count = tx.buffered_events.len();
        if event_count == 0 {
            return Err("Cannot commit empty transaction".to_string());
        }

        // Get next area_seq and update counter
        let mut counters = self.area_seq_counters.lock().await;
        let area_seq_start = counters
            .entry((DEFAULT_RF, area.to_string()))
            .or_insert(0);
        let first_area_seq = *area_seq_start;
        *area_seq_start += event_count as u64;
        drop(counters);

        // Write events to both indices
        for (idx, event) in tx.buffered_events.iter().enumerate() {
            let resource_key = Self::key_resource_event(
                DEFAULT_RF,
                area,
                &event.resource,
                event.sequence,
            );
            let area_key = Self::key_area_event(DEFAULT_RF, area, first_area_seq + idx as u64);

            let encoded = encode_event(event).map_err(|e| format!("Encode error: {:?}", e))?;

            tx.txn
                .set(resource_key, encoded.clone())
                .map_err(|e| format!("Failed to write resource index: {:?}", e))?;
            tx.txn
                .set(area_key, encoded)
                .map_err(|e| format!("Failed to write area index: {:?}", e))?;

            // Mark resource as discovered
            let discovery_key = Self::key_resource_discovery(
                DEFAULT_RF,
                area,
                &event.resource,
            );
            tx.txn
                .set(discovery_key, DISCOVERY_MARKER.to_vec())
                .map_err(|e| format!("Failed to write resource discovery: {:?}", e))?;
        }

        // Mark area as discovered
        let area_discovery_key = Self::key_area_discovery(DEFAULT_RF, area);
        tx.txn
            .set(area_discovery_key, DISCOVERY_MARKER.to_vec())
            .map_err(|e| format!("Failed to write area discovery: {:?}", e))?;

        // Update watermark to make events visible
        let watermark_key = Self::key_watermark(DEFAULT_RF, area);
        let new_watermark = (first_area_seq + event_count as u64 - 1).to_be_bytes();
        tx.txn
            .set(watermark_key, new_watermark.to_vec())
            .map_err(|e| format!("Failed to update watermark: {:?}", e))?;

        // Commit transaction
        tx.txn
            .commit()
            .map_err(|e| format!("Failed to commit transaction: {:?}", e))?;

        let last_seq = tx.first_seq + event_count as u64 - 1;
        Ok((tx.first_seq, last_seq, event_count))
    }

    /// Rollback an active append transaction
    pub async fn rollback_append(&self, channel_id: u32, route: &str) -> Result<(), String> {
        let tx_key = (channel_id, route.to_string());

        let tx = {
            let mut txns = self.active_transactions.lock().await;
            txns.remove(&tx_key).ok_or_else(|| {
                format!(
                    "No active transaction for channel {}, route {}",
                    channel_id, route
                )
            })?
        };

        tx.txn
            .rollback()
            .map_err(|e| format!("Failed to rollback transaction: {:?}", e))?;

        Ok(())
    }

    /// Handle stream operation
    pub async fn handle_operation(
        &self,
        params: StreamOperationParams<'_>,
    ) -> Result<StreamResponse, String> {
        let (area, resource) = Self::parse_route(&params.route)?;

        match params.operation {
            StreamOperation::BeginAppend => {
                let first_seq = self
                    .begin_append(
                        DEFAULT_RF,
                        area,
                        resource,
                        params.channel_id,
                        params.route,
                    )
                    .await?;
                Ok(StreamResponse::BeginAppendOk { first_seq })
            }
            StreamOperation::Append => {
                Err("Append operation not yet implemented through handler".to_string())
            }
            StreamOperation::CommitAppend => {
                let (first_seq, last_seq, event_count) = self
                    .commit_append(params.channel_id, params.route, area)
                    .await?;
                Ok(StreamResponse::CommitAppendOk {
                    first_seq,
                    last_seq,
                    event_count,
                })
            }
            StreamOperation::RollbackAppend => {
                self.rollback_append(params.channel_id, params.route)
                    .await?;
                Ok(StreamResponse::RollbackAppendOk)
            }
            StreamOperation::Read => {
                Err("Read not yet implemented".to_string())
            }
            StreamOperation::ReadArea => {
                Err("ReadArea not yet implemented".to_string())
            }
            StreamOperation::Peek => {
                Err("Peek not yet implemented".to_string())
            }
            StreamOperation::Subscribe => {
                Err("Subscribe not yet implemented".to_string())
            }
            StreamOperation::Unsubscribe => {
                Err("Unsubscribe not yet implemented".to_string())
            }
        }
    }

    /// Parse route to extract area and resource
    fn parse_route(route: &str) -> Result<(&str, &str), String> {
        let parts: Vec<&str> = route.split('/').collect();
        if parts.len() >= 3 {
            Ok((parts[parts.len() - 2], parts[parts.len() - 1]))
        } else {
            Err("Invalid route format".to_string())
        }
    }
}

/// Stream service response types
#[derive(Debug)]
pub enum StreamResponse {
    AppendResult(AppendResult),
    Events(Vec<StreamEvent>),
    AreaRead(AreaReadResponse),
    Subscription(SubscriptionInfo),
    BeginAppendOk { first_seq: u64 },
    AppendOk,
    CommitAppendOk {
        first_seq: u64,
        last_seq: u64,
        event_count: usize,
    },
    RollbackAppendOk,
}

/// Lightweight subscription info returned to subscribers
#[derive(Debug)]
pub struct SubscriptionInfo {
    pub last_resource_seq: Option<u64>,
    pub last_area_seq: Option<u64>,
    pub watermark: Option<u64>,
}

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
            .begin_append(DEFAULT_RF, "area1", "resource1", 1, "stream://area1/resource1")
            .await
            .expect("Begin append");
        let event = StreamEvent {
            sequence: 0,
            resource: "resource1".to_string(),
            body: vec![1, 2, 3],
            metadata: None,
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
            .begin_append(DEFAULT_RF, "area1", "resource1", 1, "stream://area1/resource1")
            .await
            .expect("Begin append");
        let event = StreamEvent {
            sequence: 5, // Expected 0
            resource: "resource1".to_string(),
            body: vec![1, 2, 3],
            metadata: None,
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
            .begin_append(DEFAULT_RF, "area1", "resource1", 1, "stream://area1/resource1")
            .await
            .expect("Begin append");
        let event = StreamEvent {
            sequence: 0,
            resource: "resource1".to_string(),
            body: vec![1, 2, 3],
            metadata: None,
        };
        let _ = svc
            .append_event(1, "stream://area1/resource1", event)
            .await
            .expect("Append event");

        // Act
        let result = svc.commit_append(1, "stream://area1/resource1", "area1").await;

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
            .begin_append(DEFAULT_RF, "area1", "resource1", 1, "stream://area1/resource1")
            .await
            .expect("Begin append");
        let event = StreamEvent {
            sequence: 0,
            resource: "resource1".to_string(),
            body: vec![1, 2, 3],
            metadata: None,
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
}
