//! Stream domain service - owns all stream business logic

use super::encoding::{decode_event, encode_event};
use super::types::{AppendResult, AreaReadResponse, StreamEvent, StreamOperation};
use crate::routing::RouteTable;
use crate::storage::traits::{KvStore, KvTransaction};
use std::collections::HashMap;
use std::sync::Arc;

/// Maximum number of events allowed in a single append transaction.
/// This prevents unbounded transaction sizes and memory usage.
/// Current implementation: 1 event per transaction (single append API)
/// Future batch API should enforce this limit.
const MAX_EVENTS_PER_TRANSACTION: usize = 1000;

/// Active append transaction state
struct ActiveTransaction {
    txn: Box<dyn KvTransaction>,
    first_seq: u64,
    event_count: usize,
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

/// Stream service handles event stream operations
/// - Append: add events with gap detection
/// - Read: read from resource streams
/// - ReadArea: read from area with watermark
/// - Peek: peek without advancing
/// - Subscribe: live event subscriptions
pub struct StreamService {
    kv_store: Arc<dyn KvStore>,
    subscriptions: RouteTable,
    /// Active transactions keyed by (channel_id, route) to allow concurrent appends
    active_transactions: HashMap<(u32, String), ActiveTransaction>,
}

// Key encoding for storage:
// - Resource stream event: stream:{route}:event:{resource_seq} -> StreamEvent (TLV)
// - Resource head: stream:{route}:head -> u64 (resource_seq)
// - Resource closed flag: stream:{route}:closed -> bool
// - Area event: stream:{realm}/{area}:area:{area_seq} -> (route, resource_seq)
// - Area counter: stream:{realm}/{area}:area_counter -> u64
// - Area watermark: stream:{realm}/{area}:watermark -> u64
// - Area pending: stream:{realm}/{area}:pending:{area_seq} -> bool

// TLV encoding for StreamEvent:
// TAG_SEQ (0x12): resource_seq (u64)
// TAG_AREA_SEQ (0xB0): area_seq (u64, optional)
// TAG_BODY (0x05): body (Vec<u8>)
// TAG_METADATA (0xA3): metadata (Vec<u8>, optional)
// TAG_TIMESTAMP (0xB1): created_at (u64)
// TAG_STREAM_END (0x14): is_end flag (no value)

impl StreamService {
    /// Create a new stream service
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            kv_store,
            subscriptions: RouteTable::new(),
            active_transactions: HashMap::new(),
        }
    }

    /// Process a stream operation
    pub async fn handle_operation(
        &mut self,
        params: StreamOperationParams<'_>,
    ) -> Result<StreamResponse, String> {
        match params.operation {
            StreamOperation::BeginAppend => {
                self.handle_begin_append(params.channel_id, params.route).await
            }
            StreamOperation::Append => {
                self.handle_append(params.channel_id, params.route, params.body, params.metadata, params.is_end).await
            }
            StreamOperation::CommitAppend => {
                self.handle_commit_append(params.channel_id, params.route).await
            }
            StreamOperation::RollbackAppend => {
                self.handle_rollback_append(params.channel_id, params.route).await
            }
            StreamOperation::Subscribe => {
                self.handle_subscribe(params.route).await
            }
            StreamOperation::Unsubscribe => {
                self.handle_unsubscribe(params.route).await
            }
            StreamOperation::Read => {
                self.handle_read(params.route, params.from_seq, params.limit)
                    .await
            }
            StreamOperation::ReadArea => {
                self.handle_read_area(params.route, params.from_seq, params.limit)
                    .await
            }
            StreamOperation::Peek => {
                self.handle_peek(params.route, params.from_seq, params.limit)
                    .await
            }
        }
    }

    /// Handle begin-append operation - starts a new transaction
    /// 
    /// CRITICAL FOR EVENT SOURCING:
    /// Atomically allocates a sequence range for this transaction by incrementing
    /// the stream head counter. This allows the client to know sequence numbers
    /// before commit, which is essential for:
    /// - Causality tracking (event N+1 depends on event N's sequence)
    /// - Conditional writes (CAS-style operations)
    /// - Local projections (client maintains state based on known sequences)
    /// - Idempotent retries (sequences are stable across retries)
    async fn handle_begin_append(&mut self, channel_id: u32, route: &str) -> Result<StreamResponse, String> {
        let key = (channel_id, route.to_string());
        
        // Check if transaction already active for this channel+route
        if self.active_transactions.contains_key(&key) {
            return Err("Transaction already active for this stream on this channel".to_string());
        }

        // Begin transaction for sequence allocation
        let mut alloc_txn = self.kv_store.begin_transaction()?;

        // Check if stream is closed
        let closed_key = format!("stream:{}:closed", route);
        if alloc_txn.get(closed_key.as_bytes())?.is_some() {
            let _ = alloc_txn.rollback();
            return Err("Stream is closed".to_string());
        }

        // ATOMICALLY allocate sequence range by reading and incrementing head
        // This prevents concurrent begin-append operations from getting overlapping sequences
        let head_key = format!("stream:{}:head", route);
        let current_head = if let Some(bytes) = alloc_txn.get(head_key.as_bytes())? {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                Some(u64::from_be_bytes(arr))
            } else {
                None
            }
        } else {
            None
        };
        let first_seq = match current_head {
            None => 0,
            Some(head) => head + 1,
        };

        // Reserve sequence range by updating head to first_seq + MAX_EVENTS_PER_TRANSACTION - 1
        // This allocates the maximum possible range upfront
        let reserved_head = first_seq + MAX_EVENTS_PER_TRANSACTION as u64 - 1;
        alloc_txn.put(head_key.as_bytes(), &reserved_head.to_be_bytes())?;
        
        // Commit the allocation transaction
        alloc_txn.commit().map_err(|e| format!("Failed to allocate sequence range: {}", e))?;

        // Now begin the actual event append transaction
        let txn = self.kv_store.begin_transaction()?;

        // Store transaction state
        self.active_transactions.insert(
            key,
            ActiveTransaction {
                txn,
                first_seq,
                event_count: 0,
            },
        );

        Ok(StreamResponse::BeginAppendOk { first_seq })
    }

    /// Handle commit-append operation - commits the active transaction
    /// 
    /// Commits the append transaction and reclaims any unused sequence space.
    /// Since begin-append pre-allocates MAX_EVENTS_PER_TRANSACTION sequences,
    /// we need to update the head to the actual last sequence used.
    /// 
    /// After successful commit, notifies all subscribers of the new events.
    async fn handle_commit_append(&mut self, channel_id: u32, route: &str) -> Result<StreamResponse, String> {
        let key = (channel_id, route.to_string());
        
        // Get and remove active transaction
        let active = self
            .active_transactions
            .remove(&key)
            .ok_or("No active transaction for this stream on this channel")?;

        if active.event_count == 0 {
            // Empty transaction - rollback and reclaim sequence space
            let _ = active.txn.rollback();
            
            // Reclaim the reserved sequences by resetting head
            let mut reclaim_txn = self.kv_store.begin_transaction()?;
            let head_key = format!("stream:{}:head", route);
            let actual_head = active.first_seq.saturating_sub(1);
            reclaim_txn.put(head_key.as_bytes(), &actual_head.to_be_bytes())?;
            reclaim_txn.commit().map_err(|e| format!("Failed to reclaim sequences: {}", e))?;
            
            return Err("Cannot commit empty transaction".to_string());
        }

        // Commit the transaction
        active
            .txn
            .commit()
            .map_err(|e| format!("Transaction commit failed: {}", e))?;

        let last_seq = active.first_seq + active.event_count as u64 - 1;

        // Reclaim unused sequence space if we didn't use the full allocation
        if active.event_count < MAX_EVENTS_PER_TRANSACTION {
            let mut reclaim_txn = self.kv_store.begin_transaction()?;
            let head_key = format!("stream:{}:head", route);
            reclaim_txn.put(head_key.as_bytes(), &last_seq.to_be_bytes())?;
            reclaim_txn.commit().map_err(|e| format!("Failed to reclaim sequences: {}", e))?;
        }

        // Notify all subscribers about the newly committed events
        self.notify_subscribers(route, active.first_seq, last_seq).await;

        Ok(StreamResponse::CommitAppendOk {
            first_seq: active.first_seq,
            last_seq,
            event_count: active.event_count,
        })
    }

    /// Handle rollback-append operation - rolls back the active transaction
    /// 
    /// Rolls back the append transaction and reclaims all reserved sequences.
    async fn handle_rollback_append(&mut self, channel_id: u32, route: &str) -> Result<StreamResponse, String> {
        let key = (channel_id, route.to_string());
        
        // Get and remove active transaction
        let active = self
            .active_transactions
            .remove(&key)
            .ok_or("No active transaction for this stream on this channel")?;

        // Rollback the transaction
        active
            .txn
            .rollback()
            .map_err(|e| format!("Transaction rollback failed: {}", e))?;

        // Reclaim all reserved sequences by resetting head to before this transaction
        let mut reclaim_txn = self.kv_store.begin_transaction()?;
        let head_key = format!("stream:{}:head", route);
        let actual_head = active.first_seq.saturating_sub(1);
        reclaim_txn.put(head_key.as_bytes(), &actual_head.to_be_bytes())?;
        reclaim_txn.commit().map_err(|e| format!("Failed to reclaim sequences: {}", e))?;

        Ok(StreamResponse::RollbackAppendOk)
    }

    /// Handle append operation
    /// 
    /// This method appends an event to an active transaction started with begin-append.
    /// Enforces MAX_EVENTS_PER_TRANSACTION limit to prevent unbounded transaction sizes.
    async fn handle_append(
        &mut self,
        channel_id: u32,
        route: &str,
        body: Option<Vec<u8>>,
        metadata: Option<Vec<u8>>,
        is_end: bool,
    ) -> Result<StreamResponse, String> {
        let body = body.ok_or("Body required for append")?;
        let key = (channel_id, route.to_string());

        // Get active transaction
        let active = self
            .active_transactions
            .get_mut(&key)
            .ok_or("No active transaction for this stream on this channel. Call begin-append first.")?;

        // Check transaction size limit
        if active.event_count >= MAX_EVENTS_PER_TRANSACTION {
            return Err(format!(
                "Transaction size limit exceeded (max {} events)",
                MAX_EVENTS_PER_TRANSACTION
            ));
        }

        // Calculate resource_seq for this event
        let resource_seq = active.first_seq + active.event_count as u64;

        // Create event with server-assigned timestamp and sequence
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let event = StreamEvent {
            resource_seq,
            area_seq: None, // Not finalized yet
            body,
            metadata,
            created_at: now,
            is_end,
        };

        // Encode event to TLV
        let event_bytes = encode_event(&event);

        // Store event - sequences are pre-allocated so no conflict possible
        // We can use regular put() since begin-append already reserved this sequence range
        let event_key = format!("stream:{}:event:{}", route, resource_seq);
        active.txn.put(event_key.as_bytes(), &event_bytes)?;

        // Mark stream as closed if is_end
        if is_end {
            let closed_key = format!("stream:{}:closed", route);
            active.txn.put(closed_key.as_bytes(), &[1])?;
        }

        // Increment event count
        active.event_count += 1;

        Ok(StreamResponse::AppendResult(AppendResult {
            resource_seq,
            area_seq_range: None, // Area sequences assigned on finalize
        }))
    }

    /// Finalize a stream by assigning area sequences to all events
    async fn finalize_stream(&mut self, route: &str) -> Result<std::ops::Range<u64>, String> {
        // Parse route to get realm/area
        let parts: Vec<&str> = route.split('/').collect();
        if parts.len() < 3 {
            return Err("Invalid route format for finalization".to_string());
        }
        let area_prefix = format!("{}/{}", parts[0], parts[1]);

        // Get head to know how many events to finalize
        let head_key = format!("stream:{}:head", route);
        let head = if let Some(bytes) = self.kv_store.get(head_key.as_bytes())? {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                u64::from_be_bytes(arr)
            } else {
                return Err("Invalid head value".to_string());
            }
        } else {
            return Err("Stream has no events".to_string());
        };

        let event_count = head + 1;

        // Allocate area sequences
        let counter_key = format!("stream:{}:area_counter", area_prefix);
        let start_area_seq = if let Some(bytes) = self.kv_store.get(counter_key.as_bytes())? {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                u64::from_be_bytes(arr)
            } else {
                0
            }
        } else {
            0
        };

        let end_area_seq = start_area_seq + event_count;

        // Update counter
        self.kv_store
            .put(counter_key.as_bytes(), &end_area_seq.to_be_bytes())?;

        // Update all events with area_seq and create area index
        let mut batch_writes = Vec::new();

        for i in 0..event_count {
            let resource_seq = i;
            let area_seq = start_area_seq + i;

            // Read event
            let event_key = format!("stream:{}:event:{}", route, resource_seq);
            let event_bytes = self
                .kv_store
                .get(event_key.as_bytes())?
                .ok_or(format!("Event {} not found", resource_seq))?;

            let mut event = decode_event(&event_bytes)?;

            // Assign area_seq
            event.area_seq = Some(area_seq);

            // Re-encode
            let updated_bytes = encode_event(&event);

            // Add to batch
            batch_writes.push((event_key.into_bytes(), updated_bytes));

            // Create area index entry: (route, resource_seq)
            let area_index_key = format!("stream:{}:area:{}", area_prefix, area_seq);
            let area_index_value = format!("{}:{}", route, resource_seq);
            batch_writes.push((area_index_key.into_bytes(), area_index_value.into_bytes()));
        }

        // Write batch
        self.kv_store.put_batch(batch_writes)?;

        // Advance watermark
        self.advance_watermark(&area_prefix, start_area_seq, end_area_seq)
            .await?;

        Ok(start_area_seq..end_area_seq)
    }

    /// Advance watermark to first contiguous area_seq with no gaps
    async fn advance_watermark(
        &mut self,
        area_prefix: &str,
        start: u64,
        end: u64,
    ) -> Result<(), String> {
        // Get current watermark
        let watermark_key = format!("stream:{}:watermark", area_prefix);
        let current_watermark = if let Some(bytes) =
            self.kv_store.get(watermark_key.as_bytes())?
        {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                u64::from_be_bytes(arr)
            } else {
                0
            }
        } else {
            0
        };

        // If this batch doesn't start at watermark, mark as pending
        if start != current_watermark {
            for seq in start..end {
                let pending_key = format!("stream:{}:pending:{}", area_prefix, seq);
                self.kv_store.put(pending_key.as_bytes(), &[1])?;
            }
            return Ok(());
        }

        // Advance watermark as far as we can
        let mut new_watermark = end;
        loop {
            let pending_key = format!("stream:{}:pending:{}", area_prefix, new_watermark);
            if let Some(_) = self.kv_store.get(pending_key.as_bytes())? {
                // Found pending batch, advance watermark through it
                // Need to find the end of this batch
                let mut batch_end = new_watermark;
                loop {
                    batch_end += 1;
                    let next_pending = format!("stream:{}:pending:{}", area_prefix, batch_end);
                    if self.kv_store.get(next_pending.as_bytes())?.is_none() {
                        break;
                    }
                }
                new_watermark = batch_end;

                // Clean up pending markers
                for seq in current_watermark..new_watermark {
                    let pk = format!("stream:{}:pending:{}", area_prefix, seq);
                    let _ = self.kv_store.delete(pk.as_bytes());
                }
            } else {
                break;
            }
        }

        // Update watermark
        self.kv_store
            .put(watermark_key.as_bytes(), &new_watermark.to_be_bytes())?;

        Ok(())
    }

    /// Handle read operation: read from resource stream
    async fn handle_read(
        &mut self,
        route: &str,
        from_seq: Option<u64>,
        limit: Option<usize>,
    ) -> Result<StreamResponse, String> {
        let from_seq = from_seq.unwrap_or(0);
        let limit = limit.unwrap_or(100).min(1000);

        let mut events = Vec::new();

        for i in 0..limit {
            let seq = from_seq + i as u64;
            let event_key = format!("stream:{}:event:{}", route, seq);

            match self.kv_store.get(event_key.as_bytes())? {
                Some(bytes) => {
                    let event = decode_event(&bytes)?;
                    events.push(event);
                }
                None => break,
            }
        }

        Ok(StreamResponse::Events(events))
    }

    /// Handle read-area operation: read from area with watermark
    async fn handle_read_area(
        &mut self,
        route: &str,
        from_seq: Option<u64>,
        limit: Option<usize>,
    ) -> Result<StreamResponse, String> {
        let from_seq = from_seq.unwrap_or(0);
        let limit = limit.unwrap_or(100).min(1000);

        // Get watermark
        let watermark_key = format!("stream:{}:watermark", route);
        let watermark = if let Some(bytes) = self.kv_store.get(watermark_key.as_bytes())? {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                u64::from_be_bytes(arr)
            } else {
                0
            }
        } else {
            0
        };

        let mut events = Vec::new();

        for i in 0..limit {
            let area_seq = from_seq + i as u64;

            // Only return events up to watermark
            if area_seq >= watermark {
                break;
            }

            // Look up area index
            let area_index_key = format!("stream:{}:area:{}", route, area_seq);
            if let Some(index_bytes) = self.kv_store.get(area_index_key.as_bytes())? {
                let index_str = std::str::from_utf8(&index_bytes)
                    .map_err(|e| format!("Invalid area index: {}", e))?;

                // Parse route:resource_seq
                if let Some((event_route, resource_seq_str)) = index_str.split_once(':') {
                    let resource_seq: u64 = resource_seq_str
                        .parse()
                        .map_err(|e| format!("Invalid resource_seq: {}", e))?;

                    // Read event
                    let event_key = format!("stream:{}:event:{}", event_route, resource_seq);
                    if let Some(event_bytes) = self.kv_store.get(event_key.as_bytes())? {
                        let event = decode_event(&event_bytes)?;
                        events.push(event);
                    }
                }
            }
        }

        Ok(StreamResponse::AreaRead(AreaReadResponse {
            events,
            watermark,
        }))
    }

    /// Handle peek operation: peek at the last committed event without advancing
    /// Returns only the most recent event on the stream (at head sequence)
    async fn handle_peek(
        &mut self,
        route: &str,
        _from_seq: Option<u64>,
        _limit: Option<usize>,
    ) -> Result<StreamResponse, String> {
        // Get the current head to find the last committed event
        let head_key = format!("stream:{}:head", route);
        let head_seq = if let Some(bytes) = self.kv_store.get(head_key.as_bytes())? {
            if bytes.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[..8]);
                u64::from_be_bytes(arr)
            } else {
                return Ok(StreamResponse::Events(Vec::new())); // Invalid head
            }
        } else {
            return Ok(StreamResponse::Events(Vec::new())); // No events yet
        };

        // Read the event at head sequence
        let event_key = format!("stream:{}:event:{}", route, head_seq);
        match self.kv_store.get(event_key.as_bytes())? {
            Some(bytes) => {
                let event = decode_event(&bytes)?;
                Ok(StreamResponse::Events(vec![event]))
            }
            None => Ok(StreamResponse::Events(Vec::new())), // Head exists but event missing (shouldn't happen)
        }
    }

    /// Handle subscribe operation: return current sequence info and register subscription
    /// Supports both resource-level (stream://{realm}/{area}/{resource}) and
    /// area-level (stream://{realm}/{area}/*) subscriptions
    async fn handle_subscribe(&mut self, route: &str) -> Result<StreamResponse, String> {
        // Parse route to determine if it's resource or area subscription
        let parts: Vec<&str> = route.split('/').collect();
        
        let (last_resource_seq, last_area_seq, watermark) = if parts.len() >= 3 {
            let resource_route = if parts.len() == 3 && parts[2] == "*" {
                // Area wildcard subscription: stream://{realm}/{area}/*
                None
            } else if parts.len() >= 3 {
                // Resource subscription: stream://{realm}/{area}/{resource}
                Some(route)
            } else {
                None
            };

            // Get resource head if resource subscription
            let resource_seq = if let Some(res_route) = resource_route {
                let head_key = format!("stream:{}:head", res_route);
                if let Some(bytes) = self.kv_store.get(head_key.as_bytes())? {
                    if bytes.len() == 8 {
                        let mut arr = [0u8; 8];
                        arr.copy_from_slice(&bytes[..8]);
                        Some(u64::from_be_bytes(arr))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            // Get area watermark for area subscriptions
            let area_prefix = format!("{}/{}", parts[0], parts[1]);
            let watermark_key = format!("stream:{}:watermark", area_prefix);
            let wm = if let Some(bytes) = self.kv_store.get(watermark_key.as_bytes())? {
                if bytes.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&bytes[..8]);
                    Some(u64::from_be_bytes(arr))
                } else {
                    None
                }
            } else {
                None
            };

            // Get area counter (last assigned area_seq)
            let counter_key = format!("stream:{}:area_counter", area_prefix);
            let area_seq = if let Some(bytes) = self.kv_store.get(counter_key.as_bytes())? {
                if bytes.len() == 8 {
                    let mut arr = [0u8; 8];
                    arr.copy_from_slice(&bytes[..8]);
                    let counter = u64::from_be_bytes(arr);
                    // Last assigned seq is counter - 1 (if counter > 0)
                    if counter > 0 {
                        Some(counter - 1)
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            };

            (resource_seq, area_seq, wm)
        } else {
            (None, None, None)
        };

        // TODO: Register subscription in route_table for push notifications
        // This would integrate with a pub/sub mechanism to notify on new appends
        
        Ok(StreamResponse::Subscription(SubscriptionInfo {
            last_resource_seq,
            last_area_seq,
            watermark,
        }))
    }

    /// Handle unsubscribe operation: remove subscription
    async fn handle_unsubscribe(&mut self, _route: &str) -> Result<StreamResponse, String> {
        // TODO: Remove subscription from route_table
        // For now, return success - subscriptions are lightweight and stateless
        
        Ok(StreamResponse::Subscription(SubscriptionInfo {
            last_resource_seq: None,
            last_area_seq: None,
            watermark: None,
        }))
    }

    /// Notify all subscribers about newly committed events
    /// Sends a single notification with the tail position (last_seq) of the stream
    async fn notify_subscribers(&mut self, route: &str, first_seq: u64, last_seq: u64) {
        // Find all matching subscribers for this stream route
        let subscribers = self.subscriptions.matching_subscribers(route);
        
        if subscribers.is_empty() {
            return; // No subscribers, skip notification
        }

        // Build a single notification with sequence range information
        // SubSender tuple: (route, body_opt, payload, metadata_opt, seq_opt, is_end)
        let notification = (
            route.to_string(),                      // route
            None,                                   // body_opt (no body for tail notification)
            Vec::new(),                             // payload (empty for tail notification)
            Some(format!("{}..{}", first_seq, last_seq)), // metadata contains sequence range
            Some(last_seq as u32),                  // seq_opt is the tail (last_seq)
            false,                                  // is_end
        );

        // Send notification to each subscriber
        for sub in &subscribers {
            // Send notification (ignore errors if channel is closed)
            let _ = sub.sender.send(notification.clone()).await;
        }
    }

    /// Cleanup all resources for a channel (called when connection drops)
    /// Rolls back any active append transactions for this channel and reclaims sequences
    pub async fn cleanup_channel(&mut self, channel_id: u32) {
        // Find and rollback all transactions for this channel
        let keys_to_remove: Vec<_> = self
            .active_transactions
            .keys()
            .filter(|(cid, _)| *cid == channel_id)
            .cloned()
            .collect();

        for key in keys_to_remove {
            if let Some(active) = self.active_transactions.remove(&key) {
                // Rollback the transaction (ignore errors during cleanup)
                let _ = active.txn.rollback();
                
                // Reclaim sequences (best effort, ignore errors)
                let route = &key.1;
                if let Ok(mut reclaim_txn) = self.kv_store.begin_transaction() {
                    let head_key = format!("stream:{}:head", route);
                    let actual_head = active.first_seq.saturating_sub(1);
                    if reclaim_txn.put(head_key.as_bytes(), &actual_head.to_be_bytes()).is_ok() {
                        let _ = reclaim_txn.commit();
                    }
                }
            }
        }
    }
}

impl Default for StreamService {
    fn default() -> Self {
        // For tests - use a mock store
        use crate::storage::traits::KvTransaction;
        use bytes::Bytes;

        struct MockStore;
        impl KvStore for MockStore {
            fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), String> {
                Ok(())
            }
            fn get(&self, _key: &[u8]) -> Result<Option<Bytes>, String> {
                Ok(None)
            }
            fn delete(&self, _key: &[u8]) -> Result<(), String> {
                Ok(())
            }
            fn put_batch(&self, _writes: Vec<(Vec<u8>, Vec<u8>)>) -> Result<(), String> {
                Ok(())
            }
            fn delete_batch(&self, _keys: Vec<Vec<u8>>) -> Result<(), String> {
                Ok(())
            }
            fn scan(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<(Bytes, Bytes)>, String> {
                Ok(vec![])
            }
            fn flush(&self) -> Result<(), String> {
                Ok(())
            }
            fn begin_transaction(&self) -> Result<Box<dyn KvTransaction>, String> {
                Err("Transactions not supported in mock".to_string())
            }
        }

        Self::new(Arc::new(MockStore))
    }
}

/// Stream service response types
#[derive(Debug)]
pub enum StreamResponse {
    AppendResult(AppendResult),
    Events(Vec<StreamEvent>),
    AreaRead(AreaReadResponse),
    /// Subscription info: last available resource/area sequences (lightweight)
    Subscription(SubscriptionInfo),
    /// Begin append acknowledged with first sequence number
    BeginAppendOk { first_seq: u64 },
    /// Commit append successful with range details
    CommitAppendOk {
        first_seq: u64,
        last_seq: u64,
        event_count: usize,
    },
    /// Rollback successful
    RollbackAppendOk,
}

/// Lightweight subscription info returned to subscribers
#[derive(Debug)]
pub struct SubscriptionInfo {
    pub last_resource_seq: Option<u64>,
    pub last_area_seq: Option<u64>,
    pub watermark: Option<u64>,
}
