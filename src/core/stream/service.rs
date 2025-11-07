//! Stream domain service - owns all stream business logic

use super::encoding::{decode_event, encode_event};
use super::types::{AppendResult, AreaReadResponse, StreamEvent, StreamOperation};
use crate::storage::traits::KvStore;
use std::sync::Arc;

/// Parameters for stream operations
pub struct StreamOperationParams<'a> {
    pub operation: StreamOperation,
    pub route: &'a str,
    pub resource_seq: Option<u64>,
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
#[derive(Clone)]
pub struct StreamService {
    kv_store: Arc<dyn KvStore>,
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
        Self { kv_store }
    }

    /// Process a stream operation
    pub async fn handle_operation(
        &self,
        params: StreamOperationParams<'_>,
    ) -> Result<StreamResponse, String> {
        match params.operation {
            StreamOperation::Append => {
                self.handle_append(
                    params.route,
                    params.resource_seq,
                    params.body,
                    params.metadata,
                    params.is_end,
                )
                .await
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
            StreamOperation::Subscribe => {
                // TODO: Implement subscribe (requires pub/sub integration)
                Err("Subscribe not yet implemented".to_string())
            }
        }
    }

    /// Handle append operation
    async fn handle_append(
        &self,
        route: &str,
        resource_seq: Option<u64>,
        body: Option<Vec<u8>>,
        metadata: Option<Vec<u8>>,
        is_end: bool,
    ) -> Result<StreamResponse, String> {
        let resource_seq = resource_seq.ok_or("Resource sequence required for append")?;
        let body = body.ok_or("Body required for append")?;

        // Check if stream is closed
        let closed_key = format!("stream:{}:closed", route);
        if let Some(_) = self.kv_store.get(closed_key.as_bytes())? {
            return Err("Stream is closed".to_string());
        }

        // Get current head
        let head_key = format!("stream:{}:head", route);
        let current_head = if let Some(bytes) = self.kv_store.get(head_key.as_bytes())? {
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

        // Validate sequence
        match current_head {
            None => {
                // First append must be seq 0
                if resource_seq != 0 {
                    return Err(format!(
                        "First resource_seq must be 0, got {}",
                        resource_seq
                    ));
                }
            }
            Some(head) => {
                // Check for gap
                if resource_seq != head + 1 {
                    // Check if it's idempotent retry (same seq)
                    if resource_seq == head {
                        // Verify body matches
                        let event_key = format!("stream:{}:event:{}", route, resource_seq);
                        if let Some(existing) = self.kv_store.get(event_key.as_bytes())? {
                            let existing_event = decode_event(&existing)?;
                            if existing_event.body == body {
                                // Idempotent retry - return success
                                return Ok(StreamResponse::AppendResult(AppendResult {
                                    resource_seq,
                                    area_seq_range: existing_event
                                        .area_seq
                                        .map(|s| s..s + 1),
                                }));
                            } else {
                                return Err(format!(
                                    "Sequence conflict at {}: body mismatch",
                                    resource_seq
                                ));
                            }
                        }
                    }
                    return Err(format!(
                        "Sequence gap: expected {}, got {}",
                        head + 1,
                        resource_seq
                    ));
                }
            }
        }

        // Create event
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

        // Store event
        let event_key = format!("stream:{}:event:{}", route, resource_seq);
        self.kv_store.put(event_key.as_bytes(), &event_bytes)?;

        // Update head
        self.kv_store
            .put(head_key.as_bytes(), &resource_seq.to_be_bytes())?;

        // If is_end, finalize the stream
        let area_seq_range = if is_end {
            Some(self.finalize_stream(route).await?)
        } else {
            None
        };

        // Mark stream as closed if is_end
        if is_end {
            self.kv_store.put(closed_key.as_bytes(), &[1])?;
        }

        Ok(StreamResponse::AppendResult(AppendResult {
            resource_seq,
            area_seq_range,
        }))
    }

    /// Finalize a stream by assigning area sequences to all events
    async fn finalize_stream(&self, route: &str) -> Result<std::ops::Range<u64>, String> {
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
        &self,
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
        &self,
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
        &self,
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

    /// Handle peek operation: peek without advancing
    async fn handle_peek(
        &self,
        route: &str,
        from_seq: Option<u64>,
        limit: Option<usize>,
    ) -> Result<StreamResponse, String> {
        // Peek is the same as read for stateless streams
        self.handle_read(route, from_seq, limit).await
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
}
