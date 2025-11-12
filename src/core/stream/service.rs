//! Stream domain service - DISABLED pending midge KvStore integration
//!
//! The midge KvStore trait API changed to require ColumnFamilyHandle for all operations.
//! This service implementation needs to be updated to work with the new API.
//!
//! TODO: Re-enable once midge provides default_column_family() or similar method

use super::encoding::{decode_event, encode_event};
use super::types::{AppendResult, AreaReadResponse, StreamEvent, StreamOperation};
use crate::routing::{RouteTable, DEFAULT_RF};
use crate::storage::traits::{KvStore, KvTransaction};
use std::collections::HashMap;
use std::sync::Arc;

/// Maximum number of events allowed in a single append transaction.
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
/// DISABLED - pending midge KvStore integration
pub struct StreamService {
    kv_store: Arc<dyn KvStore>,
    subscriptions: RouteTable,
    active_transactions: HashMap<(u32, String), ActiveTransaction>,
}

impl StreamService {
    /// Create a new stream service
    pub fn new(kv_store: Arc<dyn KvStore>) -> Self {
        Self {
            kv_store,
            subscriptions: RouteTable::new(),
            active_transactions: HashMap::new(),
        }
    }

    /// TODO: Implement after midge API integration
    pub async fn handle_operation(
        &mut self,
        _params: StreamOperationParams<'_>,
    ) -> Result<StreamResponse, String> {
        Err("StreamService is disabled pending midge KvStore integration".to_string())
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
    CommitAppendOk,
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
    #[tokio::test]
    async fn should_not_return_uncommitted_events_when_reading_area() {
        // This test verifies that reading area streams respects the watermark
        // and prevents consumers from getting ahead of uncommitted transactions.
        // 
        // Currently disabled pending midge KvStore integration.
        // Once StreamService is re-enabled, this should verify:
        // - BeginAppend creates active transaction
        // - Append buffers events without committing
        // - ReadArea before commit returns no events (watermark = 0)
        // - Events only visible after finalize_stream advances watermark
        todo!("Re-enable once StreamService midge integration is complete")
    }

    #[tokio::test]
    async fn should_return_events_only_after_commit_advances_watermark() {
        // This test verifies that committed and finalized events become visible
        // to area readers after the watermark advances.
        //
        // Currently disabled pending midge KvStore integration.
        // Once StreamService is re-enabled, this should verify:
        // - BeginAppend + Append + CommitAppend creates transaction
        // - finalize_stream() advances watermark and makes events visible
        // - ReadArea after finalization returns committed events
        todo!("Re-enable once StreamService midge integration is complete")
    }

    #[tokio::test]
    async fn should_only_return_events_up_to_watermark_with_out_of_order_commits() {
        // This test verifies watermark semantics with out-of-order stream commits.
        //
        // Currently disabled pending midge KvStore integration.
        // Once StreamService is re-enabled, this should verify:
        // - Stream 2 begins first but doesn't commit
        // - Stream 1 begins, commits, and finalizes
        // - ReadArea sees only Stream 1's events (watermark blocks Stream 2)
        // - When Stream 2 commits and finalizes, watermark advances
        // - ReadArea now sees both streams' events
        todo!("Re-enable once StreamService midge integration is complete")
    }
}
