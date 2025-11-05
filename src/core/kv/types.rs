//! KV domain types
//!
//! KV operations use TLV encoding with route-based key namespacing.

/// KV operation types determined by route resource or operation segment
#[derive(Debug, Clone)]
pub enum KvOperation {
    /// Put - store key-value pair (TAG_ID = key, TAG_BODY = value)
    Put,
    /// Get - retrieve value by key (TAG_ID = key)
    Get,
    /// Delete - remove key (TAG_ID = key)
    Delete,
    /// Scan - list keys from start (TAG_ID = start_key, optional limit)
    Scan,
    /// Batch - atomic multi-operation transaction (puts, inserts, deletes, etc.)
    Batch,
    /// GetMany - retrieve multiple keys (read-only batch)
    GetMany,
    /// DeleteRange - remove keys in range [start, end)
    DeleteRange,
}

impl KvOperation {
    /// Determine operation from route
    pub fn from_route(route: &crate::protocol::route::Route) -> Result<Self, String> {
        match route.operation.as_deref() {
            Some("get") => Ok(KvOperation::Get),
            Some("put") => Ok(KvOperation::Put),
            Some("delete") => Ok(KvOperation::Delete),
            Some("scan") => Ok(KvOperation::Scan),
            Some("batch") => Ok(KvOperation::Batch),
            Some("get-many") => Ok(KvOperation::GetMany),
            Some("delete-range") => Ok(KvOperation::DeleteRange),
            None => {
                // Default operation based on presence of body
                // If body present, assume Put; otherwise Get
                Ok(KvOperation::Get)
            }
            Some(op) => Err(format!("Unknown KV operation: {}", op)),
        }
    }
}
