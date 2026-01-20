//! KV domain protocol: messages, responses, and error types
//!
//! The KV domain is a thin wrapper over Midge transactions, providing:
//! - Transaction-scoped KV operations
//! - Resource (table) isolation
//! - Explicit RouteFamily → ColumnFamily mapping

use crate::runtime::routing::RouteFamily;
use bytes::Bytes;
use cntryl_midge::WriteOptions;

/// KV operation request
#[derive(Debug, Clone)]
pub enum KvMessage {
    /// Begin a transaction bound to a resource (table)
    Begin {
        route_family: RouteFamily,
        realm: String,
        area: String,
        resource: String,
        mode: TxMode,
        write_options: WriteOptions,
    },

    /// Commit a transaction by ID
    Commit { tx_id: u64 },

    /// Rollback a transaction by ID
    Rollback { tx_id: u64 },

    /// Get a value by key (requires active tx)
    Get {
        tx_id: u64,
        route_family: RouteFamily,
        resource: String,
        key: Bytes,
    },

    /// Put (upsert) a key-value pair (requires active tx)
    Put {
        tx_id: u64,
        route_family: RouteFamily,
        resource: String,
        key: Bytes,
        value: Bytes,
    },

    /// Insert a key-value pair, failing if key exists (requires active tx)
    Insert {
        tx_id: u64,
        route_family: RouteFamily,
        resource: String,
        key: Bytes,
        value: Bytes,
    },

    /// Delete a key (requires active tx)
    Delete {
        tx_id: u64,
        route_family: RouteFamily,
        resource: String,
        key: Bytes,
    },

    /// Delete a range of keys [start, end) (requires active tx)
    DeleteRange {
        tx_id: u64,
        route_family: RouteFamily,
        resource: String,
        start: Bytes,
        end: Bytes,
    },

    /// Scan a range of keys (requires active tx)
    Scan {
        tx_id: u64,
        route_family: RouteFamily,
        resource: String,
        query: ScanQuery,
    },
}

/// Transaction mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxMode {
    /// Read-only transaction
    ReadOnly,
    /// Read-write transaction
    ReadWrite,
}

/// Scan query parameters
#[derive(Debug, Clone)]
pub struct ScanQuery {
    /// Start key (inclusive), None = from beginning
    pub start: Option<Bytes>,
    /// End key (exclusive), None = to end
    pub end: Option<Bytes>,
    /// Maximum number of items to return
    pub limit: Option<usize>,
    /// Reverse scan order
    pub reverse: bool,
}

/// KV operation response
#[derive(Debug, Clone)]
pub enum KvResponse {
    /// Transaction began successfully with server-assigned ID
    BeginOk { tx_id: u64 },

    /// Transaction committed successfully
    CommitOk,

    /// Transaction rolled back successfully
    RollbackOk,

    /// Get result
    GetResult { found: bool, value: Option<Bytes> },

    /// Put succeeded
    PutOk,

    /// Insert succeeded
    InsertOk,

    /// Delete succeeded
    DeleteOk,

    /// Delete range succeeded
    DeleteRangeOk,

    /// Scan results
    ScanResult { items: Vec<KvPair>, has_more: bool },

    /// Error occurred
    Error { error: KvError },
}

/// Key-value pair
#[derive(Debug, Clone)]
pub struct KvPair {
    pub key: Bytes,
    pub value: Bytes,
}

/// KV domain errors
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvError {
    /// Invalid route format or missing required fields
    InvalidRoute(String),

    /// Invalid request parameters
    InvalidRequest(String),

    /// Realm validation failed
    InvalidRealm,

    /// Realm mismatch (transaction bound to different realm)
    RealmMismatch,

    /// Resource (table) not found or CF mapping failed
    UnknownResource(String),

    /// Invalid or unknown transaction ID
    InvalidTxId,

    /// Operation requires an active transaction
    NoActiveTx,

    /// Operation targets different resource than the active transaction
    TxScopeViolation { expected: String, actual: String },

    /// Key not found (if applicable for operation)
    NotFound,

    /// Key already exists (insert conflict)
    AlreadyExists,

    /// Transaction conflict or abort (retryable)
    Conflict(String),

    /// Backend storage unavailable
    BackendUnavailable(String),

    /// Backend storage error
    BackendError(String),
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvError::InvalidRoute(msg) => write!(f, "Invalid route: {}", msg),
            KvError::InvalidRequest(msg) => write!(f, "Invalid request: {}", msg),
            KvError::InvalidRealm => write!(f, "Invalid realm"),
            KvError::RealmMismatch => write!(f, "Realm mismatch"),
            KvError::UnknownResource(res) => write!(f, "Unknown resource: {}", res),
            KvError::InvalidTxId => write!(f, "Invalid or unknown transaction ID"),
            KvError::NoActiveTx => write!(f, "No active transaction"),
            KvError::TxScopeViolation { expected, actual } => {
                write!(
                    f,
                    "Transaction scope violation: expected resource '{}', got '{}'",
                    expected, actual
                )
            }
            KvError::NotFound => write!(f, "Key not found"),
            KvError::AlreadyExists => write!(f, "Key already exists"),
            KvError::Conflict(msg) => write!(f, "Transaction conflict: {}", msg),
            KvError::BackendUnavailable(msg) => write!(f, "Backend unavailable: {}", msg),
            KvError::BackendError(msg) => write!(f, "Backend error: {}", msg),
        }
    }
}

impl std::error::Error for KvError {}
