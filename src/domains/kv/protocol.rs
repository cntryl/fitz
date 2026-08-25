//! KV domain protocol: messages, responses, and error types
//!
//! The KV domain is a thin wrapper over Midge transactions, providing:
//! - Transaction-scoped KV operations
//! - Resource (table) isolation
//! - Explicit `RouteFamily` → `ColumnFamily` mapping

use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;
use bytes::Bytes;
use cntryl_midge::WriteOptions;
use serde::{Deserialize, Serialize};

/// Complete namespace and storage scope for one KV resource.
///
/// `route_family` is broker-internal isolation. `realm`, `area`, and `resource`
/// are the independently supplied application-visible route components.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvResourceScope {
    pub route_family: RouteFamily,
    pub realm: String,
    pub area: String,
    pub resource: String,
}

impl KvResourceScope {
    #[must_use]
    pub fn new(
        route_family: RouteFamily,
        realm: impl Into<String>,
        area: impl Into<String>,
        resource: impl Into<String>,
    ) -> Self {
        Self {
            route_family,
            realm: realm.into(),
            area: area.into(),
            resource: resource.into(),
        }
    }
}

/// KV operation request
#[derive(Debug, Clone)]
pub enum KvMessage {
    /// Begin a transaction bound to a resource (table)
    Begin {
        scope: KvResourceScope,
        mode: TxMode,
        write_options: WriteOptions,
    },

    /// Commit a transaction by ID
    Commit { tx_id: u64, scope: KvResourceScope },

    /// Rollback a transaction by ID
    Rollback { tx_id: u64, scope: KvResourceScope },

    /// Get a value by key (requires active tx)
    Get {
        tx_id: u64,
        scope: KvResourceScope,
        key: Bytes,
    },

    /// Put (upsert) a key-value pair (requires active tx)
    Put {
        tx_id: u64,
        scope: KvResourceScope,
        key: Bytes,
        value: Bytes,
    },

    /// Insert a key-value pair, failing if key exists (requires active tx)
    Insert {
        tx_id: u64,
        scope: KvResourceScope,
        key: Bytes,
        value: Bytes,
    },

    /// Delete a key (requires active tx)
    Delete {
        tx_id: u64,
        scope: KvResourceScope,
        key: Bytes,
    },

    /// Delete a range of keys [start, end) (requires active tx)
    DeleteRange {
        tx_id: u64,
        scope: KvResourceScope,
        start: Bytes,
        end: Bytes,
    },

    /// Scan a range of keys (requires active tx)
    Scan {
        tx_id: u64,
        scope: KvResourceScope,
        query: ScanQuery,
    },
}

impl KvMessage {
    #[must_use]
    pub const fn scope(&self) -> &KvResourceScope {
        match self {
            Self::Begin { scope, .. }
            | Self::Commit { scope, .. }
            | Self::Rollback { scope, .. }
            | Self::Get { scope, .. }
            | Self::Put { scope, .. }
            | Self::Insert { scope, .. }
            | Self::Delete { scope, .. }
            | Self::DeleteRange { scope, .. }
            | Self::Scan { scope, .. } => scope,
        }
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;

    #[test]
    fn should_expose_scope_for_every_kv_message_variant() {
        // Arrange
        let scope = KvResourceScope::new(RouteFamily::new(7), "realm", "area", "resource");
        let message = KvMessage::Rollback {
            tx_id: 1,
            scope: scope.clone(),
        };

        // Act
        let actual = message.scope();

        // Assert
        assert_eq!(actual, &scope);
    }
}

/// KV watch messages handled by `KvDomainSink` before actor dispatch.
#[derive(Debug, Clone)]
pub enum KvSubscriptionMessage {
    Subscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    Unsubscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
}

/// Parsed client request delivered to the KV domain sink.
#[derive(Debug, Clone)]
pub struct KvClientRequest {
    pub meta: ClientFrameMeta,
    pub frame: Result<KvClientFrame, String>,
}

impl KvClientRequest {
    pub fn new(meta: ClientFrameMeta, frame: Result<KvClientFrame, String>) -> Self {
        Self { meta, frame }
    }
}

/// KV request classified after wire parsing.
#[derive(Debug, Clone)]
pub enum KvClientFrame {
    Op(KvMessage),
    Sub(KvSubscriptionMessage),
}

/// Typed KV response to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct KvClientResponse {
    pub meta: ClientFrameMeta,
    pub response: KvResponse,
}

impl KvClientResponse {
    pub fn new(meta: ClientFrameMeta, response: KvResponse) -> Self {
        Self { meta, response }
    }
}

/// Typed KV watch notification to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct KvClientNotification {
    pub session_id: u64,
    pub route_family: RouteFamily,
    pub subscription_id: u64,
    pub route: Route,
    pub notification: KvNotification,
}

impl KvClientNotification {
    #[must_use]
    pub fn new(
        session_id: u64,
        route_family: RouteFamily,
        subscription_id: u64,
        route: Route,
        notification: KvNotification,
    ) -> Self {
        Self {
            session_id,
            route_family,
            subscription_id,
            route,
            notification,
        }
    }
}

/// Lightweight change notification for committed KV mutations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvNotification {
    pub mutation_count: u64,
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
    /// Treat `start` as exclusive rather than inclusive.
    ///
    /// Continuation needs this. A page bounded by the response byte budget can
    /// hold a single pair, and resuming from an inclusive `start` then returns
    /// that same pair forever - the scan never advances and later keys are
    /// unreachable. Resuming exclusively guarantees progress in both
    /// directions. Absent on the wire from older clients, where it defaults to
    /// `false` and the inclusive behaviour is unchanged.
    pub start_exclusive: bool,
}

/// KV operation response
#[derive(Debug, Clone)]
pub enum KvResponse {
    /// Transaction began successfully with server-assigned ID
    BeginOk { tx_id: u64 },

    /// KV watch registered successfully.
    SubscribeOk { subscription_id: u64 },

    /// KV watch removed successfully.
    UnsubscribeOk,

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

    /// Invalid exact or wildcard KV subscription pattern.
    InvalidSubscriptionPattern(String),

    /// Per-session wildcard subscription limit reached.
    SubscriptionLimit,

    /// Realm validation failed
    InvalidRealm,

    /// `RouteFamily` is invalid (for example, `id=0` maps to the default CF)
    InvalidRouteFamily,

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
            KvError::InvalidRoute(msg) => write!(f, "Invalid route: {msg}"),
            KvError::InvalidRequest(msg) => write!(f, "Invalid request: {msg}"),
            KvError::InvalidSubscriptionPattern(msg) => {
                write!(f, "Invalid subscription pattern: {msg}")
            }
            KvError::SubscriptionLimit => write!(
                f,
                "Wildcard subscription limit exceeded ({} per session)",
                crate::domains::subscription_state::MAX_WILDCARD_REGISTRATIONS_PER_SESSION
            ),
            KvError::InvalidRealm => write!(f, "Invalid realm"),
            KvError::InvalidRouteFamily => {
                write!(f, "Invalid route family (cannot be zero)")
            }
            KvError::RealmMismatch => write!(f, "Realm mismatch"),
            KvError::UnknownResource(res) => write!(f, "Unknown resource: {res}"),
            KvError::InvalidTxId => write!(f, "Invalid or unknown transaction ID"),
            KvError::NoActiveTx => write!(f, "No active transaction"),
            KvError::TxScopeViolation { expected, actual } => {
                write!(
                    f,
                    "Transaction scope violation: expected resource '{expected}', got '{actual}'"
                )
            }
            KvError::NotFound => write!(f, "Key not found"),
            KvError::AlreadyExists => write!(f, "Key already exists"),
            KvError::Conflict(msg) => write!(f, "Transaction conflict: {msg}"),
            KvError::BackendUnavailable(msg) => write!(f, "Backend unavailable: {msg}"),
            KvError::BackendError(msg) => write!(f, "Backend error: {msg}"),
        }
    }
}

impl std::error::Error for KvError {}
