use serde::{Deserialize, Serialize};

pub use crate::control::admin::{
    KvLatencySnapshot, KvResourceInventoryEntry, KvTransaction, LeaseWaiterInfo, NoticeRouteInfo,
    NoticeSubscription, ScheduleLatencyBuckets, SchedulePendingClaimInfo, StreamInfo,
    StreamLagBuckets, StreamLatencyBuckets,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvTransactionsList {
    pub transactions: Vec<KvTransaction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvByteValue {
    pub base64: String,
    pub utf8: Option<String>,
    pub len_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvCommittedValueResponse {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub key: KvByteValue,
    pub found: bool,
    pub value: Option<KvByteValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvCommittedPair {
    pub key: KvByteValue,
    pub value: KvByteValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvPrefixScanResponse {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub prefix: KvByteValue,
    pub limit: usize,
    pub has_more: bool,
    pub items: Vec<KvCommittedPair>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvRowsResponse {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub starts_with: KvByteValue,
    pub limit: usize,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub items: Vec<KvCommittedPair>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRecordsResponse {
    pub route_family: u64,
    pub realm: Option<String>,
    pub area: Option<String>,
    pub resource: Option<String>,
    pub from_offset: u64,
    pub limit: usize,
    pub has_more: bool,
    pub records: Vec<StreamAdminRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAdminRecord {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub resource_offset: u64,
    pub area_offset: Option<u64>,
    pub realm_offset: Option<u64>,
    pub created_at_ms: u64,
    pub body: KvByteValue,
    pub metadata: Option<KvByteValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleExecutionObservationList {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub offset: usize,
    pub limit: usize,
    pub has_more: bool,
    pub observations: Vec<ScheduleExecutionObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleExecutionObservation {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
    pub delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
    pub status: String,
    pub cron: String,
    pub next_run: String,
    pub last_run: Option<String>,
    pub executions_total: u64,
    pub pending_handoffs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleMissedObservationList {
    pub route_family: u64,
    pub limit: usize,
    pub observations: Vec<ScheduleMissedObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleMissedObservation {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
    pub delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
    pub fire_ms: u64,
    pub fire_at: String,
    pub claimed_at: String,
    pub age_seconds: u64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseSearchResponse {
    pub route_family: u64,
    pub limit: usize,
    pub items: Vec<LeaseSearchItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseSearchItem {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub state: String,
    pub owner_id: Option<String>,
    pub owner_session_id: Option<String>,
    pub queued_token: Option<u64>,
    pub expires_at: Option<String>,
    pub acquired_at: Option<String>,
    pub renewals: Option<usize>,
    pub pending_waiters: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeDeliveryObservationList {
    pub route_family: u64,
    pub limit: usize,
    pub observations: Vec<NoticeDeliveryObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeDeliveryObservation {
    pub route_family: u64,
    pub realm: String,
    pub area: Option<String>,
    pub resource: Option<String>,
    pub route: String,
    pub session_id: Option<String>,
    pub subscription_id: Option<u64>,
    pub status: String,
    pub notifications_received: u64,
    pub publishes_total: u64,
    pub publishes_per_minute: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcCallObservationList {
    pub route_family: u64,
    pub limit: usize,
    pub observations: Vec<RpcCallObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcCallObservation {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: Option<String>,
    pub route: String,
    pub correlation_id: Option<String>,
    pub state: String,
    pub submitted_at: Option<String>,
    pub registered_at: Option<String>,
    pub age_seconds: Option<u64>,
    pub worker_session_id: Option<String>,
    pub requests_handled: Option<u64>,
    pub average_latency_ms: Option<f64>,
}

pub(crate) struct StreamSearchRequest {
    pub family: u64,
    pub realm: Option<String>,
    pub area: Option<String>,
    pub resource: Option<String>,
    pub from_offset: u64,
    pub limit: usize,
    pub discriminator: Option<String>,
}

pub(crate) struct LeaseSearchRequest {
    pub family: u64,
    pub realm: Option<String>,
    pub area: Option<String>,
    pub resource: Option<String>,
    pub owner: Option<String>,
    pub state: Option<String>,
    pub limit: usize,
}

pub(crate) struct RpcCallObservationRequest {
    pub family: u64,
    pub realm: Option<String>,
    pub area: Option<String>,
    pub resource: Option<String>,
    pub operation: Option<String>,
    pub query: Option<String>,
    pub limit: usize,
}

pub struct ScheduleExecutionObservationRequest {
    pub family: u64,
    pub operation: Option<String>,
    pub offset: usize,
    pub limit: usize,
}

pub(crate) struct ScheduleMissedObservationRequest {
    pub family: u64,
    pub realm: Option<String>,
    pub area: Option<String>,
    pub resource: Option<String>,
    pub operation: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamsList {
    pub streams: Vec<StreamInfo>,
}

/// Collection of live in-memory Notice subscriptions for the current broker
/// process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeSubscriptionsList {
    pub subscriptions: Vec<NoticeSubscription>,
}

/// Collection of live in-memory Notice route counters for the current broker
/// process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeRoutesList {
    pub routes: Vec<NoticeRouteInfo>,
}
