//! List endpoints for admin API
//!
//! Routes:
//! - GET /api/v1/admin/kv/transactions - List active KV transactions
//! - GET /api/v1/admin/stream/streams - List active streams
//! - GET /api/v1/admin/notice/subscriptions - List active subscriptions
//! - GET /api/v1/admin/notice/routes - List routes with subscriber counts
//! - GET /api/v1/admin/queue/queues - List queues with depths
//! - GET /api/v1/admin/queue/leases - List active queue leases
//! - GET /api/v1/admin/rpc/workers - List registered RPC workers
//! - GET /api/v1/admin/rpc/pending - List pending RPC requests
//! - GET /api/v1/admin/lease/leases - List active leases
//! - GET /api/v1/admin/schedule/schedules - List schedules
//! - GET /api/v1/admin/sessions - List active sessions

use crate::boot::Runtime;
use hyper::{Body, Response};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;

// ==================== KV Domain ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvTransactionsList {
    pub transactions: Vec<KvTransaction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvTransaction {
    pub tx_id: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub mode: String,
    pub started_at: String,
    pub operations_count: usize,
    pub idle_seconds: u64,
}

pub async fn handle_list_kv_transactions(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let transactions = runtime.kv_list_transactions(realm);
    let list = KvTransactionsList { transactions };
    crate::api::admin::json_response(list)
}

// ==================== Stream Domain ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamsList {
    pub streams: Vec<StreamInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub offset: u64,
    pub watermark: u64,
    pub size_bytes: u64,
    pub sessions_active: usize,
}

pub async fn handle_list_streams(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let streams = runtime.stream_list_streams(realm);
    let list = StreamsList { streams };
    crate::api::admin::json_response(list)
}

// ==================== Notice Domain ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeSubscriptionsList {
    pub subscriptions: Vec<NoticeSubscription>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeSubscription {
    pub subscription_id: u64,
    pub session_id: String,
    pub realm: String,
    pub pattern: String,
    pub created_at: String,
    pub notifications_received: u64,
}

pub async fn handle_list_notice_subscriptions(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
    route_pattern: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let subscriptions = runtime.notice_list_subscriptions(realm, route_pattern);
    let list = NoticeSubscriptionsList { subscriptions };
    crate::api::admin::json_response(list)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeRoutesList {
    pub routes: Vec<NoticeRouteInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeRouteInfo {
    pub route: String,
    pub subscribers: usize,
    pub publishes_total: u64,
    pub publishes_per_minute: f64,
}

pub async fn handle_list_notice_routes(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let routes = runtime.notice_list_routes(realm);
    let list = NoticeRoutesList { routes };
    crate::api::admin::json_response(list)
}

// ==================== Queue Domain ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuesList {
    pub queues: Vec<QueueInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInfo {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub messages_ready: usize,
    pub messages_leased: usize,
    pub messages_total: usize,
    pub oldest_message_age_seconds: u64,
}

pub async fn handle_list_queues(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let queues = runtime.queue_list_queues(realm);
    let list = QueuesList { queues };
    crate::api::admin::json_response(list)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueLeasesList {
    pub leases: Vec<QueueLease>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueLease {
    pub message_id: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub lease_token: String,
    pub session_id: String,
    pub expires_at: String,
    pub attempts: usize,
}

pub async fn handle_list_queue_leases(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let leases = runtime.queue_list_leases(realm);
    let list = QueueLeasesList { leases };
    crate::api::admin::json_response(list)
}

// ==================== RPC Domain ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcWorkersList {
    pub workers: Vec<RpcWorker>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcWorker {
    pub session_id: String,
    pub realm: String,
    pub route: String,
    pub registered_at: String,
    pub requests_handled: u64,
    pub average_latency_ms: f64,
}

pub async fn handle_list_rpc_workers(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let workers = runtime.rpc_list_workers(realm);
    let list = RpcWorkersList { workers };
    crate::api::admin::json_response(list)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcPendingList {
    pub requests: Vec<RpcPendingRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcPendingRequest {
    pub correlation_id: String,
    pub route: String,
    pub submitted_at: String,
    pub age_seconds: u64,
    pub worker_session_id: Option<String>,
}

pub async fn handle_list_rpc_pending(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let requests = runtime.rpc_list_pending(realm);
    let list = RpcPendingList { requests };
    crate::api::admin::json_response(list)
}

// ==================== Lease Domain ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeasesList {
    pub leases: Vec<LeaseInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseInfo {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub owner_session_id: String,
    pub acquired_at: String,
    pub expires_at: String,
    pub renewals: usize,
    pub fencing_token: u64,
}

pub async fn handle_list_leases(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let leases = runtime.lease_list_leases(realm);
    let list = LeasesList { leases };
    crate::api::admin::json_response(list)
}

// ==================== Schedule Domain ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulesList {
    pub schedules: Vec<ScheduleInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleInfo {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub cron: String,
    pub next_run: String,
    pub last_run: Option<String>,
    pub executions_total: u64,
    pub enabled: bool,
}

pub async fn handle_list_schedules(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let schedules = runtime.schedule_list_schedules(realm);
    let list = SchedulesList { schedules };
    crate::api::admin::json_response(list)
}

// ==================== Sessions ====================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionsList {
    pub sessions: Vec<SessionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub session_id: String,
    pub realm: String,
    pub connected_at: String,
    pub idle_seconds: u64,
    pub messages_received: u64,
    pub messages_sent: u64,
    pub transport: String,
    pub remote_addr: String,
}

pub async fn handle_list_sessions(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let sessions = runtime.list_sessions(realm);
    let list = SessionsList { sessions };
    crate::api::admin::json_response(list)
}

// ==================== Helper Functions ====================

/// Parse query parameters from request URI
pub fn parse_query_params(uri: &hyper::Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();

    if let Some(query) = uri.query() {
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                // Simple URL decode - handle %20 for spaces at minimum
                let decoded_key = key.replace("%20", " ");
                let decoded_value = value.replace("%20", " ");
                params.insert(decoded_key, decoded_value);
            }
        }
    }

    params
}
