//! Hierarchical list endpoints for admin API

use crate::boot::Runtime;
use crate::runtime::routing::{route_quad, route_triplet};
use hyper::{Body, Response};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::convert::Infallible;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmCollection {
    pub realms: Vec<RealmEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmEntry {
    pub realm: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealmDetail {
    pub realm: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AreaCollection {
    pub realm: String,
    pub areas: Vec<AreaEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AreaEntry {
    pub area: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AreaDetail {
    pub realm: String,
    pub area: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceCollection {
    pub realm: String,
    pub area: String,
    pub resources: Vec<ResourceEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceEntry {
    pub resource: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRef {
    pub realm: String,
    pub area: String,
    pub resource: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvResourceDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub transactions_active: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueResourceDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub messages_ready: usize,
    pub messages_leased: usize,
    pub messages_total: usize,
    pub oldest_message_age_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamResourceDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub offset: u64,
    pub watermark: u64,
    pub size_bytes: u64,
    pub sessions_active: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseResourceDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub active_leases: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleResourceDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub enabled: bool,
    pub cron: Option<String>,
    pub next_run: Option<String>,
    pub executions_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeResourceDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub subscriptions_active: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationCollection {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operations: Vec<OperationEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationEntry {
    pub operation: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcOperationDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
    pub workers_registered: usize,
    pub requests_pending: usize,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeSubscriptionsList {
    pub subscriptions: Vec<NoticeSubscription>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeRoutesList {
    pub routes: Vec<NoticeRouteInfo>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeRouteInfo {
    pub route: String,
    pub subscribers: usize,
    pub publishes_total: u64,
    pub publishes_per_minute: f64,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulesList {
    pub schedules: Vec<ScheduleInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleInfo {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
    pub cron: String,
    pub next_run: String,
    pub last_run: Option<String>,
    pub executions_total: u64,
    pub enabled: bool,
}

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

impl KvTransaction {
    pub(crate) fn snapshot(
        tx_id: u64,
        session_id: u64,
        realm: &str,
        area: &str,
        resource: &str,
        started_at: &str,
    ) -> Self {
        Self {
            tx_id,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            mode: format!("session:{session_id}:readwrite"),
            started_at: started_at.to_string(),
            operations_count: 0,
            idle_seconds: 0,
        }
    }
}

impl StreamInfo {
    pub(crate) fn snapshot(
        realm: &str,
        area: &str,
        resource: &str,
        offset: u64,
        watermark: u64,
        sessions_active: usize,
    ) -> Self {
        Self {
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            offset,
            watermark,
            size_bytes: 0,
            sessions_active,
        }
    }
}

impl NoticeSubscription {
    pub(crate) fn snapshot(
        subscription_id: u64,
        session_id: u64,
        realm: &str,
        pattern: String,
        created_at: &str,
    ) -> Self {
        Self {
            subscription_id,
            session_id: session_id.to_string(),
            realm: realm.to_string(),
            pattern,
            created_at: created_at.to_string(),
            notifications_received: 0,
        }
    }
}

impl NoticeRouteInfo {
    pub(crate) fn snapshot(route: String, subscribers: usize) -> Self {
        Self {
            route,
            subscribers,
            publishes_total: 0,
            publishes_per_minute: 0.0,
        }
    }
}

impl QueueInfo {
    pub(crate) fn snapshot(realm: &str, area: &str, resource: &str) -> Self {
        Self {
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            messages_ready: 0,
            messages_leased: 0,
            messages_total: 0,
            oldest_message_age_seconds: 0,
        }
    }
}

impl RpcWorker {
    pub(crate) fn snapshot(session_id: u64, realm: &str, route: &str, registered_at: &str) -> Self {
        Self {
            session_id: session_id.to_string(),
            realm: realm.to_string(),
            route: route.to_string(),
            registered_at: registered_at.to_string(),
            requests_handled: 0,
            average_latency_ms: 0.0,
        }
    }
}

impl RpcPendingRequest {
    pub(crate) fn snapshot(
        correlation_id: String,
        caller_session_id: u64,
        submitted_at: &str,
    ) -> Self {
        Self {
            correlation_id,
            route: format!("rpc://pending/session/{caller_session_id}"),
            submitted_at: submitted_at.to_string(),
            age_seconds: 0,
            worker_session_id: None,
        }
    }
}

impl LeaseInfo {
    pub(crate) fn snapshot(
        realm: &str,
        area: &str,
        resource: &str,
        owner_session_id: &str,
        acquired_at: &str,
        expires_at: String,
        fencing_token: u64,
    ) -> Self {
        Self {
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            owner_session_id: owner_session_id.to_string(),
            acquired_at: acquired_at.to_string(),
            expires_at,
            renewals: 0,
            fencing_token,
        }
    }
}

impl ScheduleInfo {
    pub(crate) fn enabled_snapshot(
        realm: String,
        area: String,
        resource: String,
        operation: String,
        cron: String,
        next_run: &str,
    ) -> Self {
        Self {
            realm,
            area,
            resource,
            operation,
            cron,
            next_run: next_run.to_string(),
            last_run: None,
            executions_total: 0,
            enabled: true,
        }
    }

    pub(crate) fn matches_identity(
        &self,
        realm: &str,
        area: &str,
        resource: &str,
        operation: &str,
    ) -> bool {
        self.realm == realm
            && self.area == area
            && self.resource == resource
            && self.operation == operation
    }
}

#[derive(Debug, Clone)]
pub struct ResourcePath<'a> {
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
}

#[derive(Debug, Clone)]
pub struct RpcOperationPath<'a> {
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub operation: &'a str,
}

#[derive(Debug, Clone)]
struct OwnedRpcOperation {
    realm: String,
    area: String,
    resource: String,
    operation: String,
}

pub fn parse_query_params(uri: &hyper::Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if let Some(query) = uri.query() {
        for pair in query.split('&') {
            if let Some((key, value)) = pair.split_once('=') {
                params.insert(key.replace("%20", " "), value.replace("%20", " "));
            }
        }
    }
    params
}

pub fn collect_realms(resources: &[ResourceRef]) -> RealmCollection {
    let realms = resources
        .iter()
        .map(|item| item.realm.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|realm| RealmEntry { realm })
        .collect();
    RealmCollection { realms }
}

pub fn collect_areas(resources: &[ResourceRef], realm: &str) -> AreaCollection {
    let areas = resources
        .iter()
        .filter(|item| item.realm == realm)
        .map(|item| item.area.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|area| AreaEntry { area })
        .collect();
    AreaCollection {
        realm: realm.to_string(),
        areas,
    }
}

pub fn collect_resources(resources: &[ResourceRef], realm: &str, area: &str) -> ResourceCollection {
    let resources = resources
        .iter()
        .filter(|item| item.realm == realm && item.area == area)
        .map(|item| item.resource.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|resource| ResourceEntry { resource })
        .collect();
    ResourceCollection {
        realm: realm.to_string(),
        area: area.to_string(),
        resources,
    }
}

pub fn kv_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    runtime
        .kv_list_transactions(None)
        .into_iter()
        .map(|item| ResourceRef {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
        })
        .collect()
}

pub fn queue_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    runtime
        .queue_list_queues(None)
        .into_iter()
        .map(|item| ResourceRef {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
        })
        .collect()
}

pub fn stream_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    runtime
        .stream_list_streams(None)
        .into_iter()
        .map(|item| ResourceRef {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
        })
        .collect()
}

pub fn lease_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    runtime
        .lease_list_leases(None)
        .into_iter()
        .map(|item| ResourceRef {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
        })
        .collect()
}

pub fn schedule_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    runtime
        .schedule_list_schedules(None)
        .into_iter()
        .map(|item| ResourceRef {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
        })
        .collect()
}

pub fn notice_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    runtime
        .notice_list_subscriptions(None, None)
        .into_iter()
        .filter_map(|item| parse_flexible_route(&item.pattern))
        .collect()
}

pub fn rpc_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    runtime
        .rpc_list_workers(None)
        .into_iter()
        .filter_map(|item| parse_flexible_route(&item.route))
        .collect()
}

pub fn rpc_operations(runtime: &Runtime, path: &ResourcePath<'_>) -> OperationCollection {
    let operations = runtime
        .rpc_list_workers(None)
        .into_iter()
        .filter_map(|worker| parse_rpc_operation(&worker.route))
        .filter(|operation| {
            operation.realm == path.realm
                && operation.area == path.area
                && operation.resource == path.resource
        })
        .map(|operation| operation.operation)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|operation| OperationEntry { operation })
        .collect();

    OperationCollection {
        realm: path.realm.to_string(),
        area: path.area.to_string(),
        resource: path.resource.to_string(),
        operations,
    }
}

pub async fn list_sessions(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let sessions = runtime.list_sessions(realm);
    crate::api::admin::json_response(SessionsList { sessions })
}

pub async fn kv_transactions_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
) -> Result<Response<Body>, Infallible> {
    let transactions = runtime
        .kv_list_transactions(Some(path.realm))
        .into_iter()
        .filter(|tx| tx.realm == path.realm && tx.area == path.area && tx.resource == path.resource)
        .collect();
    crate::api::admin::json_response(KvTransactionsList { transactions })
}

pub async fn queue_leases_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
) -> Result<Response<Body>, Infallible> {
    let leases = runtime
        .queue_list_leases(Some(path.realm))
        .into_iter()
        .filter(|lease| {
            lease.realm == path.realm && lease.area == path.area && lease.resource == path.resource
        })
        .collect();
    crate::api::admin::json_response(QueueLeasesList { leases })
}

pub async fn notice_subscriptions_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
) -> Result<Response<Body>, Infallible> {
    let subscriptions = runtime
        .notice_list_subscriptions(Some(path.realm), None)
        .into_iter()
        .filter(|subscription| {
            parse_flexible_route(&subscription.pattern)
                .map(|route| {
                    route.realm == path.realm
                        && route.area == path.area
                        && route.resource == path.resource
                })
                .unwrap_or(false)
        })
        .collect();
    crate::api::admin::json_response(NoticeSubscriptionsList { subscriptions })
}

pub async fn rpc_workers_for_operation(
    runtime: Arc<Runtime>,
    path: &RpcOperationPath<'_>,
) -> Result<Response<Body>, Infallible> {
    let workers = runtime
        .rpc_list_workers(Some(path.realm))
        .into_iter()
        .filter(|worker| {
            parse_rpc_operation(&worker.route)
                .map(|route| {
                    route.realm == path.realm
                        && route.area == path.area
                        && route.resource == path.resource
                        && route.operation == path.operation
                })
                .unwrap_or(false)
        })
        .collect();
    crate::api::admin::json_response(RpcWorkersList { workers })
}

pub async fn rpc_pending(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
) -> Result<Response<Body>, Infallible> {
    let requests = runtime.rpc_list_pending(realm);
    crate::api::admin::json_response(RpcPendingList { requests })
}

pub fn kv_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> KvResourceDetail {
    let transactions = runtime
        .kv_list_transactions(Some(path.realm))
        .into_iter()
        .filter(|tx| tx.realm == path.realm && tx.area == path.area && tx.resource == path.resource)
        .count();
    KvResourceDetail {
        realm: path.realm.to_string(),
        area: path.area.to_string(),
        resource: path.resource.to_string(),
        transactions_active: transactions,
    }
}

pub fn queue_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> QueueResourceDetail {
    let queue = runtime
        .queue_list_queues(Some(path.realm))
        .into_iter()
        .find(|item| {
            item.realm == path.realm && item.area == path.area && item.resource == path.resource
        });
    match queue {
        Some(item) => QueueResourceDetail {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
            messages_ready: item.messages_ready,
            messages_leased: item.messages_leased,
            messages_total: item.messages_total,
            oldest_message_age_seconds: item.oldest_message_age_seconds,
        },
        None => QueueResourceDetail {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            messages_ready: 0,
            messages_leased: 0,
            messages_total: 0,
            oldest_message_age_seconds: 0,
        },
    }
}

pub fn stream_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> StreamResourceDetail {
    let stream = runtime
        .stream_list_streams(Some(path.realm))
        .into_iter()
        .find(|item| {
            item.realm == path.realm && item.area == path.area && item.resource == path.resource
        });
    match stream {
        Some(item) => StreamResourceDetail {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
            offset: item.offset,
            watermark: item.watermark,
            size_bytes: item.size_bytes,
            sessions_active: item.sessions_active,
        },
        None => StreamResourceDetail {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            offset: 0,
            watermark: 0,
            size_bytes: 0,
            sessions_active: 0,
        },
    }
}

pub fn lease_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> LeaseResourceDetail {
    let active_leases = runtime
        .lease_list_leases(Some(path.realm))
        .into_iter()
        .filter(|item| {
            item.realm == path.realm && item.area == path.area && item.resource == path.resource
        })
        .count();
    LeaseResourceDetail {
        realm: path.realm.to_string(),
        area: path.area.to_string(),
        resource: path.resource.to_string(),
        active_leases,
    }
}

pub fn schedule_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> ScheduleResourceDetail {
    let schedules = runtime
        .schedule_list_schedules(Some(path.realm))
        .into_iter()
        .filter(|item| {
            item.realm == path.realm && item.area == path.area && item.resource == path.resource
        })
        .collect::<Vec<_>>();
    if schedules.is_empty() {
        return ScheduleResourceDetail {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            enabled: false,
            cron: None,
            next_run: None,
            executions_total: 0,
        };
    }

    if schedules.len() == 1 {
        let item = schedules.into_iter().next().expect("single schedule");
        return ScheduleResourceDetail {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
            enabled: item.enabled,
            cron: Some(item.cron),
            next_run: Some(item.next_run),
            executions_total: item.executions_total,
        };
    }

    let next_run = schedules.iter().map(|item| item.next_run.as_str()).min();
    ScheduleResourceDetail {
        realm: path.realm.to_string(),
        area: path.area.to_string(),
        resource: path.resource.to_string(),
        enabled: schedules.iter().any(|item| item.enabled),
        cron: None,
        next_run: next_run.map(ToString::to_string),
        executions_total: schedules.iter().map(|item| item.executions_total).sum(),
    }
}

pub fn notice_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> NoticeResourceDetail {
    let subscriptions_active = runtime
        .notice_list_subscriptions(Some(path.realm), None)
        .into_iter()
        .filter(|item| {
            parse_flexible_route(&item.pattern)
                .map(|route| {
                    route.realm == path.realm
                        && route.area == path.area
                        && route.resource == path.resource
                })
                .unwrap_or(false)
        })
        .count();
    NoticeResourceDetail {
        realm: path.realm.to_string(),
        area: path.area.to_string(),
        resource: path.resource.to_string(),
        subscriptions_active,
    }
}

pub fn rpc_operation_detail(runtime: &Runtime, path: &RpcOperationPath<'_>) -> RpcOperationDetail {
    let workers_registered = runtime
        .rpc_list_workers(Some(path.realm))
        .into_iter()
        .filter(|worker| {
            parse_rpc_operation(&worker.route)
                .map(|route| {
                    route.realm == path.realm
                        && route.area == path.area
                        && route.resource == path.resource
                        && route.operation == path.operation
                })
                .unwrap_or(false)
        })
        .count();
    let requests_pending = runtime
        .rpc_list_pending(Some(path.realm))
        .into_iter()
        .filter(|request| request.route.contains(path.operation))
        .count();
    RpcOperationDetail {
        realm: path.realm.to_string(),
        area: path.area.to_string(),
        resource: path.resource.to_string(),
        operation: path.operation.to_string(),
        workers_registered,
        requests_pending,
    }
}

fn parse_flexible_route(route: &str) -> Option<ResourceRef> {
    route_triplet(route).map(|parts| ResourceRef {
        realm: parts.realm.to_string(),
        area: parts.area.to_string(),
        resource: parts.resource.to_string(),
    })
}

fn parse_rpc_operation(route: &str) -> Option<OwnedRpcOperation> {
    route_quad(route).map(|parts| OwnedRpcOperation {
        realm: parts.realm.to_string(),
        area: parts.area.to_string(),
        resource: parts.resource.to_string(),
        operation: parts.operation.to_string(),
    })
}
