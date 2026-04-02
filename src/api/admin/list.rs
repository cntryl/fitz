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
/// Live in-memory KV resource detail for the current broker process.
///
/// `transactions_active` counts session-scoped transactions only. It resets
/// after disconnect cleanup or broker restart and does not imply durable
/// transaction recovery.
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

/// Live in-memory Lease resource detail for the current broker process.
///
/// `active_leases` counts only leases currently tracked in memory for this
/// resource. The count drops on disconnect cleanup, resets after broker
/// restart, and does not imply durable recovery.
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

/// Live in-memory Notice resource detail for the current broker process.
///
/// `subscriptions_active` counts only currently active subscriptions matching
/// this resource. The count drops on disconnect cleanup, resets after broker
/// restart, and does not imply durable or replayable pub/sub state.
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

/// Point-in-time live in-memory RPC state for a single operation on the
/// current broker process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcOperationDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
    pub workers_registered: usize,
    pub requests_pending: usize,
}

impl KvResourceDetail {
    fn from_count(path: &ResourcePath<'_>, transactions_active: usize) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            transactions_active,
        }
    }
}

impl QueueResourceDetail {
    fn from_queue(item: QueueInfo) -> Self {
        Self {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
            messages_ready: item.messages_ready,
            messages_leased: item.messages_leased,
            messages_total: item.messages_total,
            oldest_message_age_seconds: item.oldest_message_age_seconds,
        }
    }

    fn empty(path: &ResourcePath<'_>) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            messages_ready: 0,
            messages_leased: 0,
            messages_total: 0,
            oldest_message_age_seconds: 0,
        }
    }
}

impl StreamResourceDetail {
    fn from_stream(item: StreamInfo) -> Self {
        Self {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
            offset: item.offset,
            watermark: item.watermark,
            size_bytes: item.size_bytes,
            sessions_active: item.sessions_active,
        }
    }

    fn empty(path: &ResourcePath<'_>) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            offset: 0,
            watermark: 0,
            size_bytes: 0,
            sessions_active: 0,
        }
    }
}

impl LeaseResourceDetail {
    fn from_count(path: &ResourcePath<'_>, active_leases: usize) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            active_leases,
        }
    }
}

impl ScheduleResourceDetail {
    fn empty(path: &ResourcePath<'_>) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            enabled: false,
            cron: None,
            next_run: None,
            executions_total: 0,
        }
    }

    fn from_schedule(item: ScheduleInfo) -> Self {
        Self {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
            enabled: item.enabled,
            cron: Some(item.cron),
            next_run: Some(item.next_run),
            executions_total: item.executions_total,
        }
    }

    fn aggregate(path: &ResourcePath<'_>, schedules: &[ScheduleInfo]) -> Self {
        let next_run = schedules.iter().map(|item| item.next_run.as_str()).min();
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            enabled: schedules.iter().any(|item| item.enabled),
            cron: None,
            next_run: next_run.map(ToString::to_string),
            executions_total: schedules.iter().map(|item| item.executions_total).sum(),
        }
    }
}

impl NoticeResourceDetail {
    fn from_count(path: &ResourcePath<'_>, subscriptions_active: usize) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            subscriptions_active,
        }
    }
}

impl RpcOperationDetail {
    fn from_counts(
        path: &RpcOperationPath<'_>,
        workers_registered: usize,
        requests_pending: usize,
    ) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            operation: path.operation.to_string(),
            workers_registered,
            requests_pending,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvTransactionsList {
    pub transactions: Vec<KvTransaction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// Session-scoped active KV transaction snapshot for the current broker process.
///
/// These entries reflect live in-memory transaction state only. `tx_id` is a
/// runtime handle, not a durable recovery token. Entries disappear on
/// disconnect or broker restart and are separate from committed-data
/// durability in the storage engine.
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

/// Session-scoped Notice subscription snapshot for the current broker process.
///
/// The subscription exists only in memory, disappears when the owning session
/// disconnects or the broker restarts, and is not durably recoverable or
/// replayable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeSubscription {
    pub subscription_id: u64,
    pub session_id: String,
    pub realm: String,
    pub pattern: String,
    pub created_at: String,
    pub notifications_received: u64,
}

/// Live Notice route activity for the current broker process only.
///
/// Subscriber counts and publish counters describe the running process
/// lifetime and do not represent durable history or replay state.
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

/// Collection of live in-memory RPC worker snapshots for the current broker
/// process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcWorkersList {
    pub workers: Vec<RpcWorker>,
}

/// Live in-memory RPC worker registration for the current broker process.
/// Registrations disappear on disconnect or broker restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcWorker {
    pub session_id: String,
    pub realm: String,
    pub route: String,
    pub registered_at: String,
    pub requests_handled: u64,
    pub average_latency_ms: f64,
}

/// Collection of live in-memory pending RPC request snapshots for the current
/// broker process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcPendingList {
    pub requests: Vec<RpcPendingRequest>,
}

/// Live in-memory pending RPC request tracked by the current broker process.
/// Pending requests disappear on timeout, cleanup, or broker restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcPendingRequest {
    pub correlation_id: String,
    pub route: String,
    pub submitted_at: String,
    pub age_seconds: u64,
    pub worker_session_id: Option<String>,
}

/// Collection of live in-memory Lease snapshots for the current broker
/// process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeasesList {
    pub leases: Vec<LeaseInfo>,
}

/// Session-scoped active Lease snapshot for the current broker process.
///
/// Lease ownership exists only in memory, disappears on disconnect cleanup or
/// broker restart, and is not durably recoverable. `fencing_token` is
/// process-local and resets when the broker process restarts.
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
    pub(crate) fn snapshot(
        session_id: u64,
        realm: &str,
        route: &str,
        registered_at: &str,
        requests_handled: u64,
        average_latency_ms: f64,
    ) -> Self {
        Self {
            session_id: session_id.to_string(),
            realm: realm.to_string(),
            route: route.to_string(),
            registered_at: registered_at.to_string(),
            requests_handled,
            average_latency_ms,
        }
    }
}

impl RpcPendingRequest {
    pub(crate) fn snapshot(
        correlation_id: String,
        route: &str,
        submitted_at: &str,
        age_seconds: u64,
        worker_session_id: Option<String>,
    ) -> Self {
        Self {
            correlation_id,
            route: route.to_string(),
            submitted_at: submitted_at.to_string(),
            age_seconds,
            worker_session_id,
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

impl ResourcePath<'_> {
    fn matches(&self, realm: &str, area: &str, resource: &str) -> bool {
        self.realm == realm && self.area == area && self.resource == resource
    }
}

impl ResourceRef {
    fn new(realm: String, area: String, resource: String) -> Self {
        Self {
            realm,
            area,
            resource,
        }
    }

    fn matches_path(&self, path: &ResourcePath<'_>) -> bool {
        path.matches(&self.realm, &self.area, &self.resource)
    }
}

trait IntoResourceRef {
    fn into_resource_ref(self) -> ResourceRef;
}

impl IntoResourceRef for KvTransaction {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl IntoResourceRef for QueueInfo {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl IntoResourceRef for StreamInfo {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl IntoResourceRef for LeaseInfo {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl IntoResourceRef for ScheduleInfo {
    fn into_resource_ref(self) -> ResourceRef {
        ResourceRef::new(self.realm, self.area, self.resource)
    }
}

impl RpcOperationPath<'_> {
    fn matches(&self, realm: &str, area: &str, resource: &str, operation: &str) -> bool {
        self.realm == realm
            && self.area == area
            && self.resource == resource
            && self.operation == operation
    }
}

impl OwnedRpcOperation {
    fn matches_resource_path(&self, path: &ResourcePath<'_>) -> bool {
        path.matches(&self.realm, &self.area, &self.resource)
    }

    fn matches_operation_path(&self, path: &RpcOperationPath<'_>) -> bool {
        path.matches(&self.realm, &self.area, &self.resource, &self.operation)
    }
}

fn collect_resource_refs<T: IntoResourceRef>(
    items: impl IntoIterator<Item = T>,
) -> Vec<ResourceRef> {
    items
        .into_iter()
        .map(IntoResourceRef::into_resource_ref)
        .collect()
}

fn collect_distinct_entries<T>(
    values: impl IntoIterator<Item = String>,
    entry: impl Fn(String) -> T,
) -> Vec<T> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(entry)
        .collect()
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
    RealmCollection {
        realms: collect_distinct_entries(
            resources.iter().map(|item| item.realm.clone()),
            |realm| RealmEntry { realm },
        ),
    }
}

pub fn collect_areas(resources: &[ResourceRef], realm: &str) -> AreaCollection {
    AreaCollection {
        realm: realm.to_string(),
        areas: collect_distinct_entries(
            resources
                .iter()
                .filter(|item| item.realm == realm)
                .map(|item| item.area.clone()),
            |area| AreaEntry { area },
        ),
    }
}

pub fn collect_resources(resources: &[ResourceRef], realm: &str, area: &str) -> ResourceCollection {
    ResourceCollection {
        realm: realm.to_string(),
        area: area.to_string(),
        resources: collect_distinct_entries(
            resources
                .iter()
                .filter(|item| item.realm == realm && item.area == area)
                .map(|item| item.resource.clone()),
            |resource| ResourceEntry { resource },
        ),
    }
}

pub fn kv_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    collect_resource_refs(runtime.kv_list_transactions(None))
}

pub fn queue_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    collect_resource_refs(runtime.queue_list_queues(None))
}

pub fn stream_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    collect_resource_refs(runtime.stream_list_streams(None))
}

pub fn lease_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    collect_resource_refs(runtime.lease_list_leases(None))
}

pub fn schedule_resources(runtime: &Runtime) -> Vec<ResourceRef> {
    collect_resource_refs(runtime.schedule_list_schedules(None))
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
    let operations = collect_distinct_entries(
        runtime
            .rpc_list_workers(None)
            .into_iter()
            .filter_map(|worker| parse_rpc_operation(&worker.route))
            .filter(|operation| operation.matches_resource_path(path))
            .map(|operation| operation.operation),
        |operation| OperationEntry { operation },
    );

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
        .filter(|tx| path.matches(&tx.realm, &tx.area, &tx.resource))
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
        .filter(|lease| path.matches(&lease.realm, &lease.area, &lease.resource))
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
        .filter(|subscription| matches_resource_route(&subscription.pattern, path))
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
        .filter(|worker| matches_operation_route(&worker.route, path))
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
        .filter(|tx| path.matches(&tx.realm, &tx.area, &tx.resource))
        .count();
    KvResourceDetail::from_count(path, transactions)
}

pub fn queue_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> QueueResourceDetail {
    let queue = runtime
        .queue_list_queues(Some(path.realm))
        .into_iter()
        .find(|item| path.matches(&item.realm, &item.area, &item.resource));
    match queue {
        Some(item) => QueueResourceDetail::from_queue(item),
        None => QueueResourceDetail::empty(path),
    }
}

pub fn stream_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> StreamResourceDetail {
    let stream = runtime
        .stream_list_streams(Some(path.realm))
        .into_iter()
        .find(|item| path.matches(&item.realm, &item.area, &item.resource));
    match stream {
        Some(item) => StreamResourceDetail::from_stream(item),
        None => StreamResourceDetail::empty(path),
    }
}

pub fn lease_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> LeaseResourceDetail {
    let active_leases = runtime
        .lease_list_leases(Some(path.realm))
        .into_iter()
        .filter(|item| path.matches(&item.realm, &item.area, &item.resource))
        .count();
    LeaseResourceDetail::from_count(path, active_leases)
}

pub fn schedule_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> ScheduleResourceDetail {
    let schedules = runtime
        .schedule_list_schedules(Some(path.realm))
        .into_iter()
        .filter(|item| path.matches(&item.realm, &item.area, &item.resource))
        .collect::<Vec<_>>();
    if schedules.is_empty() {
        return ScheduleResourceDetail::empty(path);
    }

    if schedules.len() == 1 {
        let item = schedules.into_iter().next().expect("single schedule");
        return ScheduleResourceDetail::from_schedule(item);
    }

    ScheduleResourceDetail::aggregate(path, &schedules)
}

pub fn notice_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> NoticeResourceDetail {
    let subscriptions_active = runtime
        .notice_list_subscriptions(Some(path.realm), None)
        .into_iter()
        .filter(|item| matches_resource_route(&item.pattern, path))
        .count();
    NoticeResourceDetail::from_count(path, subscriptions_active)
}

pub fn rpc_operation_detail(runtime: &Runtime, path: &RpcOperationPath<'_>) -> RpcOperationDetail {
    let workers_registered = runtime
        .rpc_list_workers(Some(path.realm))
        .into_iter()
        .filter(|worker| matches_operation_route(&worker.route, path))
        .count();
    let requests_pending = runtime
        .rpc_list_pending(Some(path.realm))
        .into_iter()
        .filter(|request| matches_operation_route(&request.route, path))
        .count();
    RpcOperationDetail::from_counts(path, workers_registered, requests_pending)
}

fn parse_flexible_route(route: &str) -> Option<ResourceRef> {
    route_triplet(route).map(|parts| {
        ResourceRef::new(
            parts.realm.to_string(),
            parts.area.to_string(),
            parts.resource.to_string(),
        )
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

fn matches_resource_route(route: &str, path: &ResourcePath<'_>) -> bool {
    parse_flexible_route(route).is_some_and(|parsed| parsed.matches_path(path))
}

fn matches_operation_route(route: &str, path: &RpcOperationPath<'_>) -> bool {
    parse_rpc_operation(route).is_some_and(|parsed| parsed.matches_operation_path(path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_match_resource_ref_given_matching_resource_path() {
        // Arrange
        let path = ResourcePath {
            realm: "acme",
            area: "billing",
            resource: "invoices",
        };
        let resource = ResourceRef {
            realm: "acme".to_string(),
            area: "billing".to_string(),
            resource: "invoices".to_string(),
        };

        // Act
        let result = resource.matches_path(&path);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_resource_route_given_matching_path() {
        // Arrange
        let path = ResourcePath {
            realm: "acme",
            area: "billing",
            resource: "invoices",
        };

        // Act
        let result = matches_resource_route("notice://acme/billing/invoices", &path);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_rpc_operation_given_matching_operation_path() {
        // Arrange
        let path = RpcOperationPath {
            realm: "acme",
            area: "billing",
            resource: "invoices",
            operation: "send",
        };
        let operation = OwnedRpcOperation {
            realm: "acme".to_string(),
            area: "billing".to_string(),
            resource: "invoices".to_string(),
            operation: "send".to_string(),
        };

        // Act
        let result = operation.matches_operation_path(&path);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_operation_route_given_matching_path() {
        // Arrange
        let path = RpcOperationPath {
            realm: "acme",
            area: "billing",
            resource: "invoices",
            operation: "send",
        };

        // Act
        let result = matches_operation_route("rpc://acme/billing/invoices/send", &path);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_collect_resource_refs_given_resource_items() {
        // Arrange
        let items = vec![QueueInfo::snapshot("acme", "billing", "invoices")];

        // Act
        let resources = collect_resource_refs(items);

        // Assert
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].realm, "acme");
        assert_eq!(resources[0].area, "billing");
        assert_eq!(resources[0].resource, "invoices");
    }

    #[test]
    fn should_collect_realms_given_duplicate_resources() {
        // Arrange
        let resources = vec![
            ResourceRef::new(
                "prod".to_string(),
                "billing".to_string(),
                "invoices".to_string(),
            ),
            ResourceRef::new(
                "prod".to_string(),
                "jobs".to_string(),
                "pending".to_string(),
            ),
            ResourceRef::new(
                "staging".to_string(),
                "billing".to_string(),
                "invoices".to_string(),
            ),
        ];

        // Act
        let collection = collect_realms(&resources);

        // Assert
        assert_eq!(collection.realms.len(), 2);
        assert_eq!(collection.realms[0].realm, "prod");
        assert_eq!(collection.realms[1].realm, "staging");
    }

    #[test]
    fn should_collect_areas_given_realm_filter() {
        // Arrange
        let resources = vec![
            ResourceRef::new(
                "prod".to_string(),
                "billing".to_string(),
                "invoices".to_string(),
            ),
            ResourceRef::new(
                "prod".to_string(),
                "jobs".to_string(),
                "pending".to_string(),
            ),
            ResourceRef::new(
                "staging".to_string(),
                "support".to_string(),
                "tickets".to_string(),
            ),
        ];

        // Act
        let collection = collect_areas(&resources, "prod");

        // Assert
        assert_eq!(collection.realm, "prod");
        assert_eq!(collection.areas.len(), 2);
        assert_eq!(collection.areas[0].area, "billing");
        assert_eq!(collection.areas[1].area, "jobs");
    }

    #[test]
    fn should_collect_resources_given_area_filter() {
        // Arrange
        let resources = vec![
            ResourceRef::new("prod".to_string(), "jobs".to_string(), "active".to_string()),
            ResourceRef::new(
                "prod".to_string(),
                "jobs".to_string(),
                "pending".to_string(),
            ),
            ResourceRef::new(
                "prod".to_string(),
                "billing".to_string(),
                "invoices".to_string(),
            ),
        ];

        // Act
        let collection = collect_resources(&resources, "prod", "jobs");

        // Assert
        assert_eq!(collection.realm, "prod");
        assert_eq!(collection.area, "jobs");
        assert_eq!(collection.resources.len(), 2);
        assert_eq!(collection.resources[0].resource, "active");
        assert_eq!(collection.resources[1].resource, "pending");
    }

    #[test]
    fn should_aggregate_schedule_detail_given_multiple_schedules() {
        // Arrange
        let path = ResourcePath {
            realm: "acme",
            area: "billing",
            resource: "invoices",
        };
        let schedules = vec![
            ScheduleInfo {
                realm: "acme".to_string(),
                area: "billing".to_string(),
                resource: "invoices".to_string(),
                operation: "send".to_string(),
                cron: "0 * * * *".to_string(),
                next_run: "2026-03-31T02:00:00Z".to_string(),
                last_run: None,
                executions_total: 2,
                enabled: false,
            },
            ScheduleInfo {
                realm: "acme".to_string(),
                area: "billing".to_string(),
                resource: "invoices".to_string(),
                operation: "retry".to_string(),
                cron: "*/5 * * * *".to_string(),
                next_run: "2026-03-31T01:00:00Z".to_string(),
                last_run: None,
                executions_total: 3,
                enabled: true,
            },
        ];

        // Act
        let detail = ScheduleResourceDetail::aggregate(&path, &schedules);

        // Assert
        assert!(detail.enabled);
        assert_eq!(detail.cron, None);
        assert_eq!(detail.next_run.as_deref(), Some("2026-03-31T01:00:00Z"));
        assert_eq!(detail.executions_total, 5);
    }
}
