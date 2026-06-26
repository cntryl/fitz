//! Hierarchical list endpoints for admin API

use crate::api::admin::troubleshooting::{
    self, DiagnosticSnapshot, ResourceComparison, ResourceComparisonMetrics,
    ResourceComparisonScope, ResourceComparisonSide,
};
use crate::api::http::Response;
use crate::boot::Runtime;
use crate::domains::stream::sink::AdminStreamReadRequest;
use crate::runtime::routing::{route_quad, route_triplet, RouteFamily};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::convert::Infallible;
use std::sync::Arc;

const DEFAULT_KV_SCAN_LIMIT: usize = 50;
const MAX_KV_SCAN_LIMIT: usize = 100;
const DEFAULT_ADMIN_RECORD_LIMIT: usize = 50;
const MAX_ADMIN_RECORD_LIMIT: usize = 100;

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
    pub diagnostics: DiagnosticSnapshot,
}

/// Fixed age buckets for queue snapshots.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueAgeBuckets {
    pub under_1m: usize,
    pub under_5m: usize,
    pub under_15m: usize,
    pub over_15m: usize,
}

impl QueueAgeBuckets {
    pub(crate) fn record_age_seconds(&mut self, age_seconds: u64) {
        if age_seconds < 60 {
            self.under_1m += 1;
        } else if age_seconds < 300 {
            self.under_5m += 1;
        } else if age_seconds < 900 {
            self.under_15m += 1;
        } else {
            self.over_15m += 1;
        }
    }

    pub(crate) fn merge(&mut self, other: QueueAgeBuckets) {
        self.under_1m += other.under_1m;
        self.under_5m += other.under_5m;
        self.under_15m += other.under_15m;
        self.over_15m += other.over_15m;
    }
}

/// Point-in-time Queue resource detail for the current broker process.
///
/// Counts reflect only the queue actor state currently warm in memory on this
/// broker. They are refreshed from live actors, can disappear after idle
/// eviction or broker restart, and do not represent a durable inventory of all
/// accepted queues. `backlog_age_buckets` groups ready + delayed work by age
/// so operators can see whether pressure is fresh or stale.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueResourceDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_inflight: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_message_age_seconds: u64,
    pub oldest_backlog_age_seconds: u64,
    pub backlog_age_buckets: QueueAgeBuckets,
    pub delay_age_buckets: QueueAgeBuckets,
    pub diagnostics: DiagnosticSnapshot,
}

/// Stream resource detail derived from durable committed metadata plus live
/// append-session counts for the current broker process.
///
/// `offset`, `watermark`, and `size_bytes` survive restart because they come
/// from committed stream metadata. `sessions_active` counts only currently live
/// append sessions on this broker process and resets on disconnect cleanup or
/// broker restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamResourceDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub offset: u64,
    pub watermark: u64,
    pub size_bytes: u64,
    pub sessions_active: usize,
    pub diagnostics: DiagnosticSnapshot,
}

/// Stream realm watermark detail built from committed stream state.
///
/// Watermarks are reported per RouteFamily because Stream sequencing is a hard
/// isolation boundary. `resource_count` and `area_count` reflect the committed
/// stream resources currently visible through the admin snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRealmWatermarkDetail {
    pub realm: String,
    pub area_count: usize,
    pub resource_count: usize,
    pub family_watermarks: Vec<StreamRealmWatermark>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRealmWatermark {
    pub family: u64,
    pub watermark: u64,
}

/// Stream area watermark detail built from committed stream state.
///
/// Watermarks are reported per RouteFamily because Stream sequencing is a hard
/// isolation boundary. `resource_count` reflects the committed stream resources
/// currently visible in the target area.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAreaWatermarkDetail {
    pub realm: String,
    pub area: String,
    pub resource_count: usize,
    pub family_watermarks: Vec<StreamAreaWatermark>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamAreaWatermark {
    pub family: u64,
    pub watermark: u64,
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
    pub oldest_lease_age_seconds: u64,
    pub diagnostics: DiagnosticSnapshot,
}

/// Schedule resource detail derived from the current broker's durable,
/// boot-loaded schedule definitions.
///
/// `enabled`, `cron`, and `next_run` reflect persisted schedule definitions for
/// this resource. `executions_total` reflects persisted acknowledged live
/// handoffs recorded when a claimed occurrence leaves durable pending state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleResourceDetail {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub enabled: bool,
    pub cron: Option<String>,
    pub next_run: Option<String>,
    pub executions_total: u64,
    pub diagnostics: DiagnosticSnapshot,
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
    pub diagnostics: DiagnosticSnapshot,
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
    pub slowest_worker_average_latency_ms: f64,
    pub worker_latency_buckets: RpcLatencyBuckets,
    pub diagnostics: DiagnosticSnapshot,
}

impl KvResourceDetail {
    fn from_count(path: &ResourcePath<'_>, transactions_active: usize) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            transactions_active,
            diagnostics: troubleshooting::kv_resource_diagnostics(transactions_active),
        }
    }
}

impl QueueResourceDetail {
    fn from_queue(item: QueueInfo) -> Self {
        let diagnostics = troubleshooting::queue_resource_diagnostics(
            item.messages_ready,
            item.messages_delayed,
            item.messages_inflight,
            item.messages_dead_lettered,
            item.oldest_backlog_age_seconds,
            item.delay_age_buckets,
        );
        Self {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
            messages_ready: item.messages_ready,
            messages_delayed: item.messages_delayed,
            messages_inflight: item.messages_inflight,
            messages_dead_lettered: item.messages_dead_lettered,
            messages_total: item.messages_total,
            oldest_message_age_seconds: item.oldest_message_age_seconds,
            oldest_backlog_age_seconds: item.oldest_backlog_age_seconds,
            backlog_age_buckets: item.backlog_age_buckets,
            delay_age_buckets: item.delay_age_buckets,
            diagnostics,
        }
    }

    fn empty(path: &ResourcePath<'_>) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            messages_ready: 0,
            messages_delayed: 0,
            messages_inflight: 0,
            messages_dead_lettered: 0,
            messages_total: 0,
            oldest_message_age_seconds: 0,
            oldest_backlog_age_seconds: 0,
            backlog_age_buckets: QueueAgeBuckets::default(),
            delay_age_buckets: QueueAgeBuckets::default(),
            diagnostics: troubleshooting::queue_resource_diagnostics(
                0,
                0,
                0,
                0,
                0,
                QueueAgeBuckets::default(),
            ),
        }
    }
}

impl StreamResourceDetail {
    fn from_stream(item: StreamInfo) -> Self {
        let diagnostics = troubleshooting::stream_resource_diagnostics(
            item.offset,
            item.watermark,
            item.sessions_active,
        );
        Self {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
            offset: item.offset,
            watermark: item.watermark,
            size_bytes: item.size_bytes,
            sessions_active: item.sessions_active,
            diagnostics,
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
            diagnostics: troubleshooting::stream_resource_diagnostics(0, 0, 0),
        }
    }
}

impl StreamRealmWatermarkDetail {
    pub(crate) fn snapshot(
        realm: &str,
        area_count: usize,
        resource_count: usize,
        family_watermarks: Vec<StreamRealmWatermark>,
    ) -> Self {
        Self {
            realm: realm.to_string(),
            area_count,
            resource_count,
            family_watermarks,
        }
    }
}

impl StreamRealmWatermark {
    pub(crate) fn snapshot(family: u64, watermark: u64) -> Self {
        Self { family, watermark }
    }
}

impl StreamAreaWatermarkDetail {
    pub(crate) fn snapshot(
        realm: &str,
        area: &str,
        resource_count: usize,
        family_watermarks: Vec<StreamAreaWatermark>,
    ) -> Self {
        Self {
            realm: realm.to_string(),
            area: area.to_string(),
            resource_count,
            family_watermarks,
        }
    }
}

impl StreamAreaWatermark {
    pub(crate) fn snapshot(family: u64, watermark: u64) -> Self {
        Self { family, watermark }
    }
}

impl LeaseResourceDetail {
    fn from_count(
        path: &ResourcePath<'_>,
        active_leases: usize,
        oldest_lease_age_seconds: u64,
        renewals_total: usize,
    ) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            active_leases,
            oldest_lease_age_seconds,
            diagnostics: troubleshooting::lease_resource_diagnostics(
                active_leases,
                Some(oldest_lease_age_seconds),
                renewals_total,
            ),
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
            diagnostics: troubleshooting::schedule_resource_diagnostics(false, None, None, 0),
        }
    }

    fn from_schedule(item: ScheduleInfo) -> Self {
        let diagnostics = troubleshooting::schedule_resource_diagnostics(
            item.enabled,
            Some(item.next_run.as_str()),
            item.last_run.as_deref(),
            item.executions_total,
        );
        Self {
            realm: item.realm,
            area: item.area,
            resource: item.resource,
            enabled: item.enabled,
            cron: Some(item.cron),
            next_run: Some(item.next_run),
            executions_total: item.executions_total,
            diagnostics,
        }
    }

    fn aggregate(path: &ResourcePath<'_>, schedules: &[ScheduleInfo]) -> Self {
        let next_run = schedules.iter().map(|item| item.next_run.as_str()).min();
        let last_run = schedules
            .iter()
            .filter_map(|item| item.last_run.as_deref())
            .max();
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            enabled: schedules.iter().any(|item| item.enabled),
            cron: None,
            next_run: next_run.map(ToString::to_string),
            executions_total: schedules.iter().map(|item| item.executions_total).sum(),
            diagnostics: troubleshooting::schedule_resource_diagnostics(
                schedules.iter().any(|item| item.enabled),
                next_run,
                last_run,
                schedules.iter().map(|item| item.executions_total).sum(),
            ),
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
            diagnostics: troubleshooting::notice_resource_diagnostics(subscriptions_active),
        }
    }
}

impl RpcOperationDetail {
    fn from_counts(
        path: &RpcOperationPath<'_>,
        workers_registered: usize,
        requests_pending: usize,
        slowest_worker_average_latency_ms: f64,
        worker_latency_buckets: RpcLatencyBuckets,
    ) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            operation: path.operation.to_string(),
            workers_registered,
            requests_pending,
            slowest_worker_average_latency_ms,
            worker_latency_buckets,
            diagnostics: troubleshooting::rpc_operation_diagnostics(
                workers_registered,
                requests_pending,
                Some(slowest_worker_average_latency_ms),
            ),
        }
    }
}

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
    pub limit: usize,
    pub observations: Vec<ScheduleExecutionObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleExecutionObservation {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
    pub status: String,
    pub cron: String,
    pub next_run: String,
    pub last_run: Option<String>,
    pub executions_total: u64,
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
pub struct LeaseWaiterInfo {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub owner_id: String,
    pub session_id: String,
    pub queued_token: u64,
    pub expires_at: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulePendingClaimInfo {
    pub route_family: u64,
    pub route: String,
    pub fire_ms: u64,
    pub claimed_at_ms: u64,
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

/// Stream snapshot built from durable committed resource metadata plus the
/// current broker's live append-session state.
///
/// `offset`, `watermark`, and `size_bytes` describe committed stream data that
/// survives restart. `sessions_active` counts only current-process append
/// sessions and resets on disconnect cleanup or broker restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamInfo {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub offset: u64,
    pub watermark: u64,
    pub size_bytes: u64,
    pub sessions_active: usize,
}

/// Distribution of stream family watermark lag within a realm area.
///
/// Each bucket counts visible family watermarks relative to the fastest family
/// watermark in the same area snapshot. The distribution is read-only and
/// bounded, and it helps operators see whether one family is trailing the rest
/// of the area.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamLagBuckets {
    pub caught_up: usize,
    pub under_10: usize,
    pub under_100: usize,
    pub over_100: usize,
}

impl StreamLagBuckets {
    pub(crate) fn record_lag_events(&mut self, lag_events: u64) {
        if lag_events == 0 {
            self.caught_up += 1;
        } else if lag_events < 10 {
            self.under_10 += 1;
        } else if lag_events < 100 {
            self.under_100 += 1;
        } else {
            self.over_100 += 1;
        }
    }

    pub(crate) fn merge(&mut self, other: StreamLagBuckets) {
        self.caught_up += other.caught_up;
        self.under_10 += other.under_10;
        self.under_100 += other.under_100;
        self.over_100 += other.over_100;
    }
}

/// Distribution of stream request latency for the current broker process.
///
/// The histogram is cumulative for the broker process lifetime and groups
/// observations into fixed millisecond buckets so operators can see whether
/// the tail is concentrated in the low, medium, or high latency ranges.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamLatencyBuckets {
    pub under_1ms: usize,
    pub under_5ms: usize,
    pub under_10ms: usize,
    pub under_50ms: usize,
    pub under_100ms: usize,
    pub under_500ms: usize,
    pub under_1s: usize,
    pub under_5s: usize,
    pub over_5s: usize,
}

impl StreamLatencyBuckets {
    pub(crate) fn from_histogram(buckets: [u64; 9]) -> Self {
        Self {
            under_1ms: buckets[0] as usize,
            under_5ms: buckets[1] as usize,
            under_10ms: buckets[2] as usize,
            under_50ms: buckets[3] as usize,
            under_100ms: buckets[4] as usize,
            under_500ms: buckets[5] as usize,
            under_1s: buckets[6] as usize,
            under_5s: buckets[7] as usize,
            over_5s: buckets[8] as usize,
        }
    }

    pub(crate) fn total(&self) -> usize {
        self.under_1ms
            + self.under_5ms
            + self.under_10ms
            + self.under_50ms
            + self.under_100ms
            + self.under_500ms
            + self.under_1s
            + self.under_5s
            + self.over_5s
    }

    pub(crate) fn slow_tail_count(&self) -> usize {
        self.under_500ms + self.under_1s + self.under_5s + self.over_5s
    }

    pub(crate) fn slow_tail_ratio(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            0.0
        } else {
            self.slow_tail_count() as f64 / total as f64
        }
    }
}

/// Distribution of schedule request latency for the current broker process.
///
/// The histogram is cumulative for the broker process lifetime and groups
/// observations into fixed millisecond buckets so operators can see whether
/// the tail is concentrated in the low, medium, or high latency ranges.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleLatencyBuckets {
    pub under_1ms: usize,
    pub under_5ms: usize,
    pub under_10ms: usize,
    pub under_50ms: usize,
    pub under_100ms: usize,
    pub under_500ms: usize,
    pub under_1s: usize,
    pub under_5s: usize,
    pub over_5s: usize,
}

impl ScheduleLatencyBuckets {
    pub(crate) fn from_histogram(buckets: [u64; 9]) -> Self {
        Self {
            under_1ms: buckets[0] as usize,
            under_5ms: buckets[1] as usize,
            under_10ms: buckets[2] as usize,
            under_50ms: buckets[3] as usize,
            under_100ms: buckets[4] as usize,
            under_500ms: buckets[5] as usize,
            under_1s: buckets[6] as usize,
            under_5s: buckets[7] as usize,
            over_5s: buckets[8] as usize,
        }
    }

    pub(crate) fn total(&self) -> usize {
        self.under_1ms
            + self.under_5ms
            + self.under_10ms
            + self.under_50ms
            + self.under_100ms
            + self.under_500ms
            + self.under_1s
            + self.under_5s
            + self.over_5s
    }

    pub(crate) fn slow_tail_count(&self) -> usize {
        self.under_500ms + self.under_1s + self.under_5s + self.over_5s
    }

    pub(crate) fn slow_tail_ratio(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            0.0
        } else {
            self.slow_tail_count() as f64 / total as f64
        }
    }
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
    pub route_family: u64,
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
    pub route_family: u64,
    pub route: String,
    pub subscribers: usize,
    pub publishes_total: u64,
    pub publishes_per_minute: f64,
}

/// Collection of point-in-time Queue resource snapshots for the current broker
/// process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuesList {
    pub queues: Vec<QueueInfo>,
}

/// Warm in-memory Queue snapshot for a single resource on the current broker
/// process.
///
/// Queue data remains in storage according to the configured queue write policy,
/// but these counts only reflect the current live actor state. A cold queue can
/// be absent here until traffic rehydrates it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInfo {
    pub family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_inflight: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_message_age_seconds: u64,
    pub oldest_backlog_age_seconds: u64,
    pub backlog_age_buckets: QueueAgeBuckets,
    pub delay_age_buckets: QueueAgeBuckets,
}

/// Collection of live in-memory Queue inflight snapshots for the current broker
/// process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInflightList {
    pub inflight: Vec<QueueInflight>,
}

/// Collection of live in-memory Queue dead-letter snapshots for the current
/// broker process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueDeadLettersList {
    pub messages: Vec<QueueDeadLetter>,
}

/// Live in-memory Queue inflight entry for the current broker process.
///
/// Inflight ownership, inflight tokens, and `session_id` are broker-local runtime
/// state only. They disappear on disconnect cleanup, idle actor eviction, or
/// broker restart and are not durably recoverable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInflight {
    pub message_id: u64,
    pub family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub inflight_token: String,
    pub session_id: String,
    pub expires_at: String,
    pub attempts: usize,
}

/// Live in-memory Queue dead-letter snapshot for the current broker process.
/// Dead-letter rows remain durably stored, but this endpoint only reflects
/// DLQ rows for queue actors that are currently warm on this broker process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueDeadLetter {
    pub message_id: u64,
    pub family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub dead_lettered_at: String,
    pub attempts: usize,
    pub reason: String,
}

pub(crate) struct QueueInflightSnapshot<'a> {
    pub(crate) message_id: u64,
    pub(crate) family: u64,
    pub(crate) realm: &'a str,
    pub(crate) area: &'a str,
    pub(crate) resource: &'a str,
    pub(crate) inflight_token: u64,
    pub(crate) session_id: Option<u64>,
    pub(crate) expires_at: &'a str,
    pub(crate) attempts: usize,
}

pub(crate) struct QueueInfoSnapshot<'a> {
    pub(crate) family: u64,
    pub(crate) realm: &'a str,
    pub(crate) area: &'a str,
    pub(crate) resource: &'a str,
    pub(crate) messages_ready: usize,
    pub(crate) messages_delayed: usize,
    pub(crate) messages_inflight: usize,
    pub(crate) messages_dead_lettered: usize,
    pub(crate) messages_total: usize,
    pub(crate) oldest_message_age_seconds: u64,
    pub(crate) oldest_backlog_age_seconds: u64,
    pub(crate) backlog_age_buckets: QueueAgeBuckets,
    pub(crate) delay_age_buckets: QueueAgeBuckets,
}

pub(crate) struct QueueDeadLetterSnapshot<'a> {
    pub(crate) message_id: u64,
    pub(crate) family: u64,
    pub(crate) realm: &'a str,
    pub(crate) area: &'a str,
    pub(crate) resource: &'a str,
    pub(crate) dead_lettered_at: &'a str,
    pub(crate) attempts: usize,
    pub(crate) reason: &'a str,
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
    pub route_family: u64,
    pub session_id: String,
    pub realm: String,
    pub route: String,
    pub registered_at: String,
    pub requests_handled: u64,
    pub average_latency_ms: f64,
}

/// Distribution of RPC worker average latencies for the current broker process.
///
/// The buckets are read-only and bounded. They summarize the latency profile
/// across live worker registrations without exposing per-request history.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RpcLatencyBuckets {
    pub under_5ms: usize,
    pub under_25ms: usize,
    pub under_100ms: usize,
    pub over_100ms: usize,
}

impl RpcLatencyBuckets {
    pub(crate) fn record_latency_ms(&mut self, latency_ms: f64) {
        if latency_ms < 5.0 {
            self.under_5ms += 1;
        } else if latency_ms < 25.0 {
            self.under_25ms += 1;
        } else if latency_ms < 100.0 {
            self.under_100ms += 1;
        } else {
            self.over_100ms += 1;
        }
    }

    pub(crate) fn merge(&mut self, other: RpcLatencyBuckets) {
        self.under_5ms += other.under_5ms;
        self.under_25ms += other.under_25ms;
        self.under_100ms += other.under_100ms;
        self.over_100ms += other.over_100ms;
    }
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
    pub route_family: u64,
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
    pub route_family: u64,
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

/// Schedule snapshot built from durable definitions preloaded into the current
/// broker process at boot.
///
/// Schedule definitions survive restart and downtime. `last_run` and
/// `executions_total` reflect persisted acknowledged live handoffs recorded
/// when a claimed pending occurrence leaves durable pending state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleInfo {
    pub route_family: u64,
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
    pub route_family: u64,
    pub subject: String,
    pub identity_claim: String,
    pub identity_value: String,
    pub connected_at: String,
    pub idle_seconds: u64,
    pub messages_received: u64,
    pub messages_sent: u64,
    pub transport: String,
    pub remote_addr: String,
}

pub(crate) struct StreamInfoSnapshot<'a> {
    pub route_family: u64,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub offset: u64,
    pub watermark: u64,
    pub size_bytes: u64,
    pub sessions_active: usize,
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
    pub(crate) fn snapshot(snapshot: StreamInfoSnapshot<'_>) -> Self {
        Self {
            route_family: snapshot.route_family,
            realm: snapshot.realm.to_string(),
            area: snapshot.area.to_string(),
            resource: snapshot.resource.to_string(),
            offset: snapshot.offset,
            watermark: snapshot.watermark,
            size_bytes: snapshot.size_bytes,
            sessions_active: snapshot.sessions_active,
        }
    }
}

impl NoticeSubscription {
    pub(crate) fn snapshot(
        route_family: u64,
        subscription_id: u64,
        session_id: u64,
        realm: &str,
        pattern: String,
        created_at: &str,
    ) -> Self {
        Self {
            route_family,
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
    pub(crate) fn snapshot(route_family: u64, route: String, subscribers: usize) -> Self {
        Self {
            route_family,
            route,
            subscribers,
            publishes_total: 0,
            publishes_per_minute: 0.0,
        }
    }
}

impl QueueInfo {
    pub(crate) fn snapshot(snapshot: QueueInfoSnapshot<'_>) -> Self {
        let QueueInfoSnapshot {
            family,
            realm,
            area,
            resource,
            messages_ready,
            messages_delayed,
            messages_inflight,
            messages_dead_lettered,
            messages_total,
            oldest_message_age_seconds,
            oldest_backlog_age_seconds,
            backlog_age_buckets,
            delay_age_buckets,
        } = snapshot;

        Self {
            family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            messages_ready,
            messages_delayed,
            messages_inflight,
            messages_dead_lettered,
            messages_total,
            oldest_message_age_seconds,
            oldest_backlog_age_seconds,
            backlog_age_buckets,
            delay_age_buckets,
        }
    }
}

impl QueueInflight {
    pub(crate) fn snapshot(snapshot: QueueInflightSnapshot<'_>) -> Self {
        let QueueInflightSnapshot {
            message_id,
            family,
            realm,
            area,
            resource,
            inflight_token,
            session_id,
            expires_at,
            attempts,
        } = snapshot;

        Self {
            message_id,
            family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            inflight_token: inflight_token.to_string(),
            session_id: session_id.map(|id| id.to_string()).unwrap_or_default(),
            expires_at: expires_at.to_string(),
            attempts,
        }
    }
}

impl QueueDeadLetter {
    pub(crate) fn snapshot(snapshot: QueueDeadLetterSnapshot<'_>) -> Self {
        let QueueDeadLetterSnapshot {
            message_id,
            family,
            realm,
            area,
            resource,
            dead_lettered_at,
            attempts,
            reason,
        } = snapshot;

        Self {
            message_id,
            family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            dead_lettered_at: dead_lettered_at.to_string(),
            attempts,
            reason: reason.to_string(),
        }
    }
}

impl RpcWorker {
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(crate) fn snapshot(
        route_family: u64,
        session_id: u64,
        realm: &str,
        route: &str,
        registered_at: &str,
        requests_handled: u64,
        average_latency_ms: f64,
    ) -> Self {
        Self {
            route_family,
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
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(crate) fn snapshot(
        route_family: u64,
        correlation_id: String,
        route: &str,
        submitted_at: &str,
        age_seconds: u64,
        worker_session_id: Option<String>,
    ) -> Self {
        Self {
            route_family,
            correlation_id,
            route: route.to_string(),
            submitted_at: submitted_at.to_string(),
            age_seconds,
            worker_session_id,
        }
    }
}

impl LeaseInfo {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn snapshot(
        route_family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        owner_session_id: &str,
        acquired_at: &str,
        expires_at: String,
        renewals: usize,
        fencing_token: u64,
    ) -> Self {
        Self {
            route_family,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            owner_session_id: owner_session_id.to_string(),
            acquired_at: acquired_at.to_string(),
            expires_at,
            renewals,
            fencing_token,
        }
    }
}

impl ScheduleInfo {
    pub(crate) fn enabled_snapshot(
        route_family: u64,
        realm: String,
        area: String,
        resource: String,
        operation: String,
        cron: String,
        next_run: &str,
    ) -> Self {
        Self {
            route_family,
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
    uri.query()
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default()
}

pub fn parse_optional_u64_query_param(uri: &hyper::Uri, key: &str) -> Result<Option<u64>, String> {
    let params = parse_query_params(uri);
    match params.get(key) {
        Some(value) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("Invalid {} query parameter", key)),
        None => Ok(None),
    }
}

pub fn parse_limit_query_param(
    uri: &hyper::Uri,
    default: usize,
    max: usize,
) -> Result<usize, String> {
    let params = parse_query_params(uri);
    match params.get("limit") {
        Some(value) => {
            let limit = value
                .parse::<usize>()
                .map_err(|_| "Invalid limit query parameter".to_string())?;
            if limit == 0 {
                Err("limit query parameter must be greater than zero".to_string())
            } else {
                Ok(limit.min(max))
            }
        }
        None => Ok(default.min(max)),
    }
}

pub fn parse_kv_query_bytes(uri: &hyper::Uri, key: &str) -> Result<Vec<u8>, String> {
    let params = parse_query_params(uri);
    let value = params
        .get(key)
        .ok_or_else(|| format!("Missing {} query parameter", key))?;
    let encoding = params
        .get("key_encoding")
        .map(String::as_str)
        .unwrap_or("utf8");

    match encoding {
        "utf8" => Ok(value.as_bytes().to_vec()),
        "base64" => base64::engine::general_purpose::STANDARD
            .decode(value)
            .map_err(|_| format!("Invalid base64 {} query parameter", key)),
        _ => Err("Invalid key_encoding query parameter".to_string()),
    }
}

pub fn parse_kv_scan_limit(uri: &hyper::Uri) -> Result<usize, String> {
    parse_limit_query_param(uri, DEFAULT_KV_SCAN_LIMIT, MAX_KV_SCAN_LIMIT)
}

pub fn parse_admin_record_limit(uri: &hyper::Uri) -> Result<usize, String> {
    parse_limit_query_param(uri, DEFAULT_ADMIN_RECORD_LIMIT, MAX_ADMIN_RECORD_LIMIT)
}

pub fn parse_optional_string_query_param(uri: &hyper::Uri, key: &str) -> Option<String> {
    parse_query_params(uri)
        .get(key)
        .cloned()
        .filter(|value| !value.is_empty())
}

fn kv_storage_error_response(error: &str) -> Response {
    let status = if error.to_ascii_lowercase().contains("routefamily")
        || error.to_ascii_lowercase().contains("route family")
    {
        hyper::StatusCode::BAD_REQUEST
    } else {
        hyper::StatusCode::SERVICE_UNAVAILABLE
    };
    crate::api::admin::error_response(status, error)
}

fn kv_byte_value(bytes: &[u8]) -> KvByteValue {
    KvByteValue {
        base64: base64::engine::general_purpose::STANDARD.encode(bytes),
        utf8: std::str::from_utf8(bytes).ok().map(ToString::to_string),
        len_bytes: bytes.len(),
    }
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

pub async fn list_sessions(runtime: Arc<Runtime>) -> Result<Response, Infallible> {
    let sessions = runtime.list_sessions();
    crate::api::admin::json_response(SessionsList { sessions })
}

pub async fn kv_transactions_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
) -> Result<Response, Infallible> {
    let transactions = runtime
        .kv_list_transactions(Some(path.realm))
        .into_iter()
        .filter(|tx| path.matches(&tx.realm, &tx.area, &tx.resource))
        .collect();
    crate::api::admin::json_response(KvTransactionsList { transactions })
}

pub async fn kv_committed_value_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: u64,
    key: Vec<u8>,
) -> Result<Response, Infallible> {
    match runtime.kv_get_committed_value(
        crate::runtime::routing::RouteFamily::new(family),
        path.realm,
        path.area,
        path.resource,
        &key,
    ) {
        Ok(value) => crate::api::admin::json_response(KvCommittedValueResponse {
            route_family: family,
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            key: kv_byte_value(&key),
            found: value.is_some(),
            value: value.as_deref().map(kv_byte_value),
        }),
        Err(error) => Ok(kv_storage_error_response(&error)),
    }
}

pub async fn kv_prefix_scan_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: u64,
    prefix: Vec<u8>,
    limit: usize,
) -> Result<Response, Infallible> {
    match runtime.kv_scan_committed_prefix(
        crate::runtime::routing::RouteFamily::new(family),
        path.realm,
        path.area,
        path.resource,
        &prefix,
        limit,
    ) {
        Ok((items, has_more)) => crate::api::admin::json_response(KvPrefixScanResponse {
            route_family: family,
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            prefix: kv_byte_value(&prefix),
            limit,
            has_more,
            items: items
                .into_iter()
                .map(|(key, value)| KvCommittedPair {
                    key: kv_byte_value(&key),
                    value: kv_byte_value(&value),
                })
                .collect(),
        }),
        Err(error) => Ok(kv_storage_error_response(&error)),
    }
}

fn timestamp_ms_to_rfc3339(timestamp_ms: u64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(timestamp_ms as i64)
        .map(|timestamp| timestamp.to_rfc3339())
        .unwrap_or_default()
}

fn stream_read_item_to_admin_record(
    route_family: u64,
    path: &ResourcePath<'_>,
    item: crate::domains::stream::protocol::StreamReadItem,
) -> Option<StreamAdminRecord> {
    match item {
        crate::domains::stream::protocol::StreamReadItem::Event(record) => {
            Some(StreamAdminRecord {
                route_family,
                realm: path.realm.to_string(),
                area: path.area.to_string(),
                resource: path.resource.to_string(),
                resource_offset: record.resource_offset,
                area_offset: record.area_offset,
                realm_offset: record.realm_offset,
                created_at_ms: record.created_at,
                body: kv_byte_value(record.body.as_ref()),
                metadata: record.metadata.as_deref().map(kv_byte_value),
            })
        }
        crate::domains::stream::protocol::StreamReadItem::Filtered { .. }
        | crate::domains::stream::protocol::StreamReadItem::FilteredRange { .. } => None,
    }
}

pub async fn stream_records_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: u64,
    from_offset: u64,
    limit: usize,
    discriminator: Option<String>,
) -> Result<Response, Infallible> {
    match runtime.stream_read_resource_records(AdminStreamReadRequest {
        family: RouteFamily::new(family),
        realm: path.realm,
        area: path.area,
        resource: path.resource,
        from_offset,
        limit: limit as u64,
        discriminator,
    }) {
        Ok((items, cursor)) => {
            let records = items
                .into_iter()
                .filter_map(|item| stream_read_item_to_admin_record(family, path, item))
                .collect();
            crate::api::admin::json_response(StreamRecordsResponse {
                route_family: family,
                realm: Some(path.realm.to_string()),
                area: Some(path.area.to_string()),
                resource: Some(path.resource.to_string()),
                from_offset,
                limit,
                has_more: cursor.has_more,
                records,
            })
        }
        Err(error) => Ok(crate::api::admin::error_response(
            hyper::StatusCode::SERVICE_UNAVAILABLE,
            &error,
        )),
    }
}

pub(crate) async fn stream_search(
    runtime: Arc<Runtime>,
    request: StreamSearchRequest,
) -> Result<Response, Infallible> {
    let mut remaining = request.limit;
    let mut has_more = false;
    let mut records = Vec::new();
    let streams = runtime
        .stream_list_streams(request.realm.as_deref())
        .into_iter()
        .filter(|item| item.route_family == request.family)
        .filter(|item| {
            request
                .area
                .as_ref()
                .map(|value| item.area == *value)
                .unwrap_or(true)
        })
        .filter(|item| {
            request
                .resource
                .as_ref()
                .map(|value| item.resource == *value)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();

    for stream in streams {
        if remaining == 0 {
            has_more = true;
            break;
        }
        let path = ResourcePath {
            realm: &stream.realm,
            area: &stream.area,
            resource: &stream.resource,
        };
        let response = runtime.stream_read_resource_records(AdminStreamReadRequest {
            family: RouteFamily::new(request.family),
            realm: &stream.realm,
            area: &stream.area,
            resource: &stream.resource,
            from_offset: request.from_offset,
            limit: remaining as u64,
            discriminator: request.discriminator.clone(),
        });
        let (items, cursor) = match response {
            Ok(value) => value,
            Err(error) => {
                return Ok(crate::api::admin::error_response(
                    hyper::StatusCode::SERVICE_UNAVAILABLE,
                    &error,
                ));
            }
        };
        has_more = has_more || cursor.has_more;
        for item in items {
            if let Some(record) = stream_read_item_to_admin_record(request.family, &path, item) {
                records.push(record);
                remaining = remaining.saturating_sub(1);
                if remaining == 0 {
                    break;
                }
            }
        }
    }

    crate::api::admin::json_response(StreamRecordsResponse {
        route_family: request.family,
        realm: request.realm,
        area: request.area,
        resource: request.resource,
        from_offset: request.from_offset,
        limit: request.limit,
        has_more,
        records,
    })
}

pub async fn schedule_executions_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: u64,
    limit: usize,
) -> Result<Response, Infallible> {
    let observations = runtime
        .schedule_list_schedules(Some(path.realm))
        .into_iter()
        .filter(|schedule| {
            schedule.route_family == family
                && path.matches(&schedule.realm, &schedule.area, &schedule.resource)
        })
        .take(limit)
        .map(|schedule| ScheduleExecutionObservation {
            route_family: schedule.route_family,
            realm: schedule.realm,
            area: schedule.area,
            resource: schedule.resource,
            operation: schedule.operation,
            status: if schedule.last_run.is_some() {
                "acknowledged_handoff".to_string()
            } else {
                "scheduled".to_string()
            },
            cron: schedule.cron,
            next_run: schedule.next_run,
            last_run: schedule.last_run,
            executions_total: schedule.executions_total,
        })
        .collect();

    crate::api::admin::json_response(ScheduleExecutionObservationList {
        route_family: family,
        realm: path.realm.to_string(),
        area: path.area.to_string(),
        resource: path.resource.to_string(),
        limit,
        observations,
    })
}

pub async fn schedule_missed_observations(
    runtime: Arc<Runtime>,
    family: u64,
    realm: Option<String>,
    area: Option<String>,
    resource: Option<String>,
    limit: usize,
) -> Result<Response, Infallible> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let observations = runtime
        .schedule_list_pending_claims(RouteFamily::new(family))
        .into_iter()
        .filter_map(|claim| {
            let route = route_quad(&claim.route)?;
            if realm
                .as_ref()
                .map(|value| route.realm == value)
                .unwrap_or(true)
                && area
                    .as_ref()
                    .map(|value| route.area == value)
                    .unwrap_or(true)
                && resource
                    .as_ref()
                    .map(|value| route.resource == value)
                    .unwrap_or(true)
            {
                Some(ScheduleMissedObservation {
                    route_family: family,
                    realm: route.realm.to_string(),
                    area: route.area.to_string(),
                    resource: route.resource.to_string(),
                    operation: route.operation.to_string(),
                    fire_ms: claim.fire_ms,
                    fire_at: timestamp_ms_to_rfc3339(claim.fire_ms),
                    claimed_at: timestamp_ms_to_rfc3339(claim.claimed_at_ms),
                    age_seconds: now_ms.saturating_sub(claim.claimed_at_ms) / 1_000,
                    status: "pending_handoff_ack".to_string(),
                })
            } else {
                None
            }
        })
        .take(limit)
        .collect();

    crate::api::admin::json_response(ScheduleMissedObservationList {
        route_family: family,
        limit,
        observations,
    })
}

pub(crate) async fn lease_search(
    runtime: Arc<Runtime>,
    request: LeaseSearchRequest,
) -> Result<Response, Infallible> {
    let leases = runtime.lease_list_leases(request.realm.as_deref());
    let waiters = runtime.lease_list_waiters();
    let waiter_counts = waiters.iter().fold(HashMap::new(), |mut counts, waiter| {
        *counts
            .entry((
                waiter.route_family,
                waiter.realm.clone(),
                waiter.area.clone(),
                waiter.resource.clone(),
            ))
            .or_insert(0usize) += 1;
        counts
    });
    let include_owned = request
        .state
        .as_deref()
        .map(|value| value == "owned" || value == "contention")
        .unwrap_or(true);
    let include_waiting = request
        .state
        .as_deref()
        .map(|value| value == "waiting" || value == "contention")
        .unwrap_or(true);
    let owner_matches = |value: &str| {
        request
            .owner
            .as_ref()
            .map(|needle| value.contains(needle))
            .unwrap_or(true)
    };
    let scope_matches =
        |item_family: u64, item_realm: &str, item_area: &str, item_resource: &str| {
            item_family == request.family
                && request
                    .realm
                    .as_ref()
                    .map(|value| item_realm == value)
                    .unwrap_or(true)
                && request
                    .area
                    .as_ref()
                    .map(|value| item_area == value)
                    .unwrap_or(true)
                && request
                    .resource
                    .as_ref()
                    .map(|value| item_resource == value)
                    .unwrap_or(true)
        };

    let mut items = Vec::new();
    if include_owned {
        for lease in leases {
            if items.len() >= request.limit {
                break;
            }
            if !scope_matches(
                lease.route_family,
                &lease.realm,
                &lease.area,
                &lease.resource,
            ) || !owner_matches(&lease.owner_session_id)
            {
                continue;
            }
            let pending_waiters = waiter_counts
                .get(&(
                    lease.route_family,
                    lease.realm.clone(),
                    lease.area.clone(),
                    lease.resource.clone(),
                ))
                .copied()
                .unwrap_or(0);
            if request.state.as_deref() == Some("contention") && pending_waiters == 0 {
                continue;
            }
            items.push(LeaseSearchItem {
                route_family: lease.route_family,
                realm: lease.realm,
                area: lease.area,
                resource: lease.resource,
                state: if pending_waiters > 0 {
                    "owned_with_waiters".to_string()
                } else {
                    "owned".to_string()
                },
                owner_id: Some(lease.owner_session_id.clone()),
                owner_session_id: Some(lease.owner_session_id),
                queued_token: Some(lease.fencing_token),
                expires_at: Some(lease.expires_at),
                acquired_at: Some(lease.acquired_at),
                renewals: Some(lease.renewals),
                pending_waiters,
            });
        }
    }
    if include_waiting {
        for waiter in waiters {
            if items.len() >= request.limit {
                break;
            }
            if !scope_matches(
                waiter.route_family,
                &waiter.realm,
                &waiter.area,
                &waiter.resource,
            ) || !(owner_matches(&waiter.owner_id) || owner_matches(&waiter.session_id))
            {
                continue;
            }
            items.push(LeaseSearchItem {
                route_family: waiter.route_family,
                realm: waiter.realm,
                area: waiter.area,
                resource: waiter.resource,
                state: "waiting".to_string(),
                owner_id: Some(waiter.owner_id),
                owner_session_id: Some(waiter.session_id),
                queued_token: Some(waiter.queued_token),
                expires_at: Some(waiter.expires_at),
                acquired_at: None,
                renewals: None,
                pending_waiters: 0,
            });
        }
    }

    crate::api::admin::json_response(LeaseSearchResponse {
        route_family: request.family,
        limit: request.limit,
        items,
    })
}

pub async fn notice_delivery_observations(
    runtime: Arc<Runtime>,
    family: u64,
    realm: Option<String>,
    area: Option<String>,
    resource: Option<String>,
    query: Option<String>,
    limit: usize,
) -> Result<Response, Infallible> {
    let routes = runtime.notice_list_routes(realm.as_deref());
    let route_stats: HashMap<_, _> = routes
        .into_iter()
        .filter(|route| route.route_family == family)
        .map(|route| ((route.route_family, route.route.clone()), route))
        .collect();
    let observations = runtime
        .notice_list_subscriptions(realm.as_deref(), None)
        .into_iter()
        .filter(|subscription| subscription.route_family == family)
        .filter_map(|subscription| {
            let parsed = parse_flexible_route(&subscription.pattern);
            if area
                .as_ref()
                .map(|value| parsed.as_ref().map(|parts| &parts.area) == Some(value))
                .unwrap_or(true)
                && resource
                    .as_ref()
                    .map(|value| parsed.as_ref().map(|parts| &parts.resource) == Some(value))
                    .unwrap_or(true)
                && query
                    .as_ref()
                    .map(|needle| {
                        subscription.pattern.contains(needle)
                            || subscription.session_id.contains(needle)
                            || subscription.subscription_id.to_string().contains(needle)
                    })
                    .unwrap_or(true)
            {
                let stats = route_stats.get(&(family, subscription.pattern.clone()));
                Some(NoticeDeliveryObservation {
                    route_family: family,
                    realm: subscription.realm,
                    area: parsed.as_ref().map(|parts| parts.area.clone()),
                    resource: parsed.as_ref().map(|parts| parts.resource.clone()),
                    route: subscription.pattern,
                    session_id: Some(subscription.session_id),
                    subscription_id: Some(subscription.subscription_id),
                    status: "active_subscription".to_string(),
                    notifications_received: subscription.notifications_received,
                    publishes_total: stats.map(|item| item.publishes_total).unwrap_or(0),
                    publishes_per_minute: stats
                        .map(|item| item.publishes_per_minute)
                        .unwrap_or(0.0),
                })
            } else {
                None
            }
        })
        .take(limit)
        .collect();

    crate::api::admin::json_response(NoticeDeliveryObservationList {
        route_family: family,
        limit,
        observations,
    })
}

pub(crate) async fn rpc_call_observations(
    runtime: Arc<Runtime>,
    request: RpcCallObservationRequest,
) -> Result<Response, Infallible> {
    let scope_matches = |route: &str| {
        let Some(parsed) = parse_rpc_operation(route) else {
            return false;
        };
        request
            .realm
            .as_ref()
            .map(|value| parsed.realm == *value)
            .unwrap_or(true)
            && request
                .area
                .as_ref()
                .map(|value| parsed.area == *value)
                .unwrap_or(true)
            && request
                .resource
                .as_ref()
                .map(|value| parsed.resource == *value)
                .unwrap_or(true)
            && request
                .operation
                .as_ref()
                .map(|value| parsed.operation == *value)
                .unwrap_or(true)
    };

    let mut observations = Vec::new();
    for pending in runtime.rpc_list_pending(request.realm.as_deref()) {
        if observations.len() >= request.limit {
            break;
        }
        if pending.route_family != request.family
            || !scope_matches(&pending.route)
            || !request
                .query
                .as_ref()
                .map(|needle| {
                    pending.correlation_id.contains(needle)
                        || pending.route.contains(needle)
                        || pending
                            .worker_session_id
                            .as_ref()
                            .map(|session| session.contains(needle))
                            .unwrap_or(false)
                })
                .unwrap_or(true)
        {
            continue;
        }
        if let Some(parsed) = parse_rpc_operation(&pending.route) {
            observations.push(RpcCallObservation {
                route_family: request.family,
                realm: parsed.realm,
                area: parsed.area,
                resource: parsed.resource,
                operation: Some(parsed.operation),
                route: pending.route,
                correlation_id: Some(pending.correlation_id),
                state: "pending".to_string(),
                submitted_at: Some(pending.submitted_at),
                registered_at: None,
                age_seconds: Some(pending.age_seconds),
                worker_session_id: pending.worker_session_id,
                requests_handled: None,
                average_latency_ms: None,
            });
        }
    }
    for worker in runtime.rpc_list_workers(request.realm.as_deref()) {
        if observations.len() >= request.limit {
            break;
        }
        if worker.route_family != request.family
            || !scope_matches(&worker.route)
            || !request
                .query
                .as_ref()
                .map(|needle| worker.route.contains(needle) || worker.session_id.contains(needle))
                .unwrap_or(true)
        {
            continue;
        }
        if let Some(parsed) = parse_rpc_operation(&worker.route) {
            observations.push(RpcCallObservation {
                route_family: request.family,
                realm: parsed.realm,
                area: parsed.area,
                resource: parsed.resource,
                operation: Some(parsed.operation),
                route: worker.route,
                correlation_id: None,
                state: "worker_registered".to_string(),
                submitted_at: None,
                registered_at: Some(worker.registered_at),
                age_seconds: None,
                worker_session_id: Some(worker.session_id),
                requests_handled: Some(worker.requests_handled),
                average_latency_ms: Some(worker.average_latency_ms),
            });
        }
    }

    crate::api::admin::json_response(RpcCallObservationList {
        route_family: request.family,
        limit: request.limit,
        observations,
    })
}

pub async fn queue_inflight_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> Result<Response, Infallible> {
    let inflight = runtime
        .queue_list_inflight(Some(path.realm))
        .into_iter()
        .filter(|entry| {
            path.matches(&entry.realm, &entry.area, &entry.resource)
                && family.map(|value| entry.family == value).unwrap_or(true)
        })
        .collect();
    crate::api::admin::json_response(QueueInflightList { inflight })
}

pub async fn queue_dead_letters_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> Result<Response, Infallible> {
    let messages = runtime
        .queue_list_dead_letters(Some(path.realm))
        .into_iter()
        .filter(|message| {
            path.matches(&message.realm, &message.area, &message.resource)
                && family.map(|value| message.family == value).unwrap_or(true)
        })
        .collect();
    crate::api::admin::json_response(QueueDeadLettersList { messages })
}

pub async fn notice_subscriptions_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
) -> Result<Response, Infallible> {
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
) -> Result<Response, Infallible> {
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
) -> Result<Response, Infallible> {
    let requests = runtime.rpc_list_pending(realm);
    crate::api::admin::json_response(RpcPendingList { requests })
}

pub async fn kv_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    limit: usize,
) -> Result<Response, Infallible> {
    let transactions = runtime.kv_list_transactions(Some(path.realm));
    crate::api::admin::json_response(troubleshooting::kv_resource_timeline(
        &transactions,
        path,
        limit,
    ))
}

pub async fn queue_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Result<Response, Infallible> {
    let queues = runtime.queue_list_queues(Some(path.realm));
    let inflight = runtime.queue_list_inflight(Some(path.realm));
    let dead_letters = runtime.queue_list_dead_letters(Some(path.realm));
    crate::api::admin::json_response(troubleshooting::queue_resource_timeline(
        &queues,
        &inflight,
        &dead_letters,
        path,
        family,
        limit,
    ))
}

pub async fn stream_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    limit: usize,
) -> Result<Response, Infallible> {
    let streams = runtime.stream_list_streams(Some(path.realm));
    crate::api::admin::json_response(troubleshooting::stream_resource_timeline(
        &streams, path, limit,
    ))
}

pub async fn lease_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    limit: usize,
) -> Result<Response, Infallible> {
    let leases = runtime.lease_list_leases(Some(path.realm));
    crate::api::admin::json_response(troubleshooting::lease_resource_timeline(
        &leases, path, limit,
    ))
}

pub async fn schedule_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    limit: usize,
) -> Result<Response, Infallible> {
    let schedules = runtime.schedule_list_schedules(Some(path.realm));
    crate::api::admin::json_response(troubleshooting::schedule_resource_timeline(
        &schedules,
        runtime.schedule_pending_fire_claims(),
        runtime.schedule_pending_ack_retries(),
        runtime.schedule_oldest_pending_claim_age_seconds(),
        runtime.schedule_notify_failures(),
        runtime.schedule_ack_failures(),
        runtime.schedule_overdue_normalizations(),
        runtime.schedule_pending_claims_expired_total(),
        runtime.schedule_pending_claim_cleanup_failures_total(),
        path,
        limit,
    ))
}

pub async fn notice_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    limit: usize,
) -> Result<Response, Infallible> {
    let subscriptions = runtime.notice_list_subscriptions(Some(path.realm), None);
    let routes = runtime.notice_list_routes(Some(path.realm));
    crate::api::admin::json_response(troubleshooting::notice_resource_timeline(
        &subscriptions,
        &routes,
        path,
        limit,
    ))
}

pub async fn rpc_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    limit: usize,
) -> Result<Response, Infallible> {
    let workers = runtime.rpc_list_workers(Some(path.realm));
    let pending = runtime.rpc_list_pending(Some(path.realm));
    crate::api::admin::json_response(troubleshooting::rpc_resource_timeline(
        &workers, &pending, path, limit,
    ))
}

pub fn kv_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> KvResourceDetail {
    let transactions = runtime
        .kv_list_transactions(Some(path.realm))
        .into_iter()
        .filter(|tx| path.matches(&tx.realm, &tx.area, &tx.resource))
        .count();
    KvResourceDetail::from_count(path, transactions)
}

pub fn queue_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> QueueResourceDetail {
    let queues: Vec<_> = runtime
        .queue_list_queues(Some(path.realm))
        .into_iter()
        .filter(|item| {
            path.matches(&item.realm, &item.area, &item.resource)
                && family.map(|value| item.family == value).unwrap_or(true)
        })
        .collect();

    if queues.is_empty() {
        return QueueResourceDetail::empty(path);
    }

    if family.is_some() {
        return QueueResourceDetail::from_queue(queues.into_iter().next().unwrap());
    }

    let mut detail = QueueResourceDetail::empty(path);
    for queue in queues {
        detail.messages_ready += queue.messages_ready;
        detail.messages_delayed += queue.messages_delayed;
        detail.messages_inflight += queue.messages_inflight;
        detail.messages_dead_lettered += queue.messages_dead_lettered;
        detail.messages_total += queue.messages_total;
        detail.oldest_message_age_seconds = detail
            .oldest_message_age_seconds
            .max(queue.oldest_message_age_seconds);
        detail.oldest_backlog_age_seconds = detail
            .oldest_backlog_age_seconds
            .max(queue.oldest_backlog_age_seconds);
        detail.backlog_age_buckets.merge(queue.backlog_age_buckets);
        detail.delay_age_buckets.merge(queue.delay_age_buckets);
    }

    detail.diagnostics = troubleshooting::queue_resource_diagnostics(
        detail.messages_ready,
        detail.messages_delayed,
        detail.messages_inflight,
        detail.messages_dead_lettered,
        detail.oldest_backlog_age_seconds,
        detail.delay_age_buckets,
    );
    detail
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

pub fn stream_realm_watermark_detail(runtime: &Runtime, realm: &str) -> StreamRealmWatermarkDetail {
    runtime
        .stream_realm_watermark_detail(realm)
        .unwrap_or_else(|| StreamRealmWatermarkDetail::snapshot(realm, 0, 0, Vec::new()))
}

pub fn stream_area_watermark_detail(
    runtime: &Runtime,
    realm: &str,
    area: &str,
) -> StreamAreaWatermarkDetail {
    runtime
        .stream_area_watermark_detail(realm, area)
        .unwrap_or_else(|| StreamAreaWatermarkDetail::snapshot(realm, area, 0, Vec::new()))
}

pub fn lease_detail(runtime: &Runtime, path: &ResourcePath<'_>) -> LeaseResourceDetail {
    let (active_leases, oldest_lease_age_seconds, renewals_total) = runtime
        .lease_list_leases(Some(path.realm))
        .into_iter()
        .filter(|item| path.matches(&item.realm, &item.area, &item.resource))
        .fold(
            (0usize, 0u64, 0usize),
            |(count, oldest, renewals), lease| {
                let age_seconds =
                    troubleshooting::age_seconds_since(&lease.acquired_at).unwrap_or(0);
                (
                    count + 1,
                    oldest.max(age_seconds),
                    renewals.saturating_add(lease.renewals),
                )
            },
        );
    LeaseResourceDetail::from_count(
        path,
        active_leases,
        oldest_lease_age_seconds,
        renewals_total,
    )
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
    let workers = runtime
        .rpc_list_workers(Some(path.realm))
        .into_iter()
        .filter(|worker| matches_operation_route(&worker.route, path))
        .collect::<Vec<_>>();
    let requests_pending = runtime
        .rpc_list_pending(Some(path.realm))
        .into_iter()
        .filter(|request| matches_operation_route(&request.route, path))
        .collect::<Vec<_>>();
    let latency_summary = troubleshooting::summarize_rpc_worker_latency(workers.iter());
    RpcOperationDetail::from_counts(
        path,
        workers.len(),
        requests_pending.len(),
        latency_summary.slowest_worker_average_latency_ms,
        latency_summary.worker_latency_buckets,
    )
}

fn comparison_scope(path: &ResourcePath<'_>, family: Option<u64>) -> ResourceComparisonScope {
    ResourceComparisonScope::new(path, family)
}

fn comparison_side(
    path: &ResourcePath<'_>,
    family: Option<u64>,
    diagnostics: DiagnosticSnapshot,
    metrics: ResourceComparisonMetrics,
) -> ResourceComparisonSide {
    ResourceComparisonSide {
        scope: comparison_scope(path, family),
        diagnostics,
        metrics,
    }
}

#[allow(clippy::too_many_arguments)]
fn build_resource_comparison(
    domain: &str,
    left_path: &ResourcePath<'_>,
    left_family: Option<u64>,
    left_diagnostics: DiagnosticSnapshot,
    left_metrics: ResourceComparisonMetrics,
    right_path: &ResourcePath<'_>,
    right_family: Option<u64>,
    right_diagnostics: DiagnosticSnapshot,
    right_metrics: ResourceComparisonMetrics,
) -> ResourceComparison {
    troubleshooting::compare_resource_sides(
        domain,
        comparison_side(left_path, left_family, left_diagnostics, left_metrics),
        comparison_side(right_path, right_family, right_diagnostics, right_metrics),
    )
}

fn kv_comparison_metrics(detail: &KvResourceDetail) -> ResourceComparisonMetrics {
    ResourceComparisonMetrics {
        backlog: Some(detail.transactions_active),
        inflight: None,
        ready: None,
        delayed: None,
        dead_letters: None,
        workers: None,
        subscriptions: None,
        waiters: None,
        age_seconds: detail.diagnostics.age_seconds,
        recent_transition_count: Some(detail.diagnostics.recent_transition_count),
        failure_count: Some(detail.diagnostics.failure_count),
        contention_count: None,
        operations_total: Some(detail.transactions_active as u64),
    }
}

fn queue_comparison_metrics(detail: &QueueResourceDetail) -> ResourceComparisonMetrics {
    let backlog = detail.messages_ready + detail.messages_delayed;
    ResourceComparisonMetrics {
        backlog: Some(backlog),
        inflight: Some(detail.messages_inflight),
        ready: Some(detail.messages_ready),
        delayed: Some(detail.messages_delayed),
        dead_letters: Some(detail.messages_dead_lettered),
        workers: None,
        subscriptions: None,
        waiters: None,
        age_seconds: Some(detail.oldest_backlog_age_seconds),
        recent_transition_count: Some(detail.diagnostics.recent_transition_count),
        failure_count: Some(detail.messages_dead_lettered as u64),
        contention_count: None,
        operations_total: Some(detail.messages_total as u64),
    }
}

fn stream_comparison_metrics(detail: &StreamResourceDetail) -> ResourceComparisonMetrics {
    let lag = detail.offset.saturating_sub(detail.watermark) as usize;
    ResourceComparisonMetrics {
        backlog: Some(lag),
        inflight: None,
        ready: None,
        delayed: None,
        dead_letters: None,
        workers: Some(detail.sessions_active),
        subscriptions: None,
        waiters: None,
        age_seconds: detail.diagnostics.age_seconds,
        recent_transition_count: Some(detail.diagnostics.recent_transition_count),
        failure_count: Some(detail.diagnostics.failure_count),
        contention_count: None,
        operations_total: Some(detail.offset),
    }
}

fn lease_comparison_metrics(detail: &LeaseResourceDetail) -> ResourceComparisonMetrics {
    ResourceComparisonMetrics {
        backlog: Some(detail.active_leases),
        inflight: None,
        ready: None,
        delayed: None,
        dead_letters: None,
        workers: None,
        subscriptions: None,
        waiters: None,
        age_seconds: detail.diagnostics.age_seconds,
        recent_transition_count: Some(detail.diagnostics.recent_transition_count),
        failure_count: Some(detail.diagnostics.failure_count),
        contention_count: Some(detail.diagnostics.contention_count),
        operations_total: Some(detail.active_leases as u64),
    }
}

fn notice_comparison_metrics(detail: &NoticeResourceDetail) -> ResourceComparisonMetrics {
    ResourceComparisonMetrics {
        backlog: Some(detail.subscriptions_active),
        inflight: None,
        ready: None,
        delayed: None,
        dead_letters: None,
        workers: None,
        subscriptions: Some(detail.subscriptions_active),
        waiters: None,
        age_seconds: detail.diagnostics.age_seconds,
        recent_transition_count: Some(detail.diagnostics.recent_transition_count),
        failure_count: Some(detail.diagnostics.failure_count),
        contention_count: None,
        operations_total: Some(detail.subscriptions_active as u64),
    }
}

fn schedule_comparison_metrics(detail: &ScheduleResourceDetail) -> ResourceComparisonMetrics {
    ResourceComparisonMetrics {
        backlog: None,
        inflight: None,
        ready: None,
        delayed: None,
        dead_letters: None,
        workers: None,
        subscriptions: None,
        waiters: Some(detail.diagnostics.waiter_count),
        age_seconds: detail.diagnostics.age_seconds,
        recent_transition_count: Some(detail.diagnostics.recent_transition_count),
        failure_count: Some(detail.diagnostics.failure_count),
        contention_count: Some(detail.diagnostics.contention_count),
        operations_total: Some(detail.executions_total),
    }
}

fn rpc_resource_comparison_metrics(
    workers_registered: usize,
    requests_pending: usize,
    oldest_pending_age: Option<u64>,
) -> (DiagnosticSnapshot, ResourceComparisonMetrics) {
    let diagnostics =
        troubleshooting::rpc_operation_diagnostics(workers_registered, requests_pending, None);
    (
        diagnostics,
        ResourceComparisonMetrics {
            backlog: Some(requests_pending),
            inflight: None,
            ready: None,
            delayed: None,
            dead_letters: None,
            workers: Some(workers_registered),
            subscriptions: None,
            waiters: None,
            age_seconds: oldest_pending_age,
            recent_transition_count: None,
            failure_count: None,
            contention_count: Some(requests_pending.saturating_sub(workers_registered) as u64),
            operations_total: Some((workers_registered + requests_pending) as u64),
        },
    )
}

pub fn kv_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    against: &ResourcePath<'_>,
) -> ResourceComparison {
    let left = kv_detail(runtime, path);
    let right = kv_detail(runtime, against);
    let left_metrics = kv_comparison_metrics(&left);
    let right_metrics = kv_comparison_metrics(&right);
    build_resource_comparison(
        "kv",
        path,
        None,
        left.diagnostics,
        left_metrics,
        against,
        None,
        right.diagnostics,
        right_metrics,
    )
}

pub fn queue_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    against: &ResourcePath<'_>,
    against_family: Option<u64>,
) -> ResourceComparison {
    let left = queue_detail(runtime, path, family);
    let right = queue_detail(runtime, against, against_family);
    let left_metrics = queue_comparison_metrics(&left);
    let right_metrics = queue_comparison_metrics(&right);
    build_resource_comparison(
        "queue",
        path,
        family,
        left.diagnostics,
        left_metrics,
        against,
        against_family,
        right.diagnostics,
        right_metrics,
    )
}

pub fn stream_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    against: &ResourcePath<'_>,
) -> ResourceComparison {
    let left = stream_detail(runtime, path);
    let right = stream_detail(runtime, against);
    let left_metrics = stream_comparison_metrics(&left);
    let right_metrics = stream_comparison_metrics(&right);
    build_resource_comparison(
        "stream",
        path,
        None,
        left.diagnostics,
        left_metrics,
        against,
        None,
        right.diagnostics,
        right_metrics,
    )
}

pub fn lease_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    against: &ResourcePath<'_>,
) -> ResourceComparison {
    let left = lease_detail(runtime, path);
    let right = lease_detail(runtime, against);
    let left_metrics = lease_comparison_metrics(&left);
    let right_metrics = lease_comparison_metrics(&right);
    build_resource_comparison(
        "lease",
        path,
        None,
        left.diagnostics,
        left_metrics,
        against,
        None,
        right.diagnostics,
        right_metrics,
    )
}

pub fn schedule_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    against: &ResourcePath<'_>,
) -> ResourceComparison {
    let left = schedule_detail(runtime, path);
    let right = schedule_detail(runtime, against);
    let left_metrics = schedule_comparison_metrics(&left);
    let right_metrics = schedule_comparison_metrics(&right);
    build_resource_comparison(
        "schedule",
        path,
        None,
        left.diagnostics,
        left_metrics,
        against,
        None,
        right.diagnostics,
        right_metrics,
    )
}

pub fn notice_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    against: &ResourcePath<'_>,
) -> ResourceComparison {
    let left = notice_detail(runtime, path);
    let right = notice_detail(runtime, against);
    let left_metrics = notice_comparison_metrics(&left);
    let right_metrics = notice_comparison_metrics(&right);
    build_resource_comparison(
        "notice",
        path,
        None,
        left.diagnostics,
        left_metrics,
        against,
        None,
        right.diagnostics,
        right_metrics,
    )
}

pub fn rpc_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    against: &ResourcePath<'_>,
) -> ResourceComparison {
    let left_pending = runtime
        .rpc_list_pending(Some(path.realm))
        .into_iter()
        .filter(|request| matches_resource_route(&request.route, path))
        .collect::<Vec<_>>();
    let left_workers_registered = runtime
        .rpc_list_workers(Some(path.realm))
        .into_iter()
        .filter(|worker| matches_resource_route(&worker.route, path))
        .count();
    let (left_diagnostics, left_metrics) = rpc_resource_comparison_metrics(
        left_workers_registered,
        left_pending.len(),
        left_pending.iter().map(|request| request.age_seconds).max(),
    );

    let right_pending = runtime
        .rpc_list_pending(Some(against.realm))
        .into_iter()
        .filter(|request| matches_resource_route(&request.route, against))
        .collect::<Vec<_>>();
    let right_workers_registered = runtime
        .rpc_list_workers(Some(against.realm))
        .into_iter()
        .filter(|worker| matches_resource_route(&worker.route, against))
        .count();
    let (right_diagnostics, right_metrics) = rpc_resource_comparison_metrics(
        right_workers_registered,
        right_pending.len(),
        right_pending
            .iter()
            .map(|request| request.age_seconds)
            .max(),
    );

    build_resource_comparison(
        "rpc",
        path,
        None,
        left_diagnostics,
        left_metrics,
        against,
        None,
        right_diagnostics,
        right_metrics,
    )
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
    use crate::boot::domains::{
        DomainHandles, KvDomainSink, LeaseDomainSink, NoticeDomainSink, QueueDomainSink,
        RpcDomainSink, ScheduleDomainSink, StreamDomainSink,
    };
    use crate::boot::Runtime;
    use crate::domains::schedule::store::{ScheduleInsert, ScheduleStore};
    use crate::runtime::Router;
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn current_epoch_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_millis() as u64
    }

    fn runtime_with_preloaded_schedule() -> Arc<Runtime> {
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let runtime = Arc::new(Runtime::with_admin_read_model(
            router.clone(),
            admin_read_model.clone(),
        ));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let schedule_store = ScheduleStore::new(store.clone());
        let payload = Bytes::from_static(b"nightly");
        let now_ms = current_epoch_ms();

        schedule_store
            .insert(
                1,
                ScheduleInsert {
                    route: "schedule://acme/jobs/invoices/send",
                    cron: "0 * * * *",
                    payload: &payload,
                    next_fire_ms: now_ms.saturating_add(60_000),
                    previous_fire_ms: None,
                    last_fire_ms: Some(now_ms.saturating_sub(1_000)),
                    executions_total: 7,
                },
                cntryl_midge::WriteOptions::buffered(),
            )
            .expect("insert schedule");

        let domains = Arc::new(DomainHandles {
            kv: Arc::new(KvDomainSink::new(
                store.clone(),
                router.clone(),
                admin_read_model.clone(),
            )),
            queue: Arc::new(QueueDomainSink::new(
                store.clone(),
                router.clone(),
                admin_read_model.clone(),
                cntryl_midge::WriteOptions::buffered(),
                crate::utils::idempotency::default_dedup_store(),
            )),
            notice: Arc::new(NoticeDomainSink::new(
                router.clone(),
                admin_read_model.clone(),
            )),
            stream: Arc::new(StreamDomainSink::new(
                store.clone(),
                router.clone(),
                admin_read_model.clone(),
            )),
            rpc: Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone())),
            lease: Arc::new(LeaseDomainSink::new(
                router.clone(),
                admin_read_model.clone(),
            )),
            schedule: Arc::new(ScheduleDomainSink::new(
                store,
                router,
                admin_read_model.clone(),
            )),
        });

        domains
            .schedule
            .preload_persisted_families()
            .expect("preload schedules");
        runtime.attach_domains(domains);
        runtime
    }

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
        let items = vec![QueueInfo::snapshot(QueueInfoSnapshot {
            family: 1,
            realm: "acme",
            area: "billing",
            resource: "invoices",
            messages_ready: 0,
            messages_delayed: 0,
            messages_inflight: 0,
            messages_dead_lettered: 0,
            messages_total: 0,
            oldest_message_age_seconds: 0,
            oldest_backlog_age_seconds: 0,
            backlog_age_buckets: QueueAgeBuckets::default(),
            delay_age_buckets: QueueAgeBuckets::default(),
        })];

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
                route_family: 1,
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
                route_family: 1,
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

    #[test]
    fn should_expose_persisted_schedule_execution_state_given_preloaded_runtime() {
        // Arrange
        let runtime = runtime_with_preloaded_schedule();
        let path = ResourcePath {
            realm: "acme",
            area: "jobs",
            resource: "invoices",
        };

        // Act
        let schedules = runtime.schedule_list_schedules(Some("acme"));
        let detail = schedule_detail(runtime.as_ref(), &path);

        // Assert
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].operation, "send");
        assert!(schedules[0].last_run.is_some());
        assert_eq!(schedules[0].executions_total, 7);
        assert!(detail.enabled);
        assert_eq!(detail.cron.as_deref(), Some("0 * * * *"));
        assert_eq!(detail.executions_total, 7);
        assert!(detail.next_run.is_some());
    }
}
