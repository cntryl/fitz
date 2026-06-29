// Hierarchical list endpoints for admin API

use super::*;
use crate::api::admin::troubleshooting::{self, DiagnosticSnapshot};
use serde::{Deserialize, Serialize};

pub(crate) use crate::control::admin::worse_queue_status;
pub use crate::control::admin::{
    QueueAgeBuckets, StreamAreaWatermark, StreamAreaWatermarkDetail, StreamRealmWatermark,
    StreamRealmWatermarkDetail,
};

pub(crate) const DEFAULT_KV_SCAN_LIMIT: usize = 50;
pub(crate) const MAX_KV_SCAN_LIMIT: usize = 100;
pub(crate) const DEFAULT_ADMIN_RECORD_LIMIT: usize = 50;
pub(crate) const MAX_ADMIN_RECORD_LIMIT: usize = 100;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_record_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_storage_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimate_complete: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_latency_avg_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_latency_p95_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_latency_avg_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_latency_p95_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transactions_active: Option<usize>,
}

impl ResourceEntry {
    pub(super) fn named(resource: String) -> Self {
        Self {
            resource,
            estimated_record_count: None,
            estimated_storage_bytes: None,
            estimate_complete: None,
            read_latency_avg_ms: None,
            read_latency_p95_ms: None,
            write_latency_avg_ms: None,
            write_latency_p95_ms: None,
            transactions_active: None,
        }
    }

    pub(super) fn from_kv_inventory(entry: KvResourceInventoryEntry) -> Self {
        Self {
            resource: entry.resource,
            estimated_record_count: Some(entry.estimated_record_count),
            estimated_storage_bytes: Some(entry.estimated_storage_bytes),
            estimate_complete: Some(entry.estimate_complete),
            read_latency_avg_ms: Some(entry.read_latency_avg_ms),
            read_latency_p95_ms: Some(entry.read_latency_p95_ms),
            write_latency_avg_ms: Some(entry.write_latency_avg_ms),
            write_latency_p95_ms: Some(entry.write_latency_p95_ms),
            transactions_active: Some(entry.transactions_active),
        }
    }
}

#[derive(Debug, Clone, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
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
    pub route_family: Option<u64>,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub estimated_record_count: u64,
    pub estimated_storage_bytes: u64,
    pub estimate_complete: bool,
    pub read_latency_avg_ms: f64,
    pub read_latency_p95_ms: f64,
    pub write_latency_avg_ms: f64,
    pub write_latency_p95_ms: f64,
    pub transactions_active: usize,
    pub diagnostics: DiagnosticSnapshot,
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
    pub subscriptions_active: usize,
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_inflight: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_message_age_seconds: u64,
    pub oldest_backlog_age_seconds: u64,
    pub backlog_age_buckets: QueueAgeBuckets,
    pub delay_age_buckets: QueueAgeBuckets,
    pub enqueue_success_total: u64,
    pub complete_success_total: u64,
    pub in_rate_per_second: f64,
    pub out_rate_per_second: f64,
    pub status: String,
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
    pub(super) fn from_inventory(
        path: &ResourcePath<'_>,
        inventory: Option<KvResourceInventoryEntry>,
        transactions_active: usize,
    ) -> Self {
        if let Some(inventory) = inventory {
            let diagnostics =
                troubleshooting::kv_resource_diagnostics(inventory.transactions_active);
            return Self {
                route_family: (inventory.route_family != 0).then_some(inventory.route_family),
                realm: inventory.realm,
                area: inventory.area,
                resource: inventory.resource,
                estimated_record_count: inventory.estimated_record_count,
                estimated_storage_bytes: inventory.estimated_storage_bytes,
                estimate_complete: inventory.estimate_complete,
                read_latency_avg_ms: inventory.read_latency_avg_ms,
                read_latency_p95_ms: inventory.read_latency_p95_ms,
                write_latency_avg_ms: inventory.write_latency_avg_ms,
                write_latency_p95_ms: inventory.write_latency_p95_ms,
                transactions_active: inventory.transactions_active,
                diagnostics,
            };
        }

        let diagnostics = troubleshooting::kv_resource_diagnostics(transactions_active);
        Self {
            route_family: None,
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            estimated_record_count: 0,
            estimated_storage_bytes: 0,
            estimate_complete: true,
            read_latency_avg_ms: 0.0,
            read_latency_p95_ms: 0.0,
            write_latency_avg_ms: 0.0,
            write_latency_p95_ms: 0.0,
            transactions_active,
            diagnostics,
        }
    }
}

impl QueueResourceDetail {
    pub(super) fn from_queue(item: QueueInfo) -> Self {
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
            subscriptions_active: item.subscriptions_active,
            messages_ready: item.messages_ready,
            messages_delayed: item.messages_delayed,
            messages_inflight: item.messages_inflight,
            messages_dead_lettered: item.messages_dead_lettered,
            messages_total: item.messages_total,
            oldest_message_age_seconds: item.oldest_message_age_seconds,
            oldest_backlog_age_seconds: item.oldest_backlog_age_seconds,
            backlog_age_buckets: item.backlog_age_buckets,
            delay_age_buckets: item.delay_age_buckets,
            enqueue_success_total: item.enqueue_success_total,
            complete_success_total: item.complete_success_total,
            in_rate_per_second: item.in_rate_per_second,
            out_rate_per_second: item.out_rate_per_second,
            status: item.status,
            diagnostics,
        }
    }

    pub(super) fn empty(path: &ResourcePath<'_>) -> Self {
        Self {
            realm: path.realm.to_string(),
            area: path.area.to_string(),
            resource: path.resource.to_string(),
            subscriptions_active: 0,
            messages_ready: 0,
            messages_delayed: 0,
            messages_inflight: 0,
            messages_dead_lettered: 0,
            messages_total: 0,
            oldest_message_age_seconds: 0,
            oldest_backlog_age_seconds: 0,
            backlog_age_buckets: QueueAgeBuckets::default(),
            delay_age_buckets: QueueAgeBuckets::default(),
            enqueue_success_total: 0,
            complete_success_total: 0,
            in_rate_per_second: 0.0,
            out_rate_per_second: 0.0,
            status: "idle".to_string(),
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
    pub(super) fn from_stream(item: StreamInfo) -> Self {
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

    pub(super) fn empty(path: &ResourcePath<'_>) -> Self {
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

impl LeaseResourceDetail {
    pub(super) fn from_count(
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
    pub(super) fn empty(path: &ResourcePath<'_>) -> Self {
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

    pub(super) fn from_schedule(item: ScheduleInfo) -> Self {
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

    pub(super) fn aggregate(path: &ResourcePath<'_>, schedules: &[ScheduleInfo]) -> Self {
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
    pub(super) fn from_count(path: &ResourcePath<'_>, subscriptions_active: usize) -> Self {
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
    pub(super) fn from_counts(
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
