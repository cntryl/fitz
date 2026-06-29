use super::{
    matches_family, route_quad, route_triplet, troubleshooting, worse_queue_status,
    KvResourceDetail, KvResourceInventoryEntry, LeaseResourceDetail, NoticeResourceDetail,
    OwnedRpcOperation, QueueResourceDetail, ResourceComparison, ResourceComparisonMetrics,
    ResourceComparisonScope, ResourceComparisonSide, ResourcePath, ResourceRef, RpcOperationDetail,
    RpcOperationPath, Runtime, ScheduleResourceDetail, StreamAreaWatermarkDetail,
    StreamRealmWatermarkDetail, StreamResourceDetail,
};
use crate::api::admin::troubleshooting::DiagnosticSnapshot;

pub fn kv_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> KvResourceDetail {
    let transactions = runtime
        .kv_list_transactions(Some(path.realm))
        .into_iter()
        .filter(|tx| {
            matches_family(family, tx.route_family)
                && path.matches(&tx.realm, &tx.area, &tx.resource)
        })
        .count();
    let inventory = if let Some(family) = family {
        runtime
            .kv_inventory_resource(family, path.realm, path.area, path.resource)
            .ok()
            .flatten()
    } else {
        let matching = runtime
            .kv_inventory_entries(None)
            .unwrap_or_default()
            .into_iter()
            .filter(|entry| path.matches(&entry.realm, &entry.area, &entry.resource))
            .collect::<Vec<_>>();
        if matching.is_empty() {
            None
        } else {
            Some(KvResourceInventoryEntry {
                route_family: 0,
                realm: path.realm.to_string(),
                area: path.area.to_string(),
                resource: path.resource.to_string(),
                estimated_record_count: matching
                    .iter()
                    .map(|entry| entry.estimated_record_count)
                    .sum(),
                estimated_storage_bytes: matching
                    .iter()
                    .map(|entry| entry.estimated_storage_bytes)
                    .sum(),
                estimate_complete: matching.iter().all(|entry| entry.estimate_complete),
                read_latency_avg_ms: matching
                    .iter()
                    .map(|entry| entry.read_latency_avg_ms)
                    .fold(0.0, f64::max),
                read_latency_p95_ms: matching
                    .iter()
                    .map(|entry| entry.read_latency_p95_ms)
                    .fold(0.0, f64::max),
                write_latency_avg_ms: matching
                    .iter()
                    .map(|entry| entry.write_latency_avg_ms)
                    .fold(0.0, f64::max),
                write_latency_p95_ms: matching
                    .iter()
                    .map(|entry| entry.write_latency_p95_ms)
                    .fold(0.0, f64::max),
                transactions_active: transactions,
            })
        }
    };
    KvResourceDetail::from_inventory(path, inventory, transactions)
}

#[must_use]
/// # Panics
///
/// Panics if a concrete `family` is requested and the filtered queue list is
/// unexpectedly empty after the earlier emptiness check.
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
                && family.is_none_or(|value| item.family == value)
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
        detail.subscriptions_active += queue.subscriptions_active;
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
        detail.enqueue_success_total += queue.enqueue_success_total;
        detail.complete_success_total += queue.complete_success_total;
        detail.in_rate_per_second += queue.in_rate_per_second;
        detail.out_rate_per_second += queue.out_rate_per_second;
        detail.status = worse_queue_status(&detail.status, &queue.status);
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

#[must_use]
pub fn stream_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> StreamResourceDetail {
    let stream = runtime
        .stream_list_streams(Some(path.realm))
        .into_iter()
        .find(|item| {
            matches_family(family, item.route_family)
                && path.matches(&item.realm, &item.area, &item.resource)
        });
    match stream {
        Some(item) => StreamResourceDetail::from_stream(item),
        None => StreamResourceDetail::empty(path),
    }
}

#[must_use]
pub fn stream_realm_watermark_detail(runtime: &Runtime, realm: &str) -> StreamRealmWatermarkDetail {
    runtime
        .stream_realm_watermark_detail(realm)
        .unwrap_or_else(|| StreamRealmWatermarkDetail::snapshot(realm, 0, 0, Vec::new()))
}

#[must_use]
pub fn stream_area_watermark_detail(
    runtime: &Runtime,
    realm: &str,
    area: &str,
) -> StreamAreaWatermarkDetail {
    runtime
        .stream_area_watermark_detail(realm, area)
        .unwrap_or_else(|| StreamAreaWatermarkDetail::snapshot(realm, area, 0, Vec::new()))
}

#[must_use]
pub fn lease_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> LeaseResourceDetail {
    let (active_leases, oldest_lease_age_seconds, renewals_total) = runtime
        .lease_list_leases(Some(path.realm))
        .into_iter()
        .filter(|item| {
            matches_family(family, item.route_family)
                && path.matches(&item.realm, &item.area, &item.resource)
        })
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

#[must_use]
/// # Panics
///
/// Panics if the filtered schedule list reports exactly one item but that item
/// cannot be retrieved from the iterator.
pub fn schedule_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> ScheduleResourceDetail {
    let schedules = runtime
        .schedule_list_schedules(Some(path.realm))
        .into_iter()
        .filter(|item| {
            matches_family(family, item.route_family)
                && path.matches(&item.realm, &item.area, &item.resource)
        })
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

#[must_use]
pub fn notice_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> NoticeResourceDetail {
    let subscriptions_active = runtime
        .notice_list_subscriptions(Some(path.realm), None)
        .into_iter()
        .filter(|item| {
            matches_family(family, item.route_family) && matches_resource_route(&item.pattern, path)
        })
        .count();
    NoticeResourceDetail::from_count(path, subscriptions_active)
}

#[must_use]
pub fn rpc_operation_detail(
    runtime: &Runtime,
    path: &RpcOperationPath<'_>,
    family: Option<u64>,
) -> RpcOperationDetail {
    let workers = runtime
        .rpc_list_workers(Some(path.realm))
        .into_iter()
        .filter(|worker| {
            matches_family(family, worker.route_family)
                && matches_operation_route(&worker.route, path)
        })
        .collect::<Vec<_>>();
    let requests_pending = runtime
        .rpc_list_pending(Some(path.realm))
        .into_iter()
        .filter(|request| {
            matches_family(family, request.route_family)
                && matches_operation_route(&request.route, path)
        })
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

pub(crate) fn comparison_scope(
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> ResourceComparisonScope {
    ResourceComparisonScope::new(path, family)
}

pub(crate) fn comparison_side(
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
pub(crate) fn build_resource_comparison(
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

pub(crate) fn kv_comparison_metrics(detail: &KvResourceDetail) -> ResourceComparisonMetrics {
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

pub(crate) fn queue_comparison_metrics(detail: &QueueResourceDetail) -> ResourceComparisonMetrics {
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

pub(crate) fn stream_comparison_metrics(
    detail: &StreamResourceDetail,
) -> ResourceComparisonMetrics {
    let lag = usize::try_from(detail.offset.saturating_sub(detail.watermark)).unwrap_or(usize::MAX);
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

pub(crate) fn lease_comparison_metrics(detail: &LeaseResourceDetail) -> ResourceComparisonMetrics {
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

pub(crate) fn notice_comparison_metrics(
    detail: &NoticeResourceDetail,
) -> ResourceComparisonMetrics {
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

pub(crate) fn schedule_comparison_metrics(
    detail: &ScheduleResourceDetail,
) -> ResourceComparisonMetrics {
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

pub(crate) fn rpc_resource_comparison_metrics(
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

#[must_use]
pub fn kv_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    against: &ResourcePath<'_>,
    against_family: Option<u64>,
) -> ResourceComparison {
    let left = kv_detail(runtime, path, family);
    let right = kv_detail(runtime, against, against_family);
    let left_metrics = kv_comparison_metrics(&left);
    let right_metrics = kv_comparison_metrics(&right);
    build_resource_comparison(
        "kv",
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

#[must_use]
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

#[must_use]
pub fn stream_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    against: &ResourcePath<'_>,
    against_family: Option<u64>,
) -> ResourceComparison {
    let left = stream_detail(runtime, path, family);
    let right = stream_detail(runtime, against, against_family);
    let left_metrics = stream_comparison_metrics(&left);
    let right_metrics = stream_comparison_metrics(&right);
    build_resource_comparison(
        "stream",
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

#[must_use]
pub fn lease_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    against: &ResourcePath<'_>,
    against_family: Option<u64>,
) -> ResourceComparison {
    let left = lease_detail(runtime, path, family);
    let right = lease_detail(runtime, against, against_family);
    let left_metrics = lease_comparison_metrics(&left);
    let right_metrics = lease_comparison_metrics(&right);
    build_resource_comparison(
        "lease",
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

#[must_use]
pub fn schedule_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    against: &ResourcePath<'_>,
    against_family: Option<u64>,
) -> ResourceComparison {
    let left = schedule_detail(runtime, path, family);
    let right = schedule_detail(runtime, against, against_family);
    let left_metrics = schedule_comparison_metrics(&left);
    let right_metrics = schedule_comparison_metrics(&right);
    build_resource_comparison(
        "schedule",
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

#[must_use]
pub fn notice_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    against: &ResourcePath<'_>,
    against_family: Option<u64>,
) -> ResourceComparison {
    let left = notice_detail(runtime, path, family);
    let right = notice_detail(runtime, against, against_family);
    let left_metrics = notice_comparison_metrics(&left);
    let right_metrics = notice_comparison_metrics(&right);
    build_resource_comparison(
        "notice",
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

#[must_use]
pub fn rpc_compare_detail(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    against: &ResourcePath<'_>,
    against_family: Option<u64>,
) -> ResourceComparison {
    let left_pending = runtime
        .rpc_list_pending(Some(path.realm))
        .into_iter()
        .filter(|request| {
            matches_family(family, request.route_family)
                && matches_resource_route(&request.route, path)
        })
        .collect::<Vec<_>>();
    let left_workers_registered = runtime
        .rpc_list_workers(Some(path.realm))
        .into_iter()
        .filter(|worker| {
            matches_family(family, worker.route_family)
                && matches_resource_route(&worker.route, path)
        })
        .count();
    let (left_diagnostics, left_metrics) = rpc_resource_comparison_metrics(
        left_workers_registered,
        left_pending.len(),
        left_pending.iter().map(|request| request.age_seconds).max(),
    );

    let right_pending = runtime
        .rpc_list_pending(Some(against.realm))
        .into_iter()
        .filter(|request| {
            matches_family(against_family, request.route_family)
                && matches_resource_route(&request.route, against)
        })
        .collect::<Vec<_>>();
    let right_workers_registered = runtime
        .rpc_list_workers(Some(against.realm))
        .into_iter()
        .filter(|worker| {
            matches_family(against_family, worker.route_family)
                && matches_resource_route(&worker.route, against)
        })
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
        family,
        left_diagnostics,
        left_metrics,
        against,
        against_family,
        right_diagnostics,
        right_metrics,
    )
}

pub(crate) fn parse_flexible_route(route: &str) -> Option<ResourceRef> {
    route_triplet(route).map(|parts| {
        ResourceRef::new(
            parts.realm.to_string(),
            parts.area.to_string(),
            parts.resource.to_string(),
        )
    })
}

pub(crate) fn parse_rpc_operation(route: &str) -> Option<OwnedRpcOperation> {
    route_quad(route).map(|parts| OwnedRpcOperation {
        realm: parts.realm.to_string(),
        area: parts.area.to_string(),
        resource: parts.resource.to_string(),
        operation: parts.operation.to_string(),
    })
}

pub(crate) fn matches_resource_route(route: &str, path: &ResourcePath<'_>) -> bool {
    parse_flexible_route(route).is_some_and(|parsed| parsed.matches_path(path))
}

pub(crate) fn matches_operation_route(route: &str, path: &RpcOperationPath<'_>) -> bool {
    parse_rpc_operation(route).is_some_and(|parsed| parsed.matches_operation_path(path))
}
