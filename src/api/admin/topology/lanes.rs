use crate::api::admin::list::{
    KvTransaction, LeaseInfo, NoticeSubscription, QueueInflight, QueueInfo, RpcPendingRequest,
    RpcWorker, ScheduleInfo, StreamInfo,
};
use crate::api::admin::stats;
use std::collections::BTreeMap;

use super::helpers::*;
use super::types::{
    TopologyConnectionBuilder, TopologyConnectionKind, TopologyLane, TopologyScope,
    TopologyScopedResource, TopologyState,
};

pub(super) fn queue_lane(
    stats: &stats::QueueStats,
    queues: &[QueueInfo],
    inflight: &[QueueInflight],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.messages_dead_lettered
        + stats.messages_ready
        + stats.messages_delayed
        + stats.messages_pending
        + stats.failure_total as usize
        + stats.notify_drops_total as usize
        + stats.complete_rejected_total as usize;
    let activity = stats.operations_per_second > 0.0 || stats.inflight_active > 0;
    let state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        counter("ready", "Ready", stats.messages_ready as f64),
        counter("delayed", "Delayed", stats.messages_delayed as f64),
        counter("pending", "Pending", stats.messages_pending as f64),
        counter("inflight", "Inflight", stats.inflight_active as f64),
        counter(
            "dead_letters",
            "Dead letters",
            stats.messages_dead_lettered as f64,
        ),
        counter(
            "oldest_backlog_age_seconds",
            "Oldest backlog age",
            stats.oldest_backlog_age_seconds as f64,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "queue",
        &state,
        stats.operations_per_second,
        counters.clone(),
    );

    for item in inflight {
        connections.push(topology_connection(
            (
                format!("queue-inflight:{}:{}", item.family, item.message_id),
                domain_node_id("queue"),
                session_node_id(&item.session_id),
            ),
            TopologyConnectionKind::QueueInflightConsumer,
            format!("{} / {} inflight", item.area, item.resource),
            TopologyState::Flowing,
            scope_with_session(
                scope_for_resource(&item.realm, &item.area, &item.resource, Some(item.family)),
                item.session_id.clone(),
            ),
            vec![
                counter("message_id", "Message", item.message_id as f64),
                counter("attempts", "Attempts", item.attempts as f64),
            ],
        ));
    }

    topology_lane(
        ("queue", "Queue"),
        state,
        stats.operations_per_second,
        &stats.diagnostics,
        counters,
        (stats.inflight_active, 0),
        top_queue_resources(queues),
    )
}

fn top_queue_resources(queues: &[QueueInfo]) -> Vec<TopologyScopedResource> {
    let resources = queues
        .iter()
        .map(|queue| {
            let counters = vec![
                counter("ready", "Ready", queue.messages_ready as f64),
                counter("delayed", "Delayed", queue.messages_delayed as f64),
                counter("inflight", "Inflight", queue.messages_inflight as f64),
                counter(
                    "dead_letters",
                    "Dead letters",
                    queue.messages_dead_lettered as f64,
                ),
                counter(
                    "oldest_backlog_age_seconds",
                    "Oldest backlog age",
                    queue.oldest_backlog_age_seconds as f64,
                ),
            ];
            let state = scoped_state(
                queue.messages_dead_lettered > 0,
                queue.messages_ready + queue.messages_delayed + queue.messages_inflight > 0,
                queue.messages_total > 0,
            );

            scoped_resource(
                "queue",
                format!("{} / {} / {}", queue.realm, queue.area, queue.resource),
                state,
                scope_for_resource(
                    &queue.realm,
                    &queue.area,
                    &queue.resource,
                    Some(queue.family),
                ),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}

pub(super) fn rpc_lane(
    stats: &stats::RpcStats,
    workers: &[RpcWorker],
    pending: &[RpcPendingRequest],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.requests_pending
        + stats.request_timeouts_total as usize
        + stats.backpressure_rejects_total as usize
        + stats.failure_total as usize
        + stats.responses_missing_pending_total as usize;
    let activity = stats.operations_per_second > 0.0 || stats.workers_registered > 0;
    let state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        counter("workers", "Workers", stats.workers_registered as f64),
        counter("pending", "Pending", stats.requests_pending as f64),
        counter("timeouts", "Timeouts", stats.request_timeouts_total as f64),
        counter(
            "backpressure",
            "Backpressure",
            stats.backpressure_rejects_total as f64,
        ),
        counter(
            "slowest_worker_average_latency_ms",
            "Slowest worker avg latency",
            stats.slowest_worker_average_latency_ms,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "rpc",
        &state,
        stats.operations_per_second,
        counters.clone(),
    );

    for worker in workers {
        connections.push(topology_connection(
            (
                format!("rpc-worker:{}:{}", worker.session_id, worker.route),
                session_node_id(&worker.session_id),
                domain_node_id("rpc"),
            ),
            TopologyConnectionKind::RpcWorker,
            worker.route.clone(),
            TopologyState::Flowing,
            scope_for_route(&worker.route, Some(worker.session_id.clone())),
            vec![
                counter(
                    "requests_handled",
                    "Handled",
                    worker.requests_handled as f64,
                ),
                counter(
                    "average_latency_ms",
                    "Avg latency",
                    worker.average_latency_ms,
                ),
            ],
        ));
    }

    for request in pending {
        let target = request
            .worker_session_id
            .as_deref()
            .map(session_node_id)
            .unwrap_or_else(|| format!("rpc-pending:{}", request.correlation_id));
        let state = if request.worker_session_id.is_some() {
            TopologyState::Flowing
        } else {
            TopologyState::Pressure
        };

        connections.push(topology_connection(
            (
                format!("rpc-pending:{}", request.correlation_id),
                domain_node_id("rpc"),
                target,
            ),
            TopologyConnectionKind::RpcPendingAssignment,
            request.route.clone(),
            state,
            scope_for_route(&request.route, request.worker_session_id.clone()),
            vec![counter("age_seconds", "Age", request.age_seconds as f64)],
        ));
    }

    topology_lane(
        ("rpc", "RPC"),
        state,
        stats.operations_per_second,
        &stats.diagnostics,
        counters,
        (stats.workers_registered, 0),
        top_rpc_resources(workers, pending),
    )
}

fn top_rpc_resources(
    workers: &[RpcWorker],
    pending: &[RpcPendingRequest],
) -> Vec<TopologyScopedResource> {
    #[derive(Default)]
    struct Rollup {
        route: String,
        workers: usize,
        pending: usize,
        handled: u64,
        slowest_latency_ms: f64,
        oldest_pending_age_seconds: u64,
    }

    let mut rollups: BTreeMap<String, Rollup> = BTreeMap::new();
    for worker in workers {
        let rollup = rollups.entry(worker.route.clone()).or_default();
        rollup.route = worker.route.clone();
        rollup.workers += 1;
        rollup.handled += worker.requests_handled;
        rollup.slowest_latency_ms = rollup.slowest_latency_ms.max(worker.average_latency_ms);
    }

    for request in pending {
        let rollup = rollups.entry(request.route.clone()).or_default();
        rollup.route = request.route.clone();
        rollup.pending += 1;
        rollup.oldest_pending_age_seconds =
            rollup.oldest_pending_age_seconds.max(request.age_seconds);
    }

    let resources = rollups
        .into_values()
        .map(|rollup| {
            let counters = vec![
                counter("workers", "Workers", rollup.workers as f64),
                counter("pending", "Pending", rollup.pending as f64),
                counter("handled", "Handled", rollup.handled as f64),
                counter(
                    "slowest_latency_ms",
                    "Slowest latency",
                    rollup.slowest_latency_ms,
                ),
                counter(
                    "oldest_pending_age_seconds",
                    "Oldest pending age",
                    rollup.oldest_pending_age_seconds as f64,
                ),
            ];
            let state = scoped_state(
                rollup.pending > 0 && rollup.workers == 0,
                rollup.pending > 0,
                rollup.workers > 0 || rollup.handled > 0,
            );

            scoped_resource(
                "rpc",
                rollup.route.clone(),
                state,
                scope_for_route(&rollup.route, None),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}

pub(super) fn notice_lane(
    stats: &stats::NoticeStats,
    subscriptions: &[NoticeSubscription],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.delivery_drops_total
        + stats.failure_total
        + stats.wildcard_limit_rejects_total
        + stats.unsubscribes_total;
    let activity = stats.publishes_per_second > 0.0 || stats.subscriptions_active > 0;
    let state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        counter(
            "subscriptions",
            "Subscriptions",
            stats.subscriptions_active as f64,
        ),
        counter("routes", "Routes", stats.routes_active as f64),
        counter(
            "max_route_subscribers",
            "Max route subscribers",
            stats.max_route_subscribers as f64,
        ),
        counter("delivery_drops", "Drops", stats.delivery_drops_total as f64),
        counter(
            "wildcard_rejects",
            "Wildcard rejects",
            stats.wildcard_limit_rejects_total as f64,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "notice",
        &state,
        stats.publishes_per_second,
        counters.clone(),
    );

    for subscription in subscriptions {
        connections.push(topology_connection(
            (
                format!(
                    "notice-subscription:{}:{}",
                    subscription.session_id, subscription.subscription_id
                ),
                session_node_id(&subscription.session_id),
                domain_node_id("notice"),
            ),
            TopologyConnectionKind::NoticeSubscription,
            subscription.pattern.clone(),
            TopologyState::Flowing,
            scope_for_pattern(
                &subscription.pattern,
                &subscription.realm,
                Some(subscription.session_id.clone()),
            ),
            vec![counter(
                "notifications_received",
                "Notifications",
                subscription.notifications_received as f64,
            )],
        ));
    }

    topology_lane(
        ("notice", "Notice"),
        state,
        stats.publishes_per_second,
        &stats.diagnostics,
        counters,
        (0, stats.subscriptions_active),
        top_notice_resources(subscriptions),
    )
}

fn top_notice_resources(subscriptions: &[NoticeSubscription]) -> Vec<TopologyScopedResource> {
    #[derive(Default)]
    struct Rollup {
        realm: String,
        pattern: String,
        subscriptions: usize,
        notifications: u64,
    }

    let mut rollups: BTreeMap<String, Rollup> = BTreeMap::new();
    for subscription in subscriptions {
        let rollup = rollups.entry(subscription.pattern.clone()).or_default();
        rollup.realm = subscription.realm.clone();
        rollup.pattern = subscription.pattern.clone();
        rollup.subscriptions += 1;
        rollup.notifications += subscription.notifications_received;
    }

    let resources = rollups
        .into_values()
        .map(|rollup| {
            let counters = vec![
                counter(
                    "subscriptions",
                    "Subscriptions",
                    rollup.subscriptions as f64,
                ),
                counter(
                    "notifications",
                    "Notifications",
                    rollup.notifications as f64,
                ),
            ];
            scoped_resource(
                "notice",
                rollup.pattern.clone(),
                TopologyState::Flowing,
                scope_for_pattern(&rollup.pattern, &rollup.realm, None),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}

pub(super) fn schedule_lane(
    stats: &stats::ScheduleStats,
    schedules: &[ScheduleInfo],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.pending_fire_claims
        + stats.pending_ack_retries
        + stats.notify_failures_total as usize
        + stats.ack_failures_total as usize
        + stats.create_persistence_failures_total as usize
        + stats.upsert_persistence_failures_total as usize
        + stats.cancel_persistence_failures_total as usize;
    let activity = stats.executions_per_minute > 0.0
        || stats.schedules_active > 0
        || stats.subscriptions_active > 0;
    let state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let activity_per_second = stats.executions_per_minute / 60.0;
    let counters = vec![
        counter("schedules", "Schedules", stats.schedules_active as f64),
        counter(
            "subscriptions",
            "Subscriptions",
            stats.subscriptions_active as f64,
        ),
        counter(
            "pending_claims",
            "Pending claims",
            stats.pending_fire_claims as f64,
        ),
        counter(
            "pending_ack_retries",
            "Pending ack retries",
            stats.pending_ack_retries as f64,
        ),
        counter(
            "notify_failures",
            "Notify failures",
            stats.notify_failures_total as f64,
        ),
        counter(
            "ack_failures",
            "Ack failures",
            stats.ack_failures_total as f64,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "schedule",
        &state,
        activity_per_second,
        counters.clone(),
    );

    if stats.subscriptions_active > 0 {
        connections.push(topology_connection(
            (
                "schedule-subscription-activity",
                domain_node_id("schedule"),
                "consumers:schedule",
            ),
            TopologyConnectionKind::ScheduleSubscriptionActivity,
            "Schedule subscriptions",
            TopologyState::Flowing,
            TopologyScope::default(),
            vec![counter(
                "subscriptions",
                "Subscriptions",
                stats.subscriptions_active as f64,
            )],
        ));
    }

    topology_lane(
        ("schedule", "Schedule"),
        state,
        activity_per_second,
        &stats.diagnostics,
        counters,
        (0, stats.subscriptions_active),
        top_schedule_resources(schedules),
    )
}

fn top_schedule_resources(schedules: &[ScheduleInfo]) -> Vec<TopologyScopedResource> {
    let resources = schedules
        .iter()
        .map(|schedule| {
            let counters = vec![
                counter(
                    "enabled",
                    "Enabled",
                    if schedule.enabled { 1.0 } else { 0.0 },
                ),
                counter("executions", "Executions", schedule.executions_total as f64),
            ];

            scoped_resource(
                "schedule",
                format!(
                    "{} / {} / {} / {}",
                    schedule.realm, schedule.area, schedule.resource, schedule.operation
                ),
                if schedule.enabled {
                    TopologyState::Flowing
                } else {
                    TopologyState::Quiet
                },
                TopologyScope {
                    operation: Some(schedule.operation.clone()),
                    ..scope_for_resource(&schedule.realm, &schedule.area, &schedule.resource, None)
                },
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}

pub(super) fn stream_lane(
    stats: &stats::StreamStats,
    streams: &[StreamInfo],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.notify_drops_total + stats.append_conflicts_total + stats.failure_total;
    let activity = stats.operations_per_second > 0.0
        || stats.events_total > 0
        || stats.subscriptions_active > 0
        || stats.append_sessions_active > 0;
    let state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        counter("streams", "Streams", stats.streams_active as f64),
        counter("events", "Events", stats.events_total as f64),
        counter(
            "append_sessions",
            "Append sessions",
            stats.append_sessions_active as f64,
        ),
        counter(
            "subscriptions",
            "Subscriptions",
            stats.subscriptions_active as f64,
        ),
        counter(
            "append_conflicts",
            "Append conflicts",
            stats.append_conflicts_total as f64,
        ),
        counter(
            "notify_drops",
            "Notify drops",
            stats.notify_drops_total as f64,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "stream",
        &state,
        stats.operations_per_second,
        counters.clone(),
    );

    for stream in streams.iter().filter(|stream| stream.sessions_active > 0) {
        connections.push(topology_connection(
            (
                format!(
                    "stream-append:{}:{}:{}",
                    stream.realm, stream.area, stream.resource
                ),
                domain_node_id("stream"),
                format!(
                    "stream:{}:{}:{}",
                    stream.realm, stream.area, stream.resource
                ),
            ),
            TopologyConnectionKind::StreamAppendActivity,
            format!("{} / {} append activity", stream.area, stream.resource),
            TopologyState::Flowing,
            scope_for_resource(&stream.realm, &stream.area, &stream.resource, None),
            vec![
                counter("sessions", "Sessions", stream.sessions_active as f64),
                counter("offset", "Offset", stream.offset as f64),
                counter("watermark", "Watermark", stream.watermark as f64),
            ],
        ));
    }

    topology_lane(
        ("stream", "Stream"),
        state,
        stats.operations_per_second,
        &stats.diagnostics,
        counters,
        (stats.append_sessions_active, stats.subscriptions_active),
        top_stream_resources(streams),
    )
}

fn top_stream_resources(streams: &[StreamInfo]) -> Vec<TopologyScopedResource> {
    let resources = streams
        .iter()
        .map(|stream| {
            let counters = vec![
                counter("offset", "Offset", stream.offset as f64),
                counter("watermark", "Watermark", stream.watermark as f64),
                counter("size_bytes", "Size bytes", stream.size_bytes as f64),
                counter("sessions", "Sessions", stream.sessions_active as f64),
            ];
            scoped_resource(
                "stream",
                format!("{} / {} / {}", stream.realm, stream.area, stream.resource),
                if stream.sessions_active > 0 || stream.offset > 0 {
                    TopologyState::Flowing
                } else {
                    TopologyState::Quiet
                },
                scope_for_resource(&stream.realm, &stream.area, &stream.resource, None),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}

pub(super) fn lease_lane(
    stats: &stats::LeaseStats,
    leases: &[LeaseInfo],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure =
        stats.waiter_depth + stats.acquire_timeouts_total as usize + stats.failure_total as usize;
    let activity = stats.operations_per_second > 0.0 || stats.leases_active > 0;
    let state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        counter("leases", "Leases", stats.leases_active as f64),
        counter("waiters", "Waiters", stats.waiter_depth as f64),
        counter("timeouts", "Timeouts", stats.acquire_timeouts_total as f64),
        counter(
            "forced_releases",
            "Forced releases",
            stats.forced_releases_total as f64,
        ),
        counter(
            "oldest_lease_age_seconds",
            "Oldest lease age",
            stats.oldest_lease_age_seconds as f64,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "lease",
        &state,
        stats.operations_per_second,
        counters.clone(),
    );

    for lease in leases {
        connections.push(topology_connection(
            (
                format!(
                    "lease-owner:{}:{}:{}",
                    lease.realm, lease.area, lease.resource
                ),
                domain_node_id("lease"),
                session_node_id(&lease.owner_session_id),
            ),
            TopologyConnectionKind::LeaseOwner,
            format!("{} / {} owner", lease.area, lease.resource),
            TopologyState::Flowing,
            scope_with_session(
                scope_for_resource(&lease.realm, &lease.area, &lease.resource, None),
                lease.owner_session_id.clone(),
            ),
            vec![
                counter("renewals", "Renewals", lease.renewals as f64),
                counter("fencing_token", "Fencing token", lease.fencing_token as f64),
            ],
        ));
    }

    topology_lane(
        ("lease", "Lease"),
        state,
        stats.operations_per_second,
        &stats.diagnostics,
        counters,
        (stats.leases_active, stats.waiter_depth),
        top_lease_resources(leases),
    )
}

fn top_lease_resources(leases: &[LeaseInfo]) -> Vec<TopologyScopedResource> {
    let resources = leases
        .iter()
        .map(|lease| {
            let counters = vec![
                counter("renewals", "Renewals", lease.renewals as f64),
                counter("fencing_token", "Fencing token", lease.fencing_token as f64),
            ];
            scoped_resource(
                "lease",
                format!("{} / {} / {}", lease.realm, lease.area, lease.resource),
                TopologyState::Flowing,
                scope_with_session(
                    scope_for_resource(&lease.realm, &lease.area, &lease.resource, None),
                    lease.owner_session_id.clone(),
                ),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}

pub(super) fn kv_lane(
    stats: &stats::KvStats,
    transactions: &[KvTransaction],
    connections: &mut TopologyConnectionBuilder,
) -> TopologyLane {
    let pressure = stats.commits_failed_total + stats.invalid_transaction_rejects_total;
    let activity =
        stats.operations_per_second > 0.0 || stats.transactions_active > 0 || stats.keys_total > 0;
    let state = topology_state(&stats.diagnostics, pressure > 0, activity);
    let counters = vec![
        counter("keys", "Keys", stats.keys_total as f64),
        counter(
            "transactions",
            "Transactions",
            stats.transactions_active as f64,
        ),
        counter(
            "failed_commits",
            "Failed commits",
            stats.commits_failed_total as f64,
        ),
        counter("rollbacks", "Rollbacks", stats.rollbacks_total as f64),
        counter(
            "invalid_rejects",
            "Invalid rejects",
            stats.invalid_transaction_rejects_total as f64,
        ),
    ];

    add_broker_domain_flow(
        connections,
        "kv",
        &state,
        stats.operations_per_second,
        counters.clone(),
    );

    for transaction in transactions {
        let session_id = session_id_from_transaction_mode(&transaction.mode);
        let target = session_id
            .as_deref()
            .map(session_node_id)
            .unwrap_or_else(|| format!("kv-transaction:{}", transaction.tx_id));
        let scope = if let Some(session_id) = session_id {
            scope_with_session(
                scope_for_resource(
                    &transaction.realm,
                    &transaction.area,
                    &transaction.resource,
                    None,
                ),
                session_id,
            )
        } else {
            scope_for_resource(
                &transaction.realm,
                &transaction.area,
                &transaction.resource,
                None,
            )
        };

        connections.push(topology_connection(
            (
                format!("kv-transaction:{}", transaction.tx_id),
                domain_node_id("kv"),
                target,
            ),
            TopologyConnectionKind::KvTransactionActivity,
            format!(
                "{} / {} transaction",
                transaction.area, transaction.resource
            ),
            TopologyState::Flowing,
            scope,
            vec![
                counter(
                    "operations",
                    "Operations",
                    transaction.operations_count as f64,
                ),
                counter("idle_seconds", "Idle", transaction.idle_seconds as f64),
            ],
        ));
    }

    topology_lane(
        ("kv", "KV"),
        state,
        stats.operations_per_second,
        &stats.diagnostics,
        counters,
        (stats.transactions_active, 0),
        top_kv_resources(transactions),
    )
}

fn session_id_from_transaction_mode(mode: &str) -> Option<String> {
    let mut parts = mode.split(':');
    if parts.next()? != "session" {
        return None;
    }
    parts.next().map(str::to_string)
}

fn top_kv_resources(transactions: &[KvTransaction]) -> Vec<TopologyScopedResource> {
    #[derive(Default)]
    struct Rollup {
        realm: String,
        area: String,
        resource: String,
        transactions: usize,
        operations: usize,
        max_idle_seconds: u64,
    }

    let mut rollups: BTreeMap<(String, String, String), Rollup> = BTreeMap::new();
    for transaction in transactions {
        let key = (
            transaction.realm.clone(),
            transaction.area.clone(),
            transaction.resource.clone(),
        );
        let rollup = rollups.entry(key).or_default();
        rollup.realm = transaction.realm.clone();
        rollup.area = transaction.area.clone();
        rollup.resource = transaction.resource.clone();
        rollup.transactions += 1;
        rollup.operations += transaction.operations_count;
        rollup.max_idle_seconds = rollup.max_idle_seconds.max(transaction.idle_seconds);
    }

    let resources = rollups
        .into_values()
        .map(|rollup| {
            let counters = vec![
                counter("transactions", "Transactions", rollup.transactions as f64),
                counter("operations", "Operations", rollup.operations as f64),
                counter(
                    "max_idle_seconds",
                    "Max idle",
                    rollup.max_idle_seconds as f64,
                ),
            ];
            scoped_resource(
                "kv",
                format!("{} / {} / {}", rollup.realm, rollup.area, rollup.resource),
                TopologyState::Flowing,
                scope_for_resource(&rollup.realm, &rollup.area, &rollup.resource, None),
                counters,
            )
        })
        .collect::<Vec<_>>();

    top_resources(resources)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_not_mark_kv_topology_pressure_given_only_rollbacks() {
        // Arrange
        let stats = stats::KvStats {
            transactions_active: 0,
            keys_total: 0,
            commits_failed_total: 0,
            rollbacks_total: 4,
            invalid_transaction_rejects_total: 0,
            operations_per_second: 0.0,
            diagnostics: crate::api::admin::troubleshooting::DomainDiagnostics {
                snapshot: crate::api::admin::troubleshooting::kv_resource_diagnostics(0),
            },
        };
        let mut connections = TopologyConnectionBuilder::new(8);

        // Act
        let lane = kv_lane(&stats, &[], &mut connections);

        // Assert
        assert!(matches!(lane.state, TopologyState::Quiet));
        assert_eq!(
            lane.counters
                .iter()
                .find(|counter| counter.key == "rollbacks")
                .map(|counter| counter.value),
            Some(4.0)
        );
    }
}
