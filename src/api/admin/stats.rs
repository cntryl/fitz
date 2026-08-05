//! Domain statistics endpoints
//!
//! Routes:
//! - GET /api/v1/stats - Global broker and domain statistics
//! - GET /api/v1/kv/stats - KV domain statistics
//! - GET /api/v1/stream/stats - Stream domain statistics
//! - GET /api/v1/notice/stats - Notice domain statistics
//! - GET /api/v1/queue/stats - Queue domain statistics
//! - GET /api/v1/rpc/stats - RPC domain statistics
//! - GET /api/v1/lease/stats - Lease domain statistics

use super::troubleshooting;
use crate::api::http::Response;
use crate::boot::Runtime;
use crate::runtime::routing::RouteFamily;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalStats {
    pub broker: BrokerStats,
    pub domains: DomainStats,
    pub diagnostics: troubleshooting::GlobalTroubleshootingDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerStats {
    pub uptime_seconds: u64,
    pub connections: usize,
    pub sessions: usize,
    pub realms: Vec<String>,
    pub messages_per_second: f64,
    pub router_backpressure_total: u64,
    pub router_high_lane_backpressure_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainStats {
    pub kv: KvStats,
    pub stream: StreamStats,
    pub notice: NoticeStats,
    pub queue: QueueStats,
    pub rpc: RpcStats,
    pub lease: LeaseStats,
    pub schedule: ScheduleStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvStats {
    pub transactions_active: usize,
    pub keys_total: usize,
    pub commits_failed_total: u64,
    pub invalid_transaction_rejects_total: u64,
    pub operations_per_second: f64,
    pub diagnostics: troubleshooting::DomainDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStats {
    pub streams_active: usize,
    pub append_sessions_active: usize,
    pub events_total: usize,
    pub requests_total: u64,
    pub success_total: u64,
    pub failure_total: u64,
    pub append_sessions_started_total: u64,
    pub append_sessions_ended_total: u64,
    pub append_conflicts_total: u64,
    pub notify_drops_total: u64,
    pub watermark_lag_buckets: crate::api::admin::StreamLagBuckets,
    pub request_latency_buckets: crate::api::admin::StreamLatencyBuckets,
    pub operations_per_second: f64,
    pub subscriptions_active: usize,
    pub diagnostics: troubleshooting::DomainDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeStats {
    pub subscriptions_active: usize,
    pub routes_active: usize,
    pub max_route_subscribers: usize,
    pub requests_total: u64,
    pub success_total: u64,
    pub failure_total: u64,
    pub delivery_drops_total: u64,
    pub unsubscribes_total: u64,
    pub wildcard_limit_rejects_total: u64,
    pub publishes_per_second: f64,
    pub diagnostics: troubleshooting::DomainDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_pending: usize,
    pub messages_dead_lettered: usize,
    pub oldest_message_age_seconds: u64,
    pub oldest_backlog_age_seconds: u64,
    pub backlog_age_buckets: crate::api::admin::QueueAgeBuckets,
    pub delay_age_buckets: crate::api::admin::QueueAgeBuckets,
    pub inflight_active: usize,
    pub requests_total: u64,
    pub success_total: u64,
    pub failure_total: u64,
    pub enqueues_total: u64,
    pub reserves_total: u64,
    pub completes_total: u64,
    pub releases_total: u64,
    pub extends_total: u64,
    pub notify_drops_total: u64,
    pub redeliveries_total: u64,
    pub dead_letter_transitions_total: u64,
    pub complete_rejected_total: u64,
    pub operations_per_second: f64,
    pub diagnostics: troubleshooting::DomainDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcStats {
    pub workers_registered: usize,
    pub requests_pending: usize,
    pub oldest_pending_request_age_seconds: u64,
    pub pending_routes_active: usize,
    pub slowest_worker_average_latency_ms: f64,
    pub worker_latency_buckets: crate::api::admin::RpcLatencyBuckets,
    pub requests_total: u64,
    pub success_total: u64,
    pub failure_total: u64,
    pub request_timeouts_total: u64,
    pub backpressure_rejects_total: u64,
    pub duplicate_correlation_rejects_total: u64,
    pub wrong_worker_rejects_total: u64,
    pub responses_dropped_closed_caller_total: u64,
    pub responses_missing_pending_total: u64,
    pub invalid_sequence_responses_total: u64,
    pub invalid_sequence_errors_forwarded_total: u64,
    pub invalid_sequence_errors_dropped_total: u64,
    pub operations_per_second: f64,
    pub diagnostics: troubleshooting::DomainDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseStats {
    pub leases_active: usize,
    pub waiter_depth: usize,
    pub oldest_lease_age_seconds: u64,
    pub requests_total: u64,
    pub success_total: u64,
    pub failure_total: u64,
    pub acquire_timeouts_total: u64,
    pub forced_releases_total: u64,
    pub invalid_token_rejects_total: u64,
    pub ownership_churn_total: u64,
    pub operations_per_second: f64,
    pub diagnostics: troubleshooting::DomainDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleStats {
    pub schedules_active: usize,
    pub executions_per_minute: f64,
    pub subscriptions_active: usize,
    pub pending_fire_claims: usize,
    pub pending_ack_retries: usize,
    pub oldest_pending_claim_age_seconds: u64,
    pub request_latency_buckets: crate::api::admin::ScheduleLatencyBuckets,
    pub notify_failures_total: u64,
    pub ack_failures_total: u64,
    pub overdue_normalizations_total: u64,
    pub create_persistence_failures_total: u64,
    pub upsert_persistence_failures_total: u64,
    pub cancel_persistence_failures_total: u64,
    pub diagnostics: troubleshooting::DomainDiagnostics,
}

/// Build the `/api/v1/stats` response body.
pub(crate) fn build_global_stats(runtime: &Runtime) -> GlobalStats {
    let troubleshooting::TroubleshootingSnapshot {
        global,
        kv,
        stream,
        notice,
        queue,
        rpc,
        lease,
        schedule,
    } = troubleshooting::build_troubleshooting_snapshot(runtime);

    GlobalStats {
        broker: build_broker_stats(runtime),
        domains: DomainStats {
            kv: build_kv_stats(runtime, kv),
            stream: build_stream_stats(runtime, stream),
            notice: build_notice_stats(runtime, notice),
            queue: build_queue_stats(runtime, queue),
            rpc: build_rpc_stats(runtime, rpc),
            lease: build_lease_stats(runtime, lease),
            schedule: build_schedule_stats(runtime, schedule),
        },
        diagnostics: global,
    }
}

fn build_broker_stats(runtime: &Runtime) -> BrokerStats {
    BrokerStats {
        uptime_seconds: runtime.uptime().as_secs(),
        connections: runtime.connection_count(),
        sessions: runtime.session_count(),
        realms: runtime.active_realms(),
        messages_per_second: runtime.messages_per_second(),
        router_backpressure_total: runtime.router_backpressure_total(),
        router_high_lane_backpressure_total: runtime.router_high_lane_backpressure_total(),
    }
}

fn build_kv_stats(runtime: &Runtime, diagnostics: troubleshooting::DomainDiagnostics) -> KvStats {
    KvStats {
        transactions_active: runtime.kv_transactions_active(),
        keys_total: runtime.kv_keys_total(),
        commits_failed_total: runtime.kv_commits_failed_total(),
        invalid_transaction_rejects_total: runtime.kv_invalid_transaction_rejects_total(),
        operations_per_second: runtime.kv_operations_per_second(),
        diagnostics,
    }
}

fn build_stream_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> StreamStats {
    StreamStats {
        streams_active: runtime.stream_active(),
        append_sessions_active: runtime.stream_append_sessions_active(),
        watermark_lag_buckets: runtime.stream_watermark_lag_buckets(),
        request_latency_buckets: runtime.stream_request_latency_buckets(),
        events_total: runtime.stream_events_total(),
        requests_total: runtime.stream_requests_total(),
        success_total: runtime.stream_success_total(),
        failure_total: runtime.stream_failure_total(),
        append_sessions_started_total: runtime.stream_append_sessions_started_total(),
        append_sessions_ended_total: runtime.stream_append_sessions_ended_total(),
        append_conflicts_total: runtime.stream_append_conflicts_total(),
        notify_drops_total: runtime.stream_notify_drops_total(),
        operations_per_second: runtime.stream_operations_per_second(),
        subscriptions_active: runtime.stream_subscriptions_active(),
        diagnostics,
    }
}

fn build_notice_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> NoticeStats {
    NoticeStats {
        subscriptions_active: runtime.notice_subscriptions_active(),
        routes_active: runtime.notice_routes_active(),
        max_route_subscribers: runtime.notice_max_route_subscribers(),
        requests_total: runtime.notice_requests_total(),
        success_total: runtime.notice_success_total(),
        failure_total: runtime.notice_failure_total(),
        delivery_drops_total: runtime.notice_delivery_drops_total(),
        unsubscribes_total: runtime.notice_unsubscribes_total(),
        wildcard_limit_rejects_total: runtime.notice_wildcard_limit_rejects_total(),
        publishes_per_second: runtime.notice_publishes_per_second(),
        diagnostics,
    }
}

fn build_queue_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> QueueStats {
    QueueStats {
        messages_ready: runtime.queue_messages_ready(),
        messages_delayed: runtime.queue_messages_delayed(),
        messages_pending: runtime.queue_messages_pending(),
        messages_dead_lettered: runtime.queue_messages_dead_lettered(),
        oldest_message_age_seconds: runtime.queue_oldest_message_age_seconds(),
        oldest_backlog_age_seconds: runtime.queue_oldest_backlog_age_seconds(),
        backlog_age_buckets: runtime.queue_backlog_age_buckets(),
        delay_age_buckets: runtime.queue_delay_age_buckets(),
        inflight_active: runtime.queue_inflight_active(),
        requests_total: runtime.queue_requests_total(),
        success_total: runtime.queue_success_total(),
        failure_total: runtime.queue_failure_total(),
        enqueues_total: runtime.queue_enqueues_total(),
        reserves_total: runtime.queue_reserves_total(),
        completes_total: runtime.queue_completes_total(),
        releases_total: runtime.queue_releases_total(),
        extends_total: runtime.queue_extends_total(),
        notify_drops_total: runtime.queue_notify_drops_total(),
        redeliveries_total: runtime.queue_redeliveries_total(),
        dead_letter_transitions_total: runtime.queue_dead_letter_transitions_total(),
        complete_rejected_total: runtime.queue_complete_rejected_total(),
        operations_per_second: runtime.queue_operations_per_second(),
        diagnostics,
    }
}

fn build_rpc_stats(runtime: &Runtime, diagnostics: troubleshooting::DomainDiagnostics) -> RpcStats {
    RpcStats {
        workers_registered: runtime.rpc_workers_registered(),
        requests_pending: runtime.rpc_requests_pending(),
        oldest_pending_request_age_seconds: runtime.rpc_oldest_pending_request_age_seconds(),
        pending_routes_active: runtime.rpc_pending_routes_active(),
        slowest_worker_average_latency_ms: runtime.rpc_slowest_worker_average_latency_ms(),
        worker_latency_buckets: runtime.rpc_worker_latency_buckets(),
        requests_total: runtime.rpc_requests_total(),
        success_total: runtime.rpc_success_total(),
        failure_total: runtime.rpc_failure_total(),
        request_timeouts_total: runtime.rpc_request_timeouts_total(),
        backpressure_rejects_total: runtime.rpc_backpressure_rejects_total(),
        duplicate_correlation_rejects_total: runtime.rpc_duplicate_correlation_rejects_total(),
        wrong_worker_rejects_total: runtime.rpc_wrong_worker_rejects_total(),
        responses_dropped_closed_caller_total: runtime.rpc_responses_dropped_closed_caller_total(),
        responses_missing_pending_total: runtime.rpc_responses_missing_pending_total(),
        invalid_sequence_responses_total: runtime.rpc_invalid_sequence_responses_total(),
        invalid_sequence_errors_forwarded_total: runtime
            .rpc_invalid_sequence_errors_forwarded_total(),
        invalid_sequence_errors_dropped_total: runtime.rpc_invalid_sequence_errors_dropped_total(),
        operations_per_second: runtime.rpc_operations_per_second(),
        diagnostics,
    }
}

fn build_lease_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> LeaseStats {
    LeaseStats {
        leases_active: runtime.lease_active(),
        waiter_depth: runtime.lease_waiter_depth(),
        oldest_lease_age_seconds: runtime.lease_oldest_lease_age_seconds(),
        requests_total: runtime.lease_requests_total(),
        success_total: runtime.lease_success_total(),
        failure_total: runtime.lease_failure_total(),
        acquire_timeouts_total: runtime.lease_acquire_timeouts_total(),
        forced_releases_total: runtime.lease_forced_releases_total(),
        invalid_token_rejects_total: runtime.lease_invalid_token_rejects_total(),
        ownership_churn_total: runtime.lease_ownership_churn_total(),
        operations_per_second: runtime.lease_operations_per_second(),
        diagnostics,
    }
}

fn build_schedule_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> ScheduleStats {
    ScheduleStats {
        schedules_active: runtime.schedule_active(),
        executions_per_minute: runtime.schedule_executions_per_minute(),
        subscriptions_active: runtime.schedule_subscriptions_active(),
        pending_fire_claims: runtime.schedule_pending_fire_claims(),
        pending_ack_retries: runtime.schedule_pending_ack_retries(),
        oldest_pending_claim_age_seconds: runtime.schedule_oldest_pending_claim_age_seconds(),
        request_latency_buckets: runtime.schedule_request_latency_buckets(),
        notify_failures_total: runtime.schedule_notify_failures(),
        ack_failures_total: runtime.schedule_ack_failures(),
        overdue_normalizations_total: runtime.schedule_overdue_normalizations(),
        create_persistence_failures_total: runtime.schedule_create_persistence_failures_total(),
        upsert_persistence_failures_total: runtime.schedule_upsert_persistence_failures_total(),
        cancel_persistence_failures_total: runtime.schedule_cancel_persistence_failures_total(),
        diagnostics,
    }
}

pub(crate) fn build_global_troubleshooting(
    runtime: &Runtime,
) -> troubleshooting::GlobalTroubleshootingDiagnostics {
    let troubleshooting::TroubleshootingSnapshot { global, .. } =
        troubleshooting::build_troubleshooting_snapshot(runtime);

    global
}

/// Handle `/api/v1/stats`.
pub fn handle_global_stats(runtime: &Runtime) -> Response {
    crate::api::admin::json_response(build_global_stats(runtime))
}

/// Build a stats snapshot containing only state attributable to one route
/// family. Broker-wide counters and diagnostics are intentionally omitted
/// rather than copied into a narrower authorization scope.
#[allow(clippy::too_many_lines)]
pub(crate) fn build_family_stats(runtime: &Runtime, family: u64) -> GlobalStats {
    let sessions = runtime
        .list_sessions()
        .into_iter()
        .filter(|session| session.route_family == family)
        .collect::<Vec<_>>();
    let kv_transactions = runtime
        .kv_list_transactions(None)
        .into_iter()
        .filter(|transaction| transaction.route_family == family)
        .collect::<Vec<_>>();
    let streams = runtime
        .stream_list_streams(None)
        .into_iter()
        .filter(|stream| stream.route_family == family)
        .collect::<Vec<_>>();
    let notice_subscriptions = runtime
        .notice_list_subscriptions(None, None)
        .into_iter()
        .filter(|subscription| subscription.route_family == family)
        .collect::<Vec<_>>();
    let notice_routes = runtime
        .notice_list_routes(None)
        .into_iter()
        .filter(|route| route.route_family == family)
        .collect::<Vec<_>>();
    let queues = runtime
        .queue_list_queues(None)
        .into_iter()
        .filter(|queue| queue.family == family)
        .collect::<Vec<_>>();
    let rpc_workers = runtime
        .rpc_list_workers(None)
        .into_iter()
        .filter(|worker| worker.route_family == family)
        .collect::<Vec<_>>();
    let rpc_pending = runtime
        .rpc_list_pending(None)
        .into_iter()
        .filter(|request| request.route_family == family)
        .collect::<Vec<_>>();
    let leases = runtime
        .lease_list_leases(None)
        .into_iter()
        .filter(|lease| lease.route_family == family)
        .collect::<Vec<_>>();
    let schedules = runtime
        .schedule_list_schedules(None)
        .into_iter()
        .filter(|schedule| schedule.route_family == family)
        .collect::<Vec<_>>();
    let pending_fire_claims = u32::try_from(family).map_or(0, |family| {
        runtime
            .schedule_list_pending_claims(RouteFamily::new(family))
            .len()
    });

    let mut realms = BTreeSet::new();
    realms.extend(kv_transactions.iter().map(|item| item.realm.clone()));
    realms.extend(streams.iter().map(|item| item.realm.clone()));
    realms.extend(notice_subscriptions.iter().map(|item| item.realm.clone()));
    realms.extend(queues.iter().map(|item| item.realm.clone()));
    realms.extend(rpc_workers.iter().map(|item| item.realm.clone()));
    realms.extend(leases.iter().map(|item| item.realm.clone()));
    realms.extend(schedules.iter().map(|item| item.realm.clone()));

    let queue_messages_ready = queues
        .iter()
        .map(|queue| queue.messages_ready)
        .sum::<usize>();
    let queue_messages_delayed = queues
        .iter()
        .map(|queue| queue.messages_delayed)
        .sum::<usize>();
    let queue_messages_inflight = queues
        .iter()
        .map(|queue| queue.messages_inflight)
        .sum::<usize>();
    let queue_messages_dead_lettered = queues
        .iter()
        .map(|queue| queue.messages_dead_lettered)
        .sum::<usize>();
    let queue_messages_pending = queue_messages_ready.saturating_add(queue_messages_delayed);
    let healthy = troubleshooting::healthy_domain_diagnostics();

    GlobalStats {
        broker: BrokerStats {
            uptime_seconds: 0,
            connections: sessions.len(),
            sessions: sessions.len(),
            realms: realms.into_iter().collect(),
            messages_per_second: 0.0,
            router_backpressure_total: 0,
            router_high_lane_backpressure_total: 0,
        },
        domains: DomainStats {
            kv: KvStats {
                transactions_active: kv_transactions.len(),
                keys_total: 0,
                commits_failed_total: 0,
                invalid_transaction_rejects_total: 0,
                operations_per_second: 0.0,
                diagnostics: healthy.clone(),
            },
            stream: StreamStats {
                streams_active: streams.len(),
                append_sessions_active: streams.iter().map(|stream| stream.sessions_active).sum(),
                events_total: 0,
                requests_total: 0,
                success_total: 0,
                failure_total: 0,
                append_sessions_started_total: 0,
                append_sessions_ended_total: 0,
                append_conflicts_total: 0,
                notify_drops_total: 0,
                watermark_lag_buckets: crate::api::admin::StreamLagBuckets::default(),
                request_latency_buckets: crate::api::admin::StreamLatencyBuckets::default(),
                operations_per_second: 0.0,
                subscriptions_active: 0,
                diagnostics: healthy.clone(),
            },
            notice: NoticeStats {
                subscriptions_active: notice_subscriptions.len(),
                routes_active: notice_routes.len(),
                max_route_subscribers: notice_routes
                    .iter()
                    .map(|route| route.subscribers)
                    .max()
                    .unwrap_or(0),
                requests_total: 0,
                success_total: 0,
                failure_total: 0,
                delivery_drops_total: 0,
                unsubscribes_total: 0,
                wildcard_limit_rejects_total: 0,
                publishes_per_second: 0.0,
                diagnostics: healthy.clone(),
            },
            queue: QueueStats {
                messages_ready: queue_messages_ready,
                messages_delayed: queue_messages_delayed,
                messages_pending: queue_messages_pending,
                messages_dead_lettered: queue_messages_dead_lettered,
                oldest_message_age_seconds: 0,
                oldest_backlog_age_seconds: 0,
                backlog_age_buckets: crate::api::admin::QueueAgeBuckets::default(),
                delay_age_buckets: crate::api::admin::QueueAgeBuckets::default(),
                inflight_active: queue_messages_inflight,
                requests_total: 0,
                success_total: 0,
                failure_total: 0,
                enqueues_total: 0,
                reserves_total: 0,
                completes_total: 0,
                releases_total: 0,
                extends_total: 0,
                notify_drops_total: 0,
                redeliveries_total: 0,
                dead_letter_transitions_total: 0,
                complete_rejected_total: 0,
                operations_per_second: 0.0,
                diagnostics: healthy.clone(),
            },
            rpc: RpcStats {
                workers_registered: rpc_workers.len(),
                requests_pending: rpc_pending.len(),
                oldest_pending_request_age_seconds: rpc_pending
                    .iter()
                    .map(|request| request.age_seconds)
                    .max()
                    .unwrap_or(0),
                pending_routes_active: rpc_pending
                    .iter()
                    .map(|request| request.route.as_str())
                    .collect::<BTreeSet<_>>()
                    .len(),
                slowest_worker_average_latency_ms: 0.0,
                worker_latency_buckets: crate::api::admin::RpcLatencyBuckets::default(),
                requests_total: 0,
                success_total: 0,
                failure_total: 0,
                request_timeouts_total: 0,
                backpressure_rejects_total: 0,
                duplicate_correlation_rejects_total: 0,
                wrong_worker_rejects_total: 0,
                responses_dropped_closed_caller_total: 0,
                responses_missing_pending_total: 0,
                invalid_sequence_responses_total: 0,
                invalid_sequence_errors_forwarded_total: 0,
                invalid_sequence_errors_dropped_total: 0,
                operations_per_second: 0.0,
                diagnostics: healthy.clone(),
            },
            lease: LeaseStats {
                leases_active: leases.len(),
                waiter_depth: 0,
                oldest_lease_age_seconds: 0,
                requests_total: 0,
                success_total: 0,
                failure_total: 0,
                acquire_timeouts_total: 0,
                forced_releases_total: 0,
                invalid_token_rejects_total: 0,
                ownership_churn_total: 0,
                operations_per_second: 0.0,
                diagnostics: healthy.clone(),
            },
            schedule: ScheduleStats {
                schedules_active: schedules.len(),
                executions_per_minute: 0.0,
                subscriptions_active: 0,
                pending_fire_claims,
                pending_ack_retries: 0,
                oldest_pending_claim_age_seconds: 0,
                request_latency_buckets: crate::api::admin::ScheduleLatencyBuckets::default(),
                notify_failures_total: 0,
                ack_failures_total: 0,
                overdue_normalizations_total: 0,
                create_persistence_failures_total: 0,
                upsert_persistence_failures_total: 0,
                cancel_persistence_failures_total: 0,
                diagnostics: healthy,
            },
        },
        diagnostics: troubleshooting::healthy_global_diagnostics(),
    }
}

/// Handle statistics scoped to one authorized route family.
pub fn handle_family_stats(runtime: &Runtime, family: u64) -> Response {
    crate::api::admin::json_response(build_family_stats(runtime, family))
}

/// Handle `/api/v1/troubleshooting`.
pub fn handle_global_troubleshooting(runtime: &Runtime) -> Response {
    super::json_response(build_global_troubleshooting(runtime))
}

/// Handle troubleshooting guidance scoped to one authorized route family.
pub fn handle_family_troubleshooting(_runtime: &Runtime, _family: u64) -> Response {
    super::json_response(troubleshooting::healthy_global_diagnostics())
}

/// Handle domain-specific stats endpoints
pub fn handle_domain_stats(runtime: &Runtime, domain: &str, family: Option<u64>) -> Response {
    if let Some(family) = family {
        let stats = build_family_stats(runtime, family);
        return match domain {
            "kv" => crate::api::admin::json_response(stats.domains.kv),
            "stream" => crate::api::admin::json_response(stats.domains.stream),
            "notice" => crate::api::admin::json_response(stats.domains.notice),
            "queue" => crate::api::admin::json_response(stats.domains.queue),
            "rpc" => crate::api::admin::json_response(stats.domains.rpc),
            "lease" => crate::api::admin::json_response(stats.domains.lease),
            "schedule" => crate::api::admin::json_response(stats.domains.schedule),
            _ => crate::api::admin::not_found(),
        };
    }

    let troubleshooting::TroubleshootingSnapshot {
        kv,
        stream,
        notice,
        queue,
        rpc,
        lease,
        schedule,
        ..
    } = troubleshooting::build_troubleshooting_snapshot(runtime);
    match domain {
        "kv" => handle_kv_stats(runtime, kv),
        "stream" => handle_stream_stats(runtime, stream),
        "notice" => handle_notice_stats(runtime, notice),
        "queue" => handle_queue_stats(runtime, queue),
        "rpc" => handle_rpc_stats(runtime, rpc),
        "lease" => handle_lease_stats(runtime, lease),
        "schedule" => handle_schedule_stats(runtime, schedule),
        _ => crate::api::admin::not_found(),
    }
}

fn handle_kv_stats(runtime: &Runtime, diagnostics: troubleshooting::DomainDiagnostics) -> Response {
    crate::api::admin::json_response(build_kv_stats(runtime, diagnostics))
}

fn handle_stream_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Response {
    crate::api::admin::json_response(build_stream_stats(runtime, diagnostics))
}

fn handle_notice_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Response {
    crate::api::admin::json_response(build_notice_stats(runtime, diagnostics))
}

fn handle_queue_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Response {
    crate::api::admin::json_response(build_queue_stats(runtime, diagnostics))
}

fn handle_rpc_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Response {
    crate::api::admin::json_response(build_rpc_stats(runtime, diagnostics))
}

fn handle_lease_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Response {
    crate::api::admin::json_response(build_lease_stats(runtime, diagnostics))
}

fn handle_schedule_stats(
    runtime: &Runtime,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Response {
    crate::api::admin::json_response(build_schedule_stats(runtime, diagnostics))
}
