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
use crate::boot::Runtime;
use hyper::{Body, Response};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;

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
    pub operations_per_second: f64,
    pub diagnostics: troubleshooting::DomainDiagnostics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcStats {
    pub workers_registered: usize,
    pub requests_pending: usize,
    pub oldest_pending_request_age_seconds: u64,
    pub pending_routes_active: usize,
    pub requests_total: u64,
    pub success_total: u64,
    pub failure_total: u64,
    pub request_timeouts_total: u64,
    pub backpressure_rejects_total: u64,
    pub duplicate_correlation_rejects_total: u64,
    pub wrong_worker_rejects_total: u64,
    pub responses_dropped_closed_caller_total: u64,
    pub responses_missing_pending_total: u64,
    pub acks_rejected_wrong_worker_total: u64,
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
    pub notify_failures_total: u64,
    pub ack_failures_total: u64,
    pub overdue_normalizations_total: u64,
    pub pending_claims_expired_total: u64,
    pub pending_claim_cleanup_failures_total: u64,
    pub diagnostics: troubleshooting::DomainDiagnostics,
}

/// Handle /admin/stats endpoint
pub async fn handle_global_stats(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    let troubleshooting::RuntimeDiagnostics {
        global,
        kv,
        stream,
        notice,
        queue,
        rpc,
        lease,
        schedule,
    } = troubleshooting::build_runtime_diagnostics(runtime.as_ref());
    let stats = GlobalStats {
        broker: BrokerStats {
            uptime_seconds: runtime.uptime().as_secs(),
            connections: runtime.connection_count(),
            sessions: runtime.session_count(),
            realms: runtime.active_realms(),
            messages_per_second: runtime.messages_per_second(),
        },
        domains: DomainStats {
            kv: KvStats {
                transactions_active: runtime.kv_transactions_active(),
                keys_total: runtime.kv_keys_total(),
                operations_per_second: runtime.kv_operations_per_second(),
                diagnostics: kv,
            },
            stream: StreamStats {
                streams_active: runtime.stream_active(),
                append_sessions_active: runtime.stream_append_sessions_active(),
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
                diagnostics: stream,
            },
            notice: NoticeStats {
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
                diagnostics: notice,
            },
            queue: QueueStats {
                messages_ready: runtime.queue_messages_ready(),
                messages_delayed: runtime.queue_messages_delayed(),
                messages_pending: runtime.queue_messages_pending(),
                messages_dead_lettered: runtime.queue_messages_dead_lettered(),
                oldest_message_age_seconds: runtime.queue_oldest_message_age_seconds(),
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
                operations_per_second: runtime.queue_operations_per_second(),
                diagnostics: queue,
            },
            rpc: RpcStats {
                workers_registered: runtime.rpc_workers_registered(),
                requests_pending: runtime.rpc_requests_pending(),
                oldest_pending_request_age_seconds: runtime
                    .rpc_oldest_pending_request_age_seconds(),
                pending_routes_active: runtime.rpc_pending_routes_active(),
                requests_total: runtime.rpc_requests_total(),
                success_total: runtime.rpc_success_total(),
                failure_total: runtime.rpc_failure_total(),
                request_timeouts_total: runtime.rpc_request_timeouts_total(),
                backpressure_rejects_total: runtime.rpc_backpressure_rejects_total(),
                duplicate_correlation_rejects_total: runtime
                    .rpc_duplicate_correlation_rejects_total(),
                wrong_worker_rejects_total: runtime.rpc_wrong_worker_rejects_total(),
                responses_dropped_closed_caller_total: runtime
                    .rpc_responses_dropped_closed_caller_total(),
                responses_missing_pending_total: runtime.rpc_responses_missing_pending_total(),
                acks_rejected_wrong_worker_total: runtime.rpc_acks_rejected_wrong_worker_total(),
                operations_per_second: runtime.rpc_operations_per_second(),
                diagnostics: rpc,
            },
            lease: LeaseStats {
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
                diagnostics: lease,
            },
            schedule: ScheduleStats {
                schedules_active: runtime.schedule_active(),
                executions_per_minute: runtime.schedule_executions_per_minute(),
                subscriptions_active: runtime.schedule_subscriptions_active(),
                pending_fire_claims: runtime.schedule_pending_fire_claims(),
                pending_ack_retries: runtime.schedule_pending_ack_retries(),
                oldest_pending_claim_age_seconds: runtime
                    .schedule_oldest_pending_claim_age_seconds(),
                notify_failures_total: runtime.schedule_notify_failures(),
                ack_failures_total: runtime.schedule_ack_failures(),
                overdue_normalizations_total: runtime.schedule_overdue_normalizations(),
                pending_claims_expired_total: runtime.schedule_pending_claims_expired_total(),
                pending_claim_cleanup_failures_total: runtime
                    .schedule_pending_claim_cleanup_failures_total(),
                diagnostics: schedule,
            },
        },
        diagnostics: global,
    };

    crate::api::admin::json_response(stats)
}

/// Handle domain-specific stats endpoints
pub async fn handle_domain_stats(
    runtime: Arc<Runtime>,
    domain: &str,
) -> Result<Response<Body>, Infallible> {
    let troubleshooting::RuntimeDiagnostics {
        kv,
        stream,
        notice,
        queue,
        rpc,
        lease,
        schedule,
        ..
    } = troubleshooting::build_runtime_diagnostics(runtime.as_ref());
    match domain {
        "kv" => handle_kv_stats(runtime, kv).await,
        "stream" => handle_stream_stats(runtime, stream).await,
        "notice" => handle_notice_stats(runtime, notice).await,
        "queue" => handle_queue_stats(runtime, queue).await,
        "rpc" => handle_rpc_stats(runtime, rpc).await,
        "lease" => handle_lease_stats(runtime, lease).await,
        "schedule" => handle_schedule_stats(runtime, schedule).await,
        _ => Ok(crate::api::admin::not_found()),
    }
}

async fn handle_kv_stats(
    runtime: Arc<Runtime>,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Result<Response<Body>, Infallible> {
    crate::api::admin::json_response(KvStats {
        transactions_active: runtime.kv_transactions_active(),
        keys_total: runtime.kv_keys_total(),
        operations_per_second: runtime.kv_operations_per_second(),
        diagnostics,
    })
}

async fn handle_stream_stats(
    runtime: Arc<Runtime>,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Result<Response<Body>, Infallible> {
    crate::api::admin::json_response(StreamStats {
        streams_active: runtime.stream_active(),
        append_sessions_active: runtime.stream_append_sessions_active(),
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
    })
}

async fn handle_notice_stats(
    runtime: Arc<Runtime>,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Result<Response<Body>, Infallible> {
    crate::api::admin::json_response(NoticeStats {
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
    })
}

async fn handle_queue_stats(
    runtime: Arc<Runtime>,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Result<Response<Body>, Infallible> {
    crate::api::admin::json_response(QueueStats {
        messages_ready: runtime.queue_messages_ready(),
        messages_delayed: runtime.queue_messages_delayed(),
        messages_pending: runtime.queue_messages_pending(),
        messages_dead_lettered: runtime.queue_messages_dead_lettered(),
        oldest_message_age_seconds: runtime.queue_oldest_message_age_seconds(),
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
        operations_per_second: runtime.queue_operations_per_second(),
        diagnostics,
    })
}

async fn handle_rpc_stats(
    runtime: Arc<Runtime>,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Result<Response<Body>, Infallible> {
    crate::api::admin::json_response(RpcStats {
        workers_registered: runtime.rpc_workers_registered(),
        requests_pending: runtime.rpc_requests_pending(),
        oldest_pending_request_age_seconds: runtime.rpc_oldest_pending_request_age_seconds(),
        pending_routes_active: runtime.rpc_pending_routes_active(),
        requests_total: runtime.rpc_requests_total(),
        success_total: runtime.rpc_success_total(),
        failure_total: runtime.rpc_failure_total(),
        request_timeouts_total: runtime.rpc_request_timeouts_total(),
        backpressure_rejects_total: runtime.rpc_backpressure_rejects_total(),
        duplicate_correlation_rejects_total: runtime.rpc_duplicate_correlation_rejects_total(),
        wrong_worker_rejects_total: runtime.rpc_wrong_worker_rejects_total(),
        responses_dropped_closed_caller_total: runtime.rpc_responses_dropped_closed_caller_total(),
        responses_missing_pending_total: runtime.rpc_responses_missing_pending_total(),
        acks_rejected_wrong_worker_total: runtime.rpc_acks_rejected_wrong_worker_total(),
        operations_per_second: runtime.rpc_operations_per_second(),
        diagnostics,
    })
}

async fn handle_lease_stats(
    runtime: Arc<Runtime>,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Result<Response<Body>, Infallible> {
    crate::api::admin::json_response(LeaseStats {
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
    })
}

async fn handle_schedule_stats(
    runtime: Arc<Runtime>,
    diagnostics: troubleshooting::DomainDiagnostics,
) -> Result<Response<Body>, Infallible> {
    crate::api::admin::json_response(ScheduleStats {
        schedules_active: runtime.schedule_active(),
        executions_per_minute: runtime.schedule_executions_per_minute(),
        subscriptions_active: runtime.schedule_subscriptions_active(),
        pending_fire_claims: runtime.schedule_pending_fire_claims(),
        pending_ack_retries: runtime.schedule_pending_ack_retries(),
        oldest_pending_claim_age_seconds: runtime.schedule_oldest_pending_claim_age_seconds(),
        notify_failures_total: runtime.schedule_notify_failures(),
        ack_failures_total: runtime.schedule_ack_failures(),
        overdue_normalizations_total: runtime.schedule_overdue_normalizations(),
        pending_claims_expired_total: runtime.schedule_pending_claims_expired_total(),
        pending_claim_cleanup_failures_total: runtime
            .schedule_pending_claim_cleanup_failures_total(),
        diagnostics,
    })
}

/// Parse realm filter from query string
#[allow(dead_code)] // Kept for admin endpoints that need shared realm-filter parsing.
pub fn parse_realm_filter(query: Option<&str>) -> Option<String> {
    query.and_then(|q| {
        q.split('&').find_map(|pair| {
            let mut parts = pair.split('=');
            if parts.next()? == "realm" {
                parts.next().map(|s| s.to_string())
            } else {
                None
            }
        })
    })
}
