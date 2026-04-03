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

use crate::boot::Runtime;
use hyper::{Body, Response};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalStats {
    pub broker: BrokerStats,
    pub domains: DomainStats,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamStats {
    pub streams_active: usize,
    pub events_total: usize,
    pub operations_per_second: f64,
    pub subscriptions_active: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoticeStats {
    pub subscriptions_active: usize,
    pub publishes_per_second: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_pending: usize,
    pub messages_dead_lettered: usize,
    pub leases_active: usize,
    pub operations_per_second: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcStats {
    pub workers_registered: usize,
    pub requests_pending: usize,
    pub operations_per_second: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseStats {
    pub leases_active: usize,
    pub operations_per_second: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleStats {
    pub schedules_active: usize,
    pub executions_per_minute: f64,
    pub subscriptions_active: usize,
    pub pending_fires: usize,
}

/// Handle /admin/stats endpoint
pub async fn handle_global_stats(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
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
            },
            stream: StreamStats {
                streams_active: runtime.stream_active(),
                events_total: runtime.stream_events_total(),
                operations_per_second: runtime.stream_operations_per_second(),
                subscriptions_active: runtime.stream_subscriptions_active(),
            },
            notice: NoticeStats {
                subscriptions_active: runtime.notice_subscriptions_active(),
                publishes_per_second: runtime.notice_publishes_per_second(),
            },
            queue: QueueStats {
                messages_ready: runtime.queue_messages_ready(),
                messages_delayed: runtime.queue_messages_delayed(),
                messages_pending: runtime.queue_messages_pending(),
                messages_dead_lettered: runtime.queue_messages_dead_lettered(),
                leases_active: runtime.queue_leases_active(),
                operations_per_second: runtime.queue_operations_per_second(),
            },
            rpc: RpcStats {
                workers_registered: runtime.rpc_workers_registered(),
                requests_pending: runtime.rpc_requests_pending(),
                operations_per_second: runtime.rpc_operations_per_second(),
            },
            lease: LeaseStats {
                leases_active: runtime.lease_active(),
                operations_per_second: runtime.lease_operations_per_second(),
            },
            schedule: ScheduleStats {
                schedules_active: runtime.schedule_active(),
                executions_per_minute: runtime.schedule_executions_per_minute(),
                subscriptions_active: runtime.schedule_subscriptions_active(),
                pending_fires: runtime.schedule_pending_fires(),
            },
        },
    };

    crate::api::admin::json_response(stats)
}

/// Handle domain-specific stats endpoints
pub async fn handle_domain_stats(
    runtime: Arc<Runtime>,
    domain: &str,
) -> Result<Response<Body>, Infallible> {
    match domain {
        "kv" => handle_kv_stats(runtime).await,
        "stream" => handle_stream_stats(runtime).await,
        "notice" => handle_notice_stats(runtime).await,
        "queue" => handle_queue_stats(runtime).await,
        "rpc" => handle_rpc_stats(runtime).await,
        "lease" => handle_lease_stats(runtime).await,
        "schedule" => handle_schedule_stats(runtime).await,
        _ => Ok(crate::api::admin::not_found()),
    }
}

async fn handle_kv_stats(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    let _ = runtime;
    Ok(super::not_implemented())
}

async fn handle_stream_stats(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    let _ = runtime;
    Ok(super::not_implemented())
}

async fn handle_notice_stats(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    let _ = runtime;
    Ok(super::not_implemented())
}

async fn handle_queue_stats(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    crate::api::admin::json_response(QueueStats {
        messages_ready: runtime.queue_messages_ready(),
        messages_delayed: runtime.queue_messages_delayed(),
        messages_pending: runtime.queue_messages_pending(),
        messages_dead_lettered: runtime.queue_messages_dead_lettered(),
        leases_active: runtime.queue_leases_active(),
        operations_per_second: runtime.queue_operations_per_second(),
    })
}

async fn handle_rpc_stats(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    let _ = runtime;
    Ok(super::not_implemented())
}

async fn handle_lease_stats(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    let _ = runtime;
    Ok(super::not_implemented())
}

async fn handle_schedule_stats(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    let _ = runtime;
    Ok(super::not_implemented())
}

/// Parse realm filter from query string
#[allow(dead_code)] // TODO: Use for realm-filtered stats queries
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
