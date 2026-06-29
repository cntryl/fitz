use serde::{Deserialize, Serialize};

pub use crate::control::admin::{
    LeaseInfo, QueueDeadLetter, QueueInflight, QueueInfo, RpcLatencyBuckets, RpcPendingRequest,
    RpcWorker, ScheduleInfo, SessionInfo,
};

/// Collection of point-in-time Queue resource snapshots for the current broker
/// process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuesList {
    pub queues: Vec<QueueInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueRealmCollection {
    pub realms: Vec<QueueRealmEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueRealmEntry {
    pub realm: String,
    pub area_count: usize,
    pub queue_count: usize,
    pub subscriptions_active: usize,
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_inflight: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_backlog_age_seconds: u64,
    pub enqueue_success_total: u64,
    pub complete_success_total: u64,
    pub in_rate_per_second: f64,
    pub out_rate_per_second: f64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueRealmDetail {
    pub realm: String,
    pub area_count: usize,
    pub queue_count: usize,
    pub subscriptions_active: usize,
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_inflight: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_backlog_age_seconds: u64,
    pub enqueue_success_total: u64,
    pub complete_success_total: u64,
    pub in_rate_per_second: f64,
    pub out_rate_per_second: f64,
    pub status: String,
    pub areas: Vec<QueueAreaEntry>,
    pub queues: Vec<QueueResourceEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueAreaCollection {
    pub realm: String,
    pub areas: Vec<QueueAreaEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueAreaEntry {
    pub realm: String,
    pub area: String,
    pub queue_count: usize,
    pub subscriptions_active: usize,
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_inflight: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_backlog_age_seconds: u64,
    pub enqueue_success_total: u64,
    pub complete_success_total: u64,
    pub in_rate_per_second: f64,
    pub out_rate_per_second: f64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueAreaDetail {
    pub realm: String,
    pub area: String,
    pub queue_count: usize,
    pub subscriptions_active: usize,
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_inflight: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_backlog_age_seconds: u64,
    pub enqueue_success_total: u64,
    pub complete_success_total: u64,
    pub in_rate_per_second: f64,
    pub out_rate_per_second: f64,
    pub status: String,
    pub queues: Vec<QueueResourceEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueResourceCollection {
    pub realm: String,
    pub area: String,
    pub resources: Vec<QueueResourceEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueResourceEntry {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub family_count: usize,
    pub subscriptions_active: usize,
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_inflight: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_backlog_age_seconds: u64,
    pub enqueue_success_total: u64,
    pub complete_success_total: u64,
    pub in_rate_per_second: f64,
    pub out_rate_per_second: f64,
    pub status: String,
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

/// Collection of live in-memory RPC worker snapshots for the current broker
/// process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcWorkersList {
    pub workers: Vec<RpcWorker>,
}

/// Collection of live in-memory pending RPC request snapshots for the current
/// broker process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcPendingList {
    pub requests: Vec<RpcPendingRequest>,
}

/// Collection of live in-memory Lease snapshots for the current broker
/// process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeasesList {
    pub leases: Vec<LeaseInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulesList {
    pub schedules: Vec<ScheduleInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionsList {
    pub sessions: Vec<SessionInfo>,
}
