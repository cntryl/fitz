use crate::api::admin::list::SessionInfo;
use crate::api::admin::{stats, troubleshooting};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagingTopology {
    pub generated_at: String,
    pub broker: stats::BrokerStats,
    pub diagnostics: troubleshooting::GlobalTroubleshootingDiagnostics,
    pub session_groups: Vec<TopologySessionGroup>,
    pub lanes: Vec<TopologyLane>,
    pub connections: TopologyConnectionPage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologySessionGroup {
    pub route_family: u64,
    pub sessions: usize,
    pub messages_received: u64,
    pub messages_sent: u64,
    pub transports: Vec<String>,
    pub max_idle_seconds: u64,
    pub representative_sessions: Vec<SessionInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyState {
    Quiet,
    Flowing,
    Pressure,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyLane {
    pub id: String,
    pub title: String,
    pub state: TopologyState,
    pub activity_per_second: f64,
    pub diagnostics: troubleshooting::DiagnosticSnapshot,
    pub counters: Vec<TopologyCounter>,
    pub consumers: usize,
    pub observers: usize,
    pub top_scoped_resources: Vec<TopologyScopedResource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyCounter {
    pub key: String,
    pub label: String,
    pub value: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopologyScope {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub realm: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route_family: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub area: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyScopedResource {
    pub id: String,
    pub label: String,
    pub state: TopologyState,
    pub scope: TopologyScope,
    pub counters: Vec<TopologyCounter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyConnectionKind {
    BrokerDomainFlow,
    NoticeSubscription,
    RpcWorker,
    RpcPendingAssignment,
    QueueInflightConsumer,
    LeaseOwner,
    StreamAppendActivity,
    ScheduleSubscriptionActivity,
    KvTransactionActivity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyConnection {
    pub id: String,
    pub kind: TopologyConnectionKind,
    pub source: String,
    pub target: String,
    pub label: String,
    pub state: TopologyState,
    pub scope: TopologyScope,
    pub metrics: Vec<TopologyCounter>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyConnectionPage {
    pub items: Vec<TopologyConnection>,
    pub total: usize,
    pub truncated: bool,
    pub limit: usize,
}

pub(super) struct TopologyConnectionBuilder {
    items: Vec<TopologyConnection>,
    limit: usize,
    total: usize,
}

impl TopologyConnectionBuilder {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            items: Vec::with_capacity(limit.min(64)),
            limit,
            total: 0,
        }
    }

    pub(super) fn push(&mut self, connection: TopologyConnection) {
        self.total += 1;
        if self.items.len() < self.limit {
            self.items.push(connection);
        }
    }

    pub(super) fn finish(self) -> TopologyConnectionPage {
        let truncated = self.total > self.items.len();
        TopologyConnectionPage {
            items: self.items,
            total: self.total,
            truncated,
            limit: self.limit,
        }
    }
}
