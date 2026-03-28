use crate::api::admin::{
    KvTransaction, LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueInfo, QueueLease,
    RpcPendingRequest, RpcWorker, ScheduleInfo, SessionInfo, StreamInfo,
};
use crate::session::session::SessionInfo as RuntimeSessionInfo;
use chrono::Utc;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
pub struct AdminReadModel {
    kv_transactions: RwLock<Vec<KvTransaction>>,
    streams: RwLock<Vec<StreamInfo>>,
    notice_subscriptions: RwLock<Vec<NoticeSubscription>>,
    notice_routes: RwLock<Vec<NoticeRouteInfo>>,
    queues: RwLock<Vec<QueueInfo>>,
    queue_leases: RwLock<Vec<QueueLease>>,
    rpc_workers: RwLock<Vec<RpcWorker>>,
    rpc_pending: RwLock<Vec<RpcPendingRequest>>,
    leases: RwLock<Vec<LeaseInfo>>,
    schedules: RwLock<Vec<ScheduleInfo>>,
    sessions: RwLock<HashMap<u64, SessionInfo>>,
}

impl AdminReadModel {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn replace_kv_transactions(&self, transactions: Vec<KvTransaction>) {
        *self.kv_transactions.write() = transactions;
    }

    pub fn kv_transactions(&self, realm: Option<&str>) -> Vec<KvTransaction> {
        self.kv_transactions
            .read()
            .iter()
            .filter(|item| realm.map(|needle| item.realm == needle).unwrap_or(true))
            .cloned()
            .collect()
    }

    pub fn replace_streams(&self, streams: Vec<StreamInfo>) {
        *self.streams.write() = streams;
    }

    pub fn streams(&self, realm: Option<&str>) -> Vec<StreamInfo> {
        self.streams
            .read()
            .iter()
            .filter(|item| realm.map(|needle| item.realm == needle).unwrap_or(true))
            .cloned()
            .collect()
    }

    pub fn replace_notice_subscriptions(&self, subscriptions: Vec<NoticeSubscription>) {
        *self.notice_subscriptions.write() = subscriptions;
    }

    pub fn notice_subscriptions(
        &self,
        realm: Option<&str>,
        route_pattern: Option<&str>,
    ) -> Vec<NoticeSubscription> {
        self.notice_subscriptions
            .read()
            .iter()
            .filter(|item| realm.map(|needle| item.realm == needle).unwrap_or(true))
            .filter(|item| {
                route_pattern
                    .map(|needle| item.pattern.contains(needle))
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    pub fn replace_notice_routes(&self, routes: Vec<NoticeRouteInfo>) {
        *self.notice_routes.write() = routes;
    }

    pub fn notice_routes(&self, realm: Option<&str>) -> Vec<NoticeRouteInfo> {
        self.notice_routes
            .read()
            .iter()
            .filter(|item| {
                realm
                    .map(|needle| item.route.contains(&format!("{needle}/")))
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    pub fn replace_queues(&self, queues: Vec<QueueInfo>) {
        *self.queues.write() = queues;
    }

    pub fn queues(&self, realm: Option<&str>) -> Vec<QueueInfo> {
        self.queues
            .read()
            .iter()
            .filter(|item| realm.map(|needle| item.realm == needle).unwrap_or(true))
            .cloned()
            .collect()
    }

    pub fn replace_queue_leases(&self, leases: Vec<QueueLease>) {
        *self.queue_leases.write() = leases;
    }

    pub fn queue_leases(&self, realm: Option<&str>) -> Vec<QueueLease> {
        self.queue_leases
            .read()
            .iter()
            .filter(|item| realm.map(|needle| item.realm == needle).unwrap_or(true))
            .cloned()
            .collect()
    }

    pub fn replace_rpc_workers(&self, workers: Vec<RpcWorker>) {
        *self.rpc_workers.write() = workers;
    }

    pub fn rpc_workers(&self, realm: Option<&str>) -> Vec<RpcWorker> {
        self.rpc_workers
            .read()
            .iter()
            .filter(|item| realm.map(|needle| item.realm == needle).unwrap_or(true))
            .cloned()
            .collect()
    }

    pub fn replace_rpc_pending(&self, requests: Vec<RpcPendingRequest>) {
        *self.rpc_pending.write() = requests;
    }

    pub fn rpc_pending(&self, realm: Option<&str>) -> Vec<RpcPendingRequest> {
        self.rpc_pending
            .read()
            .iter()
            .filter(|item| {
                realm
                    .map(|needle| item.route.contains(&format!("{needle}/")))
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    pub fn replace_leases(&self, leases: Vec<LeaseInfo>) {
        *self.leases.write() = leases;
    }

    pub fn leases(&self, realm: Option<&str>) -> Vec<LeaseInfo> {
        self.leases
            .read()
            .iter()
            .filter(|item| realm.map(|needle| item.realm == needle).unwrap_or(true))
            .cloned()
            .collect()
    }

    pub fn replace_schedules(&self, schedules: Vec<ScheduleInfo>) {
        *self.schedules.write() = schedules;
    }

    pub fn upsert_schedule(&self, schedule: ScheduleInfo) {
        let mut schedules = self.schedules.write();
        if let Some(existing) = schedules.iter_mut().find(|item| {
            item.realm == schedule.realm
                && item.area == schedule.area
                && item.resource == schedule.resource
                && item.operation == schedule.operation
        }) {
            // Fast path for idempotent create/upsert calls: avoid rewriting
            // the admin model when durable schedule identity is unchanged.
            if existing.cron == schedule.cron && existing.enabled == schedule.enabled {
                return;
            }
            *existing = schedule;
        } else {
            schedules.push(schedule);
        }
    }

    pub fn upsert_schedule_fields(
        &self,
        realm: String,
        area: String,
        resource: String,
        operation: String,
        cron: String,
    ) {
        let mut schedules = self.schedules.write();
        if let Some(existing) = schedules.iter_mut().find(|item| {
            item.realm == realm
                && item.area == area
                && item.resource == resource
                && item.operation == operation
        }) {
            if existing.cron == cron && existing.enabled {
                return;
            }
            existing.cron = cron;
            existing.enabled = true;
            existing.next_run = Utc::now().to_rfc3339();
            existing.last_run = None;
            existing.executions_total = 0;
        } else {
            schedules.push(ScheduleInfo {
                realm,
                area,
                resource,
                operation,
                cron,
                next_run: Utc::now().to_rfc3339(),
                last_run: None,
                executions_total: 0,
                enabled: true,
            });
        }
    }

    pub fn remove_schedule(&self, realm: &str, area: &str, resource: &str, operation: &str) {
        let mut schedules = self.schedules.write();
        schedules.retain(|item| {
            !(item.realm == realm
                && item.area == area
                && item.resource == resource
                && item.operation == operation)
        });
    }

    pub fn schedules(&self, realm: Option<&str>) -> Vec<ScheduleInfo> {
        self.schedules
            .read()
            .iter()
            .filter(|item| realm.map(|needle| item.realm == needle).unwrap_or(true))
            .cloned()
            .collect()
    }

    pub fn record_session_open(&self, session: &RuntimeSessionInfo) {
        self.sessions.write().insert(
            session.session_id,
            SessionInfo {
                session_id: session.session_id.to_string(),
                realm: session.route_family.as_u64().to_string(),
                connected_at: Utc::now().to_rfc3339(),
                idle_seconds: 0,
                messages_received: 0,
                messages_sent: 0,
                transport: session.transport_kind.to_string(),
                remote_addr: session
                    .peer_addr
                    .map(|addr| addr.to_string())
                    .unwrap_or_default(),
            },
        );
    }

    pub fn record_session_close(&self, session_id: u64) {
        self.sessions.write().remove(&session_id);
    }

    pub fn sessions(&self, realm: Option<&str>) -> Vec<SessionInfo> {
        self.sessions
            .read()
            .values()
            .filter(|item| realm.map(|needle| item.realm == needle).unwrap_or(true))
            .cloned()
            .collect()
    }
}
