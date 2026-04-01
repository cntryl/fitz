use crate::api::admin::{
    KvTransaction, LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueInfo, QueueLease,
    RpcPendingRequest, RpcWorker, ScheduleInfo, SessionInfo, StreamInfo,
};
use crate::session::session::SessionInfo as RuntimeSessionInfo;
use chrono::Utc;
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

type ScheduleIdentity = (String, String, String, String);

fn schedule_identity_key(
    realm: &str,
    area: &str,
    resource: &str,
    operation: &str,
) -> ScheduleIdentity {
    (
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        operation.to_string(),
    )
}

fn schedule_identity_for(info: &ScheduleInfo) -> ScheduleIdentity {
    schedule_identity_key(&info.realm, &info.area, &info.resource, &info.operation)
}

fn matches_realm(realm: Option<&str>, value: &str) -> bool {
    realm.map(|needle| value == needle).unwrap_or(true)
}

fn matches_substring(filter: Option<&str>, value: &str) -> bool {
    filter.map(|needle| value.contains(needle)).unwrap_or(true)
}

fn matches_route_realm(realm: Option<&str>, route: &str) -> bool {
    realm
        .map(|needle| route.contains(&format!("{needle}/")))
        .unwrap_or(true)
}

fn collect_slice_matches<T: Clone>(items: &[T], include: impl Fn(&T) -> bool) -> Vec<T> {
    items.iter().filter(|item| include(item)).cloned().collect()
}

fn collect_map_value_matches<K, T: Clone>(
    items: &HashMap<K, T>,
    include: impl Fn(&T) -> bool,
) -> Vec<T> {
    items
        .values()
        .filter(|item| include(item))
        .cloned()
        .collect()
}

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
    schedules: RwLock<BTreeMap<ScheduleIdentity, ScheduleInfo>>,
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
        let transactions = self.kv_transactions.read();
        collect_slice_matches(&transactions, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_streams(&self, streams: Vec<StreamInfo>) {
        *self.streams.write() = streams;
    }

    pub fn streams(&self, realm: Option<&str>) -> Vec<StreamInfo> {
        let streams = self.streams.read();
        collect_slice_matches(&streams, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_notice_subscriptions(&self, subscriptions: Vec<NoticeSubscription>) {
        *self.notice_subscriptions.write() = subscriptions;
    }

    pub fn notice_subscriptions(
        &self,
        realm: Option<&str>,
        route_pattern: Option<&str>,
    ) -> Vec<NoticeSubscription> {
        let subscriptions = self.notice_subscriptions.read();
        collect_slice_matches(&subscriptions, |item| {
            matches_realm(realm, &item.realm) && matches_substring(route_pattern, &item.pattern)
        })
    }

    pub fn replace_notice_routes(&self, routes: Vec<NoticeRouteInfo>) {
        *self.notice_routes.write() = routes;
    }

    pub fn notice_routes(&self, realm: Option<&str>) -> Vec<NoticeRouteInfo> {
        let routes = self.notice_routes.read();
        collect_slice_matches(&routes, |item| matches_route_realm(realm, &item.route))
    }

    pub fn replace_queues(&self, queues: Vec<QueueInfo>) {
        *self.queues.write() = queues;
    }

    pub fn queues(&self, realm: Option<&str>) -> Vec<QueueInfo> {
        let queues = self.queues.read();
        collect_slice_matches(&queues, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_queue_leases(&self, leases: Vec<QueueLease>) {
        *self.queue_leases.write() = leases;
    }

    pub fn queue_leases(&self, realm: Option<&str>) -> Vec<QueueLease> {
        let leases = self.queue_leases.read();
        collect_slice_matches(&leases, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_rpc_workers(&self, workers: Vec<RpcWorker>) {
        *self.rpc_workers.write() = workers;
    }

    pub fn rpc_workers(&self, realm: Option<&str>) -> Vec<RpcWorker> {
        let workers = self.rpc_workers.read();
        collect_slice_matches(&workers, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_rpc_pending(&self, requests: Vec<RpcPendingRequest>) {
        *self.rpc_pending.write() = requests;
    }

    pub fn rpc_pending(&self, realm: Option<&str>) -> Vec<RpcPendingRequest> {
        let pending = self.rpc_pending.read();
        collect_slice_matches(&pending, |item| matches_route_realm(realm, &item.route))
    }

    pub fn replace_leases(&self, leases: Vec<LeaseInfo>) {
        *self.leases.write() = leases;
    }

    pub fn leases(&self, realm: Option<&str>) -> Vec<LeaseInfo> {
        let leases = self.leases.read();
        collect_slice_matches(&leases, |item| matches_realm(realm, &item.realm))
    }

    pub fn replace_schedules(&self, schedules: Vec<ScheduleInfo>) {
        *self.schedules.write() = schedules
            .into_iter()
            .map(|schedule| (schedule_identity_for(&schedule), schedule))
            .collect();
    }

    pub fn upsert_schedule(&self, schedule: ScheduleInfo) {
        let mut schedules = self.schedules.write();
        let identity = schedule_identity_for(&schedule);
        if let Some(existing) = schedules.get_mut(&identity) {
            // Fast path for idempotent create/upsert calls: avoid rewriting
            // the admin model when durable schedule identity is unchanged.
            if existing.cron == schedule.cron && existing.enabled == schedule.enabled {
                return;
            }
            *existing = schedule;
        } else {
            schedules.insert(identity, schedule);
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
        let next_run = Utc::now().to_rfc3339();
        self.upsert_schedule(ScheduleInfo::enabled_snapshot(
            realm, area, resource, operation, cron, &next_run,
        ));
    }

    pub fn remove_schedule(&self, realm: &str, area: &str, resource: &str, operation: &str) {
        self.schedules
            .write()
            .remove(&schedule_identity_key(realm, area, resource, operation));
    }

    pub fn schedules(&self, realm: Option<&str>) -> Vec<ScheduleInfo> {
        let schedules = self.schedules.read();
        schedules
            .values()
            .filter(|item| matches_realm(realm, &item.realm))
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
        let sessions = self.sessions.read();
        collect_map_value_matches(&sessions, |item| matches_realm(realm, &item.realm))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_insert_enabled_schedule_snapshot_given_upsert_schedule_fields() {
        // Arrange
        let read_model = AdminReadModel::default();

        // Act
        read_model.upsert_schedule_fields(
            "acme".to_string(),
            "billing".to_string(),
            "invoices".to_string(),
            "send".to_string(),
            "0 * * * *".to_string(),
        );
        let schedules = read_model.schedules(None);

        // Assert
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].realm, "acme");
        assert_eq!(schedules[0].area, "billing");
        assert_eq!(schedules[0].resource, "invoices");
        assert_eq!(schedules[0].operation, "send");
        assert_eq!(schedules[0].cron, "0 * * * *");
        assert!(schedules[0].enabled);
        assert!(schedules[0].last_run.is_none());
        assert_eq!(schedules[0].executions_total, 0);
        assert!(!schedules[0].next_run.is_empty());
    }

    #[test]
    fn should_preserve_single_schedule_given_idempotent_upsert_schedule_fields() {
        // Arrange
        let read_model = AdminReadModel::default();
        read_model.upsert_schedule_fields(
            "acme".to_string(),
            "billing".to_string(),
            "invoices".to_string(),
            "send".to_string(),
            "0 * * * *".to_string(),
        );
        let first_schedule = read_model.schedules(None).into_iter().next().unwrap();

        // Act
        read_model.upsert_schedule_fields(
            "acme".to_string(),
            "billing".to_string(),
            "invoices".to_string(),
            "send".to_string(),
            "0 * * * *".to_string(),
        );
        let schedules = read_model.schedules(None);

        // Assert
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].next_run, first_schedule.next_run);
    }

    #[test]
    fn should_reset_schedule_state_given_changed_cron_on_upsert_schedule_fields() {
        // Arrange
        let read_model = AdminReadModel::default();
        read_model.upsert_schedule(ScheduleInfo {
            realm: "acme".to_string(),
            area: "billing".to_string(),
            resource: "invoices".to_string(),
            operation: "send".to_string(),
            cron: "0 * * * *".to_string(),
            next_run: "2026-03-31T00:00:00Z".to_string(),
            last_run: Some("2026-03-30T23:00:00Z".to_string()),
            executions_total: 42,
            enabled: false,
        });

        // Act
        read_model.upsert_schedule_fields(
            "acme".to_string(),
            "billing".to_string(),
            "invoices".to_string(),
            "send".to_string(),
            "*/5 * * * *".to_string(),
        );
        let schedules = read_model.schedules(None);

        // Assert
        assert_eq!(schedules.len(), 1);
        assert_eq!(schedules[0].cron, "*/5 * * * *");
        assert!(schedules[0].enabled);
        assert!(schedules[0].last_run.is_none());
        assert_eq!(schedules[0].executions_total, 0);
    }

    #[test]
    fn should_filter_notice_routes_given_realm() {
        // Arrange
        let read_model = AdminReadModel::default();
        read_model.replace_notice_routes(vec![
            NoticeRouteInfo::snapshot("notice://acme/app/orders".to_string(), 1),
            NoticeRouteInfo::snapshot("notice://globex/app/orders".to_string(), 2),
        ]);

        // Act
        let routes = read_model.notice_routes(Some("acme"));

        // Assert
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route, "notice://acme/app/orders");
    }

    #[test]
    fn should_filter_notice_subscriptions_given_route_pattern() {
        // Arrange
        let read_model = AdminReadModel::default();
        read_model.replace_notice_subscriptions(vec![
            NoticeSubscription::snapshot(
                1,
                10,
                "acme",
                "notice://acme/app/orders".to_string(),
                "2026-03-31T00:00:00Z",
            ),
            NoticeSubscription::snapshot(
                2,
                11,
                "acme",
                "notice://acme/app/invoices".to_string(),
                "2026-03-31T00:00:00Z",
            ),
        ]);

        // Act
        let subscriptions = read_model.notice_subscriptions(Some("acme"), Some("orders"));

        // Assert
        assert_eq!(subscriptions.len(), 1);
        assert_eq!(subscriptions[0].pattern, "notice://acme/app/orders");
    }
}
